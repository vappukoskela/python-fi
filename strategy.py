import logging
import time
import os
import csv
import threading
from collections import deque
from datetime import datetime, timezone, timedelta
from collections import deque, defaultdict

# === CODE VERSION TAG (for audit comparison) ===
CODE_VERSION = "PATCH_EPOCH_5" # increment manually when you apply new patches
CODE_VERSION = "PATCH5_2025-12-23"

rsi_fail_counter = defaultdict(int)

import pandas as pd
import numpy as np
from dateutil import parser
from dotenv import load_dotenv

# Alpaca data & trading
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, StockTradesRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.models import Bar, Trade

from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, OrderType, TimeInForce
from alpaca.trading.requests import MarketOrderRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.requests import StockLatestTradeRequest


# --- Lataa .env ---
load_dotenv()
# --- Lue API-avaimet ---
API_KEY = os.getenv("ALPACA_PAPER_API_KEY")
API_SECRET = os.getenv("ALPACA_PAPER_SECRET_KEY")
BASE_URL = os.getenv("TRADE_API_URL", "https://paper-api.alpaca.markets")
if not API_KEY or not API_SECRET:
    raise RuntimeError("API keys not found. Check your .env file.")

# --- Alusta Alpaca clientit ---
# Historiallinen data (SIM)
stock_data_client = StockHistoricalDataClient(API_KEY, API_SECRET)

# Trading client (LIVE tai paper)
trading_client = TradingClient(API_KEY, API_SECRET, paper=True)
# Testitulostus (voit poistaa myöhemmin)
print("API_KEY loaded:", API_KEY[:4], "...")
print("BASE_URL:", BASE_URL)

# === STATE / RUNTIME VARIABLES (ei CONFIG-arvoja) ===

# Symbolit, joille on ostoyritys käynnissä
pending_entries = set()

# Avoimet toimeksiannot (symbol -> order_id tai True jos ei tiedossa)
inflight_orders = {}

# Entryjen seuranta
entry_times = {}
entry_prices = {}
entry_qty = {}
exec_rows = []

tp1_hit = defaultdict(bool)

# Viimeiset poistumisajat (symbol -> datetime)
last_exit_time = defaultdict(lambda: None)

# Viimeiset ostoyritykset (symbol -> datetime)
last_trade_attempt = defaultdict(lambda: None)

# Loopin aikana käytetty budjetti (nollataan jokaisen loopin alussa)
spent_this_loop = 0.0

# Trailing stop seurantaan
highest_price_since_entry = defaultdict(float)

# Trailing stop aktivoinnin tila (symbol -> bool)
trailing_active = defaultdict(bool)

# === AUDIT TRAIL COUNTERS ===
regime_pnl = defaultdict(float)                     # net PnL per regime
regime_trades = defaultdict(int)                    # trade count per regime
exit_reason_count = defaultdict(lambda: defaultdict(int))  # exit counts per regime/reason


# Regime kill-switch flags
high_vol_paused = defaultdict(bool)
trend_paused = defaultdict(bool)
# === RECONCILIATION CONFIG ===
RECONCILIATION_ENABLED = True
RECON_MAX_STALE_MIN = 3              # consider context stale if older than N minutes without sell
RECON_FORCE_SELL_IF_ORPHAN = True    # force sell if position has no local entry context
RECON_POLL_RETRIES = 30              # extended polling for sell fill confirmation
RECON_POLL_SLEEP = 2.0               # seconds between polls
RECON_LOG_STACK = True               # extra logs for reconciliation decisions

# === CONFIG ===
# === CONFIG PROFILES ===

BULLISH_CONFIG = {
    "TP_PCT": 0.0020,
    "SL_MULTIPLIER": 1.0,
    "TS_ACTIVATION_BUFFER": 0.004,
    "TRAILING_STOP_PCT": 0.005,
    "MAX_TRADES": 8,
    "MAX_LOSS_DAY": 1.5,
    "VWAP_DELTA": 0.003,
    "EMA_DELTA": 0.0003,
    "RSI_FAIL_TICKS": 5 
}

BEARISH_CONFIG = {
    "TP_PCT": 0.0020,
    "SL_MULTIPLIER": 0.6,
    "TS_ACTIVATION_BUFFER": 0.0035,
    "TRAILING_STOP_PCT": 0.0055,
    "MAX_TRADES": 3,
    "MAX_LOSS_DAY": 0.8,
    "VWAP_DELTA": 0.0022,
    "EMA_DELTA": 0.0003,
    "RSI_FAIL_TICKS": 4
}

# === RANGE FILTER TOGGLES ===
RANGE_STRICT_TOUCH_ENABLED = True       # Require strict lower-band touch (no epsilon)
RANGE_TOUCH_EPSILON = 0.0002               # If strict touch, epsilon = 0

RANGE_VWAP_ROOM_MIN = 0.0009            # Minimum VWAP reversion room (e.g. 0.12%)
RANGE_BB_ROC_MAX = 0.0003               # Block RANGE entries if Bollinger bandwidth ROC > threshold

RANGE_TIME_STOP_ENABLED = True          # Enable RANGE time-stop exit
RANGE_TIME_STOP_SECONDS = 180           # Exit if VWAP progress fails within N seconds
RANGE_VWAP_PROGRESS_MIN = 0.35          # Require ≥40% shrink in VWAP distance


SCALP = True
LOOP_SLEEP = 0.5
TICKS_WINDOW = 300
EMA_FAST = 9
EMA_SLOW = 20
RSI_PERIOD = 14
RSI_COOL_THRESHOLD = 3    # esim. raja-arvo RSI:lle "cool down" -tilanteessa
VOL_SPIKE_MULT = 1.2
ATR_PERIOD = 20
ATR_FLOOR = 0.05  # esim. 2 senttiä NVDA:lle; kalibroi instrumentille


# --- RUN MODE ---
#"AGG_SIM" / "SIM" = backtest on historical bars; "LIVE" = live/paper trading loop
RUN_MODE = "LIVE"
MAX_HOLD_SECONDS = 999999   # example: x minutes
MIN_HOLD_SECONDS = 30    # example: x seconds grace period before indicators can trigger
TRAIL_PCT = 0.010
BUY_POWER_LIMIT = 0.05
BUY_CASH_BUFFER = 0.95
COOLDOWN_SECONDS = 80
MIN_RSI_FOR_ENTRY = 30
MAX_RSI_FOR_ENTRY = 85
MIN_TRADE_USD = 25
MARKET_DATA_CHUNK = 5
MAX_INFLIGHT_PER_SYMBOL = 1

# === REGIME CONFIGS ===
# Tailored overlays per regime (you can calibrate further with audit feedback)

DRIFT_CONFIG = {
    "TP_PCT": 0.0016,
    "SL_MULTIPLIER": 0.8,
    "TS_ACTIVATION_BUFFER": 0.003,
    "TRAILING_STOP_PCT": 0.004,
    "MAX_TRADES": 4,
    "MAX_LOSS_DAY": 1.0,
    "VWAP_DELTA": 0.002,
    "EMA_DELTA": 0.00025,
    "RSI_FAIL_TICKS": 4,
    "ENTRY_SCORE_THRESHOLD": 1.6,
    "WEIGHTS": {
        "ema_trend": 1.0,
        "slope_ok": 1.0,
        "macd_ok": 0.7,
        "vwap_ok": 1.0,
        "rsi_ok": 0.5,
        "bandwidth_ok": 0.5
    },
    "EMERGENCY_SL_PCT": 0.0045
}


TREND_CONFIG = {
    "TP_PCT": 0.0020,
    "SL_MULTIPLIER": 1.0,
    "TS_ACTIVATION_BUFFER": 0.003,
    "TRAILING_STOP_PCT": 0.005,
    "MAX_TRADES": 6,
    "MAX_LOSS_DAY": 1.2,
    "VWAP_DELTA": 0.0025,
    "EMA_DELTA": 0.0003,
    "RSI_FAIL_TICKS": 5,
    # Entry scoring thresholds
    "ENTRY_SCORE_THRESHOLD":     1.8,
    # Indicator gate weights
    "WEIGHTS": {
        "ema_trend": 1.0,      # EMA_fast > EMA_slow + slope positive
        "vwap_above": 1.0,     # location above VWAP
        "macd_momentum": 1.0,  # MACD > signal, rising histogram
        "vol_confirm": 0.7,    # volume spike vs median
        "pullback_ok": 0.6,     # pullback to EMA_slow/VWAP then re-accel
        "rsi_ok": 0.7   # new weight
    },
    # pullback tolerances (distance normalized by price)
    "PULLBACK_TOL": 0.0010,
    "EMERGENCY_SL_PCT": 0.0045 # ~0.45% hard stop
}

RANGE_CONFIG = {
    "TP_PCT": 0.0020,
    "SL_MULTIPLIER": 0.6,
    "TS_ACTIVATION_BUFFER": 0.003,
    "TRAILING_STOP_PCT": 0.004,
    "MAX_TRADES": 5,
    "MAX_LOSS_DAY": 1.0,
    "VWAP_DELTA": 0.003,
    "EMA_DELTA": 0.0002,
    "RSI_FAIL_TICKS": 4,
    "ENTRY_SCORE_THRESHOLD": 1.4,
    "WEIGHTS": {
        "lower_band_touch": 1.0,     # price near lower Bollinger band
        "rsi_uptick": 1.0,           # RSI < 35 and upticking
        "vwap_reversion": 1.0,       # distance to VWAP favorable
        "bandwidth_ok": 0.5,         # range (not too wide, not too tight)
        "vol_not_dry": 0.5,           # avoid illiquid chop
        "rsi_ok": 0.8
        
    },
    "BOLL_PERIOD": 20,
    "BOLL_STD": 2.0,
    "BANDWIDTH_MAX": 0.012,  # max relative bandwidth to still count as range
    "BANDWIDTH_MIN": 0.002  # avoid ultra-tight no-move
}

HIGH_VOL_CONFIG = {
    "TP_PCT": 0.0,
    "SL_MULTIPLIER": 0.0,
    "TS_ACTIVATION_BUFFER": 1.0,
    "TRAILING_STOP_PCT": 1.0,
    "MAX_TRADES": 0,
    "MAX_LOSS_DAY": 0.0,
    "VWAP_DELTA": 1.0,
    "EMA_DELTA": 1.0,
    "RSI_FAIL_TICKS": 99,
    "ENTRY_SCORE_THRESHOLD": 999,
    "WEIGHTS": {
        "atr_high": 0.0,          # ATR in upper decile of rolling window
        "bb_expanding": 0.0,      # Bollinger bandwidth expansion
        "macd_strong": 0.0,       # strong momentum
        "vol_roc": 0.0,           # volume rate-of-change positive
        "breakout_bar": 0.0       # price extends above recent high
    },
    "ATR_WINDOW": 50,
    "ATR_TOP_PCT": 1.0,          # top 20% percentile considered high
    "BOLL_PERIOD": 20,
    "BOLL_STD": 2.0,
    "VOL_ROC_WINDOW": 20,
    "BREAKOUT_LOOKBACK": 20
}

LOW_VOL_CONFIG = {
    "TP_PCT": 0.0012,
    "SL_MULTIPLIER": 0.7,
    "TS_ACTIVATION_BUFFER": 0.002,
    "TRAILING_STOP_PCT": 0.004,
    "MAX_TRADES": 6,
    "MAX_LOSS_DAY": 1.0,
    "VWAP_DELTA": 0.002,
    "EMA_DELTA": 0.0002,
    "RSI_FAIL_TICKS": 4,
    "ENTRY_SCORE_THRESHOLD": 1.8,
    "WEIGHTS": {
        "vwap_below": 1.0,        # price below VWAP for mean-reversion long
        "rsi_uptick": 1.0,
        "envelope_touch": 0.6,    # MA envelope lower touch
        "chop_high": 0.5,         # consolidation proxy (low bandwidth)
        "vol_ok": 0.4,             # avoid ultra-dry tape
        "rsi_ok": 0.7
    },
    "ENVELOPE_PCT": 0.0022,        # +/- around EMA_slow
    "BOLL_PERIOD": 20,
    "BOLL_STD": 2.0,
    "BANDWIDTH_CAP": 0.0045,       # low-vol consolidation cap
    "EMERGENCY_SL_PCT": 0.0045
}

# === Bias-specific overlays (defined AFTER the base dict) ===
LOW_VOL_CONFIG_BULL = dict(LOW_VOL_CONFIG)
LOW_VOL_CONFIG_BULL.update({
    "ENTRY_SCORE_THRESHOLD": 1.7, # slightly easier entries
    "TP_PCT": 0.0012,
    "OBV_REQUIRED": False, # soft requirement
    "ENTRY_CONFIRM_TICKS": 3
})

LOW_VOL_CONFIG_BEAR = dict(LOW_VOL_CONFIG)
LOW_VOL_CONFIG_BEAR.update({
    "ENTRY_SCORE_THRESHOLD": 2.5, # stricter gating
    "TP_PCT": 0.0009, # faster exits
    "VWAP_DELTA": 0.0030,
    "EMA_DELTA": 0.00035,
    "OBV_REQUIRED": True, # hard requirement
    "ENTRY_CONFIRM_TICKS": 4,
    "TIME_STOP_SECONDS": 90, # shorter time-stop
    "VWAP_PROGRESS_MIN": 0.35 # require progress shrink
})    
    
EXEC_AUDIT_ENABLED = True
EXEC_AUDIT_FILE = "audit_trades_live.csv"

# === Adaptive entry threshold (per symbol, per regime) ===
ADAPTIVE_ENTRY_ENABLED = True
ADAPTIVE_BOUNDS = (-0.3, 0.3)
_adaptive_entry_shift = defaultdict(lambda: defaultdict(float))  # sym -> regime -> shift

def adaptive_entry_update(sym, regime, outcome_label):
    if not ADAPTIVE_ENTRY_ENABLED or outcome_label not in ("good_block", "bad_block"):
        return
    # if many bad_blocks (missed winners), ease threshold by small step
    step = 0.05 if outcome_label == "bad_block" else -0.05
    new_shift = _adaptive_entry_shift[sym][regime] + step
    low, high = ADAPTIVE_BOUNDS
    _adaptive_entry_shift[sym][regime] = max(low, min(high, new_shift))

def adaptive_entry_threshold(CONFIG, sym, regime):
    # Use regime-native base thresholds, not the outer CONFIG profile
    if regime == "TREND":
        base = TREND_CONFIG.get("ENTRY_SCORE_THRESHOLD", 2.8)
    elif regime == "RANGE":
        base = RANGE_CONFIG.get("ENTRY_SCORE_THRESHOLD", 1.4)
    elif regime == "LOW_VOL":
        # choose bias-aware config if available
        if sym in _adaptive_entry_shift and "bias" in _adaptive_entry_shift[sym]:
            bias = _adaptive_entry_shift[sym]["bias"]
            base = LOW_VOL_CONFIG_BULL["ENTRY_SCORE_THRESHOLD"] if bias == "bullish" else LOW_VOL_CONFIG_BEAR["ENTRY_SCORE_THRESHOLD"]
        else:    
            base = LOW_VOL_CONFIG.get("ENTRY_SCORE_THRESHOLD", 1.8)
    elif regime == "DRIFT":
        base = DRIFT_CONFIG.get("ENTRY_SCORE_THRESHOLD", 1.6)
    else:
        base = HIGH_VOL_CONFIG.get("ENTRY_SCORE_THRESHOLD", 3.6)
    shift = _adaptive_entry_shift[sym][regime]
    return max(0.8, base + shift)


# === ENTRY GATE TOGGLE ===
USE_REGIME_ENTRY = True     # if False, uses your original buy_conditions_met
LOG_SIGNAL_STACK_ON_ACCEPT = False  # you asked for full logs; toggle to True if needed


# === LOGGING ===
logging.basicConfig(level=logging.DEBUG,
                    format="%(asctime)s %(levelname)s %(message)s",
                    filename="scalper_safe.log")
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger().addHandler(console)

logging.debug("[TRACE] Logging system initialized")

def run_simulation(symbols, start, end):
    print(f"Running simulation for {symbols} from {start} to {end}")


# --- DEBUG PATCH: logita entry_times ja last_exit_time päivitykset ---
#def debug_log_state(sym, entry_times, last_exit_time):
    #if sym in entry_times:
        #logging.debug("[DEBUG] entry_times[%s] = %s (type=%s)",
                      #sym, entry_times[sym], type(entry_times[sym]))
    #if sym in last_exit_time:
        #logging.debug("[DEBUG] last_exit_time[%s] = %s (type=%s)",
                      #sym, last_exit_time[sym], type(last_exit_time[sym]))

def write_exec_row_immediate(exec_row, symbol, run_mode):
    """
    Append a single execution row to the per-symbol exec CSV immediately.
    Safe to call from SIM and LIVE; idempotent header handling.
    """
    filename = f"{symbol}_{run_mode}_exec.csv"
    fieldnames = ["timestamp","symbol","action","price","reason","bias","pnl",
                  "ema_fast","ema_slow","rsi","vwap","regime"]
    try:
        file_exists = os.path.exists(filename) and os.path.getsize(filename) > 0
        with open(filename, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow(exec_row)     
    except Exception as e:
        logging.debug("[IO] write_exec_row_immediate failed for %s: %s", filename, e)

# === helpers: indicators ===
def _session_minutes(ts):
    # get real UTC time from Alpaca server
    server_time = datetime.now(timezone.utc)

    # US market open 14:30 UTC (winter)
    open_utc = server_time.replace(hour=14, minute=30, second=0, microsecond=0)

    return max(0, int((server_time - open_utc).total_seconds() // 60))


def compute_ema_from_series(series, period):
    if len(series) < 2:
        if len(series):
            return pd.Series([series.iloc[-1]])
        else:
            return pd.Series([float("nan")])
    return series.ewm(span=period, adjust=False).mean()

def compute_rsi_from_series(series, period=14):
    if len(series) < period + 1:
        return pd.Series([float("nan")] * len(series))
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi if isinstance(rsi, pd.Series) else pd.Series([rsi])

def compute_vwap_from_ticks(prices, sizes):
    if sizes.sum() == 0:
        return pd.Series([float("nan")] * len(prices))
    cumulative_pv = (prices * sizes).cumsum()
    cumulative_vol = sizes.cumsum()
    vwap = cumulative_pv / cumulative_vol
    return vwap if isinstance(vwap, pd.Series) else pd.Series([vwap])

def compute_atr_from_series(prices_series, period=14):
    """
    Yksinkertainen ATR-laskenta pelkistä hintasarjoista.
    Oikea ATR käyttää high/low/close -arvoja, mutta tässä
    käytetään hinnan muutosten absoluuttista liukuvaa keskiarvoa.
    """
    if len(prices_series) < period + 1:
        return float("nan")
    diffs = prices_series.diff().abs()
    atr = diffs.rolling(window=period).mean().iloc[-1]
    return float(atr) if not pd.isna(atr) else float("nan")

# === EXTRA INDICATOR HELPERS (from deques) ===
def compute_macd(series, fast=12, slow=26, signal=9):
    if len(series) < slow + signal:
        return (float('nan'), float('nan'), float('nan'))
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    macd_signal = macd_line.ewm(span=signal, adjust=False).mean()
    hist = macd_line - macd_signal
    return (float(macd_line.iloc[-1]), float(macd_signal.iloc[-1]), float(hist.iloc[-1]))

def compute_bollinger(series, period=20, std=2.0):
    if len(series) < period:
        return (float('nan'), float('nan'), float('nan'), float('nan'))
    ma = series.rolling(period).mean().iloc[-1]
    sd = series.rolling(period).std(ddof=0).iloc[-1]
    upper = ma + std * sd
    lower = ma - std * sd
    bandwidth = (upper - lower) / ma if ma != 0 else float('nan')
    return (float(upper), float(ma), float(lower), float(bandwidth))

def volume_roc(sizes_series, window=20):
    if len(sizes_series) < window + 1:
        return float('nan')
    prev = sizes_series.iloc[-window]
    curr = sizes_series.iloc[-1]
    return (curr - prev) / prev if prev > 0 else float('nan')

def recent_high(series, lookback=20):
    if len(series) < lookback:
        return float('nan')
    return float(series.iloc[-lookback:].max())

def recent_low(series, lookback=20):
    if len(series) < lookback:
        return float('nan')
    return float(series.iloc[-lookback:].min())

def ema_slope(series, period=20):
    if len(series) < period + 2:
        return float('nan')
    ema = series.ewm(span=period, adjust=False).mean()
    return float(ema.iloc[-1] - ema.iloc[-2])

def _safe_last(series_like):
    try:
        # If it's a pandas Series or similar, return last value as float
        return float(series_like.iloc[-1])
    except Exception:
        # Any problem -> return NaN so downstream code can handle it
        return float("nan")

# === PATCH 2: Multi-tick confirmation ===
ENTRY_CONFIRM_ENABLED = True
ENTRY_CONFIRM_TICKS = 3  # consecutive ticks to validate pattern

def _confirm_trend(prices_series, vwap_val, ema_slow_val):
    if len(prices_series) < ENTRY_CONFIRM_TICKS + 2 or pd.isna(vwap_val) or pd.isna(ema_slow_val):
        return False
    tail = prices_series.iloc[-ENTRY_CONFIRM_TICKS-2:]
    # pullback near EMA/VWAP then two upticks
    near_anchor = (abs(tail.iloc[-ENTRY_CONFIRM_TICKS] - ema_slow_val) / tail.iloc[-ENTRY_CONFIRM_TICKS] <= TREND_CONFIG["PULLBACK_TOL"]) or \
                  (abs(tail.iloc[-ENTRY_CONFIRM_TICKS] - vwap_val) / tail.iloc[-ENTRY_CONFIRM_TICKS] <= TREND_CONFIG["PULLBACK_TOL"])
    upticks = all(tail.iloc[i] < tail.iloc[i+1] for i in range(len(tail)-1))
    # NEW grinder confirmation path: allow TREND if last N ticks are all higher
    grinder_ok = sum(tail.diff().fillna(0) > 0) >= ENTRY_CONFIRM_TICKS
    return (near_anchor and upticks) or grinder_ok

def _confirm_range(prices_series, lower_band):
    if len(prices_series) < ENTRY_CONFIRM_TICKS + 1 or pd.isna(lower_band):
        return False
    tail = prices_series.iloc[-ENTRY_CONFIRM_TICKS-1:]
    touch = tail.iloc[-ENTRY_CONFIRM_TICKS] <= lower_band * (1 + 0.0002)
    upticks = sum(tail.diff().fillna(0) > 0) >= ENTRY_CONFIRM_TICKS - 1
    return touch and upticks

def _confirm_low_vol(prices_series, vwap_val, bias="bullish"):
    ticks_required = 3 if bias == "bullish" else 4
    if len(prices_series) < ticks_required + 1 or pd.isna(vwap_val):
        return False
    tail = prices_series.iloc[-ENTRY_CONFIRM_TICKS-1:]
    below_vwap = tail.iloc[-ENTRY_CONFIRM_TICKS] < vwap_val
    mean_rev = all(tail.iloc[i] < tail.iloc[i+1] for i in range(len(tail)-1))
    return below_vwap and mean_rev

def overlay_exit_params_by_regime(CONFIG, regime, bias="bullish"):
    adj = dict(CONFIG)
    if regime == "LOW_VOL" and bias == "bearish":
        adj["EMA_DELTA"] = 0.0003
        adj["VWAP_DELTA"] = 0.0025
        adj["TP_PCT"] = 0.0010
        adj["TIME_STOP_SECONDS"] = 90
        adj["VWAP_PROGRESS_MIN"] = 0.30
    return adj


# === PATCH 3: Session overlays ===
SESSION_OVERLAYS_ENABLED = True
SESSION_SLICES = [
    ("OPEN", 0, 30),   # first 30 minutes after market open
    ("MID", 30, 330),  # rest of session (example: 30 min to 5.5 hours)
]

def overlay_by_session(CONFIG, ts, regime):
    if not SESSION_OVERLAYS_ENABLED:
        return CONFIG
    minutes = _session_minutes(ts)
    adj = dict(CONFIG)  # shallow copy

    # OPEN session stricter entries, tighter SL, slightly higher TP
    if 0 <= minutes < 30:
        adj["ENTRY_SCORE_THRESHOLD"] = CONFIG.get("ENTRY_SCORE_THRESHOLD", 3.0) + 0.15
        adj["SL_MULTIPLIER"] = max(0.8, CONFIG["SL_MULTIPLIER"] * 0.9)
        adj["TP_PCT"] = min(CONFIG["TP_PCT"] * 1.1, CONFIG["TP_PCT"] + 0.0003)

    # MID session: allow RANGE/LOW_VOL slightly easier entries
    else:
        if regime == "TREND":
            adj["ENTRY_SCORE_THRESHOLD"] = max(2.4, CONFIG.get("ENTRY_SCORE_THRESHOLD", 3.0) - 0.2)
        elif regime == "RANGE":
            adj["ENTRY_SCORE_THRESHOLD"] = max(1.4, CONFIG.get("ENTRY_SCORE_THRESHOLD", 3.0) - 0.2)
        elif regime == "LOW_VOL":
            adj["ENTRY_SCORE_THRESHOLD"] = max(1.6, CONFIG.get("ENTRY_SCORE_THRESHOLD", 3.0) - 0.2)
            
    return adj

# === PATCH 4: ADX and Choppiness proxies ===
ADX_ENABLED = True
CHOP_ENABLED = True

def _adx_proxy(series, period=14):
    # simple directional movement proxy from closes
    if len(series) < period + 2:
        return float('nan')
    deltas = series.diff()
    plus_dm = deltas.where(deltas > 0, 0).rolling(period).sum()
    minus_dm = (-deltas.where(deltas < 0, 0)).rolling(period).sum()
    denom = plus_dm + minus_dm
    dx = 100 * abs(plus_dm - minus_dm) / denom.replace(0, np.nan)
    return float(dx.iloc[-1]) if not pd.isna(dx.iloc[-1]) else float('nan')

def _choppiness_proxy(series, period=14):
    if len(series) < period + 1:
        return float('nan')
    # ratio of sum(|returns|) to high-low range proxy
    returns_abs = series.diff().abs().rolling(period).sum().iloc[-1]
    hi_lo_range = (series.rolling(period).max() - series.rolling(period).min()).iloc[-1]
    if pd.isna(returns_abs) or pd.isna(hi_lo_range) or hi_lo_range == 0:
        return float('nan')
    # higher value => choppier (consolidation)
    return float(returns_abs / hi_lo_range)


# === PATCH 5: High-vol strict but tradable ===
# Update HIGH_VOL_CONFIG to allow entries with stricter gating
HIGH_VOL_CONFIG.update({
    "TP_PCT": 0.0030,
    "SL_MULTIPLIER": 1.2,        # wider SL per ATR
    "TS_ACTIVATION_BUFFER": 0.007,
    "TRAILING_STOP_PCT": 0.008,
    "MAX_TRADES": 2,
    "MAX_LOSS_DAY": 0.6,
    "ENTRY_SCORE_THRESHOLD": max(HIGH_VOL_CONFIG.get("ENTRY_SCORE_THRESHOLD", 3.2), 3.6),
    "ATR_TOP_PCT": 0.98,
})

AUDIT_CSV_FILE = "audit_blocks_live.csv"
# === PATCH RG: Risk governor with TP_PCT drawdown response ===
RISK_GOVERNOR_ENABLED = True
RISK_GOVERNOR_FILE = AUDIT_CSV_FILE
RISK_WINDOW_TRADES = 30
RISK_WINRATE_TARGET = 0.62
RISK_LOSS_TO_WIN_MAX = 0.7
RISK_DRAWDOWN_TP_CUT = 0.85   # cut TP_PCT to 85% of current when drawdown detected
RISK_DRAWDOWN_THRESHOLD_USD = -1500.0  # rolling net threshold
RISK_SIZE_STEP = 0.005
RISK_SIZE_MIN = 0.04
RISK_SIZE_MAX = 0.06

def _risk_stats_from_audit(rows):
    good = sum(1 for r in rows if r.get("outcome") == "good_block")
    bad  = sum(1 for r in rows if r.get("outcome") == "bad_block")
    winrate_est = good / max(1, (good + bad))
    # Approximate rolling net from blocked outcomes (conservative): good_block ~ saved SL, bad_block ~ missed TP
    # If you want exact PnL, wire in the live trade CSV similarly.
    net_est = (good * -ATR_FLOOR) + (bad * ATR_FLOOR)  # crude proxy; keeps directionality
    return winrate_est, net_est

def _apply_tp_cut_on_drawdown():
    global RANGE_CONFIG, TREND_CONFIG, LOW_VOL_CONFIG
    # Cut TP_PCT across active regimes (not HIGH_VOL) to exit faster during drawdowns
    for cfg in (RANGE_CONFIG, TREND_CONFIG, LOW_VOL_CONFIG):
        try:
            cfg["TP_PCT"] = max(0.0010, cfg["TP_PCT"] * RISK_DRAWDOWN_TP_CUT)
        except Exception:
            pass

def _risk_governor_update():
    if not RISK_GOVERNOR_ENABLED or not os.path.exists(RISK_GOVERNOR_FILE):
        return
    import csv
    rows = []
    try:
        with open(RISK_GOVERNOR_FILE, newline="") as f:
            reader = csv.DictReader(f)
            for r in reader:
                rows.append(r)
        if len(rows) < RISK_WINDOW_TRADES:
            return
        window = rows[-RISK_WINDOW_TRADES:]
        winrate_est, net_est = _risk_stats_from_audit(window)

        # Position sizing adjustment
        global BUY_POWER_LIMIT
        if winrate_est >= RISK_WINRATE_TARGET:
            BUY_POWER_LIMIT = min(RISK_SIZE_MAX, BUY_POWER_LIMIT + RISK_SIZE_STEP)
        else:
            BUY_POWER_LIMIT = max(RISK_SIZE_MIN, BUY_POWER_LIMIT - RISK_SIZE_STEP)

        # Drawdown TP cut
        if net_est <= RISK_DRAWDOWN_THRESHOLD_USD:
            _apply_tp_cut_on_drawdown()

    except Exception as e:
        logging.debug("[RISK] governor update failed: %s", e)

def log_block_event(sym, regime, reason, score):
    """
    Append a block event to audit_blocks_live.csv
    """
    try:
        file_exists = os.path.exists(AUDIT_CSV_FILE) and os.path.getsize(AUDIT_CSV_FILE) > 0
        with open(AUDIT_CSV_FILE, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["timestamp","symbol","regime","reason","score"])
            if not file_exists:
                writer.writeheader()
            writer.writerow({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "symbol": sym,
                "regime": regime,
                "reason": reason,
                "score": round(score, 4)
            })
    except Exception as e:
        logging.debug("[AUDIT] Failed to write block event: %s", e)


# === PATCH OBV: OBV slope proxy ===
def _obv_slope_proxy(prices_series, sizes_series, window=20):
    """
    Lightweight OBV proxy: cumulative volume adds on up ticks, subtracts on down ticks.
    Returns last slope over window (positive = confirming).
    """
    if len(prices_series) < window + 2 or len(sizes_series) < window + 2:
        return float('nan')
    delta = prices_series.diff()
    vol = sizes_series.fillna(0)
    obv = []
    cum = 0.0
    for i in range(1, len(prices_series)):
        if pd.isna(delta.iloc[i]) or pd.isna(vol.iloc[i]):
            continue
        if delta.iloc[i] > 0:
            cum += vol.iloc[i]
        elif delta.iloc[i] < 0:
            cum -= vol.iloc[i]
        obv.append(cum)
    if len(obv) < window + 1:
        return float('nan')
    obv_series = pd.Series(obv)
    return float(obv_series.iloc[-1] - obv_series.iloc[-window])


# === Alpaca helpers: defensive ===
def fetch_latest_trade_price_and_size_batch(stock_data_client, symbols):
    try:
        req = StockLatestTradeRequest(symbol_or_symbols=symbols)
        trades = stock_data_client.get_stock_latest_trade(req)
        out = {}
        for sym in symbols:
            t = trades.get(sym)
            if t is None:
                out[sym] = (None, None)
            else:
                try:
                    price = float(t.price)
                    size = int(getattr(t, "size", 1) or 1)
                except Exception:
                    price = float(getattr(t, "p", t.price))
                    size = int(getattr(t, "s", 1) or 1)
                out[sym] = (price, size)
        return out
    except Exception as e:
        logging.debug("Batch market-data fetch failed for %s : %s", symbols, e)
        return {s: (None, None) for s in symbols}

def calculate_buying_power_limit(trade_client_local, limit_fraction):
    try:
        account = trade_client_local.get_account()
        return float(account.buying_power) * limit_fraction
    except Exception as e:
        logging.exception("Failed to read account buying power: %s", e)
        return 0.0

def get_positions_map(trade_client_local):
    try:
        positions = trade_client_local.get_all_positions()
        return {pos.symbol: (int(float(pos.qty)), float(pos.avg_entry_price)) for pos in positions}
    except Exception as e:
        logging.debug("get_open_positions failed: %s", e)
        return {}

def get_position_qty(trade_client_local, symbol):
    try:
        pos = trade_client_local.get_open_position(symbol)
        return int(float(pos.qty))
    except Exception as e:
        logging.warning(f"get_position_qty failed for {symbol}: {e}")
        return 0

# --- NEW helper for status normalization ---
def _status_is(status, target):
    """
    Normalize Alpaca order status (enum or string) and compare to target.
    Example: _status_is(status, "filled")
    """
    try:
        val = getattr(status, "value", None)
        if isinstance(val, str):
            return val.lower() == target.lower()
        name = getattr(status, "name", None)
        if isinstance(name, str):
            return name.lower() == target.lower()
        s = str(status)
        return s.split(".")[-1].lower() == target.lower()
    except Exception:
        return False

def safe_market_buy(trade_client_local, symbol, cash_for_buy, order_lock, price_deques, size_deques, bias=None):
    # --- SESSION GATE: block early and late entries ---
    now_ts = datetime.now(timezone.utc)
    minutes = _session_minutes(now_ts)
    SESSION_LENGTH_MIN = 390 # 6.5h US cash session
    
    # Block first X minutes after open
    if minutes < 30:
        logging.info("%s - BUY blocked: session minutes=%d < 30", symbol, minutes)       
        return None

    # Block last 30 minutes before close
    if minutes >= SESSION_LENGTH_MIN - 30:
        logging.info("%s - BUY blocked: session minutes=%d >= %d (final 30 min)",
                     symbol, minutes, SESSION_LENGTH_MIN - 30)
        return None

    
    
    with order_lock:
        try:

            try:
                resp = stock_data_client.get_stock_latest_trade(
                    StockLatestTradeRequest(symbol_or_symbols=symbol)
                )
                est_price = float(resp[symbol].price)
            except Exception:
                est_price = None
            if est_price and est_price > 0:
                qty = int((cash_for_buy * BUY_CASH_BUFFER) // est_price)
            else:
                qty = 1
            if qty <= 0 or (est_price and qty * est_price < MIN_TRADE_USD):
                logging.debug("Computed buy qty too small for %s (qty=%s est_price=%s cash=%.2f)",
                              symbol, qty, est_price, cash_for_buy)
                return None
            order = MarketOrderRequest(
                symbol=symbol, qty=qty, side=OrderSide.BUY,
                type=OrderType.MARKET, time_in_force=TimeInForce.DAY
            )
            submitted = trade_client_local.submit_order(order)

            # Defensive guard: ensure deques exist and are non-empty
            if symbol not in price_deques or symbol not in size_deques:
                logging.debug("Missing deque data for %s; using empty series for indicators", symbol)
                prices_series = pd.Series(dtype=float)
                sizes_series = pd.Series(dtype=float)
            else:
                prices_series = pd.Series(price_deques[symbol])
                sizes_series = pd.Series(size_deques[symbol])
            # Debug short-series early so we can correlate with buy attempts
            if prices_series.empty:
                logging.debug("Short price series for %s at %s", symbol, datetime.now(timezone.utc))
                
            ema_fast_val = _safe_last(compute_ema_from_series(prices_series, EMA_FAST))
            ema_slow_val = _safe_last(compute_ema_from_series(prices_series, EMA_SLOW))
            rsi_val = _safe_last(compute_rsi_from_series(prices_series, RSI_PERIOD))
            vwap_val = _safe_last(compute_vwap_from_ticks(prices_series, sizes_series))
            regime_at_entry = detect_regime(prices_series, sizes_series)
            
            
            bias_val = bias if bias is not None else globals().get("day_bias", "unknown")
                      
            
            buy_row = {
                "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "symbol": symbol,
                "action": "BUY",
                "price": est_price if est_price else 0.0,
                "reason": "entry",   # or use evaluate_entry reason if available
                "bias": bias_val,
                "pnl": None,
                "ema_fast": ema_fast_val,
                "ema_slow": ema_slow_val,
                "rsi": rsi_val,
                "vwap": vwap_val,
                "regime": regime_at_entry,
                "code_version": CODE_VERSION
            }
            exec_rows.append(buy_row)
            write_exec_row_immediate(exec_rows[-1], symbol, RUN_MODE)
            logging.info(
                f"[TRADE] {symbol} [{RUN_MODE}] BUY qty={qty} @ {(est_price if est_price else 0.0):.4f} "
                f"| Time={datetime.now(timezone.utc).strftime('%H:%M:%S')} | Regime={regime_at_entry} | Bias={bias_val}"
            )

            # Optional lightweight execution audit
            if EXEC_AUDIT_ENABLED:
                try:
                    import csv
                    fieldnames = ["timestamp","symbol","action","price","reason","bias","pnl",
                                  "ema_fast","ema_slow","rsi","vwap","regime","code_version"]
                    with open(EXEC_AUDIT_FILE, "a", newline="") as f:
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        if f.tell() == 0:
                            writer.writeheader()
                        writer.writerow(buy_row)
                except Exception as e:
                    logging.warning("Failed to write BUY to audit file: %s", e)
                    
            return submitted
        except Exception as e:
            logging.exception("safe_market_buy error for %s: %s", symbol, e)
            return None

def _order_status_wait(trade_client_local, order_id, sym, max_retries=RECON_POLL_RETRIES, sleep_s=RECON_POLL_SLEEP):
    status = None
    try:
        for attempt in range(max_retries):
            confirmed = trade_client_local.get_order_by_id(order_id)
            status = getattr(confirmed, "status", None)
            logging.info("%s - RECON SELL order %s status=%s (attempt %d/%d)",
                         sym, order_id, status, attempt+1, max_retries)
            if _status_is(status, "filled"):
                return True
            time.sleep(sleep_s)
        logging.warning("%s - RECON SELL order %s not filled after %d polls (last status=%s)",
                        sym, order_id, max_retries, status)
        return False
    except Exception as e:
        logging.warning("%s - RECON status wait failed for order %s: %s", sym, order_id, e)
        return False


def safe_market_sell(trade_client_local, symbol, intended_qty, order_lock, price_deques, size_deques):
    with order_lock:
        available = get_position_qty(trade_client_local, symbol)
        if available == 0 and intended_qty > 0:
            time.sleep(1.0)
            available = get_position_qty(trade_client_local, symbol)

        qty_to_sell = min(int(intended_qty), available)
        if qty_to_sell <= 0:
            logging.info("safe_market_sell: nothing to sell for %s (available=%d intended=%s)",
                         symbol, available, intended_qty)
            return None
        try:
            order = MarketOrderRequest(
                symbol=symbol, qty=qty_to_sell, side=OrderSide.SELL,
                type=OrderType.MARKET, time_in_force=TimeInForce.DAY
            )
            submitted = trade_client_local.submit_order(order)
            order_id = getattr(submitted, "id", None)
            logging.info(f"[TRADE] {symbol} [{RUN_MODE}] SELL submitted qty={qty_to_sell} "
                         f"(available={available} intended={intended_qty}) order_id={order_id} "
                         f"| Time={datetime.now(timezone.utc).strftime('%H:%M:%S')}")

            # === SIM cleanup: remove symbol from positions_map so no_position=True again ===
            if RUN_MODE in ["SIM", "AGG_SIM"]:
                global positions_map
                positions_map.pop(symbol, None)

                # Immediate SELL append for SIM/AGG_SIM
                ref_entry = entry_prices.get(symbol, float("nan"))
                last_price = float(price_deques[symbol][-1]) if price_deques[symbol] else 0.0
                
                pnl = (last_price - ref_entry) * qty_to_sell if ref_entry else 0.0
               
                # Compute indicators for parity with BUY rows
                prices_series = pd.Series(price_deques[symbol])
                sizes_series = pd.Series(size_deques[symbol])
                ema_fast_val = _safe_last(compute_ema_from_series(prices_series, EMA_FAST))
                ema_slow_val = _safe_last(compute_ema_from_series(prices_series, EMA_SLOW))
                rsi_val = _safe_last(compute_rsi_from_series(prices_series, RSI_PERIOD))
                vwap_val = _safe_last(compute_vwap_from_ticks(prices_series, sizes_series))
                regime_at_sell = detect_regime(prices_series, sizes_series)


                bias_moment = "bullish" if ema_fast_val > ema_slow_val else "bearish"
                sell_reason = "exit"   # replace with evaluate_sell output if available
                
                sell_row = {
                    "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    "symbol": symbol,
                    "action": "SELL",
                    "price": last_price,
                    "reason": sell_reason,
                    "bias": bias_moment,
                    "pnl": round(pnl, 4),
                    "ema_fast": ema_fast_val,
                    "ema_slow": ema_slow_val,
                    "rsi": rsi_val,
                    "vwap": vwap_val,
                    "regime": regime_at_sell,
                    "code_version": CODE_VERSION
                }                
                exec_rows.append(sell_row)
                logging.info(f"[TRADE] {symbol} [{RUN_MODE}] SELL @ {last_price:.4f} | PnL={pnl:.4f} | Regime={regime_at_sell}")               

                if EXEC_AUDIT_ENABLED:
                    try:
                        import csv
                        fieldnames = ["timestamp","symbol","action","price","reason","bias","pnl",
                                      "ema_fast","ema_slow","rsi","vwap","regime","code_version" ]
                        with open(EXEC_AUDIT_FILE, "a", newline="") as f:
                            writer = csv.DictWriter(f, fieldnames=fieldnames)
                            if f.tell() == 0:
                                writer.writeheader()
                            writer.writerow(sell_row)
                    except Exception as e:
                        logging.warning("Failed to write SELL to audit file: %s", e)

                # ✅ No cleanup here; caller will handle it
                return submitted
            # === LIVE branch ===   
            if order_id:
                try:
                    # Use reconciliation polling window for truth and parity
                    max_retries = RECON_POLL_RETRIES
                    sleep_s = RECON_POLL_SLEEP
                    status = None
                    for attempt in range(max_retries):
                        confirmed = trade_client_local.get_order_by_id(order_id)
                        status = getattr(confirmed, "status", None)
                        logging.info("%s - SELL order %s status=%s (attempt %d/%d)",
                                    symbol, order_id, status, attempt+1, max_retries)
                        if _status_is(status, "filled"):
                            # --- Compute PnL and regime for parity ---
                            ref_entry = entry_prices.get(symbol, float("nan"))
                            last_price = float(getattr(confirmed, "filled_avg_price", getattr(confirmed, "price", 0.0)))
                            pnl = (last_price - ref_entry) * qty_to_sell if ref_entry else 0.0
                            
                            prices_series = pd.Series(price_deques[symbol])
                            sizes_series = pd.Series(size_deques[symbol])
                            ema_fast_val = compute_ema_from_series(prices_series, EMA_FAST).iloc[-1]
                            ema_slow_val = compute_ema_from_series(prices_series, EMA_SLOW).iloc[-1]
                            rsi_val = compute_rsi_from_series(prices_series, RSI_PERIOD).iloc[-1]
                            vwap_val = compute_vwap_from_ticks(prices_series, sizes_series).iloc[-1]
                            regime_at_sell = detect_regime(prices_series, sizes_series)

                            bias_moment = "bullish" if ema_fast_val > ema_slow_val else "bearish"
                            sell_reason = "exit"   # replace with evaluate_sell output if available
                            # --- Append SELL trade to exec_rows ---
                            sell_row = {
                                "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                                "symbol": symbol,
                                "action": "SELL",
                                "price": last_price,
                                "reason": sell_reason,   # or use evaluate_sell reason if available
                                "bias": bias_moment,
                                "pnl": round(pnl, 4),
                                "ema_fast": ema_fast_val,
                                "ema_slow": ema_slow_val,
                                "rsi": rsi_val,
                                "vwap": vwap_val,
                                "regime": regime_at_sell
                            }
                            exec_rows.append(sell_row)
                            logging.info(f"[TRADE] {symbol} [{RUN_MODE}] SELL @ {last_price:.4f} | PnL={pnl:.4f} | Regime={regime_at_sell}")

                            # audit write must be here, inside the same block
                            if EXEC_AUDIT_ENABLED:
                                try:
                                    import csv
                                    fieldnames = ["timestamp","symbol","action","price","reason","bias","pnl",
                                                  "ema_fast","ema_slow","rsi","vwap","regime"]
                                    with open(EXEC_AUDIT_FILE, "a", newline="") as f:
                                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                                        if f.tell() == 0:
                                            writer.writeheader()
                                        writer.writerow(sell_row)
                                except Exception as e:
                                    logging.warning("Failed to write SELL to audit file: %s", e)
                                 
                            return submitted
                        time.sleep(sleep_s)
                    logging.warning("%s - SELL order %s not filled within poll window; reconciliation will handle audit.",
                        symbol, order_id)
                except Exception as e:
                    logging.warning("SELL verification failed for %s: %s", symbol, e) 
            return submitted 
        except Exception as e:
            logging.exception("safe_market_sell error for %s: %s", symbol, e)
            return None

def force_liquidation_at_cutoff(trade_client_local, symbols, cutoff_hour_eet=22, cutoff_min_eet=59):
    # Convert current UTC to EET naive (UTC-5); for DST use pytz/zoneinfo
    now_utc = datetime.now(timezone.utc)
    now_eet = now_utc - timedelta(hours=2)
    if now_eet.hour > cutoff_hour_eet or (now_eet.hour == cutoff_hour_eet and now_eet.minute >= cutoff_min_eet):
        positions = trade_client_local.get_all_positions()
        for p in positions:
            s = p.symbol
            q = int(float(p.qty))
            if q > 0:
                try:
                    order = MarketOrderRequest(symbol=s, qty=q, side=OrderSide.SELL,
                                               type=OrderType.MARKET, time_in_force=TimeInForce.DAY)
                    trade_client_local.submit_order(order)
                    logging.warning("%s - EOD forced SELL qty=%d", s, q)
                except Exception as e:
                    logging.exception("%s - EOD forced sell error: %s", s, e)


def reconcile_positions(trade_client_local,
                        symbols,
                        positions_map,
                        price_deques,
                        size_deques,
                        entry_times,
                        entry_prices,
                        entry_qty,
                        entry_configs):
    """
    Ensures local state and Alpaca positions are consistent, and forces sell if exit logic says so.
    - Reattaches orphan positions (no local context).
    - Re-evaluates exits using evaluate_sell.
    - Extends sell fill polling window to avoid missed confirmations.
    """
    # --- Skip reconciliation entirely in SIM/AGG_SIM mode ---
    if RUN_MODE in ["SIM", "AGG_SIM"]:
        return
     
    if not RECONCILIATION_ENABLED:
        return

    now_ts = datetime.now(timezone.utc)

    for sym in symbols:
        qty_open, avg_entry = positions_map.get(sym, (0, 0.0))
        if qty_open <= 0:
            # If no position but local state says in trade, clean up
            if sym in entry_times or sym in entry_qty or sym in entry_configs:
                logging.info("%s - RECON cleanup: no live position, purging local entry state", sym)
                entry_times.pop(sym, None)
                entry_prices.pop(sym, None)
                entry_qty.pop(sym, None)
                entry_configs.pop(sym, None)
                trailing_active[sym] = False
                last_exit_time[sym] = now_ts
            continue

        # There is an open position on Alpaca
        has_context = (sym in entry_prices) and (sym in entry_configs) and (sym in entry_times)

        # Reattach orphan position context if missing
        if not has_context:
            if RECON_FORCE_SELL_IF_ORPHAN:
                logging.warning("%s - RECON orphan position detected (qty=%d @ %.4f). Attaching minimal context.",
                                sym, qty_open, avg_entry)
                entry_prices[sym] = avg_entry
                entry_qty[sym] = qty_open
                entry_times[sym] = last_exit_time.get(sym, None) or now_ts  # attach now if unknown
                # Pick a conservative config (use BEARISH_CONFIG unless bias is available)
                entry_configs[sym] = BEARISH_CONFIG
                trailing_active[sym] = False
            else:
                logging.info("%s - RECON orphan position detected; skip (toggle off).", sym)
                continue

        # Build local series (if insufficient data, skip)
        prices_series = pd.Series(price_deques.get(sym, []))
        sizes_series = pd.Series(size_deques.get(sym, []))

        if len(prices_series) < 5 or len(sizes_series) < 5:
            logging.info("%s - RECON insufficient local series for exit evaluation (prices=%d sizes=%d)",
                         sym, len(prices_series), len(sizes_series))
            continue

        # Exit evaluation (reuses your logic, SIM/LIVE parity)
        last_price = float(prices_series.iloc[-1])
        ref_entry = entry_prices.get(sym, avg_entry)
        config = entry_configs.get(sym)
        if config is None:
            logging.warning("%s - RECON missing entry config; attaching BEARISH_CONFIG", sym)
            config = BEARISH_CONFIG
            entry_configs[sym] = config

            if sym in entry_times and sym in entry_configs and sym in entry_prices:      
                sell, reason = evaluate_sell(
                    sym,
                    last_price,
                    ref_entry,
                    price_deques[sym],
                    size_deques[sym],
                    entry_times,
                    config,
                    current_time=now_ts,
                    regime=detect_regime(prices_series, sizes_series),
                    log_stack=True
                )

        if sell:
            # Submit sell with extended confirmation polling
            try:
                order = MarketOrderRequest(
                    symbol=sym, qty=qty_open, side=OrderSide.SELL,
                    type=OrderType.MARKET, time_in_force=TimeInForce.DAY
                )
                submitted = trade_client_local.submit_order(order)
                order_id = getattr(submitted, "id", None)
                logging.info("%s - RECON SELL submitted qty=%d @ last=%.4f | Reason=%s",
                             sym, qty_open, last_price, reason)

                filled = False
                if order_id:
                    filled = _order_status_wait(trade_client_local, order_id, sym)
                else:
                    logging.warning("%s - RECON SELL missing order_id; proceeding with position reconciliation", sym)

                # Reconcile: check if position still exists
                time.sleep(1.0)
                qty_after = get_position_qty(trade_client_local, sym)
                if filled or qty_after == 0:
                    # Clean up local state
                    entry_times.pop(sym, None)
                    entry_prices.pop(sym, None)
                    entry_qty.pop(sym, None)
                    entry_configs.pop(sym, None)
                    trailing_active[sym] = False
                    last_exit_time[sym] = now_ts
                    regime_at_sell = detect_regime(prices_series, sizes_series)
                    pnl_est = (last_price - ref_entry) * qty_open
                    regime_pnl[regime_at_sell] += float(pnl_est)
                    regime_trades[regime_at_sell] += 1
                    exit_reason_count[regime_at_sell][reason] += 1
                    logging.info("%s - RECON SELL filled or reconciled | PnL≈%.4f | Reason=%s", sym, pnl_est, reason)
                    # --- Write SELL to audit_trades_live for parity ---
                    if EXEC_AUDIT_ENABLED:
                        try:
                            import csv
                            ema_fast_val = compute_ema_from_series(prices_series, EMA_FAST).iloc[-1]
                            ema_slow_val = compute_ema_from_series(prices_series, EMA_SLOW).iloc[-1]
                            rsi_val = compute_rsi_from_series(prices_series, RSI_PERIOD).iloc[-1]
                            vwap_val = compute_vwap_from_ticks(prices_series, sizes_series).iloc[-1]

                            sell_row = {
                                "timestamp": now_ts.strftime("%Y-%m-%d %H:%M:%S"),
                                "symbol": sym,
                                "action": "SELL",
                                "price": round(last_price, 6),
                                "reason": reason or "exit",
                                "bias": "bullish" if ema_fast_val > ema_slow_val else "bearish",
                                "pnl": round(pnl_est, 4),
                                "ema_fast": ema_fast_val,
                                "ema_slow": ema_slow_val,
                                "rsi": rsi_val,
                                "vwap": vwap_val,
                                "regime": regime_at_sell
                            }
                            fieldnames = ["timestamp","symbol","action","price","reason","bias","pnl",
                                          "ema_fast","ema_slow","rsi","vwap","regime"]
                            with open(EXEC_AUDIT_FILE, "a", newline="") as f:
                                writer = csv.DictWriter(f, fieldnames=fieldnames)
                                if f.tell() == 0:
                                    writer.writeheader()
                                writer.writerow(sell_row)
                        except Exception as e:
                            logging.warning("[RECON] Failed to write SELL to audit file: %s", e)
                else:
                    logging.warning("%s - RECON SELL not confirmed; position qty still %d", sym, qty_after)
            except Exception as e:
                logging.exception("%s - RECON SELL error: %s", sym, e)
        else:
            # Optional: stale guard — if context is old and no exit triggered, log/watch
            et = entry_times.get(sym)
            age_min = ((now_ts - et).total_seconds() / 60.0) if et else None
            if age_min is not None and age_min >= RECON_MAX_STALE_MIN:
                logging.info("%s - RECON stale position age=%.1f min | last=%.4f | entry=%.4f | reason=no-exit",
                             sym, age_min, last_price, ref_entry)


# === STRATEGY HELPERS: BUY/SELL CONDITIONS ===
# === BIAS DETECTION ===
def detect_day_bias(prices_series, ema_fast_series, ema_slow_series, vwap_series):
    try:
        # === SAFETY GUARD: prevent out-of-bounds at start of session ===
        if (len(prices_series) == 0 or
            len(ema_fast_series) == 0 or
            len(ema_slow_series) == 0 or
            len(vwap_series) == 0):
            logging.debug("[BIAS] Series not ready yet, defaulting to bearish")
            return "bearish"
                
        last_price = float(prices_series.iloc[-1])
        ema_fast_now = float(ema_fast_series.iloc[-1])
        ema_slow_now = float(ema_slow_series.iloc[-1])
        vwap_now = float(vwap_series.iloc[-1])

        if ema_fast_now > ema_slow_now and last_price >= vwap_now:
            return "bullish"
        elif ema_fast_now < ema_slow_now and last_price < vwap_now:
            return "bearish"
        else:
            return "bearish"  # konservatiivinen oletus
    except Exception as e:
        logging.error("[ERROR] Bias detection failed: %s", e)
        return "bearish"

# === REGIME DETECTOR ===
# === PATCH 1: Regime smoothing and cleanup ===
# Insert just below detect_regime definition

REGIME_SMOOTH_ENABLED = True
REGIME_TRANSITION = {
    # simple sticky transitions; tune with audit
    "TREND":     {"TREND": 0.70, "RANGE": 0.15, "LOW_VOL": 0.10, "HIGH_VOL": 0.05},
    "RANGE":     {"TREND": 0.15, "RANGE": 0.65, "LOW_VOL": 0.15, "HIGH_VOL": 0.05},
    "LOW_VOL":   {"TREND": 0.10, "RANGE": 0.15, "LOW_VOL": 0.70, "HIGH_VOL": 0.05},
    "HIGH_VOL":  {"TREND": 0.10, "RANGE": 0.10, "LOW_VOL": 0.05, "HIGH_VOL": 0.75}
}
_last_regime = defaultdict(lambda: None)

def _smooth_regime(sym, raw_regime):
    if not REGIME_SMOOTH_ENABLED:
        return raw_regime
    prev = _last_regime[sym]
    if prev is None:
        _last_regime[sym] = raw_regime
        return raw_regime
    # stickiness: if raw flips but probability favors previous, keep previous
    trans = REGIME_TRANSITION.get(prev, {})
    prob_prev = trans.get(prev, 0.5)
    prob_raw = trans.get(raw_regime, 0.0)
    chosen = prev if prob_prev >= prob_raw else raw_regime
    _last_regime[sym] = chosen
    return chosen

def detect_regime(prices_series, sizes_series):
    ema_fast = compute_ema_from_series(prices_series, EMA_FAST).iloc[-1] if len(prices_series) >= 2 else float('nan')
    ema_slow = compute_ema_from_series(prices_series, EMA_SLOW).iloc[-1] if len(prices_series) >= 2 else float('nan')
    vwap_val = compute_vwap_from_ticks(prices_series, sizes_series).iloc[-1] if len(sizes_series) else float('nan')
    slope = ema_slope(prices_series, EMA_SLOW)
    atr_val = compute_atr_from_series(prices_series, ATR_PERIOD)
    upper, ma, lower, bandwidth = compute_bollinger(prices_series, period=20, std=2.0)
    macd_line, macd_signal, macd_hist = compute_macd(prices_series)
    rsi_val = compute_rsi_from_series(prices_series, RSI_PERIOD).iloc[-1]
    price = float(prices_series.iloc[-1]) if len(prices_series) else float("nan")



    minutes = _session_minutes(datetime.now(timezone.utc))
    if minutes < 40:
        return "TREND" if slope > 0 else "RANGE"


    # ATR percentile proxy
    N = 50
    if len(prices_series) >= N + ATR_PERIOD:
        atr_series = prices_series.diff().abs().rolling(ATR_PERIOD).mean()
        hist = atr_series.iloc[-N:].dropna()
        pct = (hist < atr_val).mean() if len(hist) > 10 and not pd.isna(atr_val) else 0.5
    else:
        pct = 0.5

    # Decision order: HIGH_VOL -> TREND -> LOW_VOL -> RANGE
    if (pct >= HIGH_VOL_CONFIG["ATR_TOP_PCT"]) or (not pd.isna(bandwidth) and bandwidth > RANGE_CONFIG["BANDWIDTH_MAX"]):
        raw = "HIGH_VOL"
    elif (
        not pd.isna(ema_fast) and not pd.isna(ema_slow) and (ema_fast > ema_slow) and
        not pd.isna(slope) and (slope > 0) and
        not pd.isna(vwap_val) and (prices_series.iloc[-1] >= vwap_val)
    ):
        raw = "TREND"

    # --- DRIFT regime detection ---
    if (
        not pd.isna(ema_fast) and not pd.isna(ema_slow) and ema_fast > ema_slow and
        not pd.isna(slope) and 0 < slope < 0.0008 and
        not pd.isna(macd_hist) and macd_hist > 0 and
        not pd.isna(bandwidth) and 0.0045 <= bandwidth <= 0.012 and
        not pd.isna(rsi_val) and 45 <= rsi_val <= 65 and
        not pd.isna(vwap_val) and price >= vwap_val
    ):
        return "DRIFT"

        
    elif not pd.isna(bandwidth) and (bandwidth <= min(LOW_VOL_CONFIG["BANDWIDTH_CAP"], 0.003)) and abs(slope) < 0.0003:
        raw = "LOW_VOL"
    elif not pd.isna(bandwidth) and (RANGE_CONFIG["BANDWIDTH_MIN"] <= bandwidth <= RANGE_CONFIG["BANDWIDTH_MAX"]):
        raw = "RANGE"
    else:
        raw = "RANGE"

    # Note: smoothing needs 'sym'; adapt call sites to pass symbol for smoothing
    return raw  # keep raw here; smoothing applied at call sites



# === REGIME-AWARE ENTRY SCORING (replacement gate) ===
def evaluate_entry(sym, price, size, prices_series, sizes_series, ts_val,
                   positions_map, inflight_orders, pending_entries,
                   last_exit, last_buy_time, CONFIG, regime,
                   bias=None, log_stack=False):
    """
    Returns (accept: bool, reason: str, score: float, signal_stack: dict)
    Gate is regime-dependent. Keeps your cooldown and position safety checks.
    """
    # Cooldown
    since_last_exit = (ts_val - last_exit).total_seconds() if last_exit is not None else float("inf")
    since_last_buy = (ts_val - last_buy_time[sym]).total_seconds() if last_buy_time[sym] is not None else float("inf")
    def _regime_cooldown(regime):
        return 40 if regime == "TREND" else COOLDOWN_SECONDS

    if since_last_exit < _regime_cooldown(regime) or since_last_buy < _regime_cooldown(regime):
        logging.debug(f"[BLOCK] {sym} rejected | Reason=Cooldown")
        return (False, "Cooldown", 0.0, {})
        
    # Regime kill-switch gates
    if regime == "HIGH_VOL" and high_vol_paused[sym]:
        return (False, "HIGH_VOL paused", 0.0, {})
    if regime == "TREND" and trend_paused[sym]:
        return (False, "TREND paused", 0.0, {})

    # --- HARD GATE: disable HIGH_VOL entries entirely ---
    if regime == "HIGH_VOL":
        logging.debug(f"[BLOCK] {sym} rejected | Reason=HIGH_VOL regime blocked for entries")
        return (False, "HIGH_VOL blocked", 0.0, {})

    # --- HARD GATE: disable HIGH_VOL entries entirely ---
    if regime == "HIGH_VOL":
        logging.debug(f"[BLOCK] {sym} rejected | Reason=HIGH_VOL regime blocked for entries")
        return (False, "HIGH_VOL blocked", 0.0, {})


    # FitScore gate: auto-pause regime if underperforming
    if regime_trades[regime] >= 5:
        sls = exit_reason_count[regime].get("Stop-loss", 0)
        net = regime_pnl[regime]
        if (sls / regime_trades[regime] >= 0.6) and (net < 0):
            return (False, f"{regime} paused by FitScore", 0.0, {})

    # --- RE-ENTRY COOLDOWN instead of hard block ---
    last_exit = last_exit_time.get(sym)
    if last_exit:
        if (datetime.now(timezone.utc) - last_exit).total_seconds() < 10:
            return (False, "Cooldown block", 0.0, {})
    
    # Still block if inflight or pending
    if inflight_orders.get(sym) is not None or sym in pending_entries:
        return (False, "Order flow block", 0.0, {})


    # Base features
    ema_fast = compute_ema_from_series(prices_series, EMA_FAST).iloc[-1] if len(prices_series) >= 2 else float('nan')
    ema_slow = compute_ema_from_series(prices_series, EMA_SLOW).iloc[-1] if len(prices_series) >= 2 else float('nan')
    rsi_val = compute_rsi_from_series(prices_series, RSI_PERIOD).iloc[-1] if len(prices_series) else float('nan')
    vwap_val = compute_vwap_from_ticks(prices_series, sizes_series).iloc[-1] if len(sizes_series) else float('nan')
    macd_line, macd_signal, macd_hist = compute_macd(prices_series)

    median_vol = sizes_series.median() if len(sizes_series) > 0 else float('nan')
    vol_spike = (not pd.isna(median_vol)) and (size > (median_vol * VOL_SPIKE_MULT))

    upper, boll_ma, lower, bandwidth = compute_bollinger(prices_series, period=RANGE_CONFIG["BOLL_PERIOD"], std=RANGE_CONFIG["BOLL_STD"])
    atr_val = compute_atr_from_series(prices_series, ATR_PERIOD)
    slope = ema_slope(prices_series, EMA_SLOW)

    # INSERT ADX/CHOP HERE
    adx_val = _adx_proxy(prices_series) if ADX_ENABLED else float('nan')
    chop_val = _choppiness_proxy(prices_series) if CHOP_ENABLED else float('nan')

    # RSI banding by regime + uptick check
    REGIME_RSI_BANDS = {
        "TREND": (32, 80),
        "RANGE": (28, 70),
        "LOW_VOL": (30, 75),
    }
    
    def rsi_in_band(regime, rsi):
        lo, hi = REGIME_RSI_BANDS.get(regime, (MIN_RSI_FOR_ENTRY, MAX_RSI_FOR_ENTRY))
        return (rsi >= lo) and (rsi <= hi)
    
    rsi_series_full = compute_rsi_from_series(prices_series, RSI_PERIOD)
    rsi_prev = rsi_series_full.iloc[-2] if len(rsi_series_full) >= 2 else float('nan')
    rsi_uptick = (not pd.isna(rsi_prev) and not pd.isna(rsi_val) and rsi_val > rsi_prev)
    
    # Final RSI check combines regime band + uptick
    rsi_ok = rsi_in_band(regime, rsi_val) and rsi_uptick


    # Pullback checks
    pullback_to_ema = (not pd.isna(ema_slow) and abs(price - ema_slow) / price <= TREND_CONFIG["PULLBACK_TOL"])
    pullback_to_vwap = (not pd.isna(vwap_val) and abs(price - vwap_val) / price <= TREND_CONFIG["PULLBACK_TOL"])

    # Range checks
    lower_touch = (not pd.isna(lower) and price <= lower * (1 + 0.0002))  # epsilon
    upper_touch = (not pd.isna(upper) and price >= upper * (1 - 0.0002))
    vwap_reversion_room = (not pd.isna(vwap_val) and (vwap_val - price) / vwap_val >= 0.0008)  # distance for mean reversion

    # High-vol checks
    N = HIGH_VOL_CONFIG["ATR_WINDOW"]
    atr_series = prices_series.diff().abs().rolling(ATR_PERIOD).mean() if len(prices_series) >= ATR_PERIOD else pd.Series([])
    atr_hist = atr_series.iloc[-N:].dropna() if len(atr_series) else pd.Series([])
    atr_pct = (atr_hist < atr_val).mean() if len(atr_hist) > 10 and not pd.isna(atr_val) else 0.5
    bb_expanding = (not pd.isna(bandwidth) and bandwidth > RANGE_CONFIG["BANDWIDTH_MAX"])
    vol_roc_val = volume_roc(sizes_series, HIGH_VOL_CONFIG["VOL_ROC_WINDOW"]) if len(sizes_series) else float('nan')
    vol_roc_ok = (not pd.isna(vol_roc_val) and vol_roc_val > 0.35)
    breakout_bar = (price > (recent_high(prices_series, HIGH_VOL_CONFIG["BREAKOUT_LOOKBACK"]) * 1.002))

    # Low-vol checks
    envelope_lower = (ema_slow * (1 - LOW_VOL_CONFIG["ENVELOPE_PCT"])) if not pd.isna(ema_slow) else float('nan')
    envelope_touch = (not pd.isna(envelope_lower) and price <= envelope_lower)
    chop_high = (not pd.isna(bandwidth) and bandwidth <= LOW_VOL_CONFIG["BANDWIDTH_CAP"])
    vol_ok_low = vol_spike or (not pd.isna(median_vol) and median_vol > 0)  # avoid totally dry tapes
    vwap_below = (not pd.isna(vwap_val) and price < vwap_val)

    # Base sanity filters to avoid nonsense:
    if pd.isna(ema_fast) or pd.isna(ema_slow) or pd.isna(vwap_val) or pd.isna(rsi_val):
        logging.debug(f"[BLOCK] {sym} rejected | Reason=Missing core indicators")
        return (False, "Missing core indicators", 0.0, {})

    # --- Bias-aware safety filter (global) ---
    # For bearish bias, avoid buying into deeply oversold tape that can keep falling.
    if bias == "bearish":
        if rsi_val < 35:
            logging.debug(f"[BLOCK] {sym} rejected | Reason=Bearish bias RSI<35 (rsi={rsi_val:.2f})")
            return (False, "Bearish bias RSI<35 block", 0.0, {})

    signal_stack = {}
    score = 0.0

    obv_slope = _obv_slope_proxy(prices_series, sizes_series, window=20)                   
    if regime == "TREND":
        w = TREND_CONFIG["WEIGHTS"]
        ema_trend_ok = (ema_fast > ema_slow) and (slope > 0)
        
        # Conditional VWAP delta
        strong_trend = (not pd.isna(slope) and slope > 0) and (not pd.isna(adx_val) and adx_val >= 25)
        vwap_above_ok = (price > vwap_val) and (
            ((price - vwap_val) > CONFIG["VWAP_DELTA"] * vwap_val) if not strong_trend
            else ((price - vwap_val) > (CONFIG["VWAP_DELTA"] * 0.6) * vwap_val)  # relaxed if strong_trend
        )
        macd_ok = (
            not pd.isna(macd_line) and not pd.isna(macd_signal)
            and macd_line > macd_signal
            and macd_hist > 0.03   # require stronger momentum
        )
        pullback_ok = (pullback_to_ema or pullback_to_vwap)
        vol_ok = vol_spike
        obv_ok = (not pd.isna(obv_slope) and obv_slope > 0)

        # NEW: slope-or-OBV confirmation
        slope_or_obv = (slope > 0.0008) or obv_ok

        signal_stack.update({
            "ema_trend_ok": ema_trend_ok,
            "vwap_above_ok": vwap_above_ok,
            "macd_ok": macd_ok,
            "pullback_ok": pullback_ok,
            "vol_ok": vol_ok,
            "obv_slope_ok": obv_ok
        })
        score += w["ema_trend"] if ema_trend_ok else 0.0
        score += w["vwap_above"] if vwap_above_ok else 0.0
        score += w["macd_momentum"] if macd_ok else 0.0
        score += w["pullback_ok"] if pullback_ok else 0.0
        score += w["vol_confirm"] if vol_ok else 0.0
        score += 0.3 if obv_ok else 0.0

        # INSERT ADX SCORING
        adx_ok = (not pd.isna(adx_val) and adx_val >= 20)
        signal_stack["adx_ok"] = adx_ok
        score += 0.4 if adx_ok else 0.0

    elif regime == "DRIFT":
        w = DRIFT_CONFIG["WEIGHTS"]
    
        ema_ok = (ema_fast > ema_slow)
        slope_ok = (0 < slope < 0.0008)
        macd_ok = (macd_hist > 0)
        vwap_ok = (price >= vwap_val)
        rsi_ok = (45 <= rsi_val <= 65)
        bandwidth_ok = (0.0045 <= bandwidth <= 0.012)

        signal_stack.update({
            "ema_ok": ema_ok,
            "slope_ok": slope_ok,
            "macd_ok": macd_ok,
            "vwap_ok": vwap_ok,
            "rsi_ok": rsi_ok,
            "bandwidth_ok": bandwidth_ok
        })

    
        score = (
            w["ema_trend"] * (1 if ema_ok else 0) +
            w["slope_ok"] * (1 if slope_ok else 0) +
            w["macd_ok"] * (1 if macd_ok else 0) +
            w["vwap_ok"] * (1 if vwap_ok else 0) +
            w["rsi_ok"] * (1 if rsi_ok else 0) +
            w["bandwidth_ok"] * (1 if bandwidth_ok else 0)
        )

        threshold = DRIFT_CONFIG["ENTRY_SCORE_THRESHOLD"]
        
        logging.debug(
            "[DRIFT_ENTRY][%s] score=%.2f threshold=%.2f ema_ok=%s slope_ok=%s macd_ok=%s vwap_ok=%s rsi_ok=%s bw_ok=%s",
            sym, score, threshold,
            ema_ok, slope_ok, macd_ok, vwap_ok, rsi_ok, bandwidth_ok
        )
       
           
        if score < threshold:
            return (False, "DRIFT score block", score, signal_stack)
    
        # Confirmation: last 3 ticks higher
        if not _confirm_trend(prices_series, vwap_val, ema_slow):
            return (False, "DRIFT confirm block", score, signal_stack)
    
        return (True, "entry", score, signal_stack)

    elif regime == "RANGE":
        # RANGE_BULL no longer depends on global bias.
        # We only block RANGE_BEAR if bias is explicitly bearish AND RSI is not oversold.
        
        w = RANGE_CONFIG["WEIGHTS"]

        # --- Core RANGE_BULL conditions ---

        # 1) RSI oversold band + uptick (mean-reversion long)
        rsi_band_ok = (rsi_val >= 18) and (rsi_val <= 38)
        rsi_uptick_ok = rsi_uptick

        # 2) Price at or below lower Bollinger band
        if RANGE_STRICT_TOUCH_ENABLED:
            lower_band_touch = (not pd.isna(lower) and price <= lower)
        else:
            lower_band_touch = (not pd.isna(lower) and price <= lower * (1 + RANGE_TOUCH_EPSILON))

        # 3) Bandwidth in range-friendly zone
        bandwidth_ok = (
            not pd.isna(bandwidth)
            and RANGE_CONFIG["BANDWIDTH_MIN"] <= bandwidth <= min(RANGE_CONFIG["BANDWIDTH_MAX"], 0.010)
        )

        # 4) VWAP reversion room
        vwap_rev_ok = (
            not pd.isna(vwap_val)
            and (vwap_val - price) / vwap_val >= RANGE_VWAP_ROOM_MIN
        )

        # 5) Volume not ultra-dry
        vol_not_dry = (not pd.isna(median_vol) and median_vol > 0)

        # 6) OBV slope as tape health proxy (optional but helpful)
        obv_ok = (not pd.isna(obv_slope) and obv_slope >= 0)

        # --- RANGE_BULL-specific local bias ---
        # We require: price below VWAP, oversold RSI, and healthy OBV.
        range_bull_bias_ok = (
            vwap_rev_ok and
            rsi_band_ok and
            obv_ok
        )

        if not range_bull_bias_ok:
            logging.debug(f"[BLOCK] {sym} rejected | Reason=RANGE_BULL local bias block")
            return (False, "RANGE_BULL bias block", 0.0, {})

        # --- Optional: Bollinger bandwidth ROC filter (block expanding volatility) ---
        bb_roc = 0
        if not pd.isna(bandwidth):
            try:
                bw_hist = prices_series.rolling(RANGE_CONFIG["BOLL_PERIOD"]).apply(
                    lambda x: (x.max() - x.min()) / x.mean()
                )
                if len(bw_hist) >= 2:
                    bb_roc = bandwidth - bw_hist.iloc[-2]
            except Exception:
                bb_roc = 0

        if RANGE_BB_ROC_MAX is not None and bb_roc > RANGE_BB_ROC_MAX:
            logging.debug(f"[BLOCK] {sym} rejected | Reason=Range blocked by BB ROC (bb_roc={bb_roc:.6f})")
            return (False, "Range blocked by BB ROC", 0.0, {})

        # --- Update signal_stack and score for RANGE_BULL ---
        signal_stack.update({
            "lower_band_touch": lower_band_touch,
            "rsi_band_ok": rsi_band_ok,
            "rsi_uptick": rsi_uptick_ok,
            "vwap_reversion": vwap_rev_ok,
            "bandwidth_ok": bandwidth_ok,
            "vol_not_dry": vol_not_dry,
            "obv_slope_ok": obv_ok,
        })

        # Use existing RANGE_CONFIG weights; map them to our conditions
        score += w["lower_band_touch"] if lower_band_touch else 0.0
        score += w["rsi_uptick"] if (rsi_band_ok and rsi_uptick_ok) else 0.0
        score += w["vwap_reversion"] if vwap_rev_ok else 0.0
        score += w["bandwidth_ok"] if bandwidth_ok else 0.0
        score += w["vol_not_dry"] if vol_not_dry else 0.0
        # Treat "rsi_ok" as generic RSI band condition
        score += w["rsi_ok"] if rsi_band_ok else 0.0

        # Small bonus for healthy OBV slope (doesn't have a dedicated weight in RANGE_CONFIG)
        if obv_ok:
            score += 0.3

    
    elif regime == "HIGH_VOL":
        w = HIGH_VOL_CONFIG["WEIGHTS"]
        atr_high_ok = (atr_pct >= HIGH_VOL_CONFIG["ATR_TOP_PCT"])
        bb_expand_ok = bb_expanding
        macd_strong_ok = (not pd.isna(macd_line) and not pd.isna(macd_signal) and macd_line > macd_signal and macd_hist > 0.05)
        vol_roc_ok2 = vol_roc_ok
        breakout_ok = breakout_bar

        signal_stack.update({
            "atr_high": atr_high_ok,
            "bb_expanding": bb_expand_ok,
            "macd_strong": macd_strong_ok,
            "vol_roc": vol_roc_ok2,
            "breakout_bar": breakout_ok
        })
        score += w["atr_high"] if atr_high_ok else 0.0
        score += w["bb_expanding"] if bb_expand_ok else 0.0
        score += w["macd_strong"] if macd_strong_ok else 0.0
        score += w["vol_roc"] if vol_roc_ok2 else 0.0
        score += w["breakout_bar"] if breakout_ok else 0.0

    else:  # LOW_VOL
        # STEP 1: hard-block LOW_VOL entries (exit-only regime for now)
        logging.debug(f"[BLOCK] {sym} rejected | Reason=LOW_VOL regime blocked for entries")
        return (False, "LOW_VOL blocked", 0.0, {})
        w = LOW_VOL_CONFIG["WEIGHTS"]

        # Require short-term momentum not aggressively against you:
        ema_momentum_ok = (not pd.isna(ema_fast) and not pd.isna(ema_slow) and ema_fast >= ema_slow)
        
        vwap_below_ok = vwap_below
        rsi_mr_ok = (rsi_val < 35 and rsi_uptick)
        envelope_touch_ok = envelope_touch
        chop_ok = chop_high
        vol_ok = vol_ok_low

        # Hard block: in LOW_VOL regime, do not take entries if EMA_fast << EMA_slow
        if not ema_momentum_ok:
            logging.debug(f"[BLOCK] {sym} rejected | Reason=LOW_VOL ema_momentum_ok=False (ema_fast={ema_fast:.4f} ema_slow={ema_slow:.4f})")
            return (False, "LOW_VOL EMA momentum block", 0.0, {})
            

        signal_stack.update({
            "vwap_below": vwap_below_ok,
            "rsi_uptick": rsi_mr_ok,
            "envelope_touch": envelope_touch_ok,
            "chop_high": chop_ok,
            "vol_ok": vol_ok,
            "ema_momentum_ok": ema_momentum_ok
        })
        score += w["vwap_below"] if vwap_below_ok else 0.0
        score += w["rsi_uptick"] if rsi_mr_ok else 0.0
        score += w["envelope_touch"] if envelope_touch_ok else 0.0
        score += w["chop_high"] if chop_ok else 0.0
        score += w["vol_ok"] if vol_ok else 0.0

        # INSERT CHOPPINESS SCORING
        chop_proxy_ok = (not pd.isna(chop_val) and chop_val >= 1.2)
        signal_stack["chop_proxy_ok"] = chop_ok
        score += 0.3 if chop_ok else 0.0
        
        # === NEW: OBV slope enforcement for LOW_VOL ===
        obv_ok = (not pd.isna(obv_slope) and obv_slope > 0)
        signal_stack["obv_slope_ok"] = obv_ok
        
        if bias == "bearish":
            if not obv_ok:
                return (False, "LOW_VOL bearish blocked by OBV slope<=0", score, signal_stack)
        else: # bullish
            if not obv_ok:
                score -= 0.5 # penalize but allow if other signals are strong

        # --- VWAP proximity safety filter ---
        # Avoid buying when price is very far from VWAP in either direction, to reduce chasing extremes.
        vwap_dist = abs(price - vwap_val) / vwap_val if not pd.isna(vwap_val) and vwap_val > 0 else 0.0
        VWAP_DIST_MAX = 0.025 # 1.5% from VWAP; tune as needed

        if vwap_dist > VWAP_DIST_MAX:
            logging.debug(f"[BLOCK] {sym} rejected | Reason=VWAP distance {vwap_dist:.4f} > {VWAP_DIST_MAX:.4f}")
            return (False, "VWAP distance block", 0.0, {})
                       
    # Inside evaluate_entry, before computing 'accept'
    confirm_ok = True
    if ENTRY_CONFIRM_ENABLED:
        if regime == "TREND":
            confirm_ok = _confirm_trend(prices_series, vwap_val, ema_slow)
        elif regime == "RANGE":
            confirm_ok = _confirm_range(prices_series, lower)
        elif regime == "LOW_VOL":
            confirm_ok = _confirm_low_vol(prices_series, vwap_val)
        else:  # HIGH_VOL
            # breakout confirmation: last price > recent high and two higher closes
            rh = recent_high(prices_series, HIGH_VOL_CONFIG["BREAKOUT_LOOKBACK"])
            if pd.isna(rh) or len(prices_series) < ENTRY_CONFIRM_TICKS + 1:
                confirm_ok = False
            else:
                tail = prices_series.iloc[-ENTRY_CONFIRM_TICKS-1:]
                confirm_ok = (tail.iloc[-ENTRY_CONFIRM_TICKS] > rh) and all(tail.diff().fillna(0) > 0)

                       

    # Final gate
    CONFIG_SESSION = overlay_by_session(CONFIG, ts_val, regime)                   
    threshold = adaptive_entry_threshold(CONFIG_SESSION, sym, regime)
    accept = (score >= threshold) and confirm_ok
        
    if log_stack and (accept or AUDIT_TRAIL_ENABLED):
        logging.debug(f"[ENTRY_STACK][{sym}] regime={regime} score={score:.2f} threshold={threshold} stack={signal_stack}")

    return (accept, f"Regime={regime} score={score:.2f}", score, signal_stack)

# === EXIT OVERLAY BY REGIME ===
def overlay_exit_params_by_regime(CONFIG, regime):
    """
    Adjust TP/SL buffers per regime without changing core indicators.
    """
    adj = dict(CONFIG)  # shallow copy
    if regime == "RANGE":
        adj["TP_PCT"] = max(0.0010, CONFIG["TP_PCT"] * 0.8)
        adj["SL_MULTIPLIER"] = max(0.6, CONFIG["SL_MULTIPLIER"] * 0.9)
        adj["TS_ACTIVATION_BUFFER"] = max(0.002, CONFIG["TS_ACTIVATION_BUFFER"] * 0.8)
    elif regime == "HIGH_VOL":
        adj["TP_PCT"] = min(0.0050, CONFIG["TP_PCT"] * 1.4)
        adj["SL_MULTIPLIER"] = min(1.5, CONFIG["SL_MULTIPLIER"] * 1.2)
        adj["TS_ACTIVATION_BUFFER"] = min(0.008, CONFIG["TS_ACTIVATION_BUFFER"] * 1.4)
    elif regime == "LOW_VOL":
        adj["TP_PCT"] = max(0.0010, CONFIG["TP_PCT"] * 0.9)
        adj["SL_MULTIPLIER"] = max(0.7, CONFIG["SL_MULTIPLIER"] * 0.9)
        adj["TS_ACTIVATION_BUFFER"] = max(0.0025, CONFIG["TS_ACTIVATION_BUFFER"] * 0.85)
    # TREND uses base CONFIG
    return adj



def evaluate_sell(sym, last_price, ref_entry, price_deque, size_deque, entry_times, 
                  CONFIG, ema_fast_period=EMA_FAST, ema_slow_period=EMA_SLOW, rsi_period=RSI_PERIOD, current_time=None, regime=None, log_stack=False):

    # Early exit if no entry context
    if sym not in entry_times or sym not in entry_prices or CONFIG is None:
        return False, None
    """
    # Defensive: CONFIG must be present
    if CONFIG is None:
        logging.debug("[%s] Sell skipped: missing CONFIG", sym)
        return False, None
        Exit evaluation used by both SIM and LIVE loops.
        Returns (True, reason) or (False, None).
    """
    try:
        # --- Unpack entry_times ---
        entry_record = entry_times.get(sym)
        if entry_record:
            entry_time = entry_record   # aina datetime
            if not isinstance(entry_time, datetime):
                logging.error("[%s] entry_time is not datetime: %s", sym, type(entry_time))
                return False, None
                
            now_ts = current_time or datetime.now(timezone.utc)
            elapsed = (now_ts - entry_time).total_seconds()
            logging.debug("[%s] DEBUG PATCH | entry_time=%s | elapsed=%.2f seconds",
                          sym, entry_time, elapsed)
        

                                            
        else:
            entry_time = None
            elapsed = 0
        # --- End unpack ---

        # --- Hold time check for soft exits ---
        soft_exits_allowed = (elapsed >= MIN_HOLD_SECONDS) if entry_time else False

        prices_series = pd.Series(price_deque)
        sizes_series = pd.Series(size_deque)

        # --- Optional logging block ---
        if log_stack:
            logging.debug("[%s] SELL evaluation | regime=%s | elapsed=%.2f", sym, regime, elapsed)

        # === PATCH: RANGE time-stop exit ===
        if RANGE_TIME_STOP_ENABLED and regime == "RANGE":
            if entry_time is not None and elapsed >= RANGE_TIME_STOP_SECONDS:
                vwap_series = compute_vwap_from_ticks(prices_series, sizes_series)
                vwap_val = vwap_series.iloc[-1] if len(vwap_series) else float('nan')
                if not pd.isna(vwap_val):
                    entry_dist = abs(ref_entry - vwap_val)
                    current_dist = abs(last_price - vwap_val)
                    progress = 1.0 - (current_dist / entry_dist) if entry_dist > 0 else 0
                    if progress < RANGE_VWAP_PROGRESS_MIN:
                        # --- Logging + Audit Trail ---
                        logging.info("[%s] RANGE time-stop exit triggered | elapsed=%ds | progress=%.2f",
                                     sym, int(elapsed), progress)
                        exit_reason_count[regime]["Range time-stop"] += 1
                        regime_trades[regime] += 1
                        regime_pnl[regime] += (last_price - ref_entry) * entry_qty.get(sym, 0)
                        # --- CSV Audit Trail ---
                        try:
                            _audit_write_row({
                                "timestamp": now_ts.strftime("%Y-%m-%d %H:%M:%S"),
                                "symbol": sym,
                                "reason": "Range time-stop",
                                "price": round(last_price, 6),
                                "ema_fast": (round(float(ema_fast), 6) if pd.notna(ema_fast) else None),
                                "ema_slow": (round(float(ema_slow), 6) if pd.notna(ema_slow) else None),
                                "rsi": (round(float(rsi_val), 4) if pd.notna(rsi_val) else None),
                                "vwap": (round(float(vwap_val), 6) if pd.notna(vwap_val) else None),
                                "size": int(size) if size is not None else None,
                                "median_vol": (round(float(median_vol), 4) if median_vol is not None else None),
                                "bias": bias,
                                "config_profile": ("BULLISH" if CONFIG is BULLISH_CONFIG else "BEARISH"),
                                "tp_pct": CONFIG.get("TP_PCT", None),
                                "sl_multiplier": CONFIG.get("SL_MULTIPLIER", None),
                                "outcome": "neutral_block",  # or classify later
                                "window_min": AUDIT_OUTCOME_WINDOW_MIN
                            })
                        except Exception as e:
                            logging.warning("[AUDIT][%s] CSV write failed for Range time-stop: %s", sym, e)
                        return True, "Range time-stop" 

                # --- Hard exits with regime overlay ---
        # Detect regime from local series (SIM and LIVE identical)
        regime = detect_regime(prices_series, sizes_series)
        CONFIG_E = overlay_exit_params_by_regime(CONFIG, regime)

        # --- DRIFT exit debug logging ---
        if regime == "DRIFT":
            logging.debug(
                "[DRIFT_SELL][%s] regime=%s last=%.4f ref=%.4f elapsed=%.1fs",
                sym, regime, last_price, ref_entry, elapsed
            )

        # --- LOW_VOL time-stop exit ---
        LOW_VOL_TIME_STOP_ENABLED = True
        LOW_VOL_TIME_STOP_SECONDS = 120
        
        low_vol_time_stop_hit = False
        if LOW_VOL_TIME_STOP_ENABLED and (regime == "LOW_VOL"):
            if entry_time is not None:
                low_vol_time_stop_hit = elapsed >= LOW_VOL_TIME_STOP_SECONDS
        
        if low_vol_time_stop_hit:
            return True, "Low-vol time-stop"

        tp_price = ref_entry * (1 + CONFIG_E["TP_PCT"])
        tp_hit = last_price >= tp_price

        atr_value = compute_atr_from_series(prices_series, CONFIG_E.get("ATR_PERIOD", ATR_PERIOD))
        atr_safe = max(atr_value if not pd.isna(atr_value) else 0.0, ATR_FLOOR)
        dyn_sl_price = ref_entry - (atr_safe * CONFIG_E["SL_MULTIPLIER"])
        sl_hit = last_price <= dyn_sl_price

        # --- SL grace period ---
        allow_sl = (elapsed >= MIN_HOLD_SECONDS)

        # --- Emergency SL (intrabar disaster cut) ---
        # INSERT: define emergency SL variables exactly where they are used to keep scope local
        try:
           emergency_sl_pct = float(CONFIG.get("EMERGENCY_SL_PCT", 0.01))  # default 1%
        except Exception:
            emergency_sl_pct = 0.01
        emergency_sl_hit = (last_price <= ref_entry * (1 - emergency_sl_pct))
        # NOTE: This mirrors the earlier concept while keeping SIM/LIVE parity and avoids NameError

        # ✅ DEBUG LOG 2: TP/SL‑tarkistus
        logging.debug("[%s] TP check | ref_entry=%.4f | tp_price=%.4f | last=%.4f | TP_hit=%s",
              sym, ref_entry, tp_price, last_price, tp_hit)

        logging.debug("[%s] SL check | ref_entry=%.4f | atr=%.4f | sl_price=%.4f | last=%.4f | SL_hit=%s",
                      sym, ref_entry, atr_value, dyn_sl_price, last_price, sl_hit)

        # --- Indicators for soft exits ---
        ema_fast = compute_ema_from_series(prices_series, ema_fast_period).iloc[-1] if len(prices_series) >= 2 else float('nan')
        ema_slow = compute_ema_from_series(prices_series, ema_slow_period).iloc[-1] if len(prices_series) >= 2 else float('nan')
        rsi_series = compute_rsi_from_series(prices_series, rsi_period) if len(prices_series) >= 5 else pd.Series([float('nan')])
        rsi_val = rsi_series.iloc[-1] if len(rsi_series) else float('nan')
        vwap_series = compute_vwap_from_ticks(prices_series, sizes_series) if len(prices_series) >= 1 else pd.Series([float('nan')])
        vwap_val = vwap_series.iloc[-1] if len(vwap_series) else float('nan')

        # --- Trailing stop activation ---
        if last_price >= ref_entry * (1 + CONFIG["TS_ACTIVATION_BUFFER"]):
            trailing_active[sym] = True
            highest_price_since_entry[sym] = max(highest_price_since_entry.get(sym, ref_entry), last_price)

            # ✅ DEBUG LOG 3: trailing stop aktivointi
            logging.debug("[%s] TS activated | ref_entry=%.4f | last=%.4f | buffer=%.4f",
                          sym, ref_entry, last_price, CONFIG["TS_ACTIVATION_BUFFER"])        

        # --- Trailing stop check (only if activated) ---
        trailing_stop_hit = False
        if trailing_active.get(sym, False):
            try:
                peak = highest_price_since_entry[sym]
                drawdown_pct = (peak - last_price) / peak if peak > 0 else 0
                trailing_stop_hit = drawdown_pct >= CONFIG["TRAILING_STOP_PCT"]
                logging.warning(
                    "[DEBUG][%s] Trailing stop check | peak=%.4f | last=%.4f | ref_entry=%.4f | drawdown=%.4f%% | threshold=%.4f%% | Hit=%s",
                    sym, peak, last_price, ref_entry,
                    drawdown_pct * 100,
                    CONFIG["TRAILING_STOP_PCT"] * 100,
                    trailing_stop_hit
                )
            except Exception as e:
                logging.error("[ERROR][%s] Trailing stop evaluation failed: %s", sym, e)
                trailing_stop_hit = False

        else:
            # ✅ DIAGNOSTIIKKA: trailing stop ei vielä aktiivinen
            logging.debug("[%s] TS not active | ref_entry=%.4f | last=%.4f | buffer=%.4f",
                          sym, ref_entry, last_price, CONFIG["TS_ACTIVATION_BUFFER"])

        # --- VWAP fail ---
        vwap_fail = False
        if soft_exits_allowed and not pd.isna(vwap_val):
            vwap_fail = prices_series.iloc[-1] < vwap_val * (1 - CONFIG["VWAP_DELTA"])

        # --- EMA fail ---
        ema_fail = False
        ema_condition_1 = False
        ema_condition_2 = False
        if soft_exits_allowed and not pd.isna(ema_fast) and not pd.isna(ema_slow):
            ema_condition_1 = ema_fast < ema_slow
            ema_condition_2 = last_price < ema_slow * (1 - CONFIG["EMA_DELTA"])
            ema_fail = ema_condition_1 and ema_condition_2

        # --- RSI fail ---
        rsi_fail = False
        if soft_exits_allowed and not pd.isna(rsi_val):
            # Tarkista onko RSI rajan ulkopuolella
            if (rsi_val > CONFIG.get("MAX_RSI_FOR_ENTRY", MAX_RSI_FOR_ENTRY)) \
               or (rsi_val < CONFIG.get("MIN_RSI_FOR_ENTRY", MIN_RSI_FOR_ENTRY)):
                rsi_fail_counter[sym] = rsi_fail_counter.get(sym, 0) + 1
            else:
                rsi_fail_counter[sym] = 0
        
            # Vasta kun esim. 3 peräkkäistä tickiä on ulkopuolella
            if rsi_fail_counter[sym] >= CONFIG.get("RSI_FAIL_TICKS", 3):
                rsi_fail = True    
            
            
        # ✅ DEBUG LOG 4: VWAP, EMA, RSI fail‑tilat
        logging.debug("[%s] VWAP=%.4f | last=%.4f | vwap_fail=%s", sym, vwap_val, last_price, vwap_fail)
        logging.debug("[%s] EMA fast=%.4f | slow=%.4f | last=%.4f | ema_fail=%s | cond1=%s | cond2=%s",
                      sym, ema_fast, ema_slow, last_price, ema_fail, ema_condition_1, ema_condition_2)
        logging.debug("[%s] RSI=%.2f | rsi_fail=%s", sym, rsi_val, rsi_fail)

        # --- Decision priority ---
        if tp_hit:
            return True, "Take-profit"
        if (allow_sl and sl_hit) or emergency_sl_hit:
            # Trip kill-switch if regime is HIGH_VOL
            try:
                if regime == "HIGH_VOL":
                    high_vol_paused[sym] = True
                    logging.warning("[%s] HIGH_VOL paused due to catastrophic SL", sym)
            except Exception:
                pass
            return True, "Stop-loss" 
        if trailing_stop_hit:
            return True, "Trailing stop"
        if vwap_fail:
            return True, "VWAP fail"
        if ema_fail:
            return True, "EMA fail"
        if rsi_fail and (ema_fail or vwap_fail):
            return True, "RSI+EMA/VWAP fail"

        # --- Final fallback: Max hold ---
        max_hold_hit = elapsed >= MAX_HOLD_SECONDS if entry_time else False

         # ✅ DEBUG LOG 5: Max hold tarkistus
        logging.debug("[%s] elapsed=%.2f | MAX_HOLD_SECONDS=%d | max_hold_hit=%s",
                      sym, elapsed, MAX_HOLD_SECONDS, max_hold_hit)
        
        if max_hold_hit:
            return True, "Max hold"

        return False, None

    except Exception as e:
        logging.debug("[%s] Sell evaluation failed: %s", sym, str(e))
        return False, None


# === AUDIT TRAIL (LIVE) — removable block ===
# Käyttää suoraan price_deques ja size_deques rakenteita
# Outcome labels:
#   - good_block   : filtteri esti kaupan, joka olisi mennyt tappiolle
#   - bad_block    : filtteri esti kaupan, joka olisi mennyt voitolle
#   - neutral_block: ei TP/SL osumaa seurantajakson aikana

AUDIT_TRAIL_ENABLED = True
AUDIT_OUTCOME_WINDOW_MIN = 30     # seurantajakso minuutteina


audit_csv_lock = threading.Lock()

def _audit_write_row(row_dict):
    import csv
    try:
        with audit_csv_lock:
            file_exists = os.path.exists(AUDIT_CSV_FILE)
            with open(AUDIT_CSV_FILE, mode="a", newline="") as f:
                fieldnames = [
                    "timestamp","symbol","reason","price",
                    "ema_fast","ema_slow","rsi","vwap","size",
                    "median_vol","bias","config_profile",
                    "tp_pct","sl_multiplier","outcome","window_min",
                    "regime"
                ]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                if not file_exists:
                    writer.writeheader()
                writer.writerow(row_dict)
    except Exception as e:
        logging.warning("[AUDIT] CSV write failed: %s", e)

# === Regime performance snapshot ===
PERF_SNAPSHOT_ENABLED = True
PERF_SNAPSHOT_INTERVAL = 10
_perf_counter = 0

def regime_perf_snapshot():
    global _perf_counter
    if not PERF_SNAPSHOT_ENABLED:
        return
    _perf_counter += 1
    if _perf_counter % PERF_SNAPSHOT_INTERVAL != 0:
        return
    for reg, trades in regime_trades.items():
        if trades == 0:
            continue
        sls = exit_reason_count[reg].get("Stop-loss", 0)
        wr = max(0.0, min(1.0, (trades - sls) / trades))
        logging.info(
            f"[PERF] {reg} trades={trades} win_rate≈{wr:.2f} "
            f"pnl≈{regime_pnl[reg]:.2f} reasons={dict(exit_reason_count[reg])}"
        )


def _audit_watchdog_deque(sym, ts_val, ref_entry_price, reason,
                          ema_fast, ema_slow, rsi_val, vwap_val,
                          size, median_vol, bias, CONFIG,
                          price_deques, size_deques):
    """
    Watchdog: tarkistaa TP/SL osumat suoraan deque-rakenteista
    eikä tee erillistä hintapollia.
    """
    try:
        deadline = ts_val + timedelta(minutes=AUDIT_OUTCOME_WINDOW_MIN)
        tp_pct = float(CONFIG.get("TP_PCT", 0.002))
        sl_mult = float(CONFIG.get("SL_MULTIPLIER", 1.0))
        outcome = "neutral_block"

        while datetime.now(timezone.utc) < deadline:
            if len(price_deques[sym]) == 0:
                time.sleep(1.0)
                continue
            last_price = float(price_deques[sym][-1])
            tp_hit = last_price >= ref_entry_price * (1 + tp_pct)
            sl_floor_price = ref_entry_price - (ATR_FLOOR * sl_mult)
            sl_hit = last_price <= sl_floor_price

            if tp_hit:
                outcome = "bad_block"
                break
            if sl_hit:
                outcome = "good_block"
                break

            time.sleep(1.0)
       # <-- INSERT adaptive update here
        adaptive_entry_update(sym, "DRIFT" if "DRIFT" in str(reason) else
                                   "RANGE" if "Range" in str(reason) else
                                   "TREND" if "Trend" in str(reason) else
                                   "LOW_VOL" if "Low" in str(reason) else
                                   "HIGH_VOL", outcome) 
            

        _audit_write_row({
            "timestamp": ts_val.strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": sym,
            "reason": str(reason) if reason else "rejected",
            "price": round(ref_entry_price, 6),
            "ema_fast": (round(float(ema_fast), 6) if pd.notna(ema_fast) else None),
            "ema_slow": (round(float(ema_slow), 6) if pd.notna(ema_slow) else None),
            "rsi": (round(float(rsi_val), 4) if pd.notna(rsi_val) else None),
            "vwap": (round(float(vwap_val), 6) if pd.notna(vwap_val) else None),
            "size": int(size) if size is not None else None,
            "median_vol": (round(float(median_vol), 4) if median_vol is not None else None),
            "bias": bias,
            "config_profile": ("BULLISH" if CONFIG is BULLISH_CONFIG else "BEARISH"),
            "tp_pct": tp_pct,
            "sl_multiplier": sl_mult,
            "outcome": outcome,
            "window_min": AUDIT_OUTCOME_WINDOW_MIN,
            "regime": None
        })
        logging.debug("[AUDIT][%s] outcome=%s reason=%s ref=%.4f window=%dm",
                     sym, outcome, reason, ref_entry_price, AUDIT_OUTCOME_WINDOW_MIN)
    except Exception as e:
        logging.warning("[AUDIT][%s] watchdog failed: %s", sym, e)

def audit_rejection_live(sym, ts_val, price, size, ema_fast, ema_slow,
                         rsi_val, vwap_val, sizes_series, bias, CONFIG, reason,
                         price_deques, size_deques):
    """Spawn watchdog for a rejected BUY using shared deques."""
    if not AUDIT_TRAIL_ENABLED:
        return
    try:
        median_vol = sizes_series.median() if sizes_series is not None and len(sizes_series) > 0 else None
        t = threading.Thread(
            target=_audit_watchdog_deque,
            args=(sym, ts_val, float(price), reason, ema_fast, ema_slow,
                  rsi_val, vwap_val, size, median_vol, bias, CONFIG,
                  price_deques, size_deques),
            daemon=True
        )
        t.start()
        logging.debug("[AUDIT][%s] rejection captured; watchdog started", sym)
    except Exception as e:
        logging.warning("[AUDIT][%s] could not start watchdog: %s", sym, e)
# === END AUDIT TRAIL (LIVE using deques) ===





# === MAIN ===
# === Strategy parameters ===
RSI_PERIOD = 14
RSI_COOL_THRESHOLD = 3
MAX_HOLD_SECONDS = 999999   # example: x minutes
MIN_HOLD_SECONDS = 30    # example: x seconds grace period before indicators can trigger
TRAIL_PCT = 0.010
BUY_POWER_LIMIT = 0.05
BUY_CASH_BUFFER = 0.95
COOLDOWN_SECONDS = 80
MIN_RSI_FOR_ENTRY = 30
MAX_RSI_FOR_ENTRY = 85
MIN_TRADE_USD = 25
MARKET_DATA_CHUNK = 5
MAX_INFLIGHT_PER_SYMBOL = 1      
def main():
    load_dotenv()
    global stock_data_client, trade_client, entry_times, entry_prices, entry_qty, last_exit_time
    stock_data_client = StockHistoricalDataClient(
        os.getenv("ALPACA_PAPER_API_KEY"),
        os.getenv("ALPACA_PAPER_SECRET_KEY")
    )
    trade_client = TradingClient(
        os.getenv("ALPACA_PAPER_API_KEY"),
        os.getenv("ALPACA_PAPER_SECRET_KEY"),
        paper=True
    )
    symbols = ["AAPL", "MSFT", "MU", "QCOM", "NVDA", "V", "AMD", "GOOG", "C", "EBAY", "OKTA", "TSLA", "AMZN", "ADSK", "DELL"]
    price_deques = {s: deque(maxlen=TICKS_WINDOW) for s in symbols}
    size_deques = {s: deque(maxlen=TICKS_WINDOW) for s in symbols}
    time_deques = {s: deque(maxlen=TICKS_WINDOW) for s in symbols}
    entry_times = {}
    entry_prices = {}
    entry_qty = {}
    inflight_orders = {}
    entry_configs = {}

    from datetime import datetime, timezone
    last_exit_time = {s: None for s in symbols}
    last_buy_time = {s: None for s in symbols}

    order_lock = threading.Lock()
    stop_event = threading.Event()
    pending_entries = set()   # prevent duplicate buys

    # === SIMULATION BRANCH ===
    if RUN_MODE in ["SIM", "AGG_SIM"]:
        
        from alpaca.data.requests import StockTradesRequest
        from datetime import datetime, timezone
        
    
        symbol = "NVDA"
        symbols = [symbol]  # tarvitaan deque-rakenteisiin

        # === CALL PATCH RG in LIVE loop (once per outer iteration) ===
        _risk_governor_update()

                
        # Alusta tilarakenteet
        inflight_orders = {}
        pending_entries = set()
        positions_map = {}
        last_exit_time = {s: None for s in symbols}

        # NEW: per-symbol buy guards
        last_buy_time = defaultdict(lambda: None)
        in_position_map = defaultdict(bool)
        
        start = "2025-12-23T14:30:00Z"
        end = "2025-12-23T21:00:00Z"

        
        # Debug prints (optional; safe to keep or remove)
        print("TimeFrame attrs:", [a for a in dir(TimeFrame) if not a.startswith("_")])
        print("TimeFrameUnit attrs:", [a for a in dir(TimeFrameUnit) if not a.startswith("_")])
        
        def _build_1s_timeframe():
            """
            Robustly construct a 1-second TimeFrame across Alpaca SDK variants.
            Returns a valid TimeFrame object or raises RuntimeError with helpful debug hint.
            """
            # 1) Preferred: TimeFrame(1, TimeFrameUnit.SECOND) or TimeFrame(1, TimeFrameUnit.Second)
            for unit_name in ("SECOND", "Second", "second"):
                try:
                    unit = getattr(TimeFrameUnit, unit_name)
                    try:
                        return TimeFrame(1, unit)
                    except Exception:
                        # some SDKs accept TimeFrame(1, TimeFrameUnit.SECOND) but not this call;
                        # fall through to other attempts
                        pass
                except Exception:
                    pass
        
            # 2) Some SDKs expose TimeFrame.Second or TimeFrame("1Sec") / "1S" / "1sec"
            for candidate in ("Second", "SECOND", "1Sec", "1S", "1sec", "1s"):
                try:
                    # Try attribute on TimeFrame (e.g., TimeFrame.Second)
                    if hasattr(TimeFrame, candidate):
                        return getattr(TimeFrame, candidate)
                except Exception:
                    pass
                try:
                    # Try string constructor variants
                    return TimeFrame(candidate)
                except Exception:
                    pass
                try:
                    # Try classmethod from_string if present
                    if hasattr(TimeFrame, "from_string"):
                        return TimeFrame.from_string(candidate)
                except Exception:
                    pass
        
            # 3) Last resort: try numeric constructor without unit (some SDKs accept "1S" as int)
            try:
                return TimeFrame("1S")
            except Exception:
                pass
        
            # Nothing worked — raise with debug hint
            raise RuntimeError(
                "Could not construct a 1-second TimeFrame with your Alpaca SDK. "
                "Paste the two debug prints above (TimeFrame attrs and TimeFrameUnit attrs) and I'll give a one-line fix."
            )
        
        # Build timeframe (SIM path uses this; LIVE code unchanged)
        tf = TimeFrame(1, TimeFrameUnit.Minute)

        # Prefer datetime objects for start/end to avoid SDK differences 
        try: 
            start_dt = parser.isoparse(start) if isinstance(start, str) else start 
            end_dt = parser.isoparse(end) if isinstance(end, str) else end 
        except Exception: 
            start_dt, end_dt = start, end # fall back to original values if parsing fails        
        
        logging.debug("Using timeframe=%s start=%s end=%s", tf, start_dt, end_dt)
        bars_req = StockBarsRequest(symbol_or_symbols=symbol, start=start_dt, end=end_dt, timeframe=tf)
        try:
            bars = stock_data_client.get_stock_bars(bars_req).df
        except Exception as e:
            logging.warning("get_stock_bars failed for %s: %s", symbol, e)
            bars = pd.DataFrame()

        # Defensive check: ensure we have data
        if bars is None or bars.empty:
            logging.warning("No bars returned for %s from %s to %s (timeframe=%s)", symbol, start_dt, end_dt, tf)
            # create an empty trades DataFrame with expected columns to avoid downstream crashes
            trades = pd.DataFrame(columns=["price", "size"])
            trades.index = pd.to_datetime(pd.Series(dtype="datetime64[ns]"))
        else:
            # Normalize bars to a simple per-second DataFrame with price (close) and size (volume)
            bars = bars.reset_index().set_index("timestamp")
        
            trades = pd.DataFrame({
                "price": bars["close"].astype(float),
                "size": bars["volume"].fillna(0).astype(float)
        })
        trades.index = pd.to_datetime(trades.index)
           

              
       
        # === NEW: Aggregate ticks into 1-second bars ===
        trades["bucket"] = trades.index.floor("1s")
        trades = trades.groupby("bucket").agg({
            "price": "mean",   # average price in that second
            "size": "sum"      # total volume in that second
        })

        # AGG_SIM ja SIM: nyt käytetään 1s bars
        trades = trades.dropna()
        logging.info("%s mode: using 1-second bars. Total datapoints: %d", RUN_MODE, len(trades))
        logging.info("Starting %s replay for %s from %s to %s", RUN_MODE, symbol, start, end)

        # --- Now loop over trades bar by bar ---
        for ts, row in trades.iterrows():
            try:
                ts_val = pd.to_datetime(ts, utc=True)
                price  = float(row["price"])
                size   = float(row["size"])
            except Exception as e:
                logging.error("[%s] Could not parse row: %s", RUN_MODE, e)
                continue
    
        # AGG_SIM ja SIM: molemmat käyttävät raw tick dataa
        if RUN_MODE in ["AGG_SIM", "SIM"]:
            trades = trades.dropna()
            logging.info("%s mode: using raw tick data. Total datapoints: %d",
                 RUN_MODE, len(trades))
        
    
        print(trades.head())
        logging.info("Starting %s replay for %s from %s to %s", RUN_MODE, symbol, start, end)

        max_loop_budget = 100000.0  # esim. kiinteä budjetti USD
        
        in_position = False
        entry_price = None
        entry_times = {}
        entry_prices = {}
        entry_qty = {}
        entry_configs = {} 
        last_exit_time[symbol] = None
        highest_price_since_entry = defaultdict(float)
        import csv
        csv_filename = f"{symbol}_{RUN_MODE}_trades.csv"
        csv_rows = []
        audit_rows = []
        positions_map = {}
        sell_decisions = 0
        

        last_bias = None
            
        for idx, (ts, row) in enumerate(trades.iterrows()):
            try:
                ts_val = pd.to_datetime(ts, utc=True)
                price = float(row["price"])
                size = float(row["size"])
            except Exception as e:
                logging.error("[%s] Could not parse row: %s", RUN_MODE, e)
                continue

            # --- build series for indicators ---
            prices_series = pd.Series(price_deques[symbol])
            sizes_series = pd.Series(size_deques[symbol])
        
            # --- detect regime before using it ---
            regime = detect_regime(prices_series, sizes_series)

            # --- set CONFIG based on bias before using it ---
            day_bias = detect_day_bias(
                prices_series,
                compute_ema_from_series(prices_series, EMA_FAST),
                compute_ema_from_series(prices_series, EMA_SLOW),
                compute_vwap_from_ticks(prices_series, sizes_series)
            )
            if day_bias == "bullish":
                CONFIG = BULLISH_CONFIG
            else:
                CONFIG = BEARISH_CONFIG

            # --- evaluate entry to get score and reason ---
            # This is the missing part. It sets score and reason so they exist.
            accept, reason, score, signal_stack = evaluate_entry(
                symbol,
                price,
                size,
                prices_series,
                sizes_series,
                ts_val,
                positions_map,
                inflight_orders,
                pending_entries,
                last_exit_time[symbol],
                last_buy_time,
                CONFIG,
                regime,
                log_stack=True
            )
            csv_rows.append({
                "timestamp": ts_val.strftime("%Y-%m-%d %H:%M:%S"),
                "price": price,
                "size": size,
                "regime": regime,   # once you’ve called detect_regime
                "score": score,     # from evaluate_entry
                "reason": reason    # from evaluate_entry or evaluate_sell
            })


            # --- NEW: Progress log every 2000 bars ---
            if idx % 2000 == 0:
                logging.info("[PROGRESS] %s replay at %s (%d/%d bars processed)",
                             symbol,
                             ts_val.strftime("%H:%M"),
                             idx,
                             len(trades))

    
            # --- NEW: Tick-aggregointi 1s ---
            bucket_ts = ts_val.replace(microsecond=0)  # pyöristetään sekuntitasolle
            if len(time_deques[symbol]) > 0 and time_deques[symbol][-1] == bucket_ts:
                # Päivitä viimeinen aggregaatti
                price_deques[symbol][-1] = (price_deques[symbol][-1] + price) / 2.0
                size_deques[symbol][-1] += size
            else:
                # Lisää uusi aggregaatti
                price_deques[symbol].append(price)
                size_deques[symbol].append(size)
                time_deques[symbol].append(bucket_ts)
    
            # Laske indikaattorit
            prices = pd.Series(price_deques[symbol])
            sizes_series = pd.Series(size_deques[symbol])
            ema_fast = compute_ema_from_series(prices, EMA_FAST).iloc[-1]
            ema_slow = compute_ema_from_series(prices, EMA_SLOW).iloc[-1]
            rsi_val = compute_rsi_from_series(prices, RSI_PERIOD).iloc[-1]
            vwap_val = compute_vwap_from_ticks(prices, sizes_series).iloc[-1]
    
            if pd.isna(ema_fast) or pd.isna(ema_slow) or pd.isna(rsi_val) or pd.isna(vwap_val):
                continue

            # === Bias detection ===
            day_bias = detect_day_bias(prices,
                                       compute_ema_from_series(prices, EMA_FAST),
                                       compute_ema_from_series(prices, EMA_SLOW),
                                       compute_vwap_from_ticks(prices, sizes_series))

           
            
            if day_bias == "bullish":
                CONFIG = BULLISH_CONFIG
            else:
                CONFIG = BEARISH_CONFIG

                                      
            positions_map = {} if RUN_MODE in ["SIM", "AGG_SIM"] else get_positions_map(trade_client)

            reconcile_positions(
                trade_client_local=trade_client,
                symbols=symbols,
                positions_map=positions_map,
                price_deques=price_deques,
                size_deques=size_deques,
                entry_times=entry_times,
                entry_prices=entry_prices,
                entry_qty=entry_qty,
                entry_configs=entry_configs
            )
            # --- Always initialize regime to a safe default ---
            regime = None
            if USE_REGIME_ENTRY:
                regime_raw = detect_regime(prices, sizes_series)
                regime = _smooth_regime(symbol, regime_raw)
                CONFIG_SESSION = overlay_by_session(CONFIG, ts_val, regime)
                accept, reason, score, stack = evaluate_entry(
                    symbol,
                    price,                           # price
                    size,                            # single tick size
                    prices,                          # pandas Series of prices
                    sizes_series,                    # pandas Series of sizes
                    ts_val,                          # timestamp value
                    positions_map,
                    inflight_orders,
                    pending_entries,
                    last_exit_time[symbol],          # last_exit
                    last_buy_time,                   # last_buy_time dict
                    CONFIG_SESSION,                  # config profile
                    regime,                          # regime classification
                    bias=day_bias,
                    log_stack=True                   # optional keyword        
                )
                buy = accept
            else:
                # Fallback regime if entry detection is disabled
                regime = "UNKNOWN"
            
            # --- SELL evaluation (regime guaranteed to exist) ---
            has_entry = (symbol in entry_times) and (symbol in entry_prices) and (symbol in entry_configs)
            if has_entry:
                accept_exit, reason_exit = evaluate_sell(
                    symbol,
                    price,                           # last_price
                    entry_prices.get(symbol),        # ref_entry
                    price_deques[symbol],            # price_deque
                    size_deques[symbol],             # size_deque
                    entry_times,                     # full entry_times dict
                    entry_configs[symbol],           # CONFIG for this entry
                    current_time=ts_val,             # keyword
                    regime=regime,                   # keyword
                    log_stack=True                   # keyword
                )    
            else:
                accept_exit, reason_exit = (False, None)

            # Diagnostic: log evaluate_sell result (SIM only)
            logging.debug("[SIM] evaluate_sell (early) -> accept_exit=%s reason=%s ts=%s",
                          accept_exit, reason_exit, ts_val.strftime("%H:%M:%S"))
            if accept_exit:
                sell_decisions += 1
                qty = entry_qty.get(symbol, 0)
                pnl = (price - entry_prices.get(symbol, price)) * qty
            
                logging.info(
                    f"[TRADE] {symbol} [{RUN_MODE}] SELL qty={qty} @ {price:.4f} | "
                    f"Time={ts_val.strftime('%H:%M:%S')} | Reason={reason_exit} | "
                    f"Bias={day_bias} | Config={CONFIG} | PnL={pnl:.4f}"
                )
            
                csv_rows.append({
                    "timestamp": ts_val.strftime("%Y-%m-%d %H:%M:%S"),
                    "symbol": symbol,
                    "action": "SELL",
                    "price": price,
                    "reason": reason_exit,
                    "pnl": round(pnl, 4),
                    "ema_fast": round(ema_fast, 4),
                    "ema_slow": round(ema_slow, 4),
                    "rsi": round(rsi_val, 2),
                    "vwap": round(vwap_val, 4)
                })

                # --- ALSO write to exec_rows (exec log for NVDA_AGG_SIM_exec.csv) ---
                exec_rows.append({
                    "timestamp": ts_val.strftime("%Y-%m-%d %H:%M:%S"),
                    "symbol": symbol,
                    "action": "SELL",
                    "price": price,
                    "reason": reason_exit,
                    "pnl": round(pnl, 4),
                    "ema_fast": round(ema_fast, 4),
                    "ema_slow": round(ema_slow, 4),
                    "rsi": round(rsi_val, 2),
                    "vwap": round(vwap_val, 4),
                    "regime": regime # or regime_at_sell if you prefer recomputing
                })
                # Immediate persistence for SIM
                write_exec_row_immediate(exec_rows[-1], symbol, RUN_MODE)
                
                # State cleanup
                in_position = False
                in_position_map[symbol] = False
                last_buy_time[symbol] = None

                entry_price = None
                last_exit_time[symbol] = ts_val
                highest_price_since_entry.pop(symbol, None)
                entry_configs.pop(symbol, None)

                # Important: skip entry logic on the same bar after a SELL
                continue

            # === SIM cooldown guard ===
            if last_buy_time[symbol] is not None and \
               (ts_val - last_buy_time[symbol]).total_seconds() < COOLDOWN_SECONDS:
                continue
            else:
                # Regime-aware entry (SIM): strict parity with LIVE
                regime_raw = detect_regime(prices, sizes_series)
                regime = _smooth_regime(symbol, regime_raw)
                CONFIG_SESSION = overlay_by_session(CONFIG, ts_val, regime)
                accept, reason, score, stack = evaluate_entry(
                    symbol, price, size, prices, sizes_series, ts_val,
                    positions_map, inflight_orders, pending_entries,
                    last_exit_time[symbol], last_buy_time,
                    CONFIG_SESSION, regime, log_stack=False
                )
                buy = accept

                if buy:
                    # Already in position? Skip duplicate BUY.
                    if in_position_map[symbol]:
                        continue
                    
                    # Bought very recently? Skip duplicate BUY.
                    last_ts = last_buy_time.get(symbol)
                    if last_ts is not None and (ts_val - last_ts).total_seconds() < 1:
                        continue

                    # --- Proceed with real entry ---
                    est_price = price
                    qty = int((max_loop_budget * BUY_CASH_BUFFER) // est_price)
                    if regime == "HIGH_VOL":
                        qty = max(1, int(qty * 0.5))
                    if qty <= 0 or qty * est_price < MIN_TRADE_USD:
                        continue
                    entry_price = price
                    entry_times[symbol] = ts_val
                    entry_prices[symbol] = price
                    entry_qty[symbol] = qty

                    # Mark symbol as in position
                    in_position_map[symbol] = True
                    last_buy_time[symbol] = ts_val
                    logging.info("[TRADE] %s [%s] BUY @ %.4f | %s",
                                 symbol, RUN_MODE, price, ts_val.strftime("%H:%M:%S"))
                    
                    exec_rows.append({
                        "timestamp": ts_val.strftime("%Y-%m-%d %H:%M:%S"),
                        "symbol": symbol,
                        "action": "BUY",
                        "price": price,
                        "reason": reason,
                        "pnl": None,
                        "ema_fast": round(ema_fast, 4),
                        "ema_slow": round(ema_slow, 4),
                        "rsi": round(rsi_val, 2),
                        "vwap": round(vwap_val, 4)
                    })
                    write_exec_row_immediate(exec_rows[-1], symbol, RUN_MODE)
                 
                                
                               
                highest_price_since_entry[symbol] = max(highest_price_since_entry[symbol], price)
                sell = False
                reason = "no-eval"
                has_entry = (symbol in entry_times) and (symbol in entry_prices) and (symbol in entry_configs)
                if not has_entry:
                    continue
                    
                sell, reason = evaluate_sell(
                    symbol, price, entry_prices[symbol],
                    price_deques[symbol], size_deques[symbol], entry_times, entry_configs[symbol], current_time=ts_val
                )

                # Diagnostic: log evaluate_sell result (SIM only, late branch)
                logging.debug("[SIM] evaluate_sell (late) -> sell=%s reason=%s ts=%s",
                              sell, reason, ts_val.strftime("%H:%M:%S"))
                
                if sell:
                    sell_decisions += 1
                    qty = entry_qty.get(symbol, 1)
                    pnl = (price - entry_price) * qty
                    # --- Audit trail update (SIM) ---
                    regime_at_sell = detect_regime(pd.Series(price_deques[symbol]), pd.Series(size_deques[symbol]))
                    regime_pnl[regime_at_sell] += float(pnl)
                    regime_trades[regime_at_sell] += 1
                    exit_reason_count[regime_at_sell][reason] += 1

                    # --- Regime performance snapshot (logs every N sells) ---
                    regime_perf_snapshot()
                                        
                                                           
                    logging.info(
                        f"[TRADE] {symbol} [{RUN_MODE}] SELL qty={qty} @ {price:.4f} "
                        f"| Time={ts_val.strftime('%H:%M:%S')} | Reason={reason} | Bias={day_bias} "
                        f"| Config={CONFIG} | PnL={pnl:.4f} | Regime={regime_at_sell}"
                    ) 
                    exec_rows.append({
                        "timestamp": ts_val.strftime("%Y-%m-%d %H:%M:%S"),
                        "symbol": symbol,
                        "action": "SELL",
                        "price": price,
                        "reason": reason,
                        "pnl": round(pnl, 4),
                        "ema_fast": round(ema_fast, 4),
                        "ema_slow": round(ema_slow, 4),
                        "rsi": round(rsi_val, 2), 
                        "vwap": round(vwap_val, 4),
                        "regime": regime_at_sell    
                    })
                    write_exec_row_immediate(exec_rows[-1], symbol, RUN_MODE)
                    logging.info(f"{symbol} [{RUN_MODE}] SELL @ {price:.4f} | Reason={reason} | PnL={pnl:.4f} | Time={ts_val.strftime('%Y-%m-%dT%H:%M:%S')}")
                    
                    in_position = False
                    in_position_map[symbol] = False
                    last_buy_time[symbol] = None
                    
                    entry_price = None
                    last_exit_time[symbol] = ts_val
                    highest_price_since_entry.pop(symbol, None)
                    entry_configs.pop(symbol, None)
                    trailing_active[symbol] = False
                    rsi_fail_counter[symbol] = 0
    
                            
            # Write audit/entry evaluation log
            logging.info("%s replay finished for %s", RUN_MODE, symbol)
            # Write trade execution log
            exec_filename = f"{symbol}_{RUN_MODE}_exec.csv"
            exec_fields = ["timestamp", "symbol", "action", "price", "reason", "pnl", "ema_fast", "ema_slow", "rsi", "vwap", "regime"]
            try:
                with open(exec_filename, "w", newline="") as f:
                    import csv
                    writer = csv.DictWriter(f, fieldnames=exec_fields)
                    writer.writeheader()
                    writer.writerows(exec_rows)
                                                                    
                # after writer.writerows(exec_rows)
                logging.info("[SIM DIAG] wrote exec file %s rows=%d", exec_filename, len(exec_rows))
                logging.info("[SIM DIAG] sell_decisions=%d exec_rows_len=%d", sell_decisions, len(exec_rows))            
                                                
                # Optional: log first few exec_rows for quick inspection
                for i, r in enumerate(exec_rows[:8]):
                    logging.info("[SIM DIAG] exec_rows[%d]=%s", i, r)
                    
                logging.info("Trades saved to %s", exec_filename)
            except Exception as e:
                logging.exception("[SIM DIAG] Failed to write exec file %s: %s", exec_filename, e)        
            audit_filename = f"{symbol}_{RUN_MODE}_audit.csv"
            audit_fields = ["timestamp", "price", "size", "regime", "score", "reason"]
            
            try:
                with open(audit_filename, "w", newline="") as f:
                    import csv
                    writer = csv.DictWriter(f, fieldnames=audit_fields)
                    writer.writeheader()
                    writer.writerows(audit_rows)
                logging.info(f"[SIM] Audit written to {audit_filename} ({len(audit_rows)} rows)")
            except Exception as e:
                logging.warning(f"[SIM] Could not write {audit_filename}: {e}")
     

        return
    # === END SIMULATION BRANCH ===
   


  
    def input_listener():
        try:
            while not stop_event.is_set():
                try:
                    u = input().strip().lower()
                except EOFError:
                    time.sleep(0.2)
                    continue
                if u == "exit":
                    logging.info("Received exit command — selling all positions.")
                    sell_all_positions(trade_client, order_lock)
                    stop_event.set()
                    break
        except Exception as e:
            logging.exception("input listener error: %s", e)
            stop_event.set()

    def sell_all_positions(trade_client_local, order_lock_local):
        try:
            positions = trade_client_local.get_all_positions()
            for p in positions:
                s = p.symbol
                q = int(float(p.qty))
                if q > 0:
                    try:
                        order = MarketOrderRequest(
                            symbol=s, qty=q, side=OrderSide.SELL,
                            type=OrderType.MARKET, time_in_force=TimeInForce.DAY
                        )
                        trade_client_local.submit_order(order)
                        logging.info("%s - Forced SELL qty=%d", s, q)
                    except Exception as e:
                        logging.exception("Forced sell error for %s: %s", s, e)
        except Exception as e:
            logging.exception("sell_all_positions error: %s", e)

    # Start input listener thread
    threading.Thread(target=input_listener, daemon=True).start()

    logging.info("Starting main loop with symbols: %s", symbols)

    last_bias = None
    
    while not stop_event.is_set():
        try:
            _risk_governor_update()  # apply drawdown TP cuts & pos sizing
            positions_map = get_positions_map(trade_client)
            spent_this_loop = 0.0
            max_loop_budget = calculate_buying_power_limit(trade_client, BUY_POWER_LIMIT)

            # --- EOD LIQUIDATION: force close all positions near session end ---
            now_ts = datetime.now(timezone.utc)
            minutes = _session_minutes(now_ts)
            SESSION_LENGTH_MIN = 390 # 6.5h US cash session

            if minutes >= SESSION_LENGTH_MIN - 30:
                logging.info("Session in final 30 minutes (minutes=%d); forcing liquidation of all positions.", minutes)
                sell_all_positions(trade_client, order_lock)

            for i in range(0, len(symbols), MARKET_DATA_CHUNK):
                chunk = symbols[i:i+MARKET_DATA_CHUNK]
                trades = fetch_latest_trade_price_and_size_batch(stock_data_client, chunk)

                for sym in chunk:
                    price, size = trades.get(sym, (None, None))
                    if price is None or size is None or price <= 0:
                        continue

                    # --- NEW: Tick-aggregointi 1s ---
                    bucket_ts = datetime.now(timezone.utc).replace(microsecond=0)
                    if len(time_deques[sym]) > 0 and time_deques[sym][-1] == bucket_ts:
                        price_deques[sym][-1] = (price_deques[sym][-1] + price) / 2.0
                        size_deques[sym][-1] += size
                    else:
                        price_deques[sym].append(price)
                        size_deques[sym].append(size)
                        time_deques[sym].append(bucket_ts)

                    prices = pd.Series(price_deques[sym])
                    sizes = pd.Series(size_deques[sym])
                    ema_fast = compute_ema_from_series(prices, EMA_FAST).iloc[-1]
                    ema_slow = compute_ema_from_series(prices, EMA_SLOW).iloc[-1]
                    rsi_val = compute_rsi_from_series(prices, RSI_PERIOD).iloc[-1]
                    vwap_val = compute_vwap_from_ticks(prices, sizes).iloc[-1]
                    
                    if pd.isna(ema_fast) or pd.isna(ema_slow) or pd.isna(rsi_val) or pd.isna(vwap_val):
                        continue

                    # Build helper series like in SIM
                    sizes_series = pd.Series(size_deques[sym])
                    
                    # Current timestamp for cooldown logic
                    ts_val = datetime.now(timezone.utc)
                    
                    # === Bias detection ===
                    day_bias = detect_day_bias(
                        prices,
                        compute_ema_from_series(prices, EMA_FAST),
                        compute_ema_from_series(prices, EMA_SLOW),
                        compute_vwap_from_ticks(prices, sizes)
                    )

                                        
                    if day_bias == "bullish":
                        CONFIG = BULLISH_CONFIG
                    else:
                        CONFIG = BEARISH_CONFIG
                    
                    qty_open, avg_entry = positions_map.get(sym, (0, 0.0))
                    last_exit = last_exit_time.get(sym, datetime.min.replace(tzinfo=timezone.utc))
                    
                    # clear pending once position is visible
                    if qty_open > 0 and sym in pending_entries:
                        pending_entries.discard(sym)

                    # === BUY LOGIC ===
                    since_last_buy = (
                        (datetime.now(timezone.utc) - last_buy_time[sym]).total_seconds()
                        if last_buy_time[sym] is not None else float("inf")
                    )
                    since_last_exit = (
                        (datetime.now(timezone.utc) - last_exit_time[sym]).total_seconds()
                        if last_exit_time[sym] is not None else float("inf")
                    )
                    logging.debug(f"{sym} cooldown check: buy={since_last_buy:.2f}s exit={since_last_exit:.2f}s")
                    if since_last_buy < COOLDOWN_SECONDS or since_last_exit < COOLDOWN_SECONDS:
                        logging.debug(f"{sym} - Cooldown active: buy={since_last_buy:.1f}s exit={since_last_exit:.1f}s")
                        continue

                    # --- Always initialize outputs to safe defaults ---
                    reason = None
                    score = None
                    buy = False

                    if USE_REGIME_ENTRY:
                        regime_raw = detect_regime(prices, sizes_series)
                        regime = _smooth_regime(sym, regime_raw)
                        CONFIG_SESSION = overlay_by_session(CONFIG, ts_val, regime)
                        accept, reason, score, stack = evaluate_entry(
                            sym, price, size, prices, sizes_series, ts_val,
                            positions_map, inflight_orders, pending_entries,
                            last_exit_time[sym], last_buy_time,
                            CONFIG_SESSION, regime, log_stack=False
                        )
                        buy = accept
                    else:
                        regime = "UNKNOWN"
                        # reason and score stay None
                   
                    
                    # AUDIT: jos BUY hylättiin, käynnistä watchdog dequen datalla
                    if AUDIT_TRAIL_ENABLED and not buy:
                        audit_rejection_live(
                            sym, ts_val, price, size, ema_fast, ema_slow,
                            rsi_val, vwap_val, sizes_series, day_bias, CONFIG, reason,
                            price_deques, size_deques
                        ) 

                    
                    if buy:    
                        if last_trade_attempt[sym] is not None and \
                           (datetime.now(timezone.utc) - last_trade_attempt[sym]).total_seconds() < 1.0:
                            continue
                        last_trade_attempt[sym] = datetime.now(timezone.utc)

                        est_trade_cost = price * int((max_loop_budget * BUY_CASH_BUFFER) // price)
                        if spent_this_loop + est_trade_cost > max_loop_budget:
                            logging.info(f"{sym} - Skipping buy: budget exceeded. est_cost={est_trade_cost:.2f} spent={spent_this_loop:.2f}")
                            continue

                        pending_entries.add(sym)
                        try:
                            # --- Regime-specific cash adjustment ---
                            cash_for_buy = max_loop_budget * BUY_CASH_BUFFER
                            if regime == "HIGH_VOL":
                                cash_for_buy *= 0.5   # halve position size in high volatility
                    
                            spent_this_loop += est_trade_cost  # reserve budget immediately
                            submitted = safe_market_buy(trade_client, sym, cash_for_buy, order_lock, price_deques, size_deques, bias=day_bias)
                            logging.debug(f"[TRACE] Buy submitted: {submitted}")
                            
                            if submitted:
                                inflight_orders[sym] = getattr(submitted, "id", None) or True
                                # 🔍 Retry loop for post-buy verification
                                actual_qty = 0
                                max_verify_attempts = 6
                                for attempt in range(max_verify_attempts):
                                    actual_qty = get_position_qty(trade_client, sym)
                                    logging.debug(f"[TRACE] Post-buy verification attempt {attempt+1}/{max_verify_attempts} for {sym}: actual_qty={actual_qty}")
                                    if actual_qty > 0:
                                        break
                                    time.sleep(1.0)
                              
                                if actual_qty > 0:
                                    entry_qty[sym] = actual_qty
                                    entry_prices[sym] = price
                                    entry_times[sym] = datetime.now(timezone.utc)
                                    last_buy_time[sym] = datetime.now(timezone.utc)
                                    entry_configs[sym] = CONFIG_SESSION
                                    pending_entries.discard(sym)
                                    logging.info(f"{sym} - ENTRY recorded qty={entry_qty[sym]} price={price:.2f} rsi={rsi_val:.2f} Bias={day_bias} Config={CONFIG_SESSION}")
                                    
                                else:
                                    pending_entries.discard(sym)
                                    logging.warning(f"[BUY-VERIFY] No position found for {sym} after {max_verify_attempts}s; entry not recorded.")
      
                        except Exception as e:
                            logging.exception("%s - BUY error: %s", sym, str(e))
                        finally:
                            inflight_orders.pop(sym, None)
                    


                    # === SELL LOGIC (evaluate_sell) ===
                    if qty_open > 0:
                        try:
                            last_price = float(price)
                            sell = False
                            reason = "no-eval"
                            ref_entry = entry_prices.get(sym, avg_entry)
                            config = entry_configs.get(sym)
                            if ref_entry is None or config is None:
                                logging.debug("[%s] Sell skipped: missing entry context", sym)
                                continue
                            
                            sell, reason = evaluate_sell(
                                sym,
                                last_price,
                                ref_entry,
                                price_deques[sym],
                                size_deques[sym],
                                entry_times,
                                entry_configs[sym],
                                current_time=datetime.now(timezone.utc)
                            )
                            
                            if sell:
                                submitted = safe_market_sell(trade_client, sym, qty_open, order_lock, price_deques, size_deques)
                                if submitted:
                                    entry_times.pop(sym, None)
                                    entry_prices.pop(sym, None)
                                    entry_qty.pop(sym, None)
                                    entry_configs.pop(sym, None)
                                    last_exit_time[sym] = datetime.now(timezone.utc)
                                    trailing_active[sym] = False
                                    logging.debug("%s - EXIT state cleanup completed", symbol)
                                logging.info(f"[TRADE] {sym} - SCALP SELL qty={qty_open} @ {last_price:.4f} | Reason={reason} | Bias={day_bias} | Config={CONFIG}")
                                logging.info(
                                    "%s - SCALP SELL qty=%d @ %.4f | Reason=%s | EntryRef=%.4f",
                                    sym, qty_open, last_price, reason, ref_entry
                                )
                                    # --- Audit trail update (LIVE) ---
                                regime_at_sell = detect_regime(pd.Series(price_deques[sym]), pd.Series(size_deques[sym]))
                                pnl_est = (last_price - ref_entry) * qty_open  # rough PnL estimate
                                regime_pnl[regime_at_sell] += float(pnl_est)
                                regime_trades[regime_at_sell] += 1
                                exit_reason_count[regime_at_sell][reason] += 1

                                # --- Regime performance snapshot (logs every N sells) ---
                                regime_perf_snapshot()

                                in_position = False
                                entry_price = None
                                trailing_active[sym] = False
                                last_exit_time[sym] = datetime.now(timezone.utc)
                                entry_configs.pop(sym, None)
                    
                        except Exception as e:
                            logging.error("[ERROR][%s] Sell logic failed: %s", sym, e)



                    
        except Exception as e:
                logging.exception("Main loop error: %s", e)
                time.sleep(1.0)
if __name__ == "__main__":
    main()
