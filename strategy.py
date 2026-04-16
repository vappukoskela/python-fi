
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
CODE_VERSION = "PATCH20_2026-04-17"

# === NYSE HOLIDAY CALENDAR ===
# Used by trading day stale detection to correctly handle market holidays
from datetime import date as _date
NYSE_HOLIDAYS = {
    # 2025
    _date(2025, 1, 1), _date(2025, 1, 20), _date(2025, 2, 17), _date(2025, 4, 18),
    _date(2025, 5, 26), _date(2025, 6, 19), _date(2025, 7, 4),  _date(2025, 9, 1),
    _date(2025, 11, 27), _date(2025, 12, 25),
    # 2026
    _date(2026, 1, 1),  _date(2026, 1, 19), _date(2026, 2, 16), _date(2026, 4, 3),
    _date(2026, 5, 25), _date(2026, 6, 19), _date(2026, 7, 3),  _date(2026, 9, 7),
    _date(2026, 11, 26), _date(2026, 12, 25),
    # 2027
    _date(2027, 1, 1),  _date(2027, 1, 18), _date(2027, 2, 15), _date(2027, 3, 26),
    _date(2027, 5, 31), _date(2027, 6, 18), _date(2027, 7, 5),  _date(2027, 9, 6),
    _date(2027, 11, 25), _date(2027, 12, 24),
}

def _is_nyse_trading_day(d):
    """Returns True if d is a NYSE trading day (weekday and not a holiday)."""
    return d.weekday() < 5 and d not in NYSE_HOLIDAYS

def _count_trading_days_elapsed(file_date, today_date):
    """
    Counts trading sessions that elapsed after file_date up to (not including) today_date.
    Returns 0 if file was written yesterday or today — file is fresh.
    Returns 1+ if bot missed one or more sessions — file is stale.
    Correctly handles weekends AND NYSE holidays (Good Friday, Thanksgiving etc).
    """
    count = 0
    check = file_date + timedelta(days=1)
    while check < today_date:
        if _is_nyse_trading_day(check):
            count += 1
        check += timedelta(days=1)
    return count

# === SESSION STATE — drives exit aggressiveness ===
SESSION_OPEN_FILE = "spy_session_open.txt"

SESSION_MULTIPLIERS = {
    "BULL_SESSION":    {"tp": 1.0,  "sl": 1.0},
    "NEUTRAL_SESSION": {"tp": 0.85, "sl": 0.80},
    "BEAR_SESSION":    {"tp": 0.70, "sl": 0.65},
}

_current_session_state = "NEUTRAL_SESSION"
_session_state_last_update = None

# Rocket mode tracking per symbol
_rocket_mode_active = {}
_rocket_mode_peak = {}
_rocket_mode_floor_pct = 0.0015   # 0.15% trailing floor in rocket mode
_rocket_tight_floor_pct = 0.0010  # 0.10% floor when SPY turns FALLING

rsi_fail_counter = defaultdict(int)

_eod_liquidation_fired = False
# PATCH16: Per-session symbol loss tracking
_session_loss_count = defaultdict(int)   # how many losses per symbol this session
_session_blacklist = set()               # symbols blocked for rest of session after 2 losses
SESSION_LOSS_BLACKLIST_THRESHOLD = 2     # block after this many losses

# PATCH16: Adaptive stop loss
ADAPTIVE_SL_TIGHT_PCT = 0.0025       # 0.25% tight stop in adverse conditions
ADAPTIVE_SL_CONSECUTIVE_LOSSES = 2   # tighten after this many consecutive losses
_consecutive_losses = 0              # rolling counter reset on any win

# PATCH17: Dynamic SPY bearish override thresholds
SPY_BEARISH_DIRECTION_THRESHOLD = -0.0020  # block if SPY below -0.20% AND direction FALLING
SPY_BEARISH_HARD_THRESHOLD = -0.0040       # block if SPY below -0.40% regardless of direction

# PATCH17: Condition-gated blacklist unblock
BLACKLIST_UNBLOCK_SPY_RECOVERY_PCT = 0.0030  # SPY must recover +0.30% from blacklist level
BLACKLIST_UNBLOCK_MIN_SECONDS = 3600         # 60 minutes minimum before unblock allowed
_session_blacklist_spy_level = {}            # SPY price when symbol was blacklisted
_session_blacklist_time = {}                 # timestamp when symbol was blacklisted

# PATCH17: Fill price tracking for accurate PnL accounting
_last_sell_fill_price = {}  # symbol -> confirmed fill price from safe_market_sell

# PATCH18 I4: Session block reason tracking for EOD missed opportunities report
_session_block_reasons = defaultdict(lambda: defaultdict(int))  # sym -> reason -> count

# PATCH17: DRIFT regime toggle for controlled comparison sessions
DRIFT_ENABLED = True  # set False to run TREND-only sessions without code changes

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

# === GLOBAL EXIT DEFAULTS (required by SELL and regime overlays) ===
TP_PCT = 0.004                 # 0.4% take-profit
TS_ACTIVATION_BUFFER = 0.003   # 0.3% trailing activation
TRAILING_STOP_PCT = 0.004      # 0.4% trailing stop
EMERGENCY_SL_PCT = 0.01        # 1% emergency stop
HARD_SL_PCT = 0.05             # 5% hard stop
SL_MULTIPLIER = 1.0            # default SL multiplier for regime tuning


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

# === SELL INFLIGHT GUARD (prevents double-sell between LIVE loop and reconcile) ===
_pending_sells = set()  # symbols currently being sold by the LIVE loop

# Entryjen seuranta
entry_times = {}
entry_prices = {}
entry_qty = {}
exec_rows = []

# === SHORT POSITION STATE (parallel to long entry state) ===
short_entry_times = {}
short_entry_prices = {}
short_entry_qty = {}
lowest_price_since_short = defaultdict(float)   # mirror of highest_price_since_entry
short_trailing_active = defaultdict(bool)        # mirror of trailing_active
pending_short_entries = set()                    # mirror of pending_entries

tp1_hit = defaultdict(bool)

# Viimeiset poistumisajat (symbol -> datetime)
last_exit_time = defaultdict(lambda: None)
last_exit_reason = defaultdict(lambda: None)   # tracks WHY last exit fired
TREND_REENTRY_BLOCK_SECONDS = 600              # 10 min block after trend-failure exit

# Viimeiset ostoyritykset (symbol -> datetime)
last_trade_attempt = defaultdict(lambda: None)

# Loopin aikana käytetty budjetti (nollataan jokaisen loopin alussa)
spent_this_loop = 0.0

# Trailing stop seurantaan
highest_price_since_entry = defaultdict(float)

# Trailing stop aktivoinnin tila (symbol -> bool)
trailing_active = defaultdict(bool)

# === SESSION PRICE TRACKING (per symbol) ===
session_open_price = defaultdict(lambda: None)
session_high_price = defaultdict(lambda: None)
session_low_price  = defaultdict(lambda: None)

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
RECON_FORCE_SELL_IF_ORPHAN = False   # force sell if position has no local entry context
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
MIN_HOLD_SECONDS = 90    # example: x seconds grace period before indicators can trigger
TRAIL_PCT = 0.010
BUY_POWER_LIMIT = 0.05
BUY_CASH_BUFFER = 0.95
COOLDOWN_SECONDS = 120
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
    "TP_PCT": 0.0030,          # changed from 0.0020
    "SL_MULTIPLIER": 0.9,
    "TS_ACTIVATION_BUFFER": 0.003,
    "TRAILING_STOP_PCT": 0.005,
    "MAX_TRADES": 6,
    "MAX_LOSS_DAY": 1.2,
    "VWAP_DELTA": 0.0018,
    "EMA_DELTA": 0.00022,
    "RSI_FAIL_TICKS": 5,
    # Entry scoring thresholds
    "ENTRY_SCORE_THRESHOLD":     1.8,
    # Indicator gate weights
    "WEIGHTS": {
        "ema_trend": 1.0,      # EMA_fast > EMA_slow + slope positive
        "vwap_above": 1.0,     # location above VWAP
        "macd_momentum": 0.7,  # MACD > signal, rising histogram
        "vol_confirm": 0.7,    # volume spike vs median
        "pullback_ok": 0.6,     # pullback to EMA_slow/VWAP then re-accel
        "rsi_ok": 0.7   # new weight
    },
    # pullback tolerances (distance normalized by price)
    "PULLBACK_TOL": 0.0015,
    "EMERGENCY_SL_PCT": 0.005, # changed from 0.0045
}

RANGE_CONFIG = {
    "TP_PCT": 0.0022,          # changed from 0.0020
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
    "BANDWIDTH_MIN": 0.002,  # avoid ultra-tight no-move
    "EMERGENCY_SL_PCT": 0.004,   # add this — tighter SL for range trades
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

ENTRY_AUDIT_FILE = "audit_entry_live.csv"
GATE_AUDIT_FILE = "audit_gate_live.csv"
REGIME_AUDIT_FILE = "audit_regime_live.csv"

EXEC_AUDIT_ENABLED = True
EXEC_AUDIT_FILE = "audit_trades_live.csv"

# === Adaptive entry threshold (per symbol, per regime) ===
# PATCH17: Disabled — broken in 3 ways (wrong regime labels, wrong TP/SL targets,
# state persists across sessions). TREND restored to true 1.8 minimum.
# Infrastructure kept for future proper rebuild.
ADAPTIVE_ENTRY_ENABLED = False
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
        base = TREND_CONFIG.get("ENTRY_SCORE_THRESHOLD", 1.8)
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
    # PATCH17: when adaptive system is disabled, return base threshold directly — no shift
    if not ADAPTIVE_ENTRY_ENABLED:
        return base
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
console.setLevel(logging.DEBUG)
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

from zoneinfo import ZoneInfo

# PATCH16: Finnish local time for all audit logging
AUDIT_TZ = ZoneInfo("Europe/Helsinki")

def _audit_ts(dt=None):
    """Returns current time in Finnish timezone as formatted string."""
    t = dt or datetime.now(timezone.utc)
    return t.astimezone(AUDIT_TZ).strftime("%Y-%m-%d %H:%M:%S")

def _session_minutes_from_ts(dt=None):
    """Returns minutes since NYSE market open for any timestamp."""
    t = dt or datetime.now(timezone.utc)
    return _session_minutes(t)

def _session_minutes(ts):
    # Convert timestamp to US Eastern Time (market timezone)
    ts_et = ts.astimezone(ZoneInfo("America/New_York"))

    # Market open and close in ET
    open_et = ts_et.replace(hour=9, minute=30, second=0, microsecond=0)
    close_et = ts_et.replace(hour=16, minute=0, second=0, microsecond=0)

    # Before open → return 0
    if ts_et < open_et:
        return 0

    # After close → return full session length (390 minutes)
    if ts_et > close_et:
        return 390

    # Minutes since open
    return int((ts_et - open_et).total_seconds() // 60)



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
    # Guard: if all deltas are zero (flat price), RSI is neutral 50
    if delta.abs().sum() == 0:
        return pd.Series([50.0] * len(series))
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
        
# === MARKET / SYMBOL / VOLATILITY FILTERS ===

def _trend_score_from_series(series, ema_fast=EMA_FAST, ema_slow=EMA_SLOW):
    if len(series) < ema_slow + 2:
        return 0.0
    ema_f = series.ewm(span=ema_fast, adjust=False).mean()
    ema_s = series.ewm(span=ema_slow, adjust=False).mean()
    slope = ema_s.iloc[-1] - ema_s.iloc[-3]
    spread = ema_f.iloc[-1] - ema_s.iloc[-1]
    return float(slope + spread)

def classify_trend(score, up_th=0.05, down_th=-0.05):
    if score >= up_th:
        return "bull"
    if score <= down_th:
        return "bear"
    return "flat"

def market_trend_filter(market_price_series):
    if market_price_series is None or len(market_price_series) < EMA_SLOW + 2:
        return "unknown"
    score = _trend_score_from_series(market_price_series)
    return classify_trend(score)

def symbol_trend_filter(prices_series):
    if prices_series is None or len(prices_series) < EMA_SLOW + 2:
        return "unknown"
    score = _trend_score_from_series(prices_series)
    return classify_trend(score)

def volatility_filter(prices_series, period=ATR_PERIOD):
    if prices_series is None or len(prices_series) < period + 5:
        return "unknown"
    atr_val = compute_atr_from_series(prices_series, period=period)
    upper, ma, lower, bandwidth = compute_bollinger(prices_series, period=20, std=2.0)
    if pd.isna(atr_val) or pd.isna(bandwidth):
        return "unknown"
    if bandwidth <= 0.003 and atr_val <= ATR_FLOOR * 1.2:
        return "low"
    if bandwidth >= 0.012 or atr_val >= ATR_FLOOR * 3.0:
        return "high"
    return "normal"

def range_quality_score(prices_series, vwap_val, rsi_series, boll_period=20, boll_std=2.0):
    if prices_series is None or len(prices_series) < boll_period + 2:
        return 0.0
    upper, ma, lower, bandwidth = compute_bollinger(prices_series, period=boll_period, std=boll_std)
    last_price = float(prices_series.iloc[-1])
    if pd.isna(lower) or lower <= 0:
        band_score = 0.0
    else:
        dist = (last_price - lower) / lower
        band_score = max(0.0, 1.0 - (dist / 0.004))
    if rsi_series is None or len(rsi_series) < 3:
        rsi_score = 0.0
    else:
        rsi_last = float(rsi_series.iloc[-1])
        rsi_prev = float(rsi_series.iloc[-3])
        low_enough = 20 <= rsi_last <= 40
        uptick = rsi_last > rsi_prev
        rsi_score = 1.0 if (low_enough and uptick) else 0.0
    if pd.isna(vwap_val) or vwap_val <= 0:
        vwap_score = 0.0
    else:
        vwap_dist = (vwap_val - last_price) / vwap_val
        vwap_score = 1.0 if 0.001 <= vwap_dist <= 0.004 else 0.0
    if pd.isna(bandwidth):
        vol_score = 0.0
    else:
        vol_score = 1.0 if 0.002 <= bandwidth <= 0.012 else 0.0
    score = (1.0 * band_score +
             1.0 * rsi_score +
             1.0 * vwap_score +
             0.5 * vol_score)
    return float(score)

def gate_entry(symbol, regime, prices_series, sizes_series, vwap_val, rsi_series,
               market_trend_state="unknown"):
    """
    Pre-scoring gate: blocks entries based on regime/market conflict.
    Called before evaluate_entry scoring to reject low-quality setups early.
    Returns (allowed: bool, reason: str)
    """
    sym_trend = symbol_trend_filter(prices_series)
    vol_state = volatility_filter(prices_series)
    last_price = float(prices_series.iloc[-1]) if len(prices_series) > 0 else float("nan")

    # === GATE 1: HIGH_VOL — always block entries ===
    if regime == "HIGH_VOL":
        return False, "high_vol_regime_block"

    # === GATE 2: RANGE vs bear market ===
    if regime == "RANGE" and market_trend_state == "bear":
        sym_above_vwap = (
            not pd.isna(vwap_val) and
            not pd.isna(last_price) and
            last_price > vwap_val
        )
        if not sym_above_vwap:
            logging.debug(
                "[GATE] %s RANGE blocked | market=bear sym_below_vwap "
                "(price=%.4f vwap=%.4f)",
                symbol, last_price, vwap_val
            )
            return False, "RANGE blocked: market=bear sym below VWAP"

        rq_score = range_quality_score(prices_series, vwap_val, rsi_series)
        if rq_score < 2.0:
            logging.debug(
                "[GATE] %s RANGE blocked | market=bear rq_score=%.2f < 2.0",
                symbol, rq_score
            )
            return False, f"RANGE blocked: market=bear low quality (score={rq_score:.2f})"

    elif regime == "RANGE":
        rq_score = range_quality_score(prices_series, vwap_val, rsi_series)
        if market_trend_state == "bear" and sym_trend == "bear":
            if rq_score < 2.0:
                return False, f"range_block_bear_trend_low_quality(score={rq_score:.2f})"
        else:
            if rq_score < 0.8:
                return False, f"range_block_low_quality(score={rq_score:.2f})"

    if regime == "DRIFT" and market_trend_state == "bear":
        obv_check = _obv_slope_proxy(prices_series, sizes_series, window=10)
        if pd.isna(obv_check) or obv_check <= 0:
            logging.debug(
                "[GATE] %s DRIFT blocked | market=bear OBV=%.2f <= 0",
                symbol, obv_check if not pd.isna(obv_check) else -999
            )
            return False, "DRIFT blocked: market=bear OBV non-positive"

    if regime == "LOW_VOL" and vol_state == "high":
        return False, "low_vol_block_high_volatility"

    if regime == "TREND":
        if market_trend_state == "bear" and sym_trend == "bull":
            sym_above_vwap = (
                not pd.isna(vwap_val) and
                not pd.isna(last_price) and
                last_price > vwap_val * 1.001
            )
            if not sym_above_vwap:
                return False, "trend_block_symbol_vs_market_mismatch"
            logging.debug(
                "[GATE] %s TREND allowed despite bear market | "
                "relative strength confirmed (price=%.4f vwap=%.4f)",
                symbol, last_price, vwap_val
            )

    if market_trend_state == "bear" and regime not in ("HIGH_VOL",):
        if len(prices_series) >= 10:
            recent_low_val = prices_series.iloc[-10:].min()
            if not pd.isna(last_price) and last_price <= recent_low_val * 1.0005:
                logging.debug(
                    "[GATE] %s blocked | market=bear price at/near 10-bar low "
                    "(price=%.4f low=%.4f)",
                    symbol, last_price, recent_low_val
                )
                return False, "blocked: market=bear price at 10-bar low"

    return True, "ok"



# === PATCH 2: Multi-tick confirmation ===
ENTRY_CONFIRM_ENABLED = True
ENTRY_CONFIRM_TICKS = 3  # consecutive ticks to validate pattern

def _confirm_trend(prices_series, vwap_val, ema_slow_val):
    if len(prices_series) < ENTRY_CONFIRM_TICKS + 2 or pd.isna(vwap_val) or pd.isna(ema_slow_val):
        return False
    tail = prices_series.iloc[-ENTRY_CONFIRM_TICKS-2:]
    near_anchor = (abs(tail.iloc[-ENTRY_CONFIRM_TICKS] - ema_slow_val) / tail.iloc[-ENTRY_CONFIRM_TICKS] <= TREND_CONFIG["PULLBACK_TOL"]) or \
                  (abs(tail.iloc[-ENTRY_CONFIRM_TICKS] - vwap_val) / tail.iloc[-ENTRY_CONFIRM_TICKS] <= TREND_CONFIG["PULLBACK_TOL"])
    upticks = all(tail.iloc[i] < tail.iloc[i+1] for i in range(len(tail)-1))
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
    adj = dict(CONFIG)

    # === SPY MARKET PRESSURE OVERLAY ===
    # If SPY has moved negatively from session open, tighten entry thresholds
    spy_open = globals().get("today_open_spy")
    spy_current = None
    try:
        spy_deque = globals().get("price_deques", {}).get("SPY")
        if spy_deque and len(spy_deque) > 0:
            spy_current = float(spy_deque[-1])
    except Exception:
        pass

    spy_pressure = 0.0
    if spy_open and spy_current and spy_open > 0:
        spy_move = (spy_current - spy_open) / spy_open
        if spy_move <= -0.0015:   # SPY down 0.15% or more from open
            spy_pressure = 0.4    # add 0.4 to all entry thresholds
            logging.debug("[SPY_PRESSURE] SPY move=%.3f%% — tightening entry thresholds by %.1f",
                          spy_move * 100, spy_pressure)

    # OPEN session stricter entries, tighter SL, slightly higher TP
    if 0 <= minutes < 30:
        adj["ENTRY_SCORE_THRESHOLD"] = CONFIG.get("ENTRY_SCORE_THRESHOLD", 3.0) + 0.15 + spy_pressure
        adj["SL_MULTIPLIER"] = max(0.8, CONFIG["SL_MULTIPLIER"] * 0.9)
        adj["TP_PCT"] = min(CONFIG["TP_PCT"] * 1.1, CONFIG["TP_PCT"] + 0.0003)
    else:
        if regime == "TREND":
            adj["ENTRY_SCORE_THRESHOLD"] = max(2.4, CONFIG.get("ENTRY_SCORE_THRESHOLD", 3.0) - 0.2) + spy_pressure
        elif regime == "RANGE":
            adj["ENTRY_SCORE_THRESHOLD"] = max(1.4, CONFIG.get("ENTRY_SCORE_THRESHOLD", 3.0) - 0.2) + spy_pressure
        elif regime == "LOW_VOL":
            adj["ENTRY_SCORE_THRESHOLD"] = max(1.6, CONFIG.get("ENTRY_SCORE_THRESHOLD", 3.0) - 0.2) + spy_pressure

    return adj

# === PATCH 4: ADX and Choppiness proxies ===
ADX_ENABLED = True
CHOP_ENABLED = True

# === SPY REALIZED VOLATILITY STATE ===
# Compares current SPY ATR against its own rolling baseline
# Returns one of: "NORMAL", "ELEVATED", "EXTREME"
SPY_VOL_ATR_WINDOW = 20       # ticks for current ATR
SPY_VOL_BASELINE_WINDOW = 100 # ticks for baseline ATR
SPY_VOL_ELEVATED_MULT = 1.5   # current > 1.5x baseline = ELEVATED
SPY_VOL_EXTREME_MULT = 2.5    # current > 2.5x baseline = EXTREME

def get_spy_volatility_state():
    try:
        spy_deque = globals().get("price_deques", {}).get("SPY")
        if spy_deque is None or len(spy_deque) < SPY_VOL_BASELINE_WINDOW + SPY_VOL_ATR_WINDOW:
            return "NORMAL"  # not enough data yet — default to normal
        spy_series = pd.Series(spy_deque)
        # Current ATR — last 20 ticks
        current_atr = compute_atr_from_series(spy_series, SPY_VOL_ATR_WINDOW)
        # Baseline ATR — rolling mean over last 100 ticks
        atr_series = spy_series.diff().abs().rolling(SPY_VOL_ATR_WINDOW).mean()
        baseline_atr = atr_series.iloc[-SPY_VOL_BASELINE_WINDOW:].mean()
        if pd.isna(current_atr) or pd.isna(baseline_atr) or baseline_atr == 0:
            return "NORMAL"
        ratio = current_atr / baseline_atr
        if ratio >= SPY_VOL_EXTREME_MULT:
            logging.debug(
                "[VOL_STATE] SPY volatility EXTREME | current_atr=%.4f baseline=%.4f ratio=%.2f",
                current_atr, baseline_atr, ratio
            )
            return "EXTREME"
        elif ratio >= SPY_VOL_ELEVATED_MULT:
            logging.debug(
                "[VOL_STATE] SPY volatility ELEVATED | current_atr=%.4f baseline=%.4f ratio=%.2f",
                current_atr, baseline_atr, ratio
            )
            return "ELEVATED"
        else:
            return "NORMAL"
    except Exception as e:
        logging.debug("[VOL_STATE] get_spy_volatility_state failed: %s", e)
        return "NORMAL"

# === SPY DIRECTIONAL SLOPE — detects sustained intraday fade ===
# Returns "FALLING", "FLAT", or "RISING" based on recent SPY price slope
# Uses short window (20 ticks) to detect current momentum direction
SPY_SLOPE_WINDOW = 20          # ticks for slope calculation
SPY_SLOPE_FALL_THRESH = -0.0003  # SPY falling faster than -0.03% per tick = FALLING
SPY_SLOPE_RISE_THRESH = 0.0003   # SPY rising faster than +0.03% per tick = RISING

# PATCH15: Regime-aware SPY direction thresholds
# On BULL_DAY with SPY >1% from open, relax the FALLING threshold significantly
# so brief pullbacks do not block all entries during a genuine bull session
SPY_SLOPE_FALL_THRESH_BULL = -0.0008  # much steeper decline required on bull days

# === PATCH15: Market character state ===
# Five-state intraday character updated continuously
# STRONG_BULL / MILD_BULL / NEUTRAL / RECOVERING / BEAR
_market_character = "NEUTRAL"
_market_char_candidate = None
_market_char_candidate_since = None

# === PATCH15: Williams %R period ===
WILLIAMS_R_PERIOD = 14

def compute_williams_r(prices_series, period=14):
    """
    Williams %R = (Highest High - Close) / (Highest High - Lowest Low) * -100
    Values between -100 and 0.
    Oversold below -80. Overbought above -20.
    """
    if len(prices_series) < period:
        return pd.Series([float('nan')] * len(prices_series))
    highest_high = prices_series.rolling(period).max()
    lowest_low = prices_series.rolling(period).min()
    hl_range = highest_high - lowest_low
    hl_range = hl_range.replace(0, float('nan'))
    wr = (highest_high - prices_series) / hl_range * -100
    return wr


def _update_market_character(spy_move, ts_val):
    """
    PATCH15: Continuously updates intraday market character.
    Requires 5 minutes of sustained new state before switching.
    spy_move: float, fractional move from today_open_spy (e.g. 0.015 = +1.5%)
    """
    global _market_character, _market_char_candidate, _market_char_candidate_since

    # Determine candidate character from SPY move
    if spy_move >= 0.010:
        candidate = "STRONG_BULL"
    elif spy_move >= 0.003:
        candidate = "MILD_BULL"
    elif spy_move >= -0.003:
        candidate = "NEUTRAL"
    elif spy_move >= -0.008:
        candidate = "RECOVERING"
    else:
        candidate = "BEAR"

    # Same as current — reset candidate
    if candidate == _market_character:
        _market_char_candidate = None
        _market_char_candidate_since = None
        return

    # New candidate — start timer
    if candidate != _market_char_candidate:
        _market_char_candidate = candidate
        _market_char_candidate_since = ts_val
        return

    # Candidate sustained for 5 minutes — confirm switch
    elapsed = (ts_val - _market_char_candidate_since).total_seconds()
    if elapsed >= 300:
        old = _market_character
        _market_character = candidate
        _market_char_candidate = None
        _market_char_candidate_since = None
        globals()["_market_character"] = _market_character
        logging.warning(
            "[PATCH15] Market character: %s -> %s "
            "(SPY_move=%.2f%% sustained %.0fs)",
            old, _market_character, spy_move * 100, elapsed
        )


def _get_dynamic_tp(base_tp, market_char, spy_move):
    """
    PATCH15: Returns TP_PCT adjusted for market character and SPY move strength.
    """
    if market_char == "STRONG_BULL" and spy_move >= 0.015:
        return base_tp * 3.0    # 0.90% on very strong bull days
    elif market_char == "STRONG_BULL" and spy_move >= 0.010:
        return base_tp * 2.3    # 0.69% on strong bull days
    elif market_char == "MILD_BULL" and spy_move >= 0.005:
        return base_tp * 1.7    # 0.51% on mild bull days
    elif market_char == "MILD_BULL":
        return base_tp * 1.3    # 0.39% on early mild bull
    elif market_char == "NEUTRAL":
        return base_tp * 0.85   # 0.255% — current NEUTRAL behavior
    elif market_char == "RECOVERING":
        return base_tp * 0.70   # 0.21% — tight on recovering days
    elif market_char == "BEAR":
        return base_tp * 0.60   # 0.18% — very tight on bear days
    return base_tp


def _get_symbol_mode(sym, prices_series, spy_move_from_open):
    """
    PATCH15: Returns MODE1 (conservative recovery) or MODE2 (aggressive momentum)
    based on symbol relative strength versus SPY.
    """
    if len(prices_series) < 2:
        return "MODE1"

    sym_open = globals().get(f"today_open_{sym}")
    if sym_open is None or sym_open == 0:
        return "MODE1"

    current_price = float(prices_series.iloc[-1])
    sym_move = (current_price - sym_open) / sym_open
    relative_strength = sym_move - spy_move_from_open

    market_char = globals().get("_market_character", "NEUTRAL")

    if market_char in ("STRONG_BULL", "MILD_BULL"):
        if relative_strength >= -0.002:
            return "MODE2"
        else:
            return "MODE1"

    if market_char in ("NEUTRAL", "RECOVERING", "BEAR"):
        if relative_strength >= 0.005:
            return "MODE2"
        else:
            return "MODE1"

    return "MODE1"


def evaluate_recovery_entry(sym, price, prices_series, sizes_series,
                             ts_val, positions_map, inflight_orders,
                             pending_entries, last_exit, last_buy_time):
    """
    PATCH15 Mode 1 entry — oversold recovery using Williams %R.
    Fires when WR crosses up through -80 from oversold with momentum and volume.
    """
    # PATCH19 Option A: structural guardrails — MODE1 must not bypass these blocks
    if sym == "SPY":
        return False, "SPY excluded from MODE1", 0.0, {}

    _regime_m1 = detect_regime(prices_series, sizes_series)
    if _regime_m1 == "HIGH_VOL":
        return False, "HIGH_VOL excluded from MODE1", 0.0, {}

    if get_spy_direction() == "FALLING":
        return False, "MODE1 blocked — SPY direction FALLING", 0.0, {}

    since_last_exit = (ts_val - last_exit).total_seconds() \
        if last_exit is not None else float("inf")
    since_last_buy = (ts_val - last_buy_time[sym]).total_seconds() \
        if last_buy_time[sym] is not None else float("inf")
    if since_last_exit < COOLDOWN_SECONDS or since_last_buy < COOLDOWN_SECONDS:
        return False, "Cooldown", 0.0, {}
    if inflight_orders.get(sym) is not None or sym in pending_entries:
        return False, "Order flow block", 0.0, {}
    if len(prices_series) < WILLIAMS_R_PERIOD + 2:
        return False, "Insufficient data", 0.0, {}

    wr_series = compute_williams_r(prices_series, period=WILLIAMS_R_PERIOD)
    if len(wr_series) < 2:
        return False, "WR insufficient", 0.0, {}

    wr_now  = wr_series.iloc[-1]
    wr_prev = wr_series.iloc[-2]

    if pd.isna(wr_now) or pd.isna(wr_prev):
        return False, "WR nan", 0.0, {}

    # Core signal: WR crossing up through -80 from oversold
    wr_cross_up = (wr_prev <= -80) and (wr_now > -80)
    if not wr_cross_up:
        return False, "WR no cross", 0.0, {}

    # Momentum confirmation: last 2 ticks rising
    momentum_ok = (
        len(prices_series) >= 3 and
        prices_series.iloc[-1] > prices_series.iloc[-2] > prices_series.iloc[-3]
    )
    if not momentum_ok:
        return False, "MODE1 momentum fail", 0.0, {}

    # Volume confirmation
    sizes_series_pd = pd.Series(sizes_series) if not isinstance(sizes_series, pd.Series) \
        else sizes_series
    median_vol = sizes_series_pd.median() if len(sizes_series_pd) > 0 else float('nan')
    current_vol = sizes_series_pd.iloc[-1] if len(sizes_series_pd) > 0 else float('nan')
    vol_ok = (not pd.isna(median_vol) and not pd.isna(current_vol)
              and current_vol > median_vol * 1.2)
    if not vol_ok:
        return False, "MODE1 volume fail", 0.0, {}

    # SPY not in active decline
    spy_prices_deque = globals().get("price_deques", {}).get("SPY")
    if spy_prices_deque and len(spy_prices_deque) >= 5:
        spy_prices = pd.Series(spy_prices_deque)
        if float(spy_prices.iloc[-1]) < float(spy_prices.iloc[-5]):
            return False, "MODE1 SPY declining", 0.0, {}

    signal_stack = {
        "wr_now": round(float(wr_now), 2),
        "wr_prev": round(float(wr_prev), 2),
        "wr_cross_up": wr_cross_up,
        "momentum_ok": momentum_ok,
        "vol_ok": vol_ok,
    }

    logging.info(
        "[PATCH15][MODE1][%s] Recovery entry | WR=%.1f->%.1f price=%.4f",
        sym, float(wr_prev), float(wr_now), price
    )
    return True, "MODE1_recovery", 1.0, signal_stack

def get_spy_direction():
    try:
        spy_deque = globals().get("price_deques", {}).get("SPY")
        if spy_deque is None or len(spy_deque) < SPY_SLOPE_WINDOW + 2:
            return "FLAT"
        spy_series = pd.Series(spy_deque)
        # Use EMA slope over last N ticks — smoother than raw price difference
        ema = spy_series.ewm(span=SPY_SLOPE_WINDOW, adjust=False).mean()
        # Normalize slope by price so it is comparable across SPY price levels
        slope_norm = (ema.iloc[-1] - ema.iloc[-SPY_SLOPE_WINDOW]) / ema.iloc[-SPY_SLOPE_WINDOW]
        
        # PATCH15/PATCH18: Use relaxed threshold on BULL_DAY OR when market_char is bullish.
        # PATCH18: market_char as fallback prevents NEUTRAL_DAY from locking the strict
        # threshold all session even when the market is clearly trending up intraday.
        _active_fall_thresh = SPY_SLOPE_FALL_THRESH
        _day_regime_dir = globals().get("day_regime", "NEUTRAL_DAY")
        _spy_open_dir = globals().get("today_open_spy")
        _market_char_dir = globals().get("_market_character", "NEUTRAL")
        if _spy_open_dir is not None:
            _spy_now_dir = float(spy_series.iloc[-1])
            _spy_move_dir = (_spy_now_dir - _spy_open_dir) / _spy_open_dir
            _use_relaxed_slope = (
                (_day_regime_dir == "BULL_DAY" and _spy_move_dir >= 0.010) or
                (_market_char_dir in ("STRONG_BULL", "MILD_BULL") and _spy_move_dir >= 0.003)
            )
            if _use_relaxed_slope:
                _active_fall_thresh = SPY_SLOPE_FALL_THRESH_BULL
                logging.debug(
                    "[PATCH18][SPY_DIR] relaxed fall threshold=%.5f | "
                    "day_regime=%s market_char=%s spy_move=%.2f%%",
                    _active_fall_thresh, _day_regime_dir, _market_char_dir, _spy_move_dir * 100
                )
        if slope_norm <= _active_fall_thresh:
            logging.debug(
                "[SPY_DIR] SPY direction FALLING | slope_norm=%.5f threshold=%.5f",
                slope_norm, _active_fall_thresh
            )
            return "FALLING"
        elif slope_norm >= SPY_SLOPE_RISE_THRESH:
            return "RISING"
        else:
            return "FLAT"
    except Exception as e:
        logging.debug("[SPY_DIR] get_spy_direction failed: %s", e)
        return "FLAT"
    
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
    "ATR_TOP_PCT": 0.90,
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


def log_entry_attempt(ts, symbol, regime, bias, accept, reason, score,
                      ema_fast, ema_slow, rsi, vwap, price):
    try:
        file_exists = os.path.exists(ENTRY_AUDIT_FILE) and os.path.getsize(ENTRY_AUDIT_FILE) > 0
        with open(ENTRY_AUDIT_FILE, "a", newline="") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow([
                    "timestamp","symbol","regime","bias","accept","reason","score",
                    "ema_fast","ema_slow","rsi","vwap","price"
                ])
            writer.writerow([
                ts.isoformat(), symbol, regime, bias, accept, reason, round(score,4),
                round(ema_fast,4), round(ema_slow,4), round(rsi,2),
                round(vwap,4), round(price,4)
            ])
    except Exception as e:
        logging.debug(f"[IO] entry audit failed for {symbol}: {e}")


def log_gate_event(ts, symbol, regime, allowed, gate_reason,
                   market_trend, symbol_trend, vol_state, rq_score):
    try:
        file_exists = os.path.exists(GATE_AUDIT_FILE) and os.path.getsize(GATE_AUDIT_FILE) > 0
        with open(GATE_AUDIT_FILE, "a", newline="") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow([
                    "timestamp","symbol","regime","allowed","gate_reason",
                    "market_trend","symbol_trend","vol_state","range_quality"
                ])
            writer.writerow([
                ts.isoformat(), symbol, regime, allowed, gate_reason,
                market_trend, symbol_trend, vol_state, rq_score
            ])
    except Exception as e:
        logging.debug(f"[IO] gate audit failed for {symbol}: {e}")


def log_regime_state(ts, symbol, regime, bandwidth, atr, ema_slope_val, vwap_dist):
    try:
        file_exists = os.path.exists(REGIME_AUDIT_FILE) and os.path.getsize(REGIME_AUDIT_FILE) > 0
        with open(REGIME_AUDIT_FILE, "a", newline="") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow([
                    "timestamp","symbol","regime","bandwidth","atr","ema_slope","vwap_dist"
                ])
            writer.writerow([
                ts.isoformat(), symbol, regime,
                bandwidth, atr, ema_slope_val, vwap_dist
            ])
    except Exception as e:
        logging.debug(f"[IO] regime audit failed for {symbol}: {e}")


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

_last_positions_map = {}

def get_positions_map(trade_client_local):
    global _last_positions_map
    try:
        positions = trade_client_local.get_all_positions()
        new_map = {pos.symbol: (int(float(pos.qty)), float(pos.avg_entry_price)) for pos in positions}
        _last_positions_map = new_map
        return new_map
    except Exception as e:
        logging.warning("get_positions_map failed, using last known map: %s", e)
        return _last_positions_map

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

def safe_market_buy(
    trade_client_local,
    symbol,
    cash_for_buy,
    order_lock,
    price_deques,
    size_deques,
    entry_times,
    entry_prices,
    entry_qty,
    entry_configs,
    bias=None,
    config_session=None
):
    import logging, time, csv, os
    import pandas as pd
    from datetime import datetime, timezone

    logging.info("[BUY_START] safe_market_buy start for %s", symbol)
    symbol = symbol.strip().upper()

    try:
        now_ts = datetime.now(timezone.utc)
        minutes = _session_minutes(now_ts)
        SESSION_LENGTH_MIN = 390

        if minutes < 30:
            logging.info("%s - BUY blocked: session minutes=%d < 30", symbol, minutes)
            return None
        if minutes >= SESSION_LENGTH_MIN - 30:
            logging.info("%s - BUY blocked: session minutes=%d >= %d (final 30 min)",
                         symbol, minutes, SESSION_LENGTH_MIN - 30)
            return None

        with order_lock:
            try:
                # --- Get latest price estimate ---
                try:
                    resp = stock_data_client.get_stock_latest_trade(
                        StockLatestTradeRequest(symbol_or_symbols=symbol)
                    )
                    est_price = float(resp[symbol].price)
                except Exception as e:
                    logging.warning("[BUY_PRICE_FALLBACK] %s: %s", symbol, e)
                    est_price = None

                if not est_price or est_price <= 0:
                    logging.warning("[BUY_SKIP] %s est_price invalid: %s", symbol, est_price)
                    return None

                # PATCH16: risk scaling — reduce size in adverse conditions
                _market_char_size = globals().get("_market_character", "NEUTRAL")
                _day_regime_size = globals().get("day_regime", "NEUTRAL_DAY")
                _size_multiplier = 1.0
                if _day_regime_size != "BULL_DAY" and \
                        _market_char_size in ("NEUTRAL", "RECOVERING", "BEAR"):
                    _size_multiplier = 0.6
                    logging.debug(
                        "[PATCH16][SIZE] Reducing position size to 60%% | "
                        "char=%s regime=%s",
                        _market_char_size, _day_regime_size
                    )
                qty = int((cash_for_buy * BUY_CASH_BUFFER * _size_multiplier) // est_price)
                if qty <= 0 or qty * est_price < MIN_TRADE_USD:
                    logging.info("[BUY_SKIP] %s qty too small: qty=%d est_price=%.4f", symbol, qty, est_price)
                    return None

                # --- Indicators at entry time ---
                if symbol in price_deques and symbol in size_deques:
                    prices_series = pd.Series(price_deques[symbol])
                    sizes_series = pd.Series(size_deques[symbol])
                else:
                    prices_series = pd.Series(dtype=float)
                    sizes_series = pd.Series(dtype=float)

                ema_fast_val = _safe_last(compute_ema_from_series(prices_series, EMA_FAST))
                ema_slow_val = _safe_last(compute_ema_from_series(prices_series, EMA_SLOW))
                rsi_val = _safe_last(compute_rsi_from_series(prices_series, RSI_PERIOD))
                vwap_val = _safe_last(compute_vwap_from_ticks(prices_series, sizes_series))
                regime_at_entry = detect_regime(prices_series, sizes_series)
                bias_val = bias if bias is not None else globals().get("day_bias", "unknown")

                # === SESSION POSITION METRICS AT ENTRY ===
                s_open = session_open_price.get(symbol)
                s_high = session_high_price.get(symbol)
                s_low  = session_low_price.get(symbol)
                
                dist_from_session_high = (
                    (s_high - est_price) / s_high
                    if s_high and s_high > 0 else float("nan")
                )
                move_from_open = (
                    (est_price - s_open) / s_open
                    if s_open and s_open > 0 else float("nan")
                )
                range_position = (
                    (est_price - s_low) / (s_high - s_low)
                    if s_high and s_low and s_high != s_low else float("nan")
                )
                # === SESSION POSITION ENTRY FILTERS ===
                RANGE_POSITION_MAX = 0.75
                # Tighten move_from_open threshold if SPY is negative on the day
                _spy_open = globals().get("today_open_spy")
                _spy_deque = globals().get("price_deques", {}).get("SPY")
                _spy_now = float(_spy_deque[-1]) if _spy_deque and len(_spy_deque) > 0 else None
                if _spy_open and _spy_now and _spy_open > 0 and (_spy_now - _spy_open) / _spy_open <= -0.0015:
                    MOVE_FROM_OPEN_MAX = 0.0015  # tighter on bearish SPY days
                else:
                    MOVE_FROM_OPEN_MAX = 0.002   # standard threshold
                
                if not pd.isna(range_position) and range_position > RANGE_POSITION_MAX:
                    logging.info("[BUY_BLOCK] %s blocked | range_position=%.3f > %.2f (near session high)",
                                 symbol, range_position, RANGE_POSITION_MAX)
                    return None
                
                if not pd.isna(move_from_open) and move_from_open > MOVE_FROM_OPEN_MAX:
                    logging.info("[BUY_BLOCK] %s blocked | move_from_open=%.4f > %.4f (extended from open)",
                                 symbol, move_from_open, MOVE_FROM_OPEN_MAX)
                    return None
                    
                # --- Submit order ---
                order = MarketOrderRequest(
                    symbol=symbol,
                    qty=qty,
                    side=OrderSide.BUY,
                    type=OrderType.MARKET,
                    time_in_force=TimeInForce.DAY
                )
                submitted = trade_client_local.submit_order(order)
                order_id = getattr(submitted, "id", None)
                submit_ts = datetime.now(timezone.utc)

                logging.info("[BUY_SUBMITTED] %s order_id=%s qty=%d est_price=%.4f",
                             symbol, order_id, qty, est_price)

                
                          
                           
                entry_config_dict = {
                    "regime": regime_at_entry,
                    "bias": bias_val,
                    "ema_fast": ema_fast_val,
                    "ema_slow": ema_slow_val,
                    "rsi": rsi_val,
                    "vwap": vwap_val,
                    "order_id": order_id,
                    "est_price": est_price,
                    "fill_inferred": True,
                    "TP_PCT": TREND_CONFIG["TP_PCT"] if regime_at_entry == "TREND" else TP_PCT,
                    "TS_ACTIVATION_BUFFER": TS_ACTIVATION_BUFFER,
                    "TRAILING_STOP_PCT": TRAILING_STOP_PCT,
                    "EMERGENCY_SL_PCT": TREND_CONFIG.get("EMERGENCY_SL_PCT", 0.005)
                                        if regime_at_entry == "TREND"
                                        else RANGE_CONFIG.get("EMERGENCY_SL_PCT", 0.004),
                    "HARD_SL_PCT": HARD_SL_PCT,
                    "SL_MULTIPLIER": SL_MULTIPLIER,
                    "dist_from_session_high": round(dist_from_session_high, 6)
                              if not pd.isna(dist_from_session_high) else None,
                    "move_from_open": round(move_from_open, 6)
                                      if not pd.isna(move_from_open) else None,
                    "range_position": round(range_position, 4)
                                      if not pd.isna(range_position) else None,
                }
                if config_session:
                    entry_config_dict.update(config_session)

                # Write state now — before polling
                entry_times[symbol]   = submit_ts
                entry_prices[symbol]  = est_price
                entry_qty[symbol]     = float(qty)
                entry_configs[symbol] = entry_config_dict

                highest_price_since_entry[symbol] = est_price
                trailing_active[symbol] = False
                tp1_hit[symbol] = False

                logging.info("[BUY_STATE_WRITTEN] %s entry_time=%s entry_price=%.4f qty=%d (pre-fill placeholder)",
                             symbol, submit_ts, est_price, qty)

                # Write to exec log immediately (as inferred)
                buy_row = {
                    "timestamp": _audit_ts(submit_ts),
                    "symbol": symbol,
                    "action": "BUY_CONFIRMED",
                    "price": est_price,
                    "reason": "entry_pre_fill",
                    "bias": bias_val,
                    "pnl": None,
                    "ema_fast": ema_fast_val,
                    "ema_slow": ema_slow_val,
                    "rsi": rsi_val,
                    "vwap": vwap_val,
                    "regime": regime_at_entry,
                    "code_version": CODE_VERSION,
                    "dist_from_session_high": entry_config_dict["dist_from_session_high"],
                    "move_from_open": entry_config_dict["move_from_open"],
                    "range_position": entry_config_dict["range_position"],
                }
                exec_rows.append(buy_row)
                write_exec_row_immediate(buy_row, symbol, RUN_MODE)

                if EXEC_AUDIT_ENABLED:
                    try:
                        fieldnames = ["timestamp","symbol","action","price","reason","bias","pnl",
                                      "ema_fast","ema_slow","rsi","vwap","regime","code_version",
                                      "dist_from_session_high","move_from_open","range_position",
                                      "session_minutes"]
                        with open(EXEC_AUDIT_FILE, "a", newline="") as f:
                            writer = csv.DictWriter(f, fieldnames=fieldnames)
                            if f.tell() == 0:
                                writer.writeheader()
                            writer.writerow({
                                "timestamp": _audit_ts(submit_ts),
                                "symbol": symbol,
                                "action": "BUY",
                                "price": round(est_price, 6),
                                "reason": "entry_pre_fill",
                                "bias": bias_val,
                                "pnl": None,
                                "ema_fast": round(ema_fast_val, 6) if not pd.isna(ema_fast_val) else None,
                                "ema_slow": round(ema_slow_val, 6) if not pd.isna(ema_slow_val) else None,
                                "rsi": round(rsi_val, 2) if not pd.isna(rsi_val) else None,
                                "vwap": round(vwap_val, 6) if not pd.isna(vwap_val) else None,
                                "regime": regime_at_entry,
                                "code_version": CODE_VERSION,
                                "dist_from_session_high": entry_config_dict["dist_from_session_high"],
                                "move_from_open": entry_config_dict["move_from_open"],
                                "range_position": entry_config_dict["range_position"],
                                "session_minutes": _session_minutes_from_ts(submit_ts),
                            })
                    except Exception as e:
                        logging.warning("Failed to write BUY to audit file: %s", e)

                POLL_TIMEOUT = 90
                poll_start = datetime.now(timezone.utc)
                filled_qty = 0.0
                filled_price = None
                last_status = None

                while (datetime.now(timezone.utc) - poll_start).total_seconds() < POLL_TIMEOUT:
                    try:
                        current = trade_client_local.get_order_by_id(order_id)
                        last_status = getattr(current, "status", None)
                        raw_filled_qty = getattr(current, "filled_qty", 0) or 0
                        raw_filled_price = getattr(current, "filled_avg_price", None)

                        logging.debug("[ORDER_STATUS] %s status=%s filled_qty=%s filled_avg_price=%s",
                                      symbol, last_status, raw_filled_qty, raw_filled_price)

                        try:
                            filled_qty = float(raw_filled_qty)
                        except Exception:
                            filled_qty = 0.0

                        if raw_filled_price is not None:
                            try:
                                filled_price = float(raw_filled_price)
                            except Exception:
                                filled_price = None

                        if _status_is(last_status, "filled") and filled_qty > 0:
                            break

                        if _status_is(last_status, "canceled") or _status_is(last_status, "rejected"):
                            logging.warning("[BUY_CANCELED] %s order %s status=%s — clearing state",
                                            symbol, order_id, last_status)
                            entry_times.pop(symbol, None)
                            entry_prices.pop(symbol, None)
                            entry_qty.pop(symbol, None)
                            entry_configs.pop(symbol, None)
                            highest_price_since_entry.pop(symbol, None)
                            trailing_active[symbol] = False
                            return None

                    except Exception as e:
                        logging.debug("[ORDER_STATUS_ERROR] %s: %s", symbol, e)

                    time.sleep(0.5)

                if filled_qty > 0 and filled_price is not None:
                    fill_ts = datetime.now(timezone.utc)
                    entry_times[symbol]   = fill_ts
                    entry_prices[symbol]  = filled_price
                    entry_qty[symbol]     = filled_qty
                    entry_configs[symbol]["fill_inferred"] = False
                    highest_price_since_entry[symbol] = filled_price

                    logging.info("[BUY_FILLED] %s fill_price=%.4f qty=%.2f (state updated from placeholder)",
                                 symbol, filled_price, filled_qty)

                    fill_row = {
                        "timestamp": _audit_ts(fill_ts),
                        "symbol": symbol,
                        "action": "BUY_CONFIRMED",
                        "price": filled_price,
                        "reason": "entry_fill_confirmed",
                        "bias": bias_val,
                        "pnl": None,
                        "ema_fast": ema_fast_val,
                        "ema_slow": ema_slow_val,
                        "rsi": rsi_val,
                        "vwap": vwap_val,
                        "regime": regime_at_entry,
                        "code_version": CODE_VERSION,
                        "dist_from_session_high": entry_config_dict["dist_from_session_high"],
                        "move_from_open": entry_config_dict["move_from_open"],
                        "range_position": entry_config_dict["range_position"],
                    }
                    exec_rows.append(fill_row)
                    write_exec_row_immediate(fill_row, symbol, RUN_MODE)

                else:
                    logging.warning(
                        "[BUY_TIMEOUT] %s order_id=%s not confirmed within %ds "
                        "— keeping est_price=%.4f as entry (state already written)",
                        symbol, order_id, POLL_TIMEOUT, est_price
                    )

                return submitted

            except Exception as e:
                logging.exception("[BUY_INNER_ERROR] %s: %s", symbol, e)
                if symbol in entry_configs and entry_configs[symbol].get("fill_inferred"):
                    entry_times.pop(symbol, None)
                    entry_prices.pop(symbol, None)
                    entry_qty.pop(symbol, None)
                    entry_configs.pop(symbol, None)
                return None

    except Exception as e:
        logging.exception("[BUY_OUTER_ERROR] %s: %s", symbol, e)
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

def safe_market_short(
    trade_client_local,
    symbol,
    cash_for_short,
    order_lock,
    price_deques,
    size_deques,
    short_entry_times,
    short_entry_prices,
    short_entry_qty,
    config_session=None
):
    """
    Submits a short (SELL) order when no position exists.
    Mirrors safe_market_buy exactly but writes to short_entry_* dicts.
    """
    logging.info("[SHORT_START] safe_market_short start for %s", symbol)
    symbol = symbol.strip().upper()

    try:
        now_ts = datetime.now(timezone.utc)
        minutes = _session_minutes(now_ts)
        SESSION_LENGTH_MIN = 390

        if minutes < 30:
            logging.info("%s - SHORT blocked: session minutes=%d < 30", symbol, minutes)
            return None
        if minutes >= SESSION_LENGTH_MIN - 30:
            logging.info("%s - SHORT blocked: session minutes=%d >= %d (final 30 min)",
                         symbol, minutes, SESSION_LENGTH_MIN - 30)
            return None

        with order_lock:
            try:
                try:
                    resp = stock_data_client.get_stock_latest_trade(
                        StockLatestTradeRequest(symbol_or_symbols=symbol)
                    )
                    est_price = float(resp[symbol].price)
                except Exception as e:
                    logging.warning("[SHORT_PRICE_FALLBACK] %s: %s", symbol, e)
                    est_price = None

                if not est_price or est_price <= 0:
                    logging.warning("[SHORT_SKIP] %s est_price invalid: %s", symbol, est_price)
                    return None

                qty = int((cash_for_short * BUY_CASH_BUFFER) // est_price)
                if qty <= 0 or qty * est_price < MIN_TRADE_USD:
                    logging.info("[SHORT_SKIP] %s qty too small: qty=%d est_price=%.4f",
                                 symbol, qty, est_price)
                    return None

                if symbol in price_deques and symbol in size_deques:
                    prices_series = pd.Series(price_deques[symbol])
                    sizes_series = pd.Series(size_deques[symbol])
                else:
                    prices_series = pd.Series(dtype=float)
                    sizes_series = pd.Series(dtype=float)

                ema_fast_val = _safe_last(compute_ema_from_series(prices_series, EMA_FAST))
                ema_slow_val = _safe_last(compute_ema_from_series(prices_series, EMA_SLOW))
                rsi_val      = _safe_last(compute_rsi_from_series(prices_series, RSI_PERIOD))
                vwap_val     = _safe_last(compute_vwap_from_ticks(prices_series, sizes_series))
                regime_at_entry = detect_regime(prices_series, sizes_series)

                order = MarketOrderRequest(
                    symbol=symbol,
                    qty=qty,
                    side=OrderSide.SELL,
                    type=OrderType.MARKET,
                    time_in_force=TimeInForce.DAY
                )
                submitted = trade_client_local.submit_order(order)
                order_id = getattr(submitted, "id", None)
                submit_ts = datetime.now(timezone.utc)

                logging.info("[SHORT_SUBMITTED] %s order_id=%s qty=%d est_price=%.4f",
                             symbol, order_id, qty, est_price)

                short_config = {
                    "regime": regime_at_entry,
                    "bias": "bearish",
                    "ema_fast": ema_fast_val,
                    "ema_slow": ema_slow_val,
                    "rsi": rsi_val,
                    "vwap": vwap_val,
                    "order_id": order_id,
                    "est_price": est_price,
                    "fill_inferred": True,
                    "TP_PCT": 0.002,
                    "TS_ACTIVATION_BUFFER": 0.003,
                    "TRAILING_STOP_PCT": 0.004,
                    "EMERGENCY_SL_PCT": 0.005,
                    "HARD_SL_PCT": 0.01,
                    "SL_MULTIPLIER": 1.0,
                }
                if config_session:
                    short_config.update(config_session)

                short_entry_times[symbol]  = submit_ts
                short_entry_prices[symbol] = est_price
                short_entry_qty[symbol]    = float(qty)
                lowest_price_since_short[symbol]  = est_price
                short_trailing_active[symbol]     = False

                logging.info(
                    "[SHORT_STATE_WRITTEN] %s entry_time=%s entry_price=%.4f qty=%d",
                    symbol, submit_ts, est_price, qty
                )

                short_row = {
                    "timestamp": submit_ts.strftime("%Y-%m-%d %H:%M:%S"),
                    "symbol": symbol,
                    "action": "SHORT",
                    "price": est_price,
                    "reason": "short_entry_pre_fill",
                    "bias": "bearish",
                    "pnl": None,
                    "ema_fast": ema_fast_val,
                    "ema_slow": ema_slow_val,
                    "rsi": rsi_val,
                    "vwap": vwap_val,
                    "regime": regime_at_entry,
                    "code_version": CODE_VERSION
                }
                exec_rows.append(short_row)
                write_exec_row_immediate(short_row, symbol, RUN_MODE)

                if EXEC_AUDIT_ENABLED:
                    try:
                        fieldnames = ["timestamp","symbol","action","price","reason",
                                      "bias","pnl","ema_fast","ema_slow","rsi","vwap",
                                      "regime","code_version"]
                        with open(EXEC_AUDIT_FILE, "a", newline="") as f:
                            writer = csv.DictWriter(f, fieldnames=fieldnames)
                            if f.tell() == 0:
                                writer.writeheader()
                            writer.writerow({k: (round(v, 6) if isinstance(v, float)
                                             and not pd.isna(v) else v)
                                             for k, v in short_row.items()})
                    except Exception as e:
                        logging.warning("Failed to write SHORT to audit file: %s", e)

                POLL_TIMEOUT = 90
                poll_start = datetime.now(timezone.utc)
                filled_qty   = 0.0
                filled_price = None

                while (datetime.now(timezone.utc) - poll_start).total_seconds() < POLL_TIMEOUT:
                    try:
                        current = trade_client_local.get_order_by_id(order_id)
                        last_status = getattr(current, "status", None)
                        raw_filled_qty   = getattr(current, "filled_qty", 0) or 0
                        raw_filled_price = getattr(current, "filled_avg_price", None)

                        try:
                            filled_qty = float(raw_filled_qty)
                        except Exception:
                            filled_qty = 0.0
                        if raw_filled_price is not None:
                            try:
                                filled_price = float(raw_filled_price)
                            except Exception:
                                filled_price = None

                        if _status_is(last_status, "filled") and filled_qty > 0:
                            break

                        if _status_is(last_status, "canceled") or \
                           _status_is(last_status, "rejected"):
                            logging.warning(
                                "[SHORT_CANCELED] %s order %s status=%s — clearing state",
                                symbol, order_id, last_status
                            )
                            short_entry_times.pop(symbol, None)
                            short_entry_prices.pop(symbol, None)
                            short_entry_qty.pop(symbol, None)
                            lowest_price_since_short.pop(symbol, None)
                            short_trailing_active[symbol] = False
                            return None

                    except Exception as e:
                        logging.debug("[SHORT_STATUS_ERROR] %s: %s", symbol, e)

                    time.sleep(0.5)

                if filled_qty > 0 and filled_price is not None:
                    fill_ts = datetime.now(timezone.utc)
                    short_entry_times[symbol]  = fill_ts
                    short_entry_prices[symbol] = filled_price
                    short_entry_qty[symbol]    = filled_qty
                    short_entry_prices[symbol] = filled_price
                    lowest_price_since_short[symbol] = filled_price

                    logging.info(
                        "[SHORT_FILLED] %s fill_price=%.4f qty=%.2f",
                        symbol, filled_price, filled_qty
                    )
                else:
                    logging.warning(
                        "[SHORT_TIMEOUT] %s not confirmed within %ds — "
                        "keeping est_price=%.4f",
                        symbol, POLL_TIMEOUT, est_price
                    )

                return submitted

            except Exception as e:
                logging.exception("[SHORT_INNER_ERROR] %s: %s", symbol, e)
                short_entry_times.pop(symbol, None)
                short_entry_prices.pop(symbol, None)
                short_entry_qty.pop(symbol, None)
                return None

    except Exception as e:
        logging.exception("[SHORT_OUTER_ERROR] %s: %s", symbol, e)
        return None

def safe_market_sell(trade_client_local, symbol, intended_qty, order_lock, price_deques, size_deques,
                     snap_entry_price=None):
    # PATCH18 I1: snap_entry_price passed from main loop because entry_prices[symbol]
    # is already popped before this function is called, causing pnl=nan in audit rows.
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

        # === ACCIDENTAL SHORT GUARD ===
        # If available is negative Alpaca already has a short position — never sell further
        if available < 0:
            logging.warning("[SELL_GUARD] %s available qty is NEGATIVE (%d) — refusing sell to prevent deepening short",
                            symbol, available)
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

            if RUN_MODE in ["SIM", "AGG_SIM"]:
                global positions_map
                positions_map.pop(symbol, None)

                ref_entry = entry_prices.get(symbol, float("nan"))
                last_price = float(price_deques[symbol][-1]) if price_deques[symbol] else 0.0
                
                pnl = (last_price - ref_entry) * qty_to_sell if ref_entry else 0.0
               
                prices_series = pd.Series(price_deques[symbol])
                sizes_series = pd.Series(size_deques[symbol])
                ema_fast_val = _safe_last(compute_ema_from_series(prices_series, EMA_FAST))
                ema_slow_val = _safe_last(compute_ema_from_series(prices_series, EMA_SLOW))
                rsi_val = _safe_last(compute_rsi_from_series(prices_series, RSI_PERIOD))
                vwap_val = _safe_last(compute_vwap_from_ticks(prices_series, sizes_series))
                regime_at_sell = detect_regime(prices_series, sizes_series)

                bias_moment = "bullish" if ema_fast_val > ema_slow_val else "bearish"
                sell_reason = "exit"
                
                sell_row = {
                    "timestamp": _audit_ts(),
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

                return submitted
            # === LIVE branch ===   
            if order_id:
                try:
                    max_retries = RECON_POLL_RETRIES
                    sleep_s = RECON_POLL_SLEEP
                    status = None
                    for attempt in range(max_retries):
                        confirmed = trade_client_local.get_order_by_id(order_id)
                        status = getattr(confirmed, "status", None)
                        logging.info("%s - SELL order %s status=%s (attempt %d/%d)",
                                    symbol, order_id, status, attempt+1, max_retries)
                        if _status_is(status, "filled"):
                            # PATCH18 I1: use snap_entry_price if provided (entry_prices already cleared)
                            ref_entry = snap_entry_price if snap_entry_price is not None \
                                        else entry_prices.get(symbol, float("nan"))
                            last_price = float(getattr(confirmed, "filled_avg_price", getattr(confirmed, "price", 0.0)))
                            pnl = (last_price - ref_entry) * qty_to_sell if ref_entry else 0.0
                            # PATCH17: store confirmed fill price for accurate blacklist PnL accounting
                            _last_sell_fill_price[symbol] = last_price
                            
                            prices_series = pd.Series(price_deques[symbol])
                            sizes_series = pd.Series(size_deques[symbol])
                            ema_fast_val = compute_ema_from_series(prices_series, EMA_FAST).iloc[-1]
                            ema_slow_val = compute_ema_from_series(prices_series, EMA_SLOW).iloc[-1]
                            rsi_val = compute_rsi_from_series(prices_series, RSI_PERIOD).iloc[-1]
                            vwap_val = compute_vwap_from_ticks(prices_series, sizes_series).iloc[-1]
                            regime_at_sell = detect_regime(prices_series, sizes_series)

                            bias_moment = "bullish" if ema_fast_val > ema_slow_val else "bearish"
                            sell_reason = "exit"
                            sell_row = {
                                "timestamp": _audit_ts(),
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
                                "regime": regime_at_sell
                            }
                            exec_rows.append(sell_row)
                            logging.info(f"[TRADE] {symbol} [{RUN_MODE}] SELL @ {last_price:.4f} | PnL={pnl:.4f} | Regime={regime_at_sell}")

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

def safe_market_cover(trade_client_local, symbol, intended_qty, order_lock):
    """
    Covers (closes) a short position with a BUY order.
    Mirror of safe_market_sell but for shorts.
    """
    with order_lock:
        try:
            qty_to_cover = int(intended_qty)
            if qty_to_cover <= 0:
                logging.info("[COVER_SKIP] %s qty_to_cover=%d", symbol, qty_to_cover)
                return None

            order = MarketOrderRequest(
                symbol=symbol,
                qty=qty_to_cover,
                side=OrderSide.BUY,
                type=OrderType.MARKET,
                time_in_force=TimeInForce.DAY
            )
            submitted = trade_client_local.submit_order(order)
            order_id = getattr(submitted, "id", None)

            logging.info("[COVER_SUBMITTED] %s qty=%d order_id=%s",
                         symbol, qty_to_cover, order_id)

            POLL_TIMEOUT = 90
            poll_start = datetime.now(timezone.utc)
            filled_price = None

            while (datetime.now(timezone.utc) - poll_start).total_seconds() < POLL_TIMEOUT:
                try:
                    current = trade_client_local.get_order_by_id(order_id)
                    status  = getattr(current, "status", None)
                    raw_price = getattr(current, "filled_avg_price", None)
                    if raw_price is not None:
                        try:
                            filled_price = float(raw_price)
                        except Exception:
                            pass
                    if _status_is(status, "filled"):
                        break
                except Exception as e:
                    logging.debug("[COVER_STATUS_ERROR] %s: %s", symbol, e)
                time.sleep(0.5)

            cover_price = filled_price or float(
                stock_data_client.get_stock_latest_trade(
                    StockLatestTradeRequest(symbol_or_symbols=symbol)
                )[symbol].price
            )

            ref_short = short_entry_prices.get(symbol)
            qty       = short_entry_qty.get(symbol, intended_qty)
            pnl = (ref_short - cover_price) * qty if ref_short else 0.0

            logging.info("[COVER_FILLED] %s cover_price=%.4f ref_short=%.4f PnL=%.4f",
                         symbol, cover_price,
                         ref_short if ref_short else 0, pnl)

            cover_row = {
                "timestamp": _audit_ts(),
                "symbol": symbol,
                "action": "COVER",
                "price": cover_price,
                "reason": "short_exit",
                "bias": "bearish",
                "pnl": round(pnl, 4),
                "ema_fast": None,
                "ema_slow": None,
                "rsi": None,
                "vwap": None,
                "regime": short_entry_prices.get(symbol, {}) if isinstance(
                          short_entry_prices.get(symbol), dict) else "BEAR",
                "code_version": CODE_VERSION
            }
            exec_rows.append(cover_row)
            write_exec_row_immediate(cover_row, symbol, RUN_MODE)

            if EXEC_AUDIT_ENABLED:
                try:
                    fieldnames = ["timestamp","symbol","action","price","reason",
                                  "bias","pnl","ema_fast","ema_slow","rsi","vwap",
                                  "regime","code_version"]
                    with open(EXEC_AUDIT_FILE, "a", newline="") as f:
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        if f.tell() == 0:
                            writer.writeheader()
                        writer.writerow(cover_row)
                except Exception as e:
                    logging.warning("Failed to write COVER to audit file: %s", e)

            return submitted

        except Exception as e:
            logging.exception("[COVER_ERROR] %s: %s", symbol, e)
            return None

def force_liquidation_at_cutoff(trade_client_local, symbols):
    global _eod_liquidation_fired

    now_utc = datetime.now(timezone.utc)
    now_et = now_utc.astimezone(ZoneInfo("America/New_York"))

    if not (now_et.hour > 15 or (now_et.hour == 15 and now_et.minute >= 55)):
        return

    if _eod_liquidation_fired:
        return

    _eod_liquidation_fired = True
    logging.warning("[EOD] force_liquidation_at_cutoff firing at %s ET (once-only guard active)",
                    now_et.strftime("%H:%M"))

    try:
        resp = stock_data_client.get_stock_latest_trade(
            StockLatestTradeRequest(symbol_or_symbols="SPY")
        )
        spy_close = float(resp["SPY"].price)
        _save_prev_close(spy_close)
        logging.warning("[DAY_REGIME] EOD: saved SPY close=%.4f for tomorrow", spy_close)
    except Exception as e:
        logging.warning("[DAY_REGIME] EOD: could not save SPY close: %s", e)

    positions = None
    for attempt in range(3):
        try:
            positions = trade_client_local.get_all_positions()
            break
        except Exception as e:
            logging.warning("[EOD_LIQ] get_all_positions attempt %d/3 failed: %s",
                            attempt + 1, e)
            if attempt < 2:
                time.sleep(3.0)
    if positions is None:
        logging.error("[EOD_LIQ] Cannot liquidate — get_all_positions failed")
        return
    for p in positions:
        s = p.symbol
        q = int(float(p.qty))

        if q <= 0:
            if q < 0:
                logging.warning("[EOD_GUARD] %s has negative qty=%d — skipping", s, q)
            continue

        if s in _pending_sells:
            logging.warning("[EOD_GUARD] %s sell already inflight — skipping EOD sell", s)
            continue

        try:
            order = MarketOrderRequest(
                symbol=s, qty=q, side=OrderSide.SELL,
                type=OrderType.MARKET, time_in_force=TimeInForce.DAY
            )
            trade_client_local.submit_order(order)
            logging.warning("[EOD] %s forced SELL qty=%d", s, q)

            entry_times.pop(s, None)
            entry_prices.pop(s, None)
            entry_qty.pop(s, None)
            entry_configs.pop(s, None)
            highest_price_since_entry.pop(s, None)
            trailing_active[s] = False
            last_exit_time[s] = now_utc

        except Exception as e:
            logging.exception("[EOD] forced sell error for %s: %s", s, e)

    for sym in symbols:
        session_open_price[sym] = None
        session_high_price[sym] = None
        session_low_price[sym] = None
    logging.warning("[SESSION_RESET] Session tracking cleared for %d symbols", len(symbols))

    # PATCH18 I4: write missed opportunities CSV before clearing deques
    try:
        import csv as _csv
        from datetime import date as _date_eod
        _eod_date = now_et.strftime("%Y-%m-%d")
        _missed_file = f"missed_opportunities_{_eod_date}.csv"
        _price_deques_eod = globals().get("price_deques", {})
        _session_open_eod = globals().get("session_open_price", {})
        _block_reasons_eod = globals().get("_session_block_reasons", {})
        _traded_today = set(globals().get("entry_prices", {}).keys()) | \
                        set(k for k, v in (globals().get("last_exit_time") or {}).items()
                            if v is not None and
                            hasattr(v, 'date') and v.date() == now_et.date())

        _rows = []
        for _sym, _dq in _price_deques_eod.items():
            if len(_dq) < 2:
                continue
            _s_open = _session_open_eod.get(_sym)
            if not _s_open or _s_open <= 0:
                continue
            _s_close = float(_dq[-1])
            _pct = (_s_close - _s_open) / _s_open * 100
            _reasons = _block_reasons_eod.get(_sym, {})
            _top_reason = max(_reasons, key=_reasons.get) if _reasons else "traded_or_no_data"
            _top_count = _reasons.get(_top_reason, 0)
            _traded = _sym in _traded_today
            _rows.append({
                "date": _eod_date,
                "symbol": _sym,
                "session_open": round(_s_open, 4),
                "session_close": round(_s_close, 4),
                "pct_change": round(_pct, 3),
                "traded": _traded,
                "top_block_reason": _top_reason if not _traded else "traded",
                "block_count": _top_count if not _traded else 0,
            })
        _rows.sort(key=lambda r: abs(r["pct_change"]), reverse=True)
        _fieldnames = ["date","symbol","session_open","session_close","pct_change",
                       "traded","top_block_reason","block_count"]
        with open(_missed_file, "w", newline="") as _f:
            _w = _csv.DictWriter(_f, fieldnames=_fieldnames)
            _w.writeheader()
            _w.writerows(_rows)
        logging.warning("[PATCH18] EOD missed opportunities written: %s (%d symbols)",
                        _missed_file, len(_rows))
    except Exception as _eod_err:
        logging.warning("[PATCH18] EOD missed opportunities write failed: %s", _eod_err)

    # PATCH16+PATCH17: clear session loss tracking for new day
    _session_loss_count.clear()
    _session_blacklist.clear()
    _session_blacklist_spy_level.clear()
    _session_blacklist_time.clear()
    _last_sell_fill_price.clear()
    _session_block_reasons.clear()  # PATCH18
    globals()["_consecutive_losses"] = 0
    logging.warning("[PATCH17] Session loss tracking and blacklist state cleared for new day")

    # Clear session open file at EOD so tomorrow starts fresh
    try:
        if os.path.exists(SESSION_OPEN_FILE):
            os.remove(SESSION_OPEN_FILE)
            logging.warning("[SESSION_STATE] EOD: cleared session_open file")
    except Exception as e:
        logging.warning("[SESSION_STATE] EOD: could not clear session_open file: %s", e)


def reattach_orphan_if_needed(symbol, positions_map, entry_times, entry_prices,
                               entry_qty, entry_configs, CONFIG_SESSION):
    from datetime import datetime, timezone

    onchain_qty, onchain_avg = positions_map.get(symbol, (0, 0.0))

    has_local_context = (
        symbol in entry_prices and
        entry_prices[symbol] is not None and
        symbol in entry_times and
        symbol in entry_configs
    )

    if onchain_qty > 0 and not has_local_context:
        # === REATTACH COOLDOWN GUARD ===
        # If we recently sold this symbol, the Alpaca position map may be stale.
        # Do NOT reattach for 30 seconds after a known exit to prevent double-sell shorts.
        _last_exit = last_exit_time.get(symbol)
        if _last_exit is not None:
            _secs_since_exit = (datetime.now(timezone.utc) - _last_exit).total_seconds()
            if _secs_since_exit < 30:
                logging.debug(
                    "[ORPHAN_GUARD][%s] Skipping reattach — exit was %.1fs ago (stale positions_map)",
                    symbol, _secs_since_exit
                )
                return False

        logging.warning(
            "[ORPHAN_REATTACH][%s] Position qty=%d avg=%.4f on Alpaca but no local context. "
            "Reattaching with conservative config.",
            symbol, onchain_qty, onchain_avg
        )
        now = datetime.now(timezone.utc)
        entry_prices[symbol]  = onchain_avg if onchain_avg > 0 else None
        entry_qty[symbol]     = float(onchain_qty)
        entry_times[symbol]   = now
        entry_configs[symbol] = dict(CONFIG_SESSION)
        entry_configs[symbol]["fill_inferred"] = True
        entry_configs[symbol]["orphan_reattached"] = True
        highest_price_since_entry[symbol] = onchain_avg if onchain_avg > 0 else 0.0
        trailing_active[symbol] = False

        logging.warning(
            "[ORPHAN_REATTACH][%s] State written: price=%.4f qty=%.2f",
            symbol, entry_prices[symbol] or 0, entry_qty[symbol]
        )
        return True

    if onchain_qty <= 0 and has_local_context:
        logging.warning(
            "[ORPHAN_CLEANUP][%s] Local context exists but no Alpaca position. Purging.",
            symbol
        )
        entry_times.pop(symbol, None)
        entry_prices.pop(symbol, None)
        entry_qty.pop(symbol, None)
        entry_configs.pop(symbol, None)
        highest_price_since_entry.pop(symbol, None)
        trailing_active[symbol] = False
        return False

    return has_local_context

def warmup_deques(symbols, price_deques, size_deques, time_deques, lookback_minutes=60):
    WARMUP_DURATION_SECONDS = 120
    POLL_INTERVAL_SECONDS = 5
    total_ticks = WARMUP_DURATION_SECONDS // POLL_INTERVAL_SECONDS

    logging.warning(
        "[WARMUP] Starting live tick warmup for %d symbols "
        "(%d seconds, polling every %ds, target=%d ticks per symbol)",
        len(symbols), WARMUP_DURATION_SECONDS, POLL_INTERVAL_SECONDS, total_ticks
    )

    CHUNK_SIZE = 10
    symbol_chunks = [
        symbols[i:i + CHUNK_SIZE]
        for i in range(0, len(symbols), CHUNK_SIZE)
    ]

    for tick_num in range(1, total_ticks + 1):
        tick_start = datetime.now(timezone.utc)

        for chunk in symbol_chunks:
            try:
                resp = stock_data_client.get_stock_latest_trade(
                    StockLatestTradeRequest(symbol_or_symbols=chunk)
                )
                for sym in chunk:
                    try:
                        trade = resp.get(sym)
                        if trade is None:
                            continue
                        price = float(trade.price)
                        size = float(getattr(trade, "size", 1) or 1)
                        ts_val = datetime.now(timezone.utc)
                        bucket_ts = ts_val.replace(microsecond=0)

                        if (len(time_deques[sym]) > 0 and
                                time_deques[sym][-1] == bucket_ts):
                            price_deques[sym][-1] = (
                                price_deques[sym][-1] + price
                            ) / 2.0
                            size_deques[sym][-1] += size
                        else:
                            price_deques[sym].append(price)
                            size_deques[sym].append(size)
                            time_deques[sym].append(bucket_ts)

                    except Exception as e:
                        logging.debug("[WARMUP] %s parse error: %s", sym, e)

            except Exception as e:
                logging.warning("[WARMUP] Chunk %s fetch failed: %s", chunk, e)

        if tick_num % 6 == 0 or tick_num == 1 or tick_num == total_ticks:
            sample_lens = {
                s: len(price_deques[s])
                for s in ["AAPL", "SPY", "NVDA"]
                if s in price_deques
            }
            logging.warning(
                "[WARMUP] Tick %d/%d complete | sample deque lengths: %s",
                tick_num, total_ticks, sample_lens
            )

        elapsed = (datetime.now(timezone.utc) - tick_start).total_seconds()
        sleep_time = max(0.1, POLL_INTERVAL_SECONDS - elapsed)

        if tick_num < total_ticks:
            time.sleep(sleep_time)

    filled = sum(1 for s in symbols if len(price_deques[s]) >= 20)
    logging.warning(
        "[WARMUP] Complete. %d/%d symbols have 20+ ticks. "
        "AAPL deque len=%d | SPY deque len=%d",
        filled, len(symbols),
        len(price_deques.get("AAPL", [])),
        len(price_deques.get("SPY", []))
    )

    low_fill = [s for s in symbols if len(price_deques[s]) < 15]
    if low_fill:
        logging.warning(
            "[WARMUP] %d symbols have <15 ticks (indicators may be unreliable "
            "at session start): %s",
            len(low_fill), low_fill
        )


def reconcile_positions(
    trade_client_local,                    
    symbols,
    positions_map,
    price_deques,
    size_deques,
    entry_times,
    entry_prices,
    entry_qty,
    entry_configs
):
    logging.info("[RECON_START] reconcile_positions start; symbols=%d", len(symbols))
    logging.info("[RECON_DEBUG] entry_times keys: %s", list(entry_times.keys()))
    logging.info("[RECON_DEBUG] entry_prices keys: %s", list(entry_prices.keys()))
    logging.debug("[DICT_ID_RECON] entry_times id=%s entry_prices id=%s entry_qty id=%s entry_configs id=%s",
                  id(entry_times), id(entry_prices), id(entry_qty), id(entry_configs))

    if RUN_MODE in ["SIM", "AGG_SIM"]:
        return
     
    if not RECONCILIATION_ENABLED:
        return

    logging.info(
        "[RECON_START] RUN_MODE=%s | RECONCILIATION_ENABLED=%s | RECON_FORCE_SELL_IF_ORPHAN=%s",
        RUN_MODE, RECONCILIATION_ENABLED, RECON_FORCE_SELL_IF_ORPHAN
    )

    now_ts = datetime.now(timezone.utc)

    for sym in symbols:
        if sym in _pending_sells:
            logging.debug("[RECON][%s] Skipping — sell already inflight from LIVE loop", sym)
            continue
        qty_open, avg_entry = positions_map.get(sym, (0, 0.0))
        if qty_open <= 0:
            if sym in entry_times or sym in entry_qty or sym in entry_configs:
                logging.info("%s - RECON cleanup: no live position, purging local entry state", sym)
                entry_times.pop(sym, None)
                entry_prices.pop(sym, None)
                entry_qty.pop(sym, None)
                entry_configs.pop(sym, None)
                trailing_active[sym] = False
                last_exit_time[sym] = now_ts
            continue

        has_context = (sym in entry_prices) and (sym in entry_configs) and (sym in entry_times)

        if not has_context:
            if RECON_FORCE_SELL_IF_ORPHAN:
                logging.warning("%s - RECON orphan position detected (qty=%d @ %.4f). Attaching minimal context.",
                                sym, qty_open, avg_entry)
                entry_prices[sym] = avg_entry
                entry_qty[sym] = qty_open
                entry_times[sym] = last_exit_time.get(sym, None) or now_ts
                entry_configs[sym] = BEARISH_CONFIG
                trailing_active[sym] = False
            else:
                logging.info("%s - RECON orphan position detected; skip (toggle off).", sym)
                continue

        prices_series = pd.Series(price_deques.get(sym, []))
        sizes_series = pd.Series(size_deques.get(sym, []))

        if len(prices_series) < 5 or len(sizes_series) < 5:
            logging.info("%s - RECON insufficient local series for exit evaluation (prices=%d sizes=%d)",
                         sym, len(prices_series), len(sizes_series))
            continue

        last_price = float(prices_series.iloc[-1])
        ref_entry = entry_prices.get(sym, avg_entry)
        config = entry_configs.get(sym)
        if config is None:
            logging.warning("%s - RECON missing entry config; attaching BEARISH_CONFIG", sym)
            config = BEARISH_CONFIG
            entry_configs[sym] = config

        entry_regime = entry_configs.get(sym, {}).get("regime", detect_regime(prices_series, sizes_series))          
        sell, reason = evaluate_sell(
            sym,
            last_price,
            ref_entry,
            price_deques[sym],
            size_deques[sym],
            entry_times,
            config,
            current_time=now_ts,
            regime=entry_regime,
            log_stack=True
        )

        logging.debug(
            "[RECON_SELL_DECISION][%s] should_exit=%s | reason=%s | last=%.4f | ref=%.4f",
            sym, sell, reason, last_price, ref_entry,
            len(prices_series), len(sizes_series), getattr(config, "name", str(config))
        )
 

        if sell:
            try:
                # === ACCIDENTAL SHORT GUARD IN RECONCILE ===
                recon_available = get_position_qty(trade_client_local, sym)
                if recon_available <= 0:
                    logging.warning("[RECON_GUARD] %s no position available (qty=%d) — skipping reconcile sell",
                                    sym, recon_available)
                    last_exit_time[sym] = now_ts  # ADD THIS
                    entry_times.pop(sym, None)
                    entry_prices.pop(sym, None)
                    entry_qty.pop(sym, None)
                    entry_configs.pop(sym, None)
                    continue

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

                time.sleep(1.0)
                qty_after = get_position_qty(trade_client_local, sym)
                if filled or qty_after == 0:
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
                    if EXEC_AUDIT_ENABLED:
                        try:
                            import csv
                            ema_fast_val = compute_ema_from_series(prices_series, EMA_FAST).iloc[-1]
                            ema_slow_val = compute_ema_from_series(prices_series, EMA_SLOW).iloc[-1]
                            rsi_val = compute_rsi_from_series(prices_series, RSI_PERIOD).iloc[-1]
                            vwap_val = compute_vwap_from_ticks(prices_series, sizes_series).iloc[-1]

                            sell_row = {
                                "timestamp": _audit_ts(now_ts),
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
            et = entry_times.get(sym)
            age_min = ((now_ts - et).total_seconds() / 60.0) if et else None
            if age_min is not None and age_min >= RECON_MAX_STALE_MIN:
                logging.info("%s - RECON stale position age=%.1f min | last=%.4f | entry=%.4f | reason=no-exit",
                             sym, age_min, last_price, ref_entry)


# === STRATEGY HELPERS: BUY/SELL CONDITIONS ===
# === BIAS DETECTION ===
def detect_day_bias(prices_series, ema_fast_series, ema_slow_series, vwap_series):
    try:
        if (len(prices_series) == 0 or len(ema_fast_series) == 0 or
            len(ema_slow_series) == 0 or len(vwap_series) == 0):
            return "bearish"

        last_price = float(prices_series.iloc[-1])
        ema_fast_now = float(ema_fast_series.iloc[-1])
        ema_slow_now = float(ema_slow_series.iloc[-1])
        vwap_now = float(vwap_series.iloc[-1])

        if len(prices_series) >= 10:
            recent_change = (last_price - float(prices_series.iloc[-10])) / float(prices_series.iloc[-10])
        else:
            recent_change = 0.0

        price_above_vwap = last_price >= vwap_now
        price_rising = recent_change > 0.0005

        if price_above_vwap or price_rising:
            return "bullish"
        else:
            return "bearish"

    except Exception as e:
        logging.error("[ERROR] Bias detection failed: %s", e)
        return "bearish"

# === REGIME DETECTOR ===
REGIME_SMOOTH_ENABLED = True
REGIME_TRANSITION = {
    "TREND":     {"TREND": 0.70, "RANGE": 0.15, "LOW_VOL": 0.10, "HIGH_VOL": 0.05},
    "RANGE":     {"TREND": 0.15, "RANGE": 0.65, "LOW_VOL": 0.15, "HIGH_VOL": 0.05},
    "LOW_VOL":   {"TREND": 0.10, "RANGE": 0.15, "LOW_VOL": 0.70, "HIGH_VOL": 0.05},
    "HIGH_VOL":  {"TREND": 0.10, "RANGE": 0.10, "LOW_VOL": 0.05, "HIGH_VOL": 0.75}
}
_last_regime = defaultdict(lambda: None)

# === STEP 2 PATCH: Regime confidence tracking ===
_regime_confidence = defaultdict(int)
REGIME_CONFIDENCE_MIN = 5

def _smooth_regime(sym, raw_regime):
    if not REGIME_SMOOTH_ENABLED:
        _regime_confidence[sym] = REGIME_CONFIDENCE_MIN  # treat as confirmed
        return raw_regime, REGIME_CONFIDENCE_MIN
    prev = _last_regime[sym]
    if prev is None:
        _last_regime[sym] = raw_regime
        _regime_confidence[sym] = 1
        return raw_regime, 1
    # stickiness: if raw flips but probability favors previous, keep previous
    trans = REGIME_TRANSITION.get(prev, {})
    prob_prev = trans.get(prev, 0.5)
    prob_raw = trans.get(raw_regime, 0.0)
    if prob_prev >= prob_raw:
        # keep previous regime — confidence decays by 1 when stickiness overrides
        chosen = prev
        _regime_confidence[sym] = max(1, _regime_confidence[sym] - 1)
    else:
        chosen = raw_regime
        if chosen == prev:
            # same as before — increment confidence up to 20
            _regime_confidence[sym] = min(20, _regime_confidence[sym] + 1)
        else:
            # genuine flip — reset confidence
            _regime_confidence[sym] = 1
    _last_regime[sym] = chosen
    return chosen, _regime_confidence[sym]


def detect_regime(prices_series, sizes_series, debug=False):
    MIN_PRICE_LEN = 30
    EARLY_MINUTES_TREND = 40
    N_ATR = 50
    SLOPE_NORM_THRESH = 0.000005
    VWAP_MULT = 0.998
    BANDWIDTH_RANGE = (0.003, 0.007)
    LOW_VOL_BW_CAP = 0.0035

    price = float(prices_series.iloc[-1]) if len(prices_series) else float("nan")
    ema_fast = compute_ema_from_series(prices_series, EMA_FAST).iloc[-1] if len(prices_series) >= 2 else float("nan")
    ema_slow = compute_ema_from_series(prices_series, EMA_SLOW).iloc[-1] if len(prices_series) >= 2 else float("nan")
    vwap_val = None
    try:
        vwap_series = compute_vwap_from_ticks(prices_series, sizes_series)
        vwap_val = vwap_series.iloc[-1] if len(vwap_series) else float("nan")
    except Exception:
        vwap_val = float("nan")

    slope = None
    try:
        slope = ema_slope(prices_series, EMA_SLOW)
    except Exception:
        slope = float("nan")

    atr_val = None
    try:
        atr_val = compute_atr_from_series(prices_series, ATR_PERIOD)
    except Exception:
        atr_val = float("nan")

    upper, ma, lower, bandwidth = compute_bollinger(prices_series, period=20, std=2.0)
    macd_line, macd_signal, macd_hist = compute_macd(prices_series)
    rsi_series = compute_rsi_from_series(prices_series, RSI_PERIOD)
    rsi_val = rsi_series.iloc[-1] if len(rsi_series) else float("nan")

    minutes = _session_minutes(datetime.now(timezone.utc))
    if debug:
        logging.debug(
            "[DETECT_REGIME_DEBUG] len=%d price=%.4f ema_fast=%.4f ema_slow=%.4f slope=%.6f slope_norm=%.8f vwap=%.4f bandwidth=%.6f atr=%.6f rsi=%.2f minutes=%d",
            len(prices_series),
            price,
            ema_fast if not pd.isna(ema_fast) else float("nan"),
            ema_slow if not pd.isna(ema_slow) else float("nan"),
            slope if slope is not None else float("nan"),
            (slope / price) if (slope is not None and price and price > 0) else float("nan"),
            vwap_val if not pd.isna(vwap_val) else float("nan"),
            bandwidth if not pd.isna(bandwidth) else float("nan"),
            atr_val if not pd.isna(atr_val) else float("nan"),
            rsi_val if not pd.isna(rsi_val) else float("nan"),
            minutes
        )

    if minutes < EARLY_MINUTES_TREND:
        return "TREND" if (slope is not None and slope > 0) else "RANGE"

    pct = 0.5
    if len(prices_series) >= N_ATR + ATR_PERIOD and not pd.isna(atr_val):
        atr_series = prices_series.diff().abs().rolling(ATR_PERIOD).mean()
        hist = atr_series.iloc[-N_ATR:].dropna()
        if len(hist) > 10:
            pct = (hist < atr_val).mean()

    if (
        (pct >= HIGH_VOL_CONFIG.get("ATR_TOP_PCT", 0.90) and not pd.isna(bandwidth) and bandwidth >= 0.006)
        or (not pd.isna(bandwidth) and bandwidth > RANGE_CONFIG.get("BANDWIDTH_MAX", 0.007))
    ):
        return "HIGH_VOL"

    try:
        slope_norm = (slope / price) if (slope is not None and price and price > 0) else float("nan")
    except Exception:
        slope_norm = float("nan")

    if (
        not pd.isna(ema_fast) and not pd.isna(ema_slow) and (ema_fast > ema_slow) and
        not pd.isna(slope_norm) and not pd.isna(price) and price > 0 and
        slope_norm > SLOPE_NORM_THRESH and
        not pd.isna(vwap_val) and (price >= vwap_val * VWAP_MULT)
    ):
        return "TREND"

    if (
        not pd.isna(slope) and
        not pd.isna(bandwidth) and
        BANDWIDTH_RANGE[0] <= bandwidth <= BANDWIDTH_RANGE[1] and
        0.015 <= abs(slope) <= 0.06 and
        0.30 <= pct <= 0.95 and
        not pd.isna(rsi_val) and 20 <= rsi_val <= 80
    ):
        return "DRIFT"

    if (
        not pd.isna(bandwidth) and
        bandwidth <= min(LOW_VOL_CONFIG.get("BANDWIDTH_CAP", 0.0035), 0.0035) and
        not pd.isna(slope) and abs(slope) < 0.005
    ):
        return "LOW_VOL"

    if (
        not pd.isna(bandwidth) and
        RANGE_CONFIG.get("BANDWIDTH_MIN", 0.001) <= bandwidth <= RANGE_CONFIG.get("BANDWIDTH_MAX", 0.007) and
        not pd.isna(slope) and abs(slope) <= 0.02
    ):
        return "RANGE"

    return "RANGE"


# === SHORT ENTRY EVALUATION (BEAR_DAY only) ===
def evaluate_short_entry(sym, price, size, prices_series, sizes_series, ts_val,
                         positions_map, inflight_orders, pending_entries,
                         last_exit, last_buy_time, CONFIG, regime,
                         log_stack=False):
    # === SPY SESSION MOVE GATE FOR SHORTS ===
    # Mirrors the long entry block — shorts available when SPY drops 0.5%+ from open
    # No gap between long block and short availability
    _spy_open = globals().get("today_open_spy")
    _spy_deque = globals().get("price_deques", {}).get("SPY")
    _spy_now = float(_spy_deque[-1]) if _spy_deque and len(_spy_deque) > 0 else None
    if _spy_open and _spy_now and _spy_open > 0:
        _spy_session_move = (_spy_now - _spy_open) / _spy_open
        if _spy_session_move > -0.005:
            logging.debug(
                "[SHORT_BLOCK] %s blocked | SPY session move=%.3f%% — not bearish enough",
                sym, _spy_session_move * 100
            )
            return False, "SPY not bearish enough for shorts", 0.0, {}
    else:
        # SPY data not yet available — fall back to day_regime classification
        _day_regime = globals().get("day_regime", "NEUTRAL_DAY")
        if _day_regime != "BEAR_DAY":
            return False, "not_bear_day", 0.0, {}
        
    ema_fast = compute_ema_from_series(prices_series, EMA_FAST).iloc[-1] \
               if len(prices_series) >= 2 else float('nan')
    ema_slow = compute_ema_from_series(prices_series, EMA_SLOW).iloc[-1] \
               if len(prices_series) >= 2 else float('nan')
    rsi_series_full = compute_rsi_from_series(prices_series, RSI_PERIOD)
    rsi_val = rsi_series_full.iloc[-1] if len(rsi_series_full) else float('nan')
    rsi_prev = rsi_series_full.iloc[-2] if len(rsi_series_full) >= 2 else float('nan')
    vwap_val = compute_vwap_from_ticks(prices_series, sizes_series).iloc[-1] \
               if len(prices_series) else float('nan')
    upper, boll_ma, lower, bandwidth = compute_bollinger(prices_series,
                                        period=RANGE_CONFIG["BOLL_PERIOD"],
                                        std=RANGE_CONFIG["BOLL_STD"])
    slope = ema_slope(prices_series, EMA_SLOW)
    median_vol = sizes_series.median() if len(sizes_series) > 0 else float('nan')
    macd_line, macd_signal, macd_hist = compute_macd(prices_series)
    obv_slope = _obv_slope_proxy(prices_series, sizes_series, window=20)

    if pd.isna(ema_fast) or pd.isna(ema_slow) or pd.isna(vwap_val) or pd.isna(rsi_val):
        logging.debug("[SHORT_BLOCK] %s rejected | Reason=Missing core indicators", sym)
        return False, "Missing core indicators", 0.0, {}

    if pd.isna(rsi_val) or rsi_val <= 0 or rsi_val > 100:
        logging.debug("[SHORT_BLOCK] %s rejected | Reason=RSI invalid (rsi=%.2f)",
                      sym, rsi_val if rsi_val else -1)
        return False, "RSI invalid", 0.0, {}

    since_last_exit = (ts_val - last_exit).total_seconds() \
                      if last_exit is not None else float("inf")
    since_last_buy  = (ts_val - last_buy_time[sym]).total_seconds() \
                      if last_buy_time[sym] is not None else float("inf")

    if since_last_exit < COOLDOWN_SECONDS or since_last_buy < COOLDOWN_SECONDS:
        logging.debug("[SHORT_BLOCK] %s rejected | Reason=Cooldown", sym)
        return False, "Cooldown", 0.0, {}

    if inflight_orders.get(sym) is not None or sym in pending_entries:
        return False, "Order flow block", 0.0, {}

    if sym in short_entry_prices and short_entry_prices.get(sym) is not None:
        return False, "Already short", 0.0, {}

    if sym in entry_prices and entry_prices.get(sym) is not None:
        return False, "Long position open — no short", 0.0, {}

    rsi_overbought = (not pd.isna(rsi_val) and rsi_val > 58)
    rsi_downtick = (not pd.isna(rsi_prev) and not pd.isna(rsi_val)
                    and rsi_val < rsi_prev)
    at_vwap_resistance = (not pd.isna(vwap_val) and price >= vwap_val * 0.999)
    upper_band_touch = (not pd.isna(upper) and price >= upper * 0.998)
    ema_bearish = (not pd.isna(ema_fast) and not pd.isna(ema_slow)
                   and ema_fast < ema_slow)
    slope_negative = (not pd.isna(slope) and slope < 0)
    obv_bearish = (not pd.isna(obv_slope) and obv_slope < 0)
    macd_bearish = (not pd.isna(macd_line) and not pd.isna(macd_signal)
                    and macd_line < macd_signal and macd_hist < 0)
    downticks_ok = False
    if len(prices_series) >= 3:
        last_three = prices_series.iloc[-3:]
        downticks = sum(last_three.diff().fillna(0) < 0)
        downticks_ok = (downticks >= 2)

    vol_not_dry = (not pd.isna(median_vol) and median_vol > 0)

    if not rsi_downtick:
        logging.debug("[SHORT_BLOCK] %s rejected | Reason=RSI not downticking (rsi=%.2f prev=%.2f)",
                      sym, rsi_val, rsi_prev if not pd.isna(rsi_prev) else -1)
        return False, "SHORT rsi_downtick required", 0.0, {}

    if not (at_vwap_resistance or upper_band_touch):
        logging.debug("[SHORT_BLOCK] %s rejected | Reason=Not at resistance "
                      "(price=%.4f vwap=%.4f upper=%.4f)",
                      sym, price,
                      vwap_val if not pd.isna(vwap_val) else -1,
                      upper if not pd.isna(upper) else -1)
        return False, "SHORT not at resistance", 0.0, {}

    if not downticks_ok:
        logging.debug("[SHORT_BLOCK] %s rejected | Reason=Insufficient downticks", sym)
        return False, "SHORT insufficient downticks", 0.0, {}

    signal_stack = {}
    score = 0.0

    score += 1.0 if rsi_overbought else 0.0
    score += 1.0 if at_vwap_resistance else 0.0
    score += 1.0 if upper_band_touch else 0.0
    score += 0.8 if ema_bearish else 0.0
    score += 0.7 if macd_bearish else 0.0
    score += 0.5 if slope_negative else 0.0
    score += 0.5 if obv_bearish else 0.0
    score += 0.3 if vol_not_dry else 0.0

    signal_stack.update({
        "rsi_overbought": rsi_overbought,
        "rsi_downtick": rsi_downtick,
        "at_vwap_resistance": at_vwap_resistance,
        "upper_band_touch": upper_band_touch,
        "ema_bearish": ema_bearish,
        "macd_bearish": macd_bearish,
        "slope_negative": slope_negative,
        "obv_bearish": obv_bearish,
        "downticks_ok": downticks_ok,
        "vol_not_dry": vol_not_dry,
        "rsi_val": round(rsi_val, 2),
        "vwap_val": round(vwap_val, 4) if not pd.isna(vwap_val) else None
    })

    SHORT_ENTRY_THRESHOLD = 2.5
    accept = (score >= SHORT_ENTRY_THRESHOLD)

    if log_stack or accept:
        logging.debug(
            "[SHORT_STACK][%s] score=%.2f threshold=%.2f accept=%s | %s",
            sym, score, SHORT_ENTRY_THRESHOLD, accept, signal_stack
        )

    if not accept:
        logging.debug("[SHORT_BLOCK] %s rejected | score=%.2f < %.2f",
                      sym, score, SHORT_ENTRY_THRESHOLD)
        return False, f"SHORT score={score:.2f} < {SHORT_ENTRY_THRESHOLD}", score, signal_stack

    return True, "short_entry", score, signal_stack

# === REGIME-AWARE ENTRY SCORING (replacement gate) ===
def evaluate_entry(sym, price, size, prices_series, sizes_series, ts_val,
                   positions_map, inflight_orders, pending_entries,
                   last_exit, last_buy_time, CONFIG, regime,
                   bias=None, log_stack=False):

    # PATCH20: SPY is a reference instrument only — never enter as a position
    # PATCH19 blocked SPY from MODE1 only; evaluate_entry had no exclusion.
    # SPY entered twice on Apr 16 (SM75 +$22, SM272 -$0) through TREND path.
    if sym == "SPY":
        return False, "SPY excluded from all entries", 0.0, {}

    ema_fast = compute_ema_from_series(prices_series, EMA_FAST).iloc[-1] if len(prices_series) >= 2 else float('nan')
    ema_slow = compute_ema_from_series(prices_series, EMA_SLOW).iloc[-1] if len(prices_series) >= 2 else float('nan')
    rsi_val = compute_rsi_from_series(prices_series, RSI_PERIOD).iloc[-1] if len(prices_series) else float('nan')
    vwap_val = compute_vwap_from_ticks(prices_series, sizes_series).iloc[-1] if len(sizes_eries) else float('nan')
                       
    # Cooldown
    since_last_exit = (ts_val - last_exit).total_seconds() if last_exit is not None else float("inf")
    since_last_buy = (ts_val - last_buy_time[sym]).total_seconds() if last_buy_time[sym] is not None else float("inf")
    def _regime_cooldown(regime):
        return COOLDOWN_SECONDS

    if since_last_exit < _regime_cooldown(regime) or since_last_buy < _regime_cooldown(regime):
        logging.debug(f"[BLOCK] {sym} rejected | Reason=Cooldown")
        return False, "Cooldown", 0.0, {}
                    
    if regime == "HIGH_VOL" and high_vol_paused[sym]:
        return False, "HIGH_VOL paused", 0.0, {}
                    
    if regime == "TREND" and trend_paused[sym]:
        return False, "TREND paused", 0.0, {}
           
    if regime == "HIGH_VOL":
        logging.debug(f"[BLOCK] {sym} rejected | Reason=HIGH_VOL regime blocked for entries")
        return False, "HIGH_VOL blocked", 0.0, {}
        
    # PATCH17: session blacklist with condition-gated unblock
    if sym in _session_blacklist:
        _spy_at_block = _session_blacklist_spy_level.get(sym)
        _time_at_block = _session_blacklist_time.get(sym)
        _spy_bl_deque = globals().get("price_deques", {}).get("SPY")
        _spy_now_bl = float(_spy_bl_deque[-1]) if _spy_bl_deque and len(_spy_bl_deque) > 0 else None
        _elapsed_block = (ts_val - _time_at_block).total_seconds() if _time_at_block else 0

        _spy_recovered = (
            _spy_at_block is not None and _spy_now_bl is not None and
            (_spy_now_bl - _spy_at_block) / _spy_at_block >= BLACKLIST_UNBLOCK_SPY_RECOVERY_PCT
        )
        _time_ok = _elapsed_block >= BLACKLIST_UNBLOCK_MIN_SECONDS

        if _spy_recovered and _time_ok:
            _session_blacklist.discard(sym)
            _session_loss_count[sym] = 0
            _session_blacklist_spy_level.pop(sym, None)
            _session_blacklist_time.pop(sym, None)
            logging.warning(
                "[PATCH17] %s removed from session blacklist | "
                "SPY recovered %.2f%% from block level | elapsed %.0f min",
                sym,
                (_spy_now_bl - _spy_at_block) / _spy_at_block * 100,
                _elapsed_block / 60
            )
            # fall through to normal entry evaluation
        else:
            logging.debug(
                "[BLOCK] %s session blacklist | spy_recovered=%s (need +%.1f%%) "
                "time_ok=%s (elapsed %.0f/%.0f min)",
                sym, _spy_recovered,
                BLACKLIST_UNBLOCK_SPY_RECOVERY_PCT * 100,
                _time_ok, _elapsed_block / 60,
                BLACKLIST_UNBLOCK_MIN_SECONDS / 60
            )
            return False, "Session blacklist", 0.0, {}
            
    if regime_trades[regime] >= 5:
        sls = exit_reason_count[regime].get("Stop-loss", 0)
        net = regime_pnl[regime]
        if (sls / regime_trades[regime] >= 0.6) and (net < 0):
            return False, f"{regime} paused by FitScore", 0.0, {}
               
    last_exit = last_exit_time.get(sym)
    if last_exit:
        secs_since_exit = (datetime.now(timezone.utc) - last_exit).total_seconds()
        if secs_since_exit < 10:
            return False, "Cooldown block", 0.0, {}

        _last_reason = last_exit_reason.get(sym)
        if (regime == "TREND" and
                _last_reason in ("EMA fail", "VWAP fail") and
                secs_since_exit < TREND_REENTRY_BLOCK_SECONDS):
            logging.debug(
                "[BLOCK] %s TREND re-entry blocked | last_exit_reason=%s "
                "secs_since_exit=%.0f < %d",
                sym, _last_reason, secs_since_exit, TREND_REENTRY_BLOCK_SECONDS
            )
            return False, f"TREND reentry blocked after {_last_reason}", 0.0, {}
                   
    if inflight_orders.get(sym) is not None or sym in pending_entries:
        return False, "Order flow block", 0.0, {}
            
    macd_line, macd_signal, macd_hist = compute_macd(prices_series)

    median_vol = sizes_series.median() if len(sizes_series) > 0 else float('nan')
    vol_spike = (not pd.isna(median_vol)) and (size > (median_vol * VOL_SPIKE_MULT))

    upper, boll_ma, lower, bandwidth = compute_bollinger(prices_series, period=RANGE_CONFIG["BOLL_PERIOD"], std=RANGE_CONFIG["BOLL_STD"])
    atr_val = compute_atr_from_series(prices_series, ATR_PERIOD)
    slope = ema_slope(prices_series, EMA_SLOW)

    adx_val = _adx_proxy(prices_series) if ADX_ENABLED else float('nan')
    chop_val = _choppiness_proxy(prices_series) if CHOP_ENABLED else float('nan')

    REGIME_RSI_BANDS = {
        "TREND": (32, 70),
        "RANGE": (28, 70),
        "LOW_VOL": (30, 75),
    }
    
    def rsi_in_band(regime, rsi):
        lo, hi = REGIME_RSI_BANDS.get(regime, (MIN_RSI_FOR_ENTRY, MAX_RSI_FOR_ENTRY))
        return (rsi >= lo) and (rsi <= hi)
    
    rsi_series_full = compute_rsi_from_series(prices_series, RSI_PERIOD)
    rsi_prev = rsi_series_full.iloc[-2] if len(rsi_series_full) >= 2 else float('nan')
    rsi_uptick = (not pd.isna(rsi_prev) and not pd.isna(rsi_val) and rsi_val > rsi_prev)
    
    rsi_ok = rsi_in_band(regime, rsi_val) and rsi_uptick

    pullback_to_ema = (not pd.isna(ema_slow) and abs(price - ema_slow) / price <= TREND_CONFIG["PULLBACK_TOL"])
    pullback_to_vwap = (not pd.isna(vwap_val) and abs(price - vwap_val) / price <= TREND_CONFIG["PULLBACK_TOL"])

    lower_touch = (not pd.isna(lower) and price <= lower * (1 + 0.0002))
    upper_touch = (not pd.isna(upper) and price >= upper * (1 - 0.0002))
    vwap_reversion_room = (not pd.isna(vwap_val) and (vwap_val - price) / vwap_val >= 0.0008)

    N = HIGH_VOL_CONFIG["ATR_WINDOW"]
    atr_series = prices_series.diff().abs().rolling(ATR_PERIOD).mean() if len(prices_series) >= ATR_PERIOD else pd.Series([])
    atr_hist = atr_series.iloc[-N:].dropna() if len(atr_series) else pd.Series([])
    atr_pct = (atr_hist < atr_val).mean() if len(atr_hist) > 10 and not pd.isna(atr_val) else 0.5
    bb_expanding = (not pd.isna(bandwidth) and bandwidth > RANGE_CONFIG["BANDWIDTH_MAX"])
    vol_roc_val = volume_roc(sizes_series, HIGH_VOL_CONFIG["VOL_ROC_WINDOW"]) if len(sizes_series) else float('nan')
    vol_roc_ok = (not pd.isna(vol_roc_val) and vol_roc_val > 0.35)
    breakout_bar = (price > (recent_high(prices_series, HIGH_VOL_CONFIG["BREAKOUT_LOOKBACK"]) * 1.002))

    envelope_lower = (ema_slow * (1 - LOW_VOL_CONFIG["ENVELOPE_PCT"])) if not pd.isna(ema_slow) else float('nan')
    envelope_touch = (not pd.isna(envelope_lower) and price <= envelope_lower)
    chop_high = (not pd.isna(bandwidth) and bandwidth <= LOW_VOL_CONFIG["BANDWIDTH_CAP"])
    vol_ok_low = vol_spike or (not pd.isna(median_vol) and median_vol > 0)
    vwap_below = (not pd.isna(vwap_val) and price < vwap_val)

    if pd.isna(ema_fast) or pd.isna(ema_slow) or pd.isna(vwap_val) or pd.isna(rsi_val):
        logging.debug(f"[BLOCK] {sym} rejected | Reason=Missing core indicators")
        return False, "Missing core indicators", 0.0, {}

    if pd.isna(rsi_val) or rsi_val <= 0 or rsi_val > 100:
        logging.debug("[BLOCK] %s rejected | Reason=RSI invalid (rsi=%.2f)", sym, rsi_val if rsi_val else -1)
        return False, "RSI invalid", 0.0, {}

    # PATCH15/PATCH18: RSI ceiling tiers.
    # PATCH18 C2: market_char used as fallback — prevents NEUTRAL_DAY from locking
    #             ceiling at 70 even when intraday character is clearly bullish.
    # PATCH18 I3: TREND regime baseline raised from 70 to 75 — TREND breakouts
    #             naturally elevate RSI; 70 ceiling contradicts the regime's own logic.
    _rsi_day_regime = globals().get("day_regime", "NEUTRAL_DAY")
    _rsi_market_char = globals().get("_market_character", "NEUTRAL")
    _rsi_spy_open = globals().get("today_open_spy")
    _rsi_spy_deque = globals().get("price_deques", {}).get("SPY")
    _rsi_spy_now = float(_rsi_spy_deque[-1]) if _rsi_spy_deque and len(_rsi_spy_deque) > 0 else None
    # PATCH18: bull condition = BULL_DAY OR market_char showing bullish intraday
    _rsi_bull_condition = (
        _rsi_day_regime == "BULL_DAY" or
        _rsi_market_char in ("STRONG_BULL", "MILD_BULL")
    )
    if _rsi_bull_condition and _rsi_spy_open and _rsi_spy_now:
        _rsi_spy_move = (_rsi_spy_now - _rsi_spy_open) / _rsi_spy_open
        if _rsi_spy_move >= 0.010:
            RSI_ENTRY_CEILING = 88  # very strong bull day — allow high RSI momentum entries
        elif _rsi_spy_move >= 0.005:
            RSI_ENTRY_CEILING = 82  # mild bull day — moderate relaxation
        else:
            RSI_ENTRY_CEILING = 75  # early bull / mild char — slight relaxation
    else:
        # PATCH18 I3: TREND regime raised from 70 to 75 — breakouts naturally push RSI above 70
        if regime == "TREND":
            RSI_ENTRY_CEILING = 75
        else:
            RSI_ENTRY_CEILING = 70  # RANGE/DRIFT/other — keep strict ceiling
            
    if rsi_val > RSI_ENTRY_CEILING:
        logging.debug("[BLOCK] %s rejected | Reason=RSI overbought at entry (rsi=%.2f > %d)",
                      sym, rsi_val, RSI_ENTRY_CEILING)
        return False, f"RSI overbought block (rsi={rsi_val:.1f})", 0.0, {}

    # === PATCH17: Dynamic SPY bearish override — two-condition logic ===
    # Replaces the single -0.50% static gate which missed sustained soft declines.
    # Condition 1 (hard): SPY below -0.40% from open — block regardless of direction.
    # Condition 2 (dynamic): SPY below -0.20% AND direction FALLING — sustained fade block.
    _spy_open = globals().get("today_open_spy")
    _spy_deque = globals().get("price_deques", {}).get("SPY")
    _spy_now = float(_spy_deque[-1]) if _spy_deque and len(_spy_deque) > 0 else None
    if _spy_open and _spy_now and _spy_open > 0:
        _spy_session_move = (_spy_now - _spy_open) / _spy_open
        _spy_dir_entry = get_spy_direction()
        if _spy_session_move <= SPY_BEARISH_HARD_THRESHOLD:
            logging.debug(
                "[BLOCK] %s blocked | SPY move=%.3f%% <= %.1f%% — hard bearish override",
                sym, _spy_session_move * 100, SPY_BEARISH_HARD_THRESHOLD * 100
            )
            return False, "SPY session bearish override", 0.0, {}
        if _spy_session_move <= SPY_BEARISH_DIRECTION_THRESHOLD and _spy_dir_entry == "FALLING":
            logging.debug(
                "[BLOCK] %s blocked | SPY move=%.3f%% AND direction=FALLING — dynamic bearish override",
                sym, _spy_session_move * 100
            )
            return False, "SPY dynamic bearish override", 0.0, {}

    # === SPY REALIZED VOLATILITY ENTRY FILTER ===
    _vol_state = get_spy_volatility_state()
    if _vol_state == "EXTREME":
        logging.debug(
            "[BLOCK] %s blocked | SPY volatility EXTREME — no entries in chaotic market",
            sym
        )
        return False, "SPY volatility EXTREME — entries blocked", 0.0, {}
    if _vol_state == "ELEVATED":
        # In elevated volatility only allow entry if price is very close to VWAP
        # Tighten the VWAP extension limit from 0.3% to 0.1%
        if not pd.isna(vwap_val) and vwap_val > 0:
            _tight_extension = (price - vwap_val) / vwap_val
            if _tight_extension > 0.001:
                logging.debug(
                    "[BLOCK] %s blocked | SPY volatility ELEVATED + price too extended (%.4f > 0.001)",
                    sym, _tight_extension
                )
                return False, "SPY volatility ELEVATED — tight VWAP extension block", 0.0, {}

    # === SPY DIRECTIONAL FADE BLOCK ===
    # Block new entries when SPY is in a sustained intraday downtrend
    # This catches afternoon weakness even on positive SPY days
    # Different from ATR volatility — this measures direction not choppiness
    _spy_direction = get_spy_direction()
    if _spy_direction == "FALLING":
        logging.debug(
            "[BLOCK] %s blocked | SPY direction FALLING — no new longs during sustained fade",
            sym
        )
        return False, "SPY direction falling — entry blocked", 0.0, {}
                       
    market_trend = globals().get("market_trend_state", "unknown")
    gate_ok, gate_reason = gate_entry(
        sym, regime, prices_series, sizes_series, vwap_val,
        compute_rsi_from_series(prices_series, RSI_PERIOD),
        market_trend_state=market_trend
    )
    if not gate_ok:
        logging.debug("[BLOCK] %s rejected by gate_entry | Reason=%s", sym, gate_reason)
        return False, gate_reason, 0.0, {}

    # === PATCH17: TREND/DRIFT gate with DRIFT_ENABLED toggle ===
    _allowed_entry_regimes = ("TREND", "DRIFT") if DRIFT_ENABLED else ("TREND",)
    if regime not in _allowed_entry_regimes:
        logging.debug(
            "[BLOCK] %s blocked | regime=%s — only %s entries allowed",
            sym, regime, "/".join(_allowed_entry_regimes)
        )
        return False, f"Regime {regime} blocked — only {'/'.join(_allowed_entry_regimes)}", 0.0, {}

    # === VWAP PROXIMITY GUARDS for TREND and DRIFT ===
    # Price must be above VWAP — below VWAP contradicts bullish momentum
    if not pd.isna(vwap_val) and vwap_val > 0:
        if price <= vwap_val:
            logging.debug(
                "[BLOCK] %s blocked | price=%.4f <= vwap=%.4f — below VWAP on TREND/DRIFT entry",
                sym, price, vwap_val
            )
            return False, "TREND/DRIFT entry blocked — price below VWAP", 0.0, {}
        # Price must not be too extended above VWAP — move already run
        _vwap_extension = (price - vwap_val) / vwap_val
        if _vwap_extension > 0.003:
            logging.debug(
                "[BLOCK] %s blocked | vwap_extension=%.4f > 0.003 — price too extended above VWAP",
                sym, _vwap_extension
            )
            return False, "TREND/DRIFT entry blocked — price too extended above VWAP", 0.0, {}

    # PATCH16: bearish bias + RANGE is a falling knife — hard block
    if bias == "bearish" and regime == "RANGE":
        logging.debug(
            "[BLOCK] %s rejected | Reason=Bearish bias + RANGE hard block (rsi=%.2f)",
            sym, rsi_val
        )
        return False, "Bearish+RANGE hard block", 0.0, {}

    if bias == "bearish":
        logging.debug(
            "[BLOCK] %s rejected | Reason=Bearish bias hard block (rsi=%.2f)",
            sym, rsi_val
        )
        return False, "Bearish bias hard block", 0.0, {}
               
    signal_stack = {}
    score = 0.0

    obv_slope = _obv_slope_proxy(prices_series, sizes_series, window=20)                   
    if regime == "TREND":
        w = TREND_CONFIG["WEIGHTS"]
        # PATCH14: 5-bar breakout as primary trigger, EMA as filter not trigger
        _BREAKOUT_BARS = 5
        _BREAKOUT_MIN_PCT = 0.0005  # price must be at least 0.05% above 5-bar high
        _five_bar_high = prices_series.iloc[-(_BREAKOUT_BARS + 1):-1].max() \
            if len(prices_series) >= _BREAKOUT_BARS + 1 else float('nan')
        breakout_5bar = (
            not pd.isna(_five_bar_high) and
            price > _five_bar_high * (1 + _BREAKOUT_MIN_PCT)
        )
        # Momentum guard: last 2 bars must both be rising
        _momentum_ok = (
            len(prices_series) >= 3 and
            prices_series.iloc[-1] > prices_series.iloc[-2] > prices_series.iloc[-3]
        ) if len(prices_series) >= 3 else False
        # EMA remains as filter: trend must be up, not trigger
        ema_filter_ok = (ema_fast > ema_slow)
        # Entry requires breakout + momentum + EMA filter
        ema_trend_ok = breakout_5bar and _momentum_ok and ema_filter_ok
        signal_stack["breakout_5bar"] = breakout_5bar
        signal_stack["momentum_ok"] = _momentum_ok
        signal_stack["ema_filter_ok"] = ema_filter_ok
        logging.debug(
            "[PATCH14][%s] breakout_5bar=%s 5bar_high=%.4f price=%.4f "
            "momentum_ok=%s ema_filter=%s",
            sym, breakout_5bar,
            _five_bar_high if not pd.isna(_five_bar_high) else -1,
            price, _momentum_ok, ema_filter_ok
        )
        strong_trend = (not pd.isna(slope) and slope > 0) and \
                       (not pd.isna(adx_val) and adx_val >= 25)
        
        strong_trend = (not pd.isna(slope) and slope > 0) and (not pd.isna(adx_val) and adx_val >= 25)
        vwap_above_ok = (price > vwap_val) and (
            ((price - vwap_val) > CONFIG["VWAP_DELTA"] * vwap_val) if not strong_trend
            else ((price - vwap_val) > (CONFIG["VWAP_DELTA"] * 0.6) * vwap_val)
        )
        macd_ok = (
            not pd.isna(macd_line) and not pd.isna(macd_signal)
            and macd_line > macd_signal
            and macd_hist > 0.03
        )
        pullback_ok = (pullback_to_ema or pullback_to_vwap)
        vol_ok = vol_spike
        obv_ok = (not pd.isna(obv_slope) and obv_slope > 0)

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

        adx_ok = (not pd.isna(adx_val) and adx_val >= 20)
        signal_stack["adx_ok"] = adx_ok
        score += 0.4 if adx_ok else 0.0

        # PATCH17: SPY momentum as weighted scoring component
        # Makes SPY context part of entry quality score, not just an external veto
        _spy_dir_score = get_spy_direction()
        _market_char_score = globals().get("_market_character", "NEUTRAL")

        # PATCH18 I2: reduced bonus weights — RISING +0.5→+0.3, MILD_BULL +0.3→+0.15
        # Evidence: NVDA base score 1.30 was carried to 2.10 by bonuses alone on Apr 13.
        # Symbol had no breakout, no momentum, no MACD, not above VWAP — should have been rejected.
        if _spy_dir_score == "RISING":
            score += 0.3   # PATCH18: reduced from 0.5
            signal_stack["spy_momentum"] = "+0.3 (SPY RISING)"
        elif _spy_dir_score == "FALLING":
            score -= 1.0
            signal_stack["spy_momentum"] = "-1.0 (SPY FALLING)"
        else:
            signal_stack["spy_momentum"] = "0.0 (SPY FLAT)"

        if _market_char_score in ("STRONG_BULL", "MILD_BULL"):
            score += 0.15  # PATCH18: reduced from 0.3
            signal_stack["market_char_score"] = f"+0.15 ({_market_char_score})"
        elif _market_char_score in ("RECOVERING", "BEAR"):
            score -= 0.3
            signal_stack["market_char_score"] = f"-0.3 ({_market_char_score})"
        else:
            signal_stack["market_char_score"] = f"0.0 ({_market_char_score})"

        logging.debug(
            "[PATCH18][TREND_SCORE][%s] spy_dir=%s char=%s "
            "spy_adj=%.2f char_adj=%.2f final_score=%.2f threshold=%.2f",
            sym, _spy_dir_score, _market_char_score,
            0.3 if _spy_dir_score == "RISING" else (-1.0 if _spy_dir_score == "FALLING" else 0.0),
            0.15 if _market_char_score in ("STRONG_BULL", "MILD_BULL") else
            (-0.3 if _market_char_score in ("RECOVERING", "BEAR") else 0.0),
            score,
            adaptive_entry_threshold(CONFIG, sym, regime)
        )

    
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
            return False, "DRIFT score block", score, signal_stack
                    
        if not _confirm_trend(prices_series, vwap_val, ema_slow):
            return False, "DRIFT confirm block", score, signal_stack
                   
        return True, "entry", score, signal_stack
            
    elif regime == "RANGE":
        w = RANGE_CONFIG["WEIGHTS"]

        rsi_band_ok = (rsi_val >= 18) and (rsi_val <= 38)
        rsi_uptick_ok = rsi_uptick

        if RANGE_STRICT_TOUCH_ENABLED:
            lower_band_touch = (not pd.isna(lower) and price <= lower)
        else:
            lower_band_touch = (not pd.isna(lower) and price <= lower * (1 + RANGE_TOUCH_EPSILON))

        bandwidth_ok = (
            not pd.isna(bandwidth)
            and RANGE_CONFIG["BANDWIDTH_MIN"] <= bandwidth <= min(RANGE_CONFIG["BANDWIDTH_MAX"], 0.010)
        )

        vwap_rev_ok = (
            not pd.isna(vwap_val)
            and (vwap_val - price) / vwap_val >= RANGE_VWAP_ROOM_MIN
        )

        vol_not_dry = (not pd.isna(median_vol) and median_vol > 0)

        obv_ok = (not pd.isna(obv_slope) and obv_slope >= 0)

        range_bull_bias_ok = (
            vwap_rev_ok and
            rsi_band_ok 
        )

        if not range_bull_bias_ok:
            logging.debug(f"[BLOCK] {sym} rejected | Reason=RANGE_BULL local bias block")
            return False, "RANGE_BULL bias block", 0.0, {}

        if len(prices_series) >= 3:
            last_three = prices_series.iloc[-3:]
            upticks = sum(last_three.diff().fillna(0) > 0)
            if upticks < 2:
                logging.debug("[BLOCK] %s rejected | Reason=RANGE insufficient upticks (%d/2)", sym, upticks)
                return False, "RANGE insufficient upticks", 0.0, {}
                
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
            return False, "Range blocked by BB ROC", 0.0, {}
                
        signal_stack.update({
            "lower_band_touch": lower_band_touch,
            "rsi_band_ok": rsi_band_ok,
            "rsi_uptick": rsi_uptick_ok,
            "vwap_reversion": vwap_rev_ok,
            "bandwidth_ok": bandwidth_ok,
            "vol_not_dry": vol_not_dry,
            "obv_slope_ok": obv_ok,
        })

        score += w["lower_band_touch"] if lower_band_touch else 0.0
        score += w["rsi_uptick"] if (rsi_band_ok and rsi_uptick_ok) else 0.0
        score += w["vwap_reversion"] if vwap_rev_ok else 0.0
        score += w["bandwidth_ok"] if bandwidth_ok else 0.0
        score += w["vol_not_dry"] if vol_not_dry else 0.0
        score += w["rsi_ok"] if rsi_band_ok else 0.0

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
        logging.debug(f"[BLOCK] {sym} rejected | Reason=LOW_VOL regime blocked for entries")
        return False, "LOW_VOL blocked", 0.0, {}
            
        w = LOW_VOL_CONFIG["WEIGHTS"]

        ema_momentum_ok = (not pd.isna(ema_fast) and not pd.isna(ema_slow) and ema_fast >= ema_slow)
        
        vwap_below_ok = vwap_below
        rsi_mr_ok = (rsi_val < 35 and rsi_uptick)
        envelope_touch_ok = envelope_touch
        chop_ok = chop_high
        vol_ok = vol_ok_low

        if not ema_momentum_ok:
            logging.debug(f"[BLOCK] {sym} rejected | Reason=LOW_VOL ema_momentum_ok=False (ema_fast={ema_fast:.4f} ema_slow={ema_slow:.4f})")
            return False, "LOW_VOL EMA momentum block", 0.0, {}
                          

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

        chop_proxy_ok = (not pd.isna(chop_val) and chop_val >= 1.2)
        signal_stack["chop_proxy_ok"] = chop_ok
        score += 0.3 if chop_ok else 0.0
        
        obv_ok = (not pd.isna(obv_slope) and obv_slope > 0)
        signal_stack["obv_slope_ok"] = obv_ok
        
        if bias == "bearish":
            if not obv_ok:
                return False, "LOW_VOL bearish blocked by OBV slope<=0", score, signal_stack
                                    
        else:
            if not obv_ok:
                score -= 0.5

        vwap_dist = abs(price - vwap_val) / vwap_val if not pd.isna(vwap_val) and vwap_val > 0 else 0.0
        VWAP_DIST_MAX = 0.025

        if vwap_dist > VWAP_DIST_MAX:
            logging.debug(f"[BLOCK] {sym} rejected | Reason=VWAP distance {vwap_dist:.4f} > {VWAP_DIST_MAX:.4f}")
            return False, "VWAP distance block", 0.0, {}
                
                       
    confirm_ok = True
    if ENTRY_CONFIRM_ENABLED:
        if regime == "TREND":
            confirm_ok = _confirm_trend(prices_series, vwap_val, ema_slow)
        elif regime == "RANGE":
            confirm_ok = _confirm_range(prices_series, lower)
        elif regime == "LOW_VOL":
            confirm_ok = _confirm_low_vol(prices_series, vwap_val)
        else:
            rh = recent_high(prices_series, HIGH_VOL_CONFIG["BREAKOUT_LOOKBACK"])
            if pd.isna(rh) or len(prices_series) < ENTRY_CONFIRM_TICKS + 1:
                confirm_ok = False
            else:
                tail = prices_series.iloc[-ENTRY_CONFIRM_TICKS-1:]
                confirm_ok = (tail.iloc[-ENTRY_CONFIRM_TICKS] > rh) and all(tail.diff().fillna(0) > 0)

    CONFIG_SESSION = overlay_by_session(CONFIG, ts_val, regime)                   
    threshold = adaptive_entry_threshold(CONFIG_SESSION, sym, regime)
    accept = (score >= threshold) and confirm_ok
        
    if log_stack and (accept or AUDIT_TRAIL_ENABLED):
        logging.debug(f"[ENTRY_STACK][{sym}] regime={regime} score={score:.2f} threshold={threshold} stack={signal_stack}")

    return (
        accept,
            f"Regime={regime} score={score:.2f}",
            score,
            signal_stack,
    )

# === EXIT OVERLAY BY REGIME ===
def overlay_exit_params_by_regime(CONFIG, regime):
    base_tp      = CONFIG.get("TP_PCT", TP_PCT)
    base_sl_mult = CONFIG.get("SL_MULTIPLIER", SL_MULTIPLIER)
    base_ts_act  = CONFIG.get("TS_ACTIVATION_BUFFER", TS_ACTIVATION_BUFFER)

    adj = dict(CONFIG)

    if regime == "RANGE":
        adj["TP_PCT"] = max(0.0010, base_tp * 0.8)
        adj["SL_MULTIPLIER"] = max(0.6, base_sl_mult * 0.9)
        adj["TS_ACTIVATION_BUFFER"] = max(0.002, base_ts_act * 0.8)

    elif regime == "HIGH_VOL":
        adj["TP_PCT"] = min(0.0050, base_tp * 1.4)
        adj["SL_MULTIPLIER"] = min(1.5, base_sl_mult * 1.2)
        adj["TS_ACTIVATION_BUFFER"] = min(0.008, base_ts_act * 1.4)

    elif regime == "LOW_VOL":
        adj["TP_PCT"] = max(0.0010, base_tp * 0.9)
        adj["SL_MULTIPLIER"] = max(0.7, base_sl_mult * 0.9)
        adj["TS_ACTIVATION_BUFFER"] = max(0.0025, base_ts_act * 0.85)

    return adj


def evaluate_short_exit(sym, last_price, ref_short_entry, CONFIG,
                        current_time=None, regime=None):
    if ref_short_entry is None or ref_short_entry == 0:
        return False, None

    now_ts = current_time or datetime.now(timezone.utc)
    entry_time = short_entry_times.get(sym)
    elapsed = (now_ts - entry_time).total_seconds() \
              if isinstance(entry_time, datetime) else 0.0

    minutes = _session_minutes(now_ts)
    if minutes >= 360:
        return True, "SHORT EOD exit"

    if last_price >= ref_short_entry * 1.010:
        return True, "SHORT emergency SL"

    emergency_sl_pct = float(CONFIG.get("EMERGENCY_SL_PCT", 0.005))
    if last_price >= ref_short_entry * (1 + emergency_sl_pct):
        return True, "SHORT stop-loss"

    tp_pct = float(CONFIG.get("TP_PCT", 0.002))
    if last_price <= ref_short_entry * (1 - tp_pct):
        return True, "SHORT take-profit"

    ts_activation = float(CONFIG.get("TS_ACTIVATION_BUFFER", 0.003))
    trailing_pct  = float(CONFIG.get("TRAILING_STOP_PCT", 0.004))

    if last_price <= ref_short_entry * (1 - ts_activation):
        short_trailing_active[sym] = True
        lowest_price_since_short[sym] = min(
            lowest_price_since_short.get(sym, ref_short_entry),
            last_price
        )

    if short_trailing_active.get(sym, False):
        trough = lowest_price_since_short.get(sym, ref_short_entry)
        pullback_pct = (last_price - trough) / trough if trough > 0 else 0
        if pullback_pct >= trailing_pct:
            return True, "SHORT trailing stop"

    if elapsed >= 180:
        progress = (ref_short_entry - last_price) / ref_short_entry
        if progress < 0.001:
            return True, "SHORT time-stop"

    if elapsed >= MAX_HOLD_SECONDS:
        return True, "SHORT max hold"

    return False, None

def evaluate_sell(
    sym,
    last_price,
    ref_entry,
    price_deque,
    size_deque,
    entry_times,
    CONFIG,
    ema_fast_period=EMA_FAST,
    ema_slow_period=EMA_SLOW,
    rsi_period=RSI_PERIOD,
    current_time=None,
    regime=None,
    log_stack=False,
    entry_config=None,        # Option C: entry snapshot (vwap, rsi at entry)
    regime_confidence=None    # Option B: current consecutive-tick confidence count
):
    logging.debug("[%s] ENTER evaluate_sell | last_price=%.4f | regime=%s",
                  sym, last_price, regime)

    if CONFIG is None:
        return False, None

    if ref_entry is None or ref_entry == 0:
        logging.warning("[%s] ref_entry missing or zero; using last_price as fallback", sym)
        ref_entry = last_price


    try:
        now_ts = current_time or datetime.now(timezone.utc)
        entry_time = entry_times.get(sym)
        if isinstance(entry_time, datetime):
            elapsed = (now_ts - entry_time).total_seconds()
        else:
            entry_time = None
            elapsed = 0.0

        # ============================================================
        # 1. HARD TIME-BASED EXIT (EOD EXIT)
        # ============================================================
        minutes = _session_minutes(now_ts)
        SESSION_LENGTH_MIN = 390
        if minutes >= SESSION_LENGTH_MIN - 30:
            logging.info("[%s] EXIT evaluate_sell | reason=EOD_exit | minutes=%d",
                         sym, minutes)
            return True, "EOD exit"

        # ============================================================
        # 1b. APPLY SESSION STATE MULTIPLIERS
        # ============================================================
        _session_state = globals().get("_current_session_state", "NEUTRAL_SESSION")
        CONFIG = apply_session_exit_multipliers(CONFIG, _session_state)

        # ============================================================
        # 2. EMERGENCY EXITS (catastrophic SL)
        # ============================================================
        if last_price <= ref_entry * 0.95:
            logging.info("[%s] EXIT evaluate_sell | reason=5%% stop-loss | last=%.4f | ref=%.4f",
                         sym, last_price, ref_entry)
            _rocket_mode_active.pop(sym, None)
            _rocket_mode_peak.pop(sym, None)
            return True, "5% stop-loss"

        # PATCH16: adaptive stop — tighter in adverse market conditions
        _market_char_sl = globals().get("_market_character", "NEUTRAL")
        _day_regime_sl = globals().get("day_regime", "NEUTRAL_DAY")
        _consec_losses = globals().get("_consecutive_losses", 0)
        _use_tight_sl = (
            _market_char_sl in ("NEUTRAL", "RECOVERING", "BEAR") or
            _day_regime_sl != "BULL_DAY" and _consec_losses >= ADAPTIVE_SL_CONSECUTIVE_LOSSES
        )
        if _use_tight_sl:
            emergency_sl_pct = ADAPTIVE_SL_TIGHT_PCT
            logging.debug(
                "[PATCH16][SL] %s using tight stop %.4f | char=%s regime=%s consec=%d",
                sym, emergency_sl_pct, _market_char_sl, _day_regime_sl, _consec_losses
            )
        else:
            emergency_sl_pct = float(CONFIG.get("EMERGENCY_SL_PCT", 0.01))
        if last_price <= ref_entry * (1 - emergency_sl_pct):
            logging.info("[%s] EXIT evaluate_sell | reason=Emergency SL (session=%s) | "
                         "last=%.4f | ref=%.4f | sl_pct=%.4f",
                         sym, _session_state, last_price, ref_entry, emergency_sl_pct)
            _rocket_mode_active.pop(sym, None)
            _rocket_mode_peak.pop(sym, None)
            return True, "Emergency SL"

        # === SPY REALIZED VOLATILITY EXIT ADJUSTMENT ===
        # In high volatility widen soft exit thresholds so normal pullbacks
        # do not trigger premature exits
        _vol_state_exit = get_spy_volatility_state()
        _spy_dir_exit = get_spy_direction()
        _vol_multiplier = 1.0

        if _vol_state_exit == "ELEVATED":
            _vol_multiplier = 1.5
            logging.debug("[VOL_EXIT][%s] ELEVATED volatility — widening exit thresholds x1.5", sym)
        elif _vol_state_exit == "EXTREME":
            _vol_multiplier = 2.0
            logging.debug("[VOL_EXIT][%s] EXTREME volatility — widening exit thresholds x2.0", sym)

        # === SPY DIRECTIONAL EXIT ACCELERATOR ===
        # When SPY is in sustained downtrend, tighten exit thresholds to exit faster
        # This is the OPPOSITE of the volatility widening above — direction overrides choppiness
        # SPY falling + position open = get out sooner not later
        if _spy_dir_exit == "FALLING":
            _vol_multiplier = max(0.5, _vol_multiplier * 0.6)
            logging.debug(
                "[SPY_EXIT][%s] SPY direction FALLING — tightening exit thresholds to x%.2f",
                sym, _vol_multiplier
            )

        # ============================================================
        # 3. INDICATORS
        # ============================================================
        prices_series = pd.Series(price_deque)
        sizes_series = pd.Series(size_deque)

        ema_fast = compute_ema_from_series(prices_series, ema_fast_period).iloc[-1]
        ema_slow = compute_ema_from_series(prices_series, ema_slow_period).iloc[-1]
        rsi_series = compute_rsi_from_series(prices_series, rsi_period)
        rsi_val = rsi_series.iloc[-1]
        vwap_val = compute_vwap_from_ticks(prices_series, sizes_series).iloc[-1]

        regime_local = regime or detect_regime(prices_series, sizes_series)
        entry_regime_locked = (
            entry_config.get("regime") if entry_config else None
        ) or regime_local
        CONFIG_E = overlay_exit_params_by_regime(CONFIG, entry_regime_locked)

        soft_exits_allowed = elapsed >= MIN_HOLD_SECONDS

        # ============================================================
        # 4. TRAILING STOP
        # ============================================================
        TS_ACTIVATION_BUFFER = CONFIG.get("TS_ACTIVATION_BUFFER", 0.003)
        TRAILING_STOP_PCT = CONFIG.get("TRAILING_STOP_PCT", 0.004)

        if last_price >= ref_entry * (1 + TS_ACTIVATION_BUFFER):
            trailing_active[sym] = True
            highest_price_since_entry[sym] = max(
                highest_price_since_entry.get(sym, ref_entry),
                last_price
            )

        if trailing_active.get(sym, False):
            peak = highest_price_since_entry.get(sym, ref_entry)
            drawdown_pct = (peak - last_price) / peak if peak > 0 else 0
            if drawdown_pct >= TRAILING_STOP_PCT:
                logging.info("[%s] EXIT evaluate_sell | reason=Trailing stop | peak=%.4f | last=%.4f",
                             sym, peak, last_price)
                _rocket_mode_active.pop(sym, None)
                _rocket_mode_peak.pop(sym, None)
                return True, "Trailing stop"

        # ============================================================
        # 5. TAKE-PROFIT + ROCKET MODE
        # ============================================================
        # PATCH15: Dynamic TP based on market character
        _tp_base = CONFIG_E.get("TP_PCT", TP_PCT)
        _tp_market_char = globals().get("_market_character", "NEUTRAL")
        _tp_spy_open = globals().get("today_open_spy")
        _tp_spy_deque = globals().get("price_deques", {}).get("SPY")
        _tp_spy_now = float(_tp_spy_deque[-1]) \
            if _tp_spy_deque and len(_tp_spy_deque) > 0 else None
        _tp_spy_move = (_tp_spy_now - _tp_spy_open) / _tp_spy_open \
            if _tp_spy_open and _tp_spy_now else 0.0
        tp_pct = _get_dynamic_tp(_tp_base, _tp_market_char, _tp_spy_move)
        if tp_pct != _tp_base:
            logging.debug(
                "[PATCH15][TP] %s dynamic_tp=%.4f base_tp=%.4f char=%s spy_move=%.2f%%",
                sym, tp_pct, _tp_base, _tp_market_char, _tp_spy_move * 100
            )
        tp_price = ref_entry * (1 + tp_pct)
        _spy_dir_rocket = get_spy_direction()
        _profit_pct = (last_price - ref_entry) / ref_entry

        # Activate rocket mode when 2/3 of TP reached + SPY rising + symbol rising
        if not _rocket_mode_active.get(sym, False):
            _rocket_activation_threshold = tp_pct * 0.67
            _sym_ema_fast = _safe_last(compute_ema_from_series(prices_series, EMA_FAST))
            _sym_ema_slow = _safe_last(compute_ema_from_series(prices_series, EMA_SLOW))
            _sym_slope_ok = (not pd.isna(_sym_ema_fast) and not pd.isna(_sym_ema_slow)
                             and _sym_ema_fast > _sym_ema_slow)
            if (_profit_pct >= _rocket_activation_threshold
                    and _spy_dir_rocket == "RISING"
                    and _sym_slope_ok):
                _rocket_mode_active[sym] = True
                _rocket_mode_peak[sym] = last_price
                logging.warning(
                    "[ROCKET] %s activated | price=%.4f peak=%.4f floor=%.4f "
                    "session=%s profit=%.3f%%",
                    sym, last_price, last_price,
                    last_price * (1 - _rocket_mode_floor_pct),
                    _session_state, _profit_pct * 100
                )

        if _rocket_mode_active.get(sym, False):
            # Update peak
            if last_price > _rocket_mode_peak.get(sym, last_price):
                _rocket_mode_peak[sym] = last_price
                logging.warning(
                    "[ROCKET] %s peak updated | price=%.4f new_peak=%.4f new_floor=%.4f",
                    sym, last_price, last_price,
                    last_price * (1 - _rocket_mode_floor_pct)
                )
            # Tighten floor if SPY turns FALLING
            _active_floor_pct = (_rocket_tight_floor_pct
                                  if _spy_dir_rocket == "FALLING"
                                  else _rocket_mode_floor_pct)
            peak = _rocket_mode_peak.get(sym, last_price)
            drawdown_from_peak = (peak - last_price) / peak if peak > 0 else 0
            if drawdown_from_peak >= _active_floor_pct:
                logging.warning(
                    "[ROCKET] %s exit triggered | price=%.4f peak=%.4f "
                    "drawdown=%.3f%% floor=%.3f%% spy_dir=%s",
                    sym, last_price, peak,
                    drawdown_from_peak * 100, _active_floor_pct * 100,
                    _spy_dir_rocket
                )
                _rocket_mode_active.pop(sym, None)
                _rocket_mode_peak.pop(sym, None)
                return True, "Rocket trailing exit"
            # Still in rocket mode — do NOT exit at normal TP
            logging.debug(
                "[ROCKET] %s holding | price=%.4f peak=%.4f drawdown=%.3f%% floor=%.3f%%",
                sym, last_price, peak,
                drawdown_from_peak * 100, _active_floor_pct * 100
            )
        else:
            # Normal TP — not in rocket mode
            if last_price >= tp_price:
                logging.info(
                    "[%s] EXIT evaluate_sell | reason=Take-profit (session=%s) | "
                    "last=%.4f | ref=%.4f | tp_pct=%.4f",
                    sym, _session_state, last_price, ref_entry, tp_pct
                )
                return True, "Take-profit"

        # ============================================================
        # 6. TREND FAILURE EXITS (VWAP, EMA, RSI) with Option B + C suppression
        # ============================================================

        # Option B: regime confidence gate
        regime_confirmed = (
            regime_confidence is None or
            regime_confidence >= REGIME_CONFIDENCE_MIN
        )

        # Option C: momentum health check (inline helper)
        def _momentum_healthy():
            if entry_config is None:
                return False
            entry_vwap = entry_config.get("vwap")
            entry_rsi  = entry_config.get("rsi")
            if entry_vwap is None or pd.isna(entry_vwap) or entry_vwap <= 0:
                return False
            if entry_rsi is None or pd.isna(entry_rsi):
                return False
            # Check 1: price not too far below entry VWAP
            vwap_ok = last_price >= entry_vwap * 0.9985
            # Check 2: positive 10-tick slope
            slope_ok = (
                len(prices_series) >= 10 and
                float(prices_series.iloc[-1]) > float(prices_series.iloc[-10])
            )
            # Check 3: RSI not collapsed
            rsi_ok = (not pd.isna(rsi_val) and
                      rsi_val >= entry_rsi - 20 and
                      rsi_val >= 35)
            result = vwap_ok and slope_ok and rsi_ok
            if result:
                logging.debug(
                    "[OPTION_C][%s] momentum_healthy=True vwap_ok=%s slope_ok=%s rsi_ok=%s",
                    sym, vwap_ok, slope_ok, rsi_ok
                )
            return result

        # --- VWAP fail ---
        VWAP_DELTA = CONFIG.get("VWAP_DELTA", 0.0015) * _vol_multiplier
        vwap_fail = soft_exits_allowed and last_price < vwap_val * (1 - VWAP_DELTA)

        if vwap_fail:
            momentum_ok = _momentum_healthy()
            if momentum_ok:
                logging.info("[SUPPRESS][%s] VWAP fail suppressed by momentum | last=%.4f vwap=%.4f conf=%s",
                             sym, last_price, vwap_val,
                             regime_confidence if regime_confidence is not None else "N/A")
            elif not regime_confirmed:
                logging.info("[SUPPRESS][%s] VWAP fail suppressed — regime not confirmed (conf=%s)",
                             sym, regime_confidence)
            else:
                logging.info("[%s] EXIT evaluate_sell | reason=VWAP fail | last=%.4f | vwap=%.4f",
                             sym, last_price, vwap_val)
                return True, "VWAP fail"

        # --- EMA fail ---
        EMA_DELTA = CONFIG.get("EMA_DELTA", 0.001) * _vol_multiplier
        ema_fail = soft_exits_allowed and (
            ema_fast < ema_slow and
            last_price < ema_slow * (1 - EMA_DELTA)
        )

        if ema_fail:
            momentum_ok = _momentum_healthy()
            if momentum_ok:
                logging.info("[SUPPRESS][%s] EMA fail suppressed by momentum | last=%.4f ema_slow=%.4f conf=%s",
                             sym, last_price, ema_slow,
                             regime_confidence if regime_confidence is not None else "N/A")
            elif not regime_confirmed:
                logging.info("[SUPPRESS][%s] EMA fail suppressed — regime not confirmed (conf=%s)",
                             sym, regime_confidence)
            else:
                logging.info("[%s] EXIT evaluate_sell | reason=EMA fail | last=%.4f | ema_slow=%.4f",
                             sym, last_price, ema_slow)
                return True, "EMA fail"

        # --- RSI fail ---
        RSI_FAIL_TICKS = CONFIG.get("RSI_FAIL_TICKS", 2)
        rsi_fail = False

        if soft_exits_allowed:
            if rsi_val > CONFIG.get("MAX_RSI_FOR_ENTRY", MAX_RSI_FOR_ENTRY) or \
               rsi_val < CONFIG.get("MIN_RSI_FOR_ENTRY", MIN_RSI_FOR_ENTRY):
                rsi_fail_counter[sym] = rsi_fail_counter.get(sym, 0) + 1
            else:
                rsi_fail_counter[sym] = 0

            if rsi_fail_counter[sym] >= RSI_FAIL_TICKS:
                rsi_fail = True

        if rsi_fail:
            momentum_ok = _momentum_healthy()
            if momentum_ok:
                logging.info("[SUPPRESS][%s] RSI fail suppressed by momentum | rsi=%.2f conf=%s",
                             sym, rsi_val,
                             regime_confidence if regime_confidence is not None else "N/A")
            elif not regime_confirmed:
                logging.info("[SUPPRESS][%s] RSI fail suppressed — regime not confirmed (conf=%s)",
                             sym, regime_confidence)
            else:
                logging.info("[%s] EXIT evaluate_sell | reason=RSI fail | rsi=%.2f",
                             sym, rsi_val)
                return True, "RSI fail"

        # ============================================================
        # 7. TIME-STOP EXITS (RANGE, LOW_VOL)
        # ============================================================

        # RANGE time-stop (only fires when regime is confirmed)
        if regime_local == "RANGE" and regime_confirmed:
            RANGE_TIME_STOP_SECONDS = CONFIG.get("RANGE_TIME_STOP_SECONDS", 900)
            RANGE_VWAP_PROGRESS_MIN = CONFIG.get("RANGE_VWAP_PROGRESS_MIN", 0.15)

            if elapsed >= RANGE_TIME_STOP_SECONDS:
                entry_dist = abs(ref_entry - vwap_val)
                current_dist = abs(last_price - vwap_val)
                progress = 1 - (current_dist / entry_dist) if entry_dist > 0 else 0

                if progress < RANGE_VWAP_PROGRESS_MIN:
                    logging.info("[%s] EXIT evaluate_sell | reason=Range time-stop | progress=%.3f",
                                 sym, progress)
                    return True, "Range time-stop"

        # LOW_VOL time-stop
        if regime_local == "LOW_VOL":
            LOW_VOL_TIME_STOP_SECONDS = CONFIG.get("LOW_VOL_TIME_STOP_SECONDS", 300)
            if elapsed >= LOW_VOL_TIME_STOP_SECONDS:
                logging.info("[%s] EXIT evaluate_sell | reason=Low-vol time-stop",
                             sym)
                return True, "Low-vol time-stop"

        # ============================================================
        # 8. MAX HOLD (final fallback)
        # ============================================================
        MAX_HOLD_SECONDS = CONFIG.get("MAX_HOLD_SECONDS", 3600)
        if elapsed >= MAX_HOLD_SECONDS:
            logging.info("[%s] EXIT evaluate_sell | reason=Max hold | elapsed=%.1f",
                         sym, elapsed)
            return True, "Max hold"

        return False, None

    except Exception as e:
        logging.error("[%s] evaluate_sell failed: %s", sym, e)
        return False, None




# === AUDIT TRAIL (LIVE) — removable block ===
AUDIT_TRAIL_ENABLED = True
AUDIT_OUTCOME_WINDOW_MIN = 30


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
PREV_CLOSE_FILE = "spy_prev_close.txt"

def _save_prev_close(price):
    try:
        with open(PREV_CLOSE_FILE, "w") as f:
            f.write(f"{price:.4f}")
        logging.info("[DAY_REGIME] Saved SPY prev_close=%.4f to %s", price, PREV_CLOSE_FILE)
    except Exception as e:
        logging.warning("[DAY_REGIME] Could not save prev_close: %s", e)

def _load_prev_close():
    try:
        if os.path.exists(PREV_CLOSE_FILE):
            with open(PREV_CLOSE_FILE, "r") as f:
                val = float(f.read().strip())
            logging.info("[DAY_REGIME] Loaded prev_close=%.4f from %s", val, PREV_CLOSE_FILE)
            return val
    except Exception as e:
        logging.warning("[DAY_REGIME] Could not load prev_close: %s", e)
    return None

def _save_session_open(price):
    try:
        with open(SESSION_OPEN_FILE, "w") as f:
            f.write(f"{price:.4f}")
        logging.info("[SESSION_STATE] Saved session_open=%.4f to %s", price, SESSION_OPEN_FILE)
    except Exception as e:
        logging.warning("[SESSION_STATE] Could not save session_open: %s", e)

def _load_session_open():
    try:
        if os.path.exists(SESSION_OPEN_FILE):
            file_age_hours = (
                datetime.now(timezone.utc) -
                datetime.fromtimestamp(
                    os.path.getmtime(SESSION_OPEN_FILE), tz=timezone.utc
                )
            ).total_seconds() / 3600
            if file_age_hours > 20:
                logging.warning("[SESSION_STATE] session_open file is %.1f hours old — stale, ignoring",
                                file_age_hours)
                return None
            with open(SESSION_OPEN_FILE, "r") as f:
                val = float(f.read().strip())
            logging.info("[SESSION_STATE] Loaded session_open=%.4f from %s", val, SESSION_OPEN_FILE)
            return val
    except Exception as e:
        logging.warning("[SESSION_STATE] Could not load session_open: %s", e)
    return None

def get_session_state():
    """
    Determines current session aggressiveness state.
    Returns: BULL_SESSION, NEUTRAL_SESSION, or BEAR_SESSION.
    Called at startup and every 5 minutes intraday.
    """
    try:
        spy_deque = globals().get("price_deques", {}).get("SPY")
        spy_now = float(spy_deque[-1]) if spy_deque and len(spy_deque) > 0 else None
        if spy_now is None:
            return "NEUTRAL_SESSION"
        session_open = globals().get("today_open_spy") or _load_session_open() or _load_prev_close()
        if session_open is None:
            return "NEUTRAL_SESSION"
        spy_move = (spy_now - session_open) / session_open
        spy_dir = get_spy_direction()
        if spy_move >= 0.003 and spy_dir in ("RISING", "FLAT"):
            state = "BULL_SESSION"
        elif spy_move <= -0.005 or (spy_move <= -0.002 and spy_dir == "FALLING"):
            state = "BEAR_SESSION"
        elif spy_move < 0 and spy_dir == "FALLING":
            state = "BEAR_SESSION"
        elif spy_move >= 0.001 and spy_dir == "FALLING":
            state = "NEUTRAL_SESSION"
        else:
            state = "NEUTRAL_SESSION"
        logging.warning(
            "[SESSION_STATE] state=%s | spy_move=%.3f%% | spy_dir=%s | "
            "session_open=%.4f | spy_now=%.4f",
            state, spy_move * 100, spy_dir, session_open, spy_now
        )
        return state
    except Exception as e:
        logging.warning("[SESSION_STATE] get_session_state failed: %s", e)
        return "NEUTRAL_SESSION"

def apply_session_exit_multipliers(CONFIG, session_state):
    """Returns CONFIG copy with TP_PCT and EMERGENCY_SL_PCT adjusted by session state."""
    adj = dict(CONFIG)
    mults = SESSION_MULTIPLIERS.get(session_state, SESSION_MULTIPLIERS["NEUTRAL_SESSION"])
    if "TP_PCT" in adj:
        adj["TP_PCT"] = max(0.0008, adj["TP_PCT"] * mults["tp"])
    if "EMERGENCY_SL_PCT" in adj:
        adj["EMERGENCY_SL_PCT"] = max(0.002, adj["EMERGENCY_SL_PCT"] * mults["sl"])
    if mults["tp"] != 1.0 or mults["sl"] != 1.0:
        logging.debug(
            "[SESSION_MULT] session=%s tp_mult=%.2f sl_mult=%.2f TP=%.4f SL=%.4f",
            session_state, mults["tp"], mults["sl"],
            adj.get("TP_PCT", 0), adj.get("EMERGENCY_SL_PCT", 0)
        )
    return adj

def classify_day_regime(stock_data_client_local, spy_deque):
    try:
        try:
            resp = stock_data_client_local.get_stock_latest_trade(
                StockLatestTradeRequest(symbol_or_symbols="SPY")
            )
            spy_now = float(resp["SPY"].price)
            logging.info("[DAY_REGIME] SPY latest trade price: %.4f", spy_now)
        except Exception as e:
            logging.warning("[DAY_REGIME] Could not fetch SPY latest trade: %s", e)
            return "NEUTRAL_DAY"

        prev_close = _load_prev_close()

        if prev_close is None:
            logging.warning(
                "[DAY_REGIME] No prev_close file found. "
                "Saving current SPY=%.4f as baseline. Defaulting NEUTRAL_DAY. "
                "Classification will work from tomorrow onwards.",
                spy_now
            )
            _save_prev_close(spy_now)
            return "NEUTRAL_DAY"

        try:
            file_mtime_dt = datetime.fromtimestamp(
                os.path.getmtime(PREV_CLOSE_FILE), tz=timezone.utc
            ).astimezone(ZoneInfo("America/New_York"))
            now_et = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
            trading_days_elapsed = _count_trading_days_elapsed(
                file_mtime_dt.date(), now_et.date()
            )
            logging.debug(
                "[DAY_REGIME] prev_close file written=%s today=%s trading_days_elapsed=%d",
                file_mtime_dt.date(), now_et.date(), trading_days_elapsed
            )
            if trading_days_elapsed >= 1:
                logging.warning(
                    "[DAY_REGIME] prev_close file is %d trading session(s) old — stale. "
                    "Refreshing baseline with current SPY=%.4f and defaulting NEUTRAL_DAY.",
                    trading_days_elapsed, spy_now
                )
                _save_prev_close(spy_now)
                return "NEUTRAL_DAY"
        except Exception:
            pass

        gap_pct = (spy_now - prev_close) / prev_close

        spy_prices = pd.Series(spy_deque)
        spy_rsi = _safe_last(compute_rsi_from_series(spy_prices, RSI_PERIOD))
        rsi_available = not pd.isna(spy_rsi)

        logging.warning(
            "[DAY_REGIME] prev_close=%.4f spy_now=%.4f gap_pct=%.4f spy_rsi=%s",
            prev_close, spy_now, gap_pct,
            f"{spy_rsi:.1f}" if rsi_available else "N/A (no warmup data)"
        )

        if gap_pct <= -0.005:
            # No shorting — treat bear gaps as neutral, entries blocked
            # by SPY session bearish override in evaluate_entry
            result = "NEUTRAL_DAY"
        elif gap_pct >= 0.005:
            _spy_deque_len = len(spy_deque) if spy_deque else 0
            if rsi_available and spy_rsi < 40 and _spy_deque_len >= 100:
                result = "NEUTRAL_DAY"
            else:
                result = "BULL_DAY"
        else:
            if not rsi_available:
                result = "NEUTRAL_DAY"
            elif spy_rsi >= 55:
                result = "BULL_DAY"
            elif spy_rsi <= 45:
                result = "NEUTRAL_DAY"
            else:
                result = "NEUTRAL_DAY"

        return result

    except Exception as e:
        logging.warning("[DAY_REGIME] Classification failed: %s — defaulting NEUTRAL_DAY", e)
        return "NEUTRAL_DAY"


def _wait_for_935_et():
    while True:
        now_et = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
        if now_et.hour > 9 or (now_et.hour == 9 and now_et.minute >= 35):
            break
        logging.info("[DAY_REGIME] Waiting for 9:35 ET... current=%02d:%02d ET",
                     now_et.hour, now_et.minute)
        time.sleep(10)

logging.warning(">>> MAIN LOOP IS RUNNING FROM THIS FILE <<<")
    
RSI_PERIOD = 14
RSI_COOL_THRESHOLD = 3
MAX_HOLD_SECONDS = 999999
MIN_HOLD_SECONDS = 90
TRAIL_PCT = 0.010
BUY_POWER_LIMIT = 0.05
BUY_CASH_BUFFER = 0.95
COOLDOWN_SECONDS = 120
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

    entry_times = {}
    entry_prices = {}
    entry_qty = {}
    entry_configs = {}
    highest_price_since_entry = {}
    trailing_active = {}

    short_entry_times  = {}
    short_entry_prices = {}
    short_entry_qty    = {}
    
    try:
        positions = trading_client.get_all_positions()
        for p in positions:
            sym = p.symbol.upper()
            q = float(p.qty)

            if q <= 0:
                logging.warning(
                    "[RESTORE_GUARD] %s has qty=%.2f on Alpaca at startup — "
                    "skipping restore to prevent short amplification",
                    sym, q
                )
                continue

            entry_prices[sym] = float(p.avg_entry_price)
            entry_qty[sym] = q
            entry_times[sym] = datetime.now(timezone.utc)
            entry_configs[sym] = {"restored": True}
            highest_price_since_entry[sym] = entry_prices[sym]
            trailing_active[sym] = False
            logging.warning("[RESTORE] Restored %s: qty=%.2f entry=%.4f", sym, q, entry_prices[sym])
    except Exception as e:
        logging.error("[RESTORE] Failed to restore positions: %s", e)

    # === STARTUP SHORT CLEANUP ===
    try:
        all_positions = trading_client.get_all_positions()
        for p in all_positions:
            sym = p.symbol.upper()
            q = int(float(p.qty))
            if q < 0:
                cover_qty = abs(q)
                logging.warning(
                    "[STARTUP_COVER] Found short %s qty=%d at startup — covering immediately",
                    sym, cover_qty
                )
                try:
                    order = MarketOrderRequest(
                        symbol=sym,
                        qty=cover_qty,
                        side=OrderSide.BUY,
                        type=OrderType.MARKET,
                        time_in_force=TimeInForce.DAY
                    )
                    trading_client.submit_order(order)
                    logging.warning("[STARTUP_COVER] %s cover order submitted qty=%d", sym, cover_qty)
                    time.sleep(2.0)
                except Exception as cover_err:
                    logging.error("[STARTUP_COVER] Failed to cover %s: %s", sym, cover_err)
    except Exception as e:
        logging.error("[STARTUP_COVER] Failed to check shorts at startup: %s", e)

    
    
    inflight_orders = {}
                
    symbols = [
        # Core tech momentum — highest priority
        "NVDA", "AMD", "TSLA", "AAPL", "MSFT", "AMZN", "GOOG", "META",
        # Semiconductors
        "MU", "QCOM", "AVGO", "SMCI",
        # Software and cloud
        "CRM", "ORCL", "ADSK", "NFLX", "PLTR", "SHOP",
        # Financials
        "V", "JPM", "C",
        # Other high beta
        "UBER", "SQ",
        # ETFs — SPY must stay as reference
        "SPY", "QQQ", "IWM", "XLK", "XLF",
    ]
    price_deques = {s: deque(maxlen=TICKS_WINDOW) for s in symbols}
    size_deques = {s: deque(maxlen=TICKS_WINDOW) for s in symbols}
    time_deques = {s: deque(maxlen=TICKS_WINDOW) for s in symbols}
    globals()["price_deques"] = price_deques  # make accessible to evaluate_entry, SPY filters
    
    from datetime import datetime, timezone
    global last_exit_time
    last_exit_time = defaultdict(lambda: None)
    for s in symbols:
        last_exit_time[s] = None
    last_buy_time = {s: None for s in symbols}

    order_lock = threading.Lock()
    stop_event = threading.Event()
    pending_entries = set()

    if RUN_MODE in ["SIM", "AGG_SIM"]:
        
        from alpaca.data.requests import StockTradesRequest
        from datetime import datetime, timezone
        
    
        symbol = "NVDA"
        symbols = [symbol]

        _risk_governor_update()

                
        inflight_orders = {}
        pending_entries = set()
        positions_map = {}
        last_exit_time = {s: None for s in symbols}

        last_buy_time = defaultdict(lambda: None)
        in_position_map = defaultdict(bool)
        
        start = "2025-12-23T14:30:00Z"
        end = "2025-12-23T21:00:00Z"

        
        print("TimeFrame attrs:", [a for a in dir(TimeFrame) if not a.startswith("_")])
        print("TimeFrameUnit attrs:", [a for a in dir(TimeFrameUnit) if not a.startswith("_")])
        
        def _build_1s_timeframe():
            for unit_name in ("SECOND", "Second", "second"):
                try:
                    unit = getattr(TimeFrameUnit, unit_name)
                    try:
                        return TimeFrame(1, unit)
                    except Exception:
                        pass
                except Exception:
                    pass
        
            for candidate in ("Second", "SECOND", "1Sec", "1S", "1sec", "1s"):
                try:
                    if hasattr(TimeFrame, candidate):
                        return getattr(TimeFrame, candidate)
                except Exception:
                    pass
                try:
                    return TimeFrame(candidate)
                except Exception:
                    pass
                try:
                    if hasattr(TimeFrame, "from_string"):
                        return TimeFrame.from_string(candidate)
                except Exception:
                    pass
        
            try:
                return TimeFrame("1S")
            except Exception:
                pass
        
            raise RuntimeError(
                "Could not construct a 1-second TimeFrame with your Alpaca SDK. "
                "Paste the two debug prints above (TimeFrame attrs and TimeFrameUnit attrs) and I'll give a one-line fix."
            )
        
        tf = TimeFrame(1, TimeFrameUnit.Minute)

        try: 
            start_dt = parser.isoparse(start) if isinstance(start, str) else start 
            end_dt = parser.isoparse(end) if isinstance(end, str) else end 
        except Exception: 
            start_dt, end_dt = start, end
        
        logging.debug("Using timeframe=%s start=%s end=%s", tf, start_dt, end_dt)
        bars_req = StockBarsRequest(symbol_or_symbols=symbol, start=start_dt, end=end_dt, timeframe=tf)
        try:
            bars = stock_data_client.get_stock_bars(bars_req).df
        except Exception as e:
            logging.warning("get_stock_bars failed for %s: %s", symbol, e)
            bars = pd.DataFrame()

        if bars is None or bars.empty:
            logging.warning("No bars returned for %s from %s to %s (timeframe=%s)", symbol, start_dt, end_dt, tf)
            trades = pd.DataFrame(columns=["price", "size"])
            trades.index = pd.to_datetime(pd.Series(dtype="datetime64[ns]"))
        else:
            bars = bars.reset_index().set_index("timestamp")
        
            trades = pd.DataFrame({
                "price": bars["close"].astype(float),
                "size": bars["volume"].fillna(0).astype(float)
        })
        trades.index = pd.to_datetime(trades.index)
           

        trades["bucket"] = trades.index.floor("1s")
        trades = trades.groupby("bucket").agg({
            "price": "mean",
            "size": "sum"
        })

        trades = trades.dropna()
        logging.info("%s mode: using 1-second bars. Total datapoints: %d", RUN_MODE, len(trades))
        logging.info("Starting %s replay for %s from %s to %s", RUN_MODE, symbol, start, end)

        for ts, row in trades.iterrows():
            try:
                ts_val = pd.to_datetime(ts, utc=True)
                price  = float(row["price"])
                size   = float(row["size"])
            except Exception as e:
                logging.error("[%s] Could not parse row: %s", RUN_MODE, e)
                continue
    
        if RUN_MODE in ["AGG_SIM", "SIM"]:
            trades = trades.dropna()
            logging.info("%s mode: using raw tick data. Total datapoints: %d",
                 RUN_MODE, len(trades))
        
    
        print(trades.head())
        logging.info("Starting %s replay for %s from %s to %s", RUN_MODE, symbol, start, end)

        max_loop_budget = 100000.0
        
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

            prices_series = pd.Series(price_deques[symbol])
            sizes_series = pd.Series(size_deques[symbol])
        
            regime = detect_regime(prices_series, sizes_series)

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
                "regime": regime,
                "score": score,
                "reason": reason
            })


            if idx % 2000 == 0:
                logging.info("[PROGRESS] %s replay at %s (%d/%d bars processed)",
                             symbol,
                             ts_val.strftime("%H:%M"),
                             idx,
                             len(trades))

    
            bucket_ts = ts_val.replace(microsecond=0)
            if len(time_deques[symbol]) > 0 and time_deques[symbol][-1] == bucket_ts:
                price_deques[symbol][-1] = (price_deques[symbol][-1] + price) / 2.0
                size_deques[symbol][-1] += size
            else:
                price_deques[symbol].append(price)
                size_deques[symbol].append(size)
                time_deques[symbol].append(bucket_ts)

            # === UPDATE SESSION HIGH/LOW/OPEN TRACKING ===
            if session_open_price[symbol] is None:
                session_open_price[symbol] = price
                session_high_price[symbol] = price
                session_low_price[symbol]  = price
            else:
                session_high_price[symbol] = max(session_high_price[symbol], price)
                session_low_price[symbol]  = min(session_low_price[symbol],  price)
    
            prices = pd.Series(price_deques[symbol])
            sizes_series = pd.Series(size_deques[symbol])
            ema_fast = compute_ema_from_series(prices, EMA_FAST).iloc[-1]
            ema_slow = compute_ema_from_series(prices, EMA_SLOW).iloc[-1]
            rsi_val = compute_rsi_from_series(prices, RSI_PERIOD).iloc[-1]
            vwap_val = compute_vwap_from_ticks(prices, sizes_series).iloc[-1]
    
            if pd.isna(ema_fast) or pd.isna(ema_slow) or pd.isna(rsi_val) or pd.isna(vwap_val):
                continue

            if symbol == "SPY":
                try:
                    market_series = pd.Series(price_deques["SPY"])
                    market_trend_state = market_trend_filter(market_series)
                    globals()["market_trend_state"] = market_trend_state
                    logging.debug(f"[MARKET] trend_state={market_trend_state}")
                except Exception as e:
                    logging.debug(f"[MARKET] trend update failed: {e}")

            day_bias = detect_day_bias(prices,
                                       compute_ema_from_series(prices, EMA_FAST),
                                       compute_ema_from_series(prices, EMA_SLOW),
                                       compute_vwap_from_ticks(prices, sizes_series))

           
            
            if day_bias == "bullish":
                CONFIG = BULLISH_CONFIG
            else:
                CONFIG = BEARISH_CONFIG

                                      
            positions_map = {} if RUN_MODE in ["SIM", "AGG_SIM"] else get_positions_map(trade_client)

            logging.info("[LOOP] Calling reconcile_positions for %d symbols", len(symbols))

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
            regime = None
            if USE_REGIME_ENTRY:
                regime_raw = detect_regime(prices, sizes_series)
                regime, regime_conf = _smooth_regime(symbol, regime_raw)
                CONFIG_SESSION = overlay_by_session(CONFIG, ts_val, regime)
                accept, reason, score, stack = evaluate_entry(
                    symbol,
                    price,
                    size,
                    prices,
                    sizes_series,
                    ts_val,
                    positions_map,
                    inflight_orders,
                    pending_entries,
                    last_exit_time[symbol],
                    last_buy_time,
                    CONFIG_SESSION,
                    regime,
                    bias=day_bias,
                    log_stack=True
                )

                log_entry_attempt(
                    ts_val,
                    symbol,
                    regime,
                    day_bias,
                    accept,
                    reason,
                    score,
                    ema_fast_val,
                    ema_slow_val,
                    rsi_val,
                    vwap_val,
                    price
                )
                
                buy = accept
            else:
                regime = "UNKNOWN"
                regime_conf = REGIME_CONFIDENCE_MIN
            
            has_entry = (symbol in entry_times) and (symbol in entry_prices) and (symbol in entry_configs)
            if has_entry:
                accept_exit, reason_exit = evaluate_sell(
                    symbol,
                    price,
                    entry_prices.get(symbol),
                    price_deques[symbol],
                    size_deques[symbol],
                    entry_times,
                    entry_configs[symbol],
                    current_time=ts_val,
                    regime=regime,
                    log_stack=True,
                    entry_config=entry_configs.get(symbol),
                    regime_confidence=regime_conf
                )    
            else:
                accept_exit, reason_exit = (False, None)

            logging.debug(
                "[SELL_DECISION_SIM][%s] has_entry=%s | accept_exit=%s | reason=%s | regime=%s | last=%.4f | ref=%.4f",
                symbol, has_entry, accept_exit, reason_exit, regime, price, entry_prices.get(symbol, price)
            )

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
                    "regime": regime
                })
                write_exec_row_immediate(exec_rows[-1], symbol, RUN_MODE)
                
                in_position = False
                in_position_map[symbol] = False
                last_buy_time[symbol] = None

                entry_price = None
                last_exit_time[symbol] = ts_val
                highest_price_since_entry.pop(symbol, None)
                entry_configs.pop(symbol, None)

                continue

            if last_buy_time[symbol] is not None and \
               (ts_val - last_buy_time[symbol]).total_seconds() < COOLDOWN_SECONDS:
                continue
            else:
                regime_raw = detect_regime(prices, sizes_series)
                regime, regime_conf = _smooth_regime(symbol, regime_raw)
                CONFIG_SESSION = overlay_by_session(CONFIG, ts_val, regime)
                accept, reason, score, stack = evaluate_entry(
                    symbol, price, size, prices, sizes_series, ts_val,
                    positions_map, inflight_orders, pending_entries,
                    last_exit_time[symbol], last_buy_time,
                    CONFIG_SESSION, regime, log_stack=False
                )
                buy = accept

                if buy:
                    if in_position_map[symbol]:
                        continue
                    
                    last_ts = last_buy_time.get(symbol)
                    if last_ts is not None and (ts_val - last_ts).total_seconds() < 1:
                        continue

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
                    price_deques[symbol], size_deques[symbol], entry_times, entry_configs[symbol],
                    current_time=ts_val,
                    entry_config=entry_configs.get(symbol),
                    regime_confidence=regime_conf
                )

                logging.debug("[SIM] evaluate_sell (late) -> sell=%s reason=%s ts=%s",
                              sell, reason, ts_val.strftime("%H:%M:%S"))
                
                if sell:
                    sell_decisions += 1
                    qty = entry_qty.get(symbol, 1)
                    pnl = (price - entry_price) * qty
                    regime_at_sell = detect_regime(pd.Series(price_deques[symbol]), pd.Series(size_deques[symbol]))
                    regime_pnl[regime_at_sell] += float(pnl)
                    regime_trades[regime_at_sell] += 1
                    exit_reason_count[regime_at_sell][reason] += 1

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
    
                            
            logging.info("%s replay finished for %s", RUN_MODE, symbol)
            exec_filename = f"{symbol}_{RUN_MODE}_exec.csv"
            exec_fields = ["timestamp", "symbol", "action", "price", "reason", "pnl", "ema_fast", "ema_slow", "rsi", "vwap", "regime"]
            try:
                with open(exec_filename, "w", newline="") as f:
                    import csv
                    writer = csv.DictWriter(f, fieldnames=exec_fields)
                    writer.writeheader()
                    writer.writerows(exec_rows)
                                                                    
                logging.info("[SIM DIAG] wrote exec file %s rows=%d", exec_filename, len(exec_rows))
                logging.info("[SIM DIAG] sell_decisions=%d exec_rows_len=%d", sell_decisions, len(exec_rows))            
                                                
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

    if RUN_MODE == "LIVE":
        warmup_deques(symbols, price_deques, size_deques, time_deques, lookback_minutes=60)
        _wait_for_935_et()
        day_regime = classify_day_regime(stock_data_client, price_deques["SPY"])
        globals()["day_regime"] = day_regime
        # === USE PREV CLOSE AS SPY REFERENCE PRICE ===
        # Alpaca free subscription does not allow historical bar fetches (403 error).
        # Instead use the previous day close saved by force_liquidation_at_cutoff.
        # This matches what Yahoo Finance shows and is a reliable reference.
        _spy_prev_close = _load_prev_close()
        if _spy_prev_close is not None:
            globals()["today_open_spy"] = _spy_prev_close
            logging.warning("[DAY_REGIME] Using prev_close=%.4f as SPY reference price", _spy_prev_close)
        else:
            globals()["today_open_spy"] = None
            logging.warning("[DAY_REGIME] No prev_close available — SPY bearish override will be inactive today")

        # Save session open for late-start recovery
        try:
            _resp = stock_data_client.get_stock_latest_trade(
                StockLatestTradeRequest(symbol_or_symbols="SPY")
            )
            _spy_open_now = float(_resp["SPY"].price)
            _existing_open = _load_session_open()
            if _existing_open is None:
                _save_session_open(_spy_open_now)
                logging.warning("[SESSION_STATE] Session open saved: %.4f", _spy_open_now)
            else:
                logging.warning("[SESSION_STATE] Session open already exists: %.4f — keeping original",
                                _existing_open)
        except Exception as e:
            logging.warning("[SESSION_STATE] Could not save session open: %s", e)

        # Initialize session state
        globals()["_current_session_state"] = "NEUTRAL_SESSION"
        globals()["_session_state_last_update"] = None
        logging.warning("[SESSION_STATE] Initial state=NEUTRAL_SESSION (will update after warmup)")
  
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
                    sell_all_positions(trading_client, order_lock)
                    stop_event.set()
                    break
        except Exception as e:
            logging.exception("input listener error: %s", e)
            stop_event.set()

    def sell_all_positions(trade_client_local, order_lock_local):
        try:
            positions = None
            for attempt in range(3):
                try:
                    positions = trade_client_local.get_all_positions()
                    break
                except Exception as e:
                    logging.warning(
                        "[EOD] get_all_positions attempt %d/3 failed: %s",
                        attempt + 1, e
                    )
                    if attempt < 2:
                        time.sleep(3.0)
            if positions is None:
                logging.error("[EOD] get_all_positions failed all 3 attempts — cannot sell")
                return
            for p in positions:
                s = p.symbol
                q = int(float(p.qty))
                if q > 0:
                    for sell_attempt in range(3):
                        try:
                            order = MarketOrderRequest(
                                symbol=s, qty=q,
                                side=OrderSide.SELL,
                                type=OrderType.MARKET,
                                time_in_force=TimeInForce.DAY
                            )
                            trade_client_local.submit_order(order)
                            logging.info(
                                "[EOD] %s forced SELL qty=%d attempt %d",
                                s, q, sell_attempt + 1
                            )
                            break
                        except Exception as e:
                            logging.warning(
                                "[EOD] sell %s attempt %d/3 failed: %s",
                                s, sell_attempt + 1, e
                            )
                            if sell_attempt < 2:
                                time.sleep(3.0)
        except Exception as e:
            logging.exception("sell_all_positions error: %s", e)

    threading.Thread(target=input_listener, daemon=True).start()

    logging.info("Starting main loop with symbols: %s", symbols)

    last_bias = None

    logging.info("LIVE mode: starting unified loop with full indicator + gating pipeline")

    while not stop_event.is_set():
        
        try:
            loop_start = datetime.now(timezone.utc)

            _risk_governor_update()

            positions_map = get_positions_map(trading_client)
            spent_this_loop = 0.0
            max_loop_budget = calculate_buying_power_limit(
                trade_client_local=trade_client,
                limit_fraction=BUY_POWER_LIMIT
            )
            MAX_CONCURRENT_POSITIONS = 3
            current_open_positions = len([s for s in entry_prices if entry_prices.get(s) is not None])
            

            for symbol in symbols:
                # PATCH16: retry fetch up to 3 times on connection errors
                _fetch_price = None
                _fetch_size = 1.0
                for _fetch_attempt in range(3):
                    try:
                        req = StockLatestTradeRequest(symbol_or_symbols=symbol)
                        resp = stock_data_client.get_stock_latest_trade(req)
                        trade = resp[symbol]
                        _fetch_price = float(trade.price)
                        _fetch_size = float(trade.size) if hasattr(trade, "size") else 1.0
                        break
                    except Exception as e:
                        if _fetch_attempt < 2:
                            logging.debug(
                                "[LIVE] Fetch retry %d/3 for %s: %s",
                                _fetch_attempt + 1, symbol, e
                            )
                            time.sleep(0.3)
                        else:
                            logging.debug(
                                "[LIVE] Failed to fetch trade for %s after 3 attempts: %s",
                                symbol, e
                            )
                if _fetch_price is None:
                    continue
                price = _fetch_price
                size = _fetch_size
                ts_val = datetime.now(timezone.utc)

               
                bucket_ts = ts_val.replace(microsecond=0)

                if len(time_deques[symbol]) > 0 and time_deques[symbol][-1] == bucket_ts:
                    price_deques[symbol][-1] = (price_deques[symbol][-1] + price) / 2.0
                    size_deques[symbol][-1] += size
                else:
                    price_deques[symbol].append(price)
                    size_deques[symbol].append(size)
                    time_deques[symbol].append(bucket_ts)

                # Add this immediately after:
                if session_open_price[symbol] is None:
                    session_open_price[symbol] = price
                    session_high_price[symbol] = price
                    session_low_price[symbol]  = price
                    # PATCH15: capture per-symbol session open for relative strength
                    if globals().get(f"today_open_{symbol}") is None:
                        globals()[f"today_open_{symbol}"] = price
                        logging.debug("[PATCH15] %s session open: %.4f", symbol, price)
                else:
                    session_high_price[symbol] = max(session_high_price[symbol], price)
                    session_low_price[symbol]  = min(session_low_price[symbol],  price)

                prices_series = pd.Series(price_deques[symbol])
                sizes_series = pd.Series(size_deques[symbol])

                if len(prices_series) < 5:
                    continue

                ema_fast_val = _safe_last(compute_ema_from_series(prices_series, EMA_FAST))
                ema_slow_val = _safe_last(compute_ema_from_series(prices_series, EMA_SLOW))
                rsi_series = compute_rsi_from_series(prices_series, RSI_PERIOD)
                rsi_val = _safe_last(rsi_series)
                vwap_val = _safe_last(compute_vwap_from_ticks(prices_series, sizes_series))

                if any(pd.isna(x) for x in [ema_fast_val, ema_slow_val, rsi_val, vwap_val]):
                    logging.debug(
                        f"[SKIP] {symbol} indicators NaN "
                        f"ema_fast={ema_fast_val} ema_slow={ema_slow_val} rsi={rsi_val} vwap={vwap_val} "
                        f"len_prices={len(prices_series)}"
                    )
                    if symbol not in entry_prices:
                        continue
                    logging.warning(
                        "[SKIP_OVERRIDE][%s] Indicators NaN but position is open — forcing sell evaluation",
                        symbol
                    )

                if symbol == "SPY":
                    try:
                        market_series = pd.Series(price_deques["SPY"])
                        market_trend_state = market_trend_filter(market_series)
                        globals()["market_trend_state"] = market_trend_state
                        logging.debug(f"[MARKET] trend_state={market_trend_state}")
                        # PATCH15: update intraday market character
                        _spy_open_char = globals().get("today_open_spy")
                        if _spy_open_char is not None:
                            _spy_move_char = (price - _spy_open_char) / _spy_open_char
                            _update_market_character(_spy_move_char, ts_val)

                        if globals().get("today_open_spy") is None:
                            # PATCH13: Use true 9:30 open bar price instead of first tick price
                            # First tick can arrive minutes into session on fast-moving days
                            _true_open = None
                            try:
                                from alpaca.data.requests import StockBarsRequest
                                from alpaca.data.timeframe import TimeFrame
                                import datetime as _dt
                                _today = ts_val.date()
                                _open_start = datetime(
                                    _today.year, _today.month, _today.day,
                                    13, 30, 0, tzinfo=timezone.utc
                                )  # 9:30 ET = 13:30 UTC
                                _open_end = _open_start + timedelta(minutes=2)
                                _bars_req = StockBarsRequest(
                                    symbol_or_symbols="SPY",
                                    start=_open_start,
                                    end=_open_end,
                                    timeframe=TimeFrame.Minute
                                )
                                _bars = stock_data_client.get_stock_bars(_bars_req).df
                                if _bars is not None and not _bars.empty:
                                    _true_open = float(_bars["open"].iloc[0])
                                    logging.info(
                                        "[DAY_REGIME] PATCH13: True 9:30 open bar fetched: %.4f", 
                                        _true_open
                                    )
                            except Exception as _e:
                                logging.warning(
                                    "[DAY_REGIME] PATCH13: Could not fetch true open bar: %s "
                                    "— falling back to first tick %.4f", _e, price
                                )
                            globals()["today_open_spy"] = _true_open if _true_open is not None else price
                            globals()["today_low_spy"] = globals()["today_open_spy"]
                            logging.info(
                                "[DAY_REGIME] SPY open price captured: %.4f", 
                                globals()["today_open_spy"]
                            )
                        else:
                            globals()["today_low_spy"] = min(
                                globals().get("today_low_spy", price), price
                            )
                        
                        _today_open = globals().get("today_open_spy")
                        _today_low = globals().get("today_low_spy", _today_open)
                        _current_day_regime = globals().get("day_regime", "NEUTRAL_DAY")
                        if _today_open is not None:
                            _spy_move = (price - _today_open) / _today_open
                            _session_min = _session_minutes(ts_val)
                            if _session_min > 30 and _session_min % 5 == 0:
                                if _spy_move <= -0.010 and _current_day_regime not in ("BEAR_DAY", "NEUTRAL_DAY"):
                                    logging.warning(
                                        "[DAY_REGIME] OVERRIDE → NEUTRAL_DAY "
                                        "(SPY move=%.2f%% from open — longs blocked by SPY filter)", _spy_move * 100
                                    )
                                    globals()["day_regime"] = "NEUTRAL_DAY"
                                    
                                # PATCH18: lowered from 1.0% to 0.5% — today 1.0% only reached
                                # at minute 366, 6 min after entry block. 0.5% fires ~10:30 ET.
                                elif _spy_move >= 0.005 and _current_day_regime != "BULL_DAY":
                                    logging.warning(
                                        "[DAY_REGIME] OVERRIDE → BULL_DAY "
                                        "(SPY move=%.2f%% from open)", _spy_move * 100
                                    )
                                    globals()["day_regime"] = "BULL_DAY"
                                    
                                elif (_current_day_regime == "BEAR_DAY" and
                                        _today_low is not None and
                                        _today_low > 0):
                                    _recovery_from_low = (price - _today_low) / _today_low
                                    if _recovery_from_low >= 0.006:
                                        logging.warning(
                                            "[DAY_REGIME] OVERRIDE → NEUTRAL_DAY "
                                            "(SPY recovered %.2f%% from session low=%.4f)",
                                            _recovery_from_low * 100, _today_low
                                        )
                                        globals()["day_regime"] = "NEUTRAL_DAY"
                    except Exception as e:
                        logging.debug(f"[MARKET] trend update failed: {e}")

                # --- Detect regime ---
                regime_raw = detect_regime(prices_series, sizes_series)
                regime, regime_conf = _smooth_regime(symbol, regime_raw)

                upper, ma, lower, bandwidth = compute_bollinger(prices_series, period=20, std=2.0)
                atr_val = compute_atr_from_series(prices_series, period=ATR_PERIOD)
                ema_slope_val = ema_slope(prices_series, period=EMA_SLOW)
                vwap_dist = (vwap_val - price) / vwap_val if vwap_val > 0 else float("nan")
                log_regime_state(ts_val, symbol, regime, bandwidth, atr_val, ema_slope_val, vwap_dist)


                day_bias = detect_day_bias(
                    prices_series,
                    compute_ema_from_series(prices_series, EMA_FAST),
                    compute_ema_from_series(prices_series, EMA_SLOW),
                    compute_vwap_from_ticks(prices_series, sizes_series)
                )

                CONFIG = BULLISH_CONFIG if day_bias == "bullish" else BEARISH_CONFIG
                CONFIG_SESSION = overlay_by_session(CONFIG, ts_val, regime)

                                                
                reattach_orphan_if_needed(
                    symbol, positions_map, entry_times, entry_prices,
                    entry_qty, entry_configs, CONFIG_SESSION
                )

                has_entry = (
                    symbol in entry_prices and
                    entry_prices.get(symbol) is not None and
                    symbol in entry_times and
                    symbol in entry_configs
                )

                accept_exit = False
                reason_exit = None

                if has_entry:
                    ref_entry = entry_prices.get(symbol)
                    active_config = entry_configs.get(symbol, CONFIG_SESSION)

                    entry_regime = entry_configs.get(symbol, {}).get("regime", regime)
                    accept_exit, reason_exit = evaluate_sell(
                        symbol,
                        price,
                        ref_entry,
                        price_deques[symbol],
                        size_deques[symbol],
                        entry_times,
                        active_config,
                        current_time=ts_val,
                        regime=entry_regime,
                        log_stack=True,
                        entry_config=entry_configs.get(symbol),
                        regime_confidence=_regime_confidence.get(symbol, REGIME_CONFIDENCE_MIN)
                    )

                    if accept_exit:
                        if symbol in _pending_sells:
                            logging.warning("[SELL_GUARD] %s already in _pending_sells — skipping duplicate sell", symbol)
                            continue

                        _pending_sells.add(symbol)
                        _snap_qty   = entry_qty.pop(symbol, 0)
                        _snap_price = entry_prices.pop(symbol, None)
                        _snap_time  = entry_times.pop(symbol, None)
                        _snap_cfg   = entry_configs.pop(symbol, None)
                        highest_price_since_entry.pop(symbol, None)
                        trailing_active[symbol] = False
                        last_exit_time[symbol] = ts_val
                        last_exit_reason[symbol] = reason_exit
                        # Clear stale fill price before sell so we can detect if new one arrives
                        _last_sell_fill_price.pop(symbol, None)

                        try:
                            safe_market_sell(
                                trade_client_local=trading_client,
                                symbol=symbol,
                                intended_qty=_snap_qty,
                                order_lock=order_lock,
                                price_deques=price_deques,
                                size_deques=size_deques,
                                snap_entry_price=_snap_price  # PATCH18 I1: fix pnl=nan in audit
                            )
                        except Exception as _sell_err:
                            logging.error("[SELL_GUARD][%s] safe_market_sell raised: %s — restoring state", symbol, _sell_err)
                            if _snap_price is not None:
                                entry_qty[symbol]    = _snap_qty
                                entry_prices[symbol] = _snap_price
                                entry_times[symbol]  = _snap_time
                                entry_configs[symbol] = _snap_cfg
                                highest_price_since_entry[symbol] = _snap_price
                            _pending_sells.discard(symbol)
                            continue
                        finally:
                            _pending_sells.discard(symbol)

                        # PATCH17: use confirmed fill price for accurate loss accounting
                        # Falls back to deque snapshot price if fill not yet confirmed
                        _fill_price = _last_sell_fill_price.get(symbol)
                        _price_for_pnl = _fill_price if _fill_price else price
                        pnl = (_price_for_pnl - _snap_price) * _snap_qty if _snap_price else 0.0
                        if _fill_price:
                            logging.debug(
                                "[PATCH17][PNL] %s using fill_price=%.4f (not deque=%.4f) pnl=%.2f",
                                symbol, _fill_price, price, pnl
                            )
                        else:
                            logging.debug(
                                "[PATCH17][PNL] %s fill_price not available — using deque=%.4f pnl=%.2f",
                                symbol, price, pnl
                            )

                        if pnl < 0:
                            _session_loss_count[symbol] += 1
                            globals()["_consecutive_losses"] = \
                                globals().get("_consecutive_losses", 0) + 1
                            if _session_loss_count[symbol] >= SESSION_LOSS_BLACKLIST_THRESHOLD:
                                _session_blacklist.add(symbol)
                                # PATCH17: record SPY level and timestamp at blacklist moment
                                _bl_spy_dq = price_deques.get("SPY")
                                _session_blacklist_spy_level[symbol] = float(_bl_spy_dq[-1]) \
                                    if _bl_spy_dq and len(_bl_spy_dq) > 0 else None
                                _session_blacklist_time[symbol] = ts_val
                                logging.warning(
                                    "[PATCH17] %s added to session blacklist after %d losses "
                                    "(pnl=%.2f fill=%.4f spy_at_block=%s)",
                                    symbol, _session_loss_count[symbol], pnl, _price_for_pnl,
                                    f"{_session_blacklist_spy_level.get(symbol):.4f}"
                                    if _session_blacklist_spy_level.get(symbol) else "N/A"
                                )
                        else:
                            globals()["_consecutive_losses"] = 0

                        qty = _snap_qty  # restore for audit row below

                        exec_rows.append({
                            "timestamp": _audit_ts(ts_val),
                            "symbol": symbol,
                            "action": "SELL",
                            "price": price,
                            "reason": reason_exit,
                            "bias": day_bias,
                            "pnl": round(pnl, 4),
                            "ema_fast": round(ema_fast_val, 4),
                            "ema_slow": round(ema_slow_val, 4),
                            "rsi": round(rsi_val, 2),
                            "vwap": round(vwap_val, 4),
                            "regime": regime
                        })
                        write_exec_row_immediate(exec_rows[-1], symbol, RUN_MODE)

                       

                        continue

                budget_exhausted = (len([s for s in entry_prices if entry_prices.get(s) is not None]) >= MAX_CONCURRENT_POSITIONS)
                if budget_exhausted:
                    logging.debug("[BUDGET] %s skipped — max positions reached", symbol)
                    continue

                _day_regime = globals().get("day_regime", "NEUTRAL_DAY")
                
                # PATCH15: per-symbol mode selection
                _spy_open_mode = globals().get("today_open_spy")
                _spy_deque_mode = globals().get("price_deques", {}).get("SPY")
                _spy_now_mode = float(_spy_deque_mode[-1]) \
                    if _spy_deque_mode and len(_spy_deque_mode) > 0 else None
                _spy_move_mode = (_spy_now_mode - _spy_open_mode) / _spy_open_mode \
                    if _spy_open_mode and _spy_now_mode else 0.0
                _symbol_mode = _get_symbol_mode(symbol, prices_series, _spy_move_mode)

                # PATCH15: MODE1 recovery entry check
                # PATCH19 Option C: context gate — MODE1 only fires in supportive conditions
                _rec_accept = False
                _rec_reason = None
                _rec_stack = {}
                _market_char_m1 = globals().get("_market_character", "NEUTRAL")
                _mode1_context_ok = (
                    day_bias != "bearish" and
                    _market_char_m1 != "BEAR"
                )
                if (_symbol_mode == "MODE1" and
                        _mode1_context_ok and
                        (symbol not in entry_prices or entry_prices.get(symbol) is None)):
                    _rec_accept, _rec_reason, _, _rec_stack = evaluate_recovery_entry(
                        symbol, price, prices_series, sizes_series,
                        ts_val, positions_map, inflight_orders,
                        pending_entries, last_exit_time[symbol], last_buy_time
                    )
                    if _rec_accept:
                        logging.info(
                            "[PATCH19][MODE1] %s recovery entry | price=%.4f | %s",
                            symbol, price, _rec_stack
                        )

                
                accept, reason, score, stack = evaluate_entry(
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
                    CONFIG_SESSION,
                    regime,
                    bias=day_bias,
                    log_stack=True
                )

                # PATCH18 C1: log every entry attempt in LIVE mode
                # Was only called in SIM branch — audit_entry_live.csv empty since Dec 17
                try:
                    log_entry_attempt(
                        ts_val, symbol, regime, day_bias,
                        accept, reason, score,
                        ema_fast_val, ema_slow_val, rsi_val, vwap_val, price
                    )
                except Exception as _log_err:
                    logging.debug("[PATCH18] log_entry_attempt failed for %s: %s", symbol, _log_err)

                # PATCH18 I4: track dominant block reason per symbol for EOD report
                if not accept:
                    _session_block_reasons[symbol][reason] = \
                        _session_block_reasons[symbol].get(reason, 0) + 1

                budget_exhausted = (len([s for s in entry_prices if entry_prices.get(s) is not None]) >= MAX_CONCURRENT_POSITIONS)
                # PATCH15: combine MODE1 recovery and MODE2 evaluate_entry signals
                if not accept and _rec_accept:
                    accept = True
                    reason = _rec_reason
                    stack = _rec_stack

                if accept:
                    if symbol in entry_prices and entry_prices.get(symbol) is not None:
                        logging.info("[SKIP] %s already in position — skipping duplicate BUY", symbol)
                        continue

                    if symbol in pending_entries:
                        logging.info("[SKIP] %s already pending — skipping duplicate BUY", symbol)
                        continue

                    pending_entries.add(symbol)
                    try:
                        if spent_this_loop + (price * 10) > max_loop_budget:
                            logging.info("[BUDGET] %s skipped — spent_this_loop=%.2f would exceed max=%.2f",
                                         symbol, spent_this_loop, max_loop_budget)
                            continue

                        current_open_positions = len([s for s in entry_prices if entry_prices.get(s) is not None])
                        budget_exhausted = (current_open_positions >= MAX_CONCURRENT_POSITIONS)
                        if budget_exhausted:
                            logging.debug("[BUDGET] Max concurrent positions reached (%d) — entries blocked this loop",
                                         current_open_positions)
                        else:
                            estimated_cost = price * int((max_loop_budget * BUY_CASH_BUFFER) // price)
                            spent_this_loop += estimated_cost

                            safe_market_buy(
                                trade_client_local=trading_client,
                                symbol=symbol,
                                cash_for_buy=max_loop_budget,
                                order_lock=order_lock,
                                price_deques=price_deques,
                                size_deques=size_deques,
                                entry_times=entry_times,
                                entry_prices=entry_prices,
                                entry_qty=entry_qty,
                                entry_configs=entry_configs,
                                bias=day_bias,
                                config_session=CONFIG_SESSION
                            )
                    finally:
                        pending_entries.discard(symbol)

            # === ENTRY DIAGNOSTIC — silent monitoring to detect blocked entries ===
                

            # === UPDATE SESSION STATE EVERY 5 MINUTES ===
            _now_for_state = datetime.now(timezone.utc)
            _last_state_update = globals().get("_session_state_last_update")
            _should_update = (
                _last_state_update is None or
                (_now_for_state - _last_state_update).total_seconds() >= 300
            )
            if _should_update:
                _new_state = get_session_state()
                _old_state = globals().get("_current_session_state", "NEUTRAL_SESSION")
                globals()["_current_session_state"] = _new_state
                globals()["_session_state_last_update"] = _now_for_state
                if _new_state != _old_state:
                    logging.warning(
                        "[SESSION_STATE] *** STATE CHANGED: %s → %s *** "
                        "Exit thresholds updated for all open positions.",
                        _old_state, _new_state
                    )

            # === ENTRY DIAGNOSTIC ===
            _spy_open_diag = globals().get("today_open_spy")
            _spy_deque_diag = globals().get("price_deques", {}).get("SPY")
            _spy_move_pct = 0.0
            if _spy_open_diag and _spy_deque_diag and len(_spy_deque_diag) > 0:
                _spy_move_pct = (float(_spy_deque_diag[-1]) - _spy_open_diag) / _spy_open_diag * 100
            _open_longs = len([s for s in entry_prices if entry_prices.get(s) is not None])
            _open_shorts = len([s for s in short_entry_prices if short_entry_prices.get(s) is not None])
            _active_rockets = [s for s in _rocket_mode_active if _rocket_mode_active.get(s)]
            logging.debug(
                "[ENTRY_DIAG] SPY_move=%.2f%% | day_regime=%s | market_char=%s | "
                "session=%s | longs=%d | shorts=%d | longs_blocked=%s | "
                "shorts_available=%s | rocket_active=%s",
                _spy_move_pct,
                globals().get("day_regime", "UNKNOWN"),
                globals().get("_market_character", "NEUTRAL"),
                globals().get("_current_session_state", "NEUTRAL_SESSION"),
                _open_longs,
                _open_shorts,
                "YES" if _spy_move_pct <= -0.5 else "NO",
                "YES" if _spy_move_pct <= -0.5 else "NO",
                ",".join(_active_rockets) if _active_rockets else "none"
            )

            # === EOD FORCED LIQUIDATION ===
            # Runs at 15:55 ET
            # Closes all positions and saves SPY prev_close for tomorrow's day_regime
            force_liquidation_at_cutoff(trading_client, symbols)
            
           
            elapsed = (datetime.now(timezone.utc) - loop_start).total_seconds()
            if elapsed < LOOP_SLEEP:
                time.sleep(LOOP_SLEEP - elapsed)

        except Exception as e:
            logging.exception(f"[LIVE LOOP] error: {e}")
            time.sleep(1.0)
         
if __name__ == "__main__":
    main()

           
