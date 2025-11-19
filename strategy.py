import logging
import time
import os
import threading
from collections import deque
from datetime import datetime, timezone, timedelta
from collections import deque, defaultdict

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
    "TP_PCT": 0.0025,
    "SL_MULTIPLIER": 0.6,
    "TS_ACTIVATION_BUFFER": 0.004,
    "TRAILING_STOP_PCT": 0.006,
    "MAX_TRADES": 3,
    "MAX_LOSS_DAY": 0.9,
    "VWAP_DELTA": 0.002,
    "EMA_DELTA": 0.0003,
    "RSI_FAIL_TICKS": 5
}


SCALP = True
LOOP_SLEEP = 0.5
TICKS_WINDOW = 300
EMA_FAST = 9
EMA_SLOW = 20
RSI_PERIOD = 14
RSI_COOL_THRESHOLD = 3    # esim. raja-arvo RSI:lle "cool down" -tilanteessa
VOL_SPIKE_MULT = 1.2
ATR_PERIOD = 10
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

TREND_CONFIG = {
    "TP_PCT": 0.0020,
    "SL_MULTIPLIER": 1.0,
    "TS_ACTIVATION_BUFFER": 0.004,
    "TRAILING_STOP_PCT": 0.005,
    "MAX_TRADES": 8,
    "MAX_LOSS_DAY": 1.5,
    "VWAP_DELTA": 0.003,
    "EMA_DELTA": 0.0003,
    "RSI_FAIL_TICKS": 5,
    # Entry scoring thresholds
    "ENTRY_SCORE_THRESHOLD": 1.6,
    # Indicator gate weights
    "WEIGHTS": {
        "ema_trend": 1.0,      # EMA_fast > EMA_slow + slope positive
        "vwap_above": 0.8,     # location above VWAP
        "macd_momentum": 0.8,  # MACD > signal, rising histogram
        "vol_confirm": 0.6,    # volume spike vs median
        "pullback_ok": 0.6,     # pullback to EMA_slow/VWAP then re-accel
        "rsi_ok": 0.7   # new weight
    },
    # pullback tolerances (distance normalized by price)
    "PULLBACK_TOL": 0.0012
}

RANGE_CONFIG = {
    "TP_PCT": 0.0015,
    "SL_MULTIPLIER": 0.8,
    "TS_ACTIVATION_BUFFER": 0.003,
    "TRAILING_STOP_PCT": 0.004,
    "MAX_TRADES": 6,
    "MAX_LOSS_DAY": 1.2,
    "VWAP_DELTA": 0.002,
    "EMA_DELTA": 0.0002,
    "RSI_FAIL_TICKS": 4,
    "ENTRY_SCORE_THRESHOLD": 0.6,
    "WEIGHTS": {
        "lower_band_touch": 1.0,     # price near lower Bollinger band
        "rsi_uptick": 1.0,           # RSI < 35 and upticking
        "vwap_reversion": 0.8,       # distance to VWAP favorable
        "bandwidth_ok": 0.4,         # range (not too wide, not too tight)
        "vol_not_dry": 0.5,           # avoid illiquid chop
        "rsi_ok": 0.7
        
    },
    "BOLL_PERIOD": 20,
    "BOLL_STD": 2.0,
    "BANDWIDTH_MAX": 0.01,  # max relative bandwidth to still count as range
    "BANDWIDTH_MIN": 0.002  # avoid ultra-tight no-move
}

HIGH_VOL_CONFIG = {
    "TP_PCT": 0.0035,
    "SL_MULTIPLIER": 1.2,
    "TS_ACTIVATION_BUFFER": 0.006,
    "TRAILING_STOP_PCT": 0.007,
    "MAX_TRADES": 4,
    "MAX_LOSS_DAY": 1.0,
    "VWAP_DELTA": 0.003,
    "EMA_DELTA": 0.0003,
    "RSI_FAIL_TICKS": 4,
    "ENTRY_SCORE_THRESHOLD": 3.2,
    "WEIGHTS": {
        "atr_high": 1.0,          # ATR in upper decile of rolling window
        "bb_expanding": 0.8,      # Bollinger bandwidth expansion
        "macd_strong": 0.8,       # strong momentum
        "vol_roc": 0.6,           # volume rate-of-change positive
        "breakout_bar": 0.6       # price extends above recent high
    },
    "ATR_WINDOW": 50,
    "ATR_TOP_PCT": 0.8,          # top 20% percentile considered high
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
    "ENVELOPE_PCT": 0.002,        # +/- around EMA_slow
    "BOLL_PERIOD": 20,
    "BOLL_STD": 2.0,
    "BANDWIDTH_CAP": 0.003       # low-vol consolidation cap
}

# === ENTRY GATE TOGGLE ===
USE_REGIME_ENTRY = True     # if False, uses your original buy_conditions_met
LOG_SIGNAL_STACK_ON_ACCEPT = False  # you asked for full logs; toggle to True if needed


# === LOGGING ===
logging.basicConfig(level=logging.INFO,
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

# === helpers: indicators ===
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

def safe_market_buy(trade_client_local, symbol, cash_amount, order_lock):
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
                qty = int((cash_amount * BUY_CASH_BUFFER) // est_price)
            else:
                qty = 1
            if qty <= 0 or (est_price and qty * est_price < MIN_TRADE_USD):
                logging.debug("Computed buy qty too small for %s (qty=%s est_price=%s cash=%.2f)",
                              symbol, qty, est_price, cash_amount)
                return None
            order = MarketOrderRequest(
                symbol=symbol, qty=qty, side=OrderSide.BUY,
                type=OrderType.MARKET, time_in_force=TimeInForce.DAY
            )
            submitted = trade_client_local.submit_order(order)
            logging.info("%s - BUY submitted qty=%d (est_price=%s cash=%.2f)",
                         symbol, qty, str(est_price), cash_amount)
            logging.info("%s - BUY assumed filled qty=%d", symbol, qty)
            return submitted
        except Exception as e:
            logging.exception("safe_market_buy error for %s: %s", symbol, e)
            return None

def safe_market_sell(trade_client_local, symbol, intended_qty, order_lock):
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
            logging.info("%s - SELL submitted qty=%d (available=%d intended=%s) order_id=%s",
                         symbol, qty_to_sell, available, intended_qty, order_id)

            if order_id:
                try:
                    max_retries = 5
                    status = None
                    for attempt in range(max_retries):
                        confirmed = trade_client_local.get_order_by_id(order_id)
                        status = getattr(confirmed, "status", None)
                        logging.info("%s - SELL order %s status=%s (attempt %d/%d)",
                                     symbol, order_id, status, attempt+1, max_retries)
                        if status == "filled":
                            entry_times.pop(symbol, None)
                            entry_prices.pop(symbol, None)
                            entry_qty.pop(symbol, None)
                            entry_configs.pop(symbol, None)
                            last_exit_time[symbol] = datetime.now(timezone.utc)
                            logging.info("%s - EXIT state cleanup completed", symbol)
                            break
                        time.sleep(1.0)
                    else:
                        logging.warning("%s - SELL order %s not filled after %d retries (last status=%s)",
                                        symbol, order_id, max_retries, status)
                except Exception as e:
                    logging.warning("%s - Could not verify SELL order %s: %s", symbol, order_id, e)

            return submitted
        except Exception as e:
            logging.exception("safe_market_sell error for %s: %s", symbol, e)
            return None

# === STRATEGY HELPERS: BUY/SELL CONDITIONS ===
# === BIAS DETECTION ===
def detect_day_bias(prices_series, ema_fast_series, ema_slow_series, vwap_series):
    try:
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
def detect_regime(prices_series, sizes_series):
    """
    Classify intraday regime using only locally computed features.
    Returns one of: "TREND", "RANGE", "HIGH_VOL", "LOW_VOL".
    """
    # VWAP, EMA context
    ema_fast = compute_ema_from_series(prices_series, EMA_FAST).iloc[-1] if len(prices_series) >= 2 else float('nan')
    ema_slow = compute_ema_from_series(prices_series, EMA_SLOW).iloc[-1] if len(prices_series) >= 2 else float('nan')
    vwap_val = compute_vwap_from_ticks(prices_series, sizes_series).iloc[-1] if len(sizes_series) else float('nan')
    slope = ema_slope(prices_series, EMA_SLOW)

    # Vol and bandwidth
    atr_val = compute_atr_from_series(prices_series, ATR_PERIOD)
    upper, ma, lower, bandwidth = compute_bollinger(prices_series, period=20, std=2.0)

    # Heuristics:
    # - HIGH_VOL: ATR relatively high vs recent distribution + bandwidth expansion
    # - LOW_VOL: bandwidth very low
    # - TREND: EMA_fast > EMA_slow, slope positive, price on the correct side of VWAP
    # - RANGE: otherwise, with bandwidth within moderate bounds

    # Compute a simple ATR percentile proxy over last N:
    N = 50
    if len(prices_series) >= N + 2:
        atr_series = prices_series.diff().abs().rolling(ATR_PERIOD).mean()
        hist = atr_series.iloc[-N:].dropna()
        if len(hist) > 10 and not pd.isna(atr_val):
            pct = (hist < atr_val).mean()  # fraction below current ATR
        else:
            pct = 0.5
    else:
        pct = 0.5

    # Decision (reordered: HIGH_VOL → TREND → LOW_VOL → RANGE)
    # HIGH_VOL: loosened to require either ATR percentile OR bandwidth expansion
    if (pct >= HIGH_VOL_CONFIG["ATR_TOP_PCT"]) or (not pd.isna(bandwidth) and bandwidth > RANGE_CONFIG["BANDWIDTH_MAX"]):
        return "HIGH_VOL"

    # TREND: loosened to allow slope >= 0 and requireed to allow slope >= 0 and require price above VWAP
    if (not pd.isna(ema_fast) and not pd.isna(ema_slow) and ema_fast > ema_slow) \
       and (not pd.isna(slope) and slope >= 0) \
 price above VWAP
    if (not pd.isna(ema_fast) and not pd.isna(ema_slow) and ema_fast > ema_slow) \
       and (not pd.isna(slope) and slope >= 0) \
       and (not pd.isna(vwap_val) and prices_series.iloc[-1] >= vwap_val):
        return "TREND"

    #       and (not pd.isna(vwap_val) and prices_series.iloc[-1] >= vwap_val):
        return "TREND"

    # LOW_VOL: only if LOW_VOL: only if bandwidth is very tight
    if not pd.isna(bandwidth) and bandwidth <= LOW_VOL_CONFIG["BANDWIDTH_CAP"]:
        return "LOW_VOL"
    # RANGE: explicit fallback when bandwidth bandwidth is very tight
    if not pd.isna(bandwidth) and bandwidth <= LOW_VOL_CONFIG["BANDWIDTH_CAP"]:
        return "LOW_VOL"

    # RANGE: explicit fallback when bandwidth is moderate
    
    if not pd.isna(bandwidth) and RANGE_CONFIG["BANDWIDTH_MIN"] <= bandwidth <= RANGE_CONFIG["BANDWIDTH_MAX"]:
        return "RANGE"

    # Default fallback
    return "RANGE"    



# === REGIME-AWARE ENTRY SCORING (replacement gate) ===
def evaluate_entry(sym, price, size, prices_series, sizes_series, ts_val,
                   positions_map, inflight_orders, pending_entries,
                   last_exit, last_buy_time, CONFIG, regime,
                   log_stack=False):
    """
    Returns (accept: bool, reason: str, score: float, signal_stack: dict)
    Gate is regime-dependent. Keeps your cooldown and position safety checks.
    """
    # Cooldown
    since_last_exit = (ts_val - last_exit).total_seconds() if last_exit is not None else float("inf")
    since_last_buy = (ts_val - last_buy_time[sym]).total_seconds() if last_buy_time[sym] is not None else float("inf")
    if since_last_exit < COOLDOWN_SECONDS or since_last_buy < COOLDOWN_SECONDS:
        return (False, "Cooldown", 0.0, {})

    # Position/order checks
    no_position = positions_map.get(sym, (0, 0.0))[0] == 0
    inflight_none = inflight_orders.get(sym) is None
    not_pending = sym not in pending_entries
    if not (no_position and inflight_none and not_pending):
        return (False, "Position/order block", 0.0, {})

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

    # RSI uptick check
    rsi_series_full = compute_rsi_from_series(prices_series, RSI_PERIOD)
    rsi_prev = rsi_series_full.iloc[-2] if len(rsi_series_full) >= 2 else float('nan')
    rsi_uptick = (not pd.isna(rsi_prev) and not pd.isna(rsi_val) and rsi_val > rsi_prev)

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
    vol_roc_ok = (not pd.isna(vol_roc_val) and vol_roc_val > 0.2)
    breakout_bar = (price > recent_high(prices_series, HIGH_VOL_CONFIG["BREAKOUT_LOOKBACK"]))

    # Low-vol checks
    envelope_lower = (ema_slow * (1 - LOW_VOL_CONFIG["ENVELOPE_PCT"])) if not pd.isna(ema_slow) else float('nan')
    envelope_touch = (not pd.isna(envelope_lower) and price <= envelope_lower)
    chop_high = (not pd.isna(bandwidth) and bandwidth <= LOW_VOL_CONFIG["BANDWIDTH_CAP"])
    vol_ok_low = vol_spike or (not pd.isna(median_vol) and median_vol > 0)  # avoid totally dry tapes
    vwap_below = (not pd.isna(vwap_val) and price < vwap_val)

    # Base sanity filters to avoid nonsense:
    if pd.isna(ema_fast) or pd.isna(ema_slow) or pd.isna(vwap_val) or pd.isna(rsi_val):
        return (False, "Missing core indicators", 0.0, {})

    signal_stack = {}
    score = 0.0

    if regime == "TREND":
        w = TREND_CONFIG["WEIGHTS"]
        ema_trend_ok = (ema_fast > ema_slow) and (slope > 0)
        vwap_above_ok = (price > vwap_val) and ((price - vwap_val) > CONFIG["VWAP_DELTA"] * vwap_val)
        macd_ok = (not pd.isna(macd_line) and not pd.isna(macd_signal) and macd_line > macd_signal and macd_hist > 0)
        pullback_ok = (pullback_to_ema or pullback_to_vwap)
        vol_ok = vol_spike

        signal_stack.update({
            "ema_trend_ok": ema_trend_ok,
            "vwap_above_ok": vwap_above_ok,
            "macd_ok": macd_ok,
            "pullback_ok": pullback_ok,
            "vol_ok": vol_ok
        })
        score += w["ema_trend"] if ema_trend_ok else 0.0
        score += w["vwap_above"] if vwap_above_ok else 0.0
        score += w["macd_momentum"] if macd_ok else 0.0
        score += w["pullback_ok"] if pullback_ok else 0.0
        score += w["vol_confirm"] if vol_ok else 0.0

    elif regime == "RANGE":
        w = RANGE_CONFIG["WEIGHTS"]
        lb_touch = lower_touch
        rsi_mean_rev = (rsi_val < 35 and rsi_uptick)
        vwap_rev_ok = vwap_reversion_room
        bandwidth_ok = (not pd.isna(bandwidth) and RANGE_CONFIG["BANDWIDTH_MIN"] <= bandwidth <= RANGE_CONFIG["BANDWIDTH_MAX"])
        vol_ok = (not pd.isna(median_vol) and median_vol > 0)

        signal_stack.update({
            "lower_band_touch": lb_touch,
            "rsi_uptick": rsi_mean_rev,
            "vwap_reversion": vwap_rev_ok,
            "bandwidth_ok": bandwidth_ok,
            "vol_not_dry": vol_ok,
            "rsi_ok": (MIN_RSI_FOR_ENTRY <= rsi_val <= MAX_RSI_FOR_ENTRY)
         
        })
        score += w["lower_band_touch"] if lb_touch else 0.0
        score += w["rsi_uptick"] if rsi_mean_rev else 0.0
        score += w["vwap_reversion"] if vwap_rev_ok else 0.0
        score += w["bandwidth_ok"] if bandwidth_ok else 0.0
        score += w["vol_not_dry"] if vol_ok else 0.0
        score += w["rsi_ok"] if (MIN_RSI_FOR_ENTRY <= rsi_val <= MAX_RSI_FOR_ENTRY) else 0.0

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
        w = LOW_VOL_CONFIG["WEIGHTS"]
        vwap_below_ok = vwap_below
        rsi_mr_ok = (rsi_val < 35 and rsi_uptick)
        envelope_touch_ok = envelope_touch
        chop_ok = chop_high
        vol_ok = vol_ok_low

        signal_stack.update({
            "vwap_below": vwap_below_ok,
            "rsi_uptick": rsi_mr_ok,
            "envelope_touch": envelope_touch_ok,
            "chop_high": chop_ok,
            "vol_ok": vol_ok
        })
        score += w["vwap_below"] if vwap_below_ok else 0.0
        score += w["rsi_uptick"] if rsi_mr_ok else 0.0
        score += w["envelope_touch"] if envelope_touch_ok else 0.0
        score += w["chop_high"] if chop_ok else 0.0
        score += w["vol_ok"] if vol_ok else 0.0

   

    # Final gate
    threshold = CONFIG.get("ENTRY_SCORE_THRESHOLD", 3.0)
    accept = (score >= threshold)

    if log_stack and (accept or AUDIT_TRAIL_ENABLED):
        logging.info(f"[ENTRY_STACK][{sym}] regime={regime} score={score:.2f} threshold={threshold} stack={signal_stack}")

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
        adj["TS_ACTIVATION_BUFFER"] = max(0.002, CONFIG["TS_ACTIVATION_BUFFER"] * 0.9)
    # TREND uses base CONFIG
    return adj



def evaluate_sell(sym, last_price, ref_entry, price_deque, size_deque, entry_times,
                  CONFIG, ema_fast_period=EMA_FAST, ema_slow_period=EMA_SLOW, rsi_period=RSI_PERIOD, current_time=None):
    """
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

                # --- Hard exits with regime overlay ---
        # Detect regime from local series (SIM and LIVE identical)
        regime = detect_regime(prices_series, sizes_series)
        CONFIG_E = overlay_exit_params_by_regime(CONFIG, regime)

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
        logging.error("[%s] Sell evaluation failed: %s", sym, str(e))
        return False, None


# === AUDIT TRAIL (LIVE) — removable block ===
# Käyttää suoraan price_deques ja size_deques rakenteita
# Outcome labels:
#   - good_block   : filtteri esti kaupan, joka olisi mennyt tappiolle
#   - bad_block    : filtteri esti kaupan, joka olisi mennyt voitolle
#   - neutral_block: ei TP/SL osumaa seurantajakson aikana

AUDIT_TRAIL_ENABLED = True
AUDIT_OUTCOME_WINDOW_MIN = 30     # seurantajakso minuutteina
AUDIT_CSV_FILE = "audit_blocks_live.csv"

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
                    "tp_pct","sl_multiplier","outcome","window_min"
                ]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                if not file_exists:
                    writer.writeheader()
                writer.writerow(row_dict)
    except Exception as e:
        logging.warning("[AUDIT] CSV write failed: %s", e)

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
            "window_min": AUDIT_OUTCOME_WINDOW_MIN
        })
        logging.info("[AUDIT][%s] outcome=%s reason=%s ref=%.4f window=%dm",
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

                
        # Alusta tilarakenteet
        inflight_orders = {}
        pending_entries = set()
        last_exit_time = {s: None for s in symbols}
        last_buy_time = {s: None for s in symbols}

        start = "2025-11-14T14:30:00Z"
        end = "2025-11-14T21:00:00Z"
    
        req = StockTradesRequest(symbol_or_symbols=symbol, start=start, end=end)
        trades = stock_data_client.get_stock_trades(req).df
    
        # Aikaleima indeksiin
        trades.index = pd.to_datetime(trades.index.get_level_values(1))
    
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

        last_bias = None
            
        for ts, row in trades.iterrows():
            try:
                ts_val = pd.to_datetime(ts, utc=True)
                price = float(row["price"])
                size = float(row["size"])
            except Exception as e:
                logging.error("[%s] Could not parse row: %s", RUN_MODE, e)
                continue
    
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

                                      
            positions_map = {}
    
            if USE_REGIME_ENTRY:
                regime = detect_regime(prices, sizes_series)
                accept, reason, score, stack = evaluate_entry(
                    symbol, price, size, prices, sizes_series, ts_val,
                    positions_map, inflight_orders, pending_entries,
                    last_exit_time[symbol], last_buy_time,
                    CONFIG, regime, log_stack=False
                )
                buy = accept
            else:
                buy, reason = buy_conditions_met(
                    symbol, price, size, ema_fast, ema_slow, rsi_val, vwap_val,
                    sizes_series, prices, last_exit_time[symbol], positions_map,
                    inflight_orders, pending_entries, last_buy_time, ts_val, CONFIG
                )

                if buy:
                    csv_rows.append({
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

                    entry_price = price
                    entry_times[symbol] = ts_val
                    entry_prices[symbol] = price
                    est_price = price
                    qty = int((max_loop_budget * BUY_CASH_BUFFER) // est_price)
                    if qty <= 0 or qty * est_price < MIN_TRADE_USD:
                        logging.info("%s - Skipping buy: qty too small (est_price=%.2f)",
                                     symbol, est_price)
                        continue
                    entry_qty[symbol] = qty
                    
                    
                    in_position = True
                    highest_price_since_entry[symbol] = price
                    last_buy_time[symbol] = ts_val
                    # --- PATCH: jäädytä config position ajaksi ---
                    entry_configs[symbol] = CONFIG
                    logging.info(f"{symbol} [{RUN_MODE}] BUY @ {price:.4f} | Trigger={reason} | Bias={day_bias} | Config={CONFIG}")
                else:
                    highest_price_since_entry[symbol] = max(highest_price_since_entry[symbol], price)
                    sell, reason = evaluate_sell(
                        symbol, price, entry_prices[symbol],
                        price_deques[symbol], size_deques[symbol], entry_times, entry_configs[symbol], current_time=ts_val
                    )
                if sell:
                    qty = entry_qty.get(symbol, 1)
                    pnl = (price - entry_price) * qty
                    logging.info(f"{symbol} [{RUN_MODE}] SELL qty={qty} @ {price:.4f} | Reason={reason} | Bias={day_bias} | Config={CONFIG} | PnL={pnl:.4f}")
                    csv_rows.append({
                    "timestamp": ts_val.strftime("%Y-%m-%d %H:%M:%S"),
                    "symbol": symbol,
                    "action": "SELL",
                    "price": price,
                    "reason": reason,
                    "pnl": round(pnl, 4),
                    "ema_fast": round(ema_fast, 4),
                    "ema_slow": round(ema_slow, 4),
                    "rsi": round(rsi_val, 2),
                    "vwap": round(vwap_val, 4)
                })

                    logging.info(f"{symbol} [{RUN_MODE}] SELL @ {price:.4f} | Reason={reason} | PnL={pnl:.4f} | Time={ts_val.strftime('%Y-%m-%dT%H:%M:%S')}")
                    in_position = False
                    entry_price = None
                    last_exit_time[symbol] = ts_val
                    highest_price_since_entry.pop(symbol, None)
                    entry_configs.pop(symbol, None)
    
        logging.info("%s replay finished for %s", RUN_MODE, symbol)
        with open(csv_filename, mode="w", newline="") as f:
            fieldnames = ["timestamp", "symbol", "action", "price", "reason", "pnl", "ema_fast", "ema_slow", "rsi", "vwap"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(csv_rows)
        
        logging.info("Trades saved to %s", csv_filename)

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
            positions = trade_client_local.get_open_positions()
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
            positions_map = get_positions_map(trade_client)
            spent_this_loop = 0.0
            max_loop_budget = calculate_buying_power_limit(trade_client, BUY_POWER_LIMIT)

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
                    logging.info(f"{sym} cooldown check: buy={since_last_buy:.2f}s exit={since_last_exit:.2f}s")
                    if since_last_buy < COOLDOWN_SECONDS or since_last_exit < COOLDOWN_SECONDS:
                        logging.info(f"{sym} - Cooldown active: buy={since_last_buy:.1f}s exit={since_last_exit:.1f}s")
                        continue

                    if USE_REGIME_ENTRY:
                        regime = detect_regime(prices, sizes_series)
                        accept, reason, score, stack = evaluate_entry(
                            sym, price, size, prices, sizes_series, ts_val,
                            positions_map, inflight_orders, pending_entries,
                            last_exit_time[sym], last_buy_time,
                            CONFIG, regime, log_stack=False
                        )
                        buy = accept
                    else:
                        buy, reason = buy_conditions_met(
                            sym, price, size, ema_fast, ema_slow, rsi_val, vwap_val,
                            sizes_series, prices, last_exit_time[sym], positions_map,
                            inflight_orders, pending_entries, last_buy_time, ts_val, CONFIG
                        )

                    
                    
                    # AUDIT: jos BUY hylättiin, käynnistä watchdog dequen datalla
                    if AUDIT_TRAIL_ENABLED and not buy:
                        audit_rejection_live(
                            sym, ts_val, price, size, ema_fast, ema_slow,
                            rsi_val, vwap_val, sizes_series, day_bias, CONFIG, reason,
                            price_deques, size_deques
                        ) 

                    
                    if buy:    
                        if (datetime.now(timezone.utc) - last_trade_attempt[sym]).total_seconds() < 1.0:
                            continue
                        last_trade_attempt[sym] = datetime.now(timezone.utc)

                        est_trade_cost = price * int((max_loop_budget * BUY_CASH_BUFFER) // price)
                        if spent_this_loop + est_trade_cost > max_loop_budget:
                            logging.info(f"{sym} - Skipping buy: budget exceeded. est_cost={est_trade_cost:.2f} spent={spent_this_loop:.2f}")
                            continue

                        pending_entries.add(sym)
                        try:
                            spent_this_loop += est_trade_cost  # reserve budget immediately
                            submitted = safe_market_buy(trade_client, sym, max_loop_budget * BUY_CASH_BUFFER, order_lock)
                            logging.debug(f"[TRACE] Buy submitted: {submitted}")
                            if submitted:
                                inflight_orders[sym] = getattr(submitted, "id", None) or True
                                # 🔍 Retry loop for post-buy verification
                                actual_qty = 0
                                for attempt in range(3):
                                    actual_qty = get_position_qty(trade_client, sym)
                                    logging.debug(f"[TRACE] Post-buy verification attempt {attempt+1} for {sym}: actual_qty={actual_qty}")
                                    if actual_qty > 0:
                                        break
                                    time.sleep(1.0)
                              
                                if actual_qty > 0:
                                    entry_qty[sym] = actual_qty
                                    entry_prices[sym] = price
                                    entry_times[sym] = datetime.now(timezone.utc)
                                    last_buy_time[sym] = datetime.now(timezone.utc)
                                    entry_configs[sym] = CONFIG
                                    logging.info(f"{sym} - ENTRY recorded qty={entry_qty[sym]} price={price:.2f} rsi={rsi_val:.2f} Bias={day_bias} Config={CONFIG}")
                                    
                                else:
                                    logging.warning(f"[TRACE] Buy assumed filled but no position found for {sym}")
      
                        except Exception as e:
                            logging.exception("%s - BUY error: %s", sym, str(e))
                        finally:
                            inflight_orders.pop(sym, None)
                    


                    # === SELL LOGIC (evaluate_sell) ===
                    if qty_open > 0:
                        try:
                            last_price = float(price)
                            ref_entry = entry_prices.get(sym, avg_entry)
                            
                            if sym in entry_configs:
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
                            else:
                                logging.error("[%s] Sell skipped: no entry_config found", sym)
                                continue
                            if sell:
                                submitted = safe_market_sell(trade_client, sym, qty_open, order_lock)
                                logging.info(f"{sym} - SCALP SELL qty={qty_open} @ {last_price:.4f} | Reason={reason} | Bias={day_bias} | Config={CONFIG}")
                                logging.info(
                                    "%s - SCALP SELL qty=%d @ %.4f | Reason=%s | EntryRef=%.4f",
                                    sym, qty_open, last_price, reason, ref_entry
                                )
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
