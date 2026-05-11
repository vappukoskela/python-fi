"""
Intraday Breakout Scalper
Implementation of strategy specification v1.0

Strategy: Long-only intraday breakout scalping on liquid US large-cap equities.
Entry: 15-minute price breakout + volume confirmation, with market state filters.
Exit: 0.5% stop / 1.0% TP / 30-min timeout, with runner mode in bullish conditions.
Risk: 2% daily kill switch, per-symbol session block, EOD forced close.

This file implements the specification document; the document is the source of
truth. Where code and spec disagree, the spec is correct and the code must be
fixed.
"""

# ============================================================================
# SECTION 1: IMPORTS
# ============================================================================

import os
import csv
import time
import logging
import threading
from collections import deque, defaultdict
from datetime import datetime, timezone, timedelta, date
from zoneinfo import ZoneInfo

import pandas as pd
from dotenv import load_dotenv

# Alpaca SDK
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.requests import (
    StockBarsRequest,
    StockLatestTradeRequest,
)
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, OrderType, TimeInForce
from alpaca.trading.requests import MarketOrderRequest


# ============================================================================
# SECTION 2: CONFIGURATION
# ============================================================================

load_dotenv()

API_KEY = os.getenv("ALPACA_PAPER_API_KEY")
API_SECRET = os.getenv("ALPACA_PAPER_SECRET_KEY")
BASE_URL = os.getenv("TRADE_API_URL", "https://paper-api.alpaca.markets")

if not API_KEY or not API_SECRET:
    raise RuntimeError("API keys not found. Check your .env file.")

# Alpaca clients — one for market data, one for trading.
# `paper=True` uses Alpaca's paper trading environment.
stock_data_client = StockHistoricalDataClient(API_KEY, API_SECRET)
trading_client = TradingClient(API_KEY, API_SECRET, paper=True)


# ============================================================================
# SECTION 3: UNIVERSE
# ============================================================================
# 24 tradable symbols + SPY as reference instrument (never traded).
# Per spec section 9: dropped ETFs (QQQ, IWM, XLK, XLF) from previous version.

TRADABLE_UNIVERSE = [
    # Mega-cap tech
    "NVDA", "AMD", "TSLA", "AAPL", "MSFT", "AMZN", "GOOG", "META",
    # Semiconductors
    "MU", "QCOM", "AVGO", "SMCI",
    # Software / cloud
    "CRM", "ORCL", "ADSK", "NFLX", "PLTR", "SHOP",
    # Financials
    "V", "JPM", "C",
    # Other high-beta
    "UBER", "SQ",
]

REFERENCE_SYMBOL = "SPY"  # Used for market state; never traded.

# Combined list — all symbols the system fetches data for.
ALL_SYMBOLS = TRADABLE_UNIVERSE + [REFERENCE_SYMBOL]


# ============================================================================
# SECTION 4: STRATEGY PARAMETERS
# ============================================================================
# All numerical parameters from the spec, grouped by section.
# Every parameter has a justification in the strategy_specification.md document.
# Changes to these values require updating the spec first.

# --- Market state (Spec Section 3) ---
SPY_BULL_LEVEL = 0.005          # +0.5% from open
SPY_BULL_EXIT = 0.003           # +0.3% — hysteresis exit from bullish
SPY_BEAR_LEVEL = -0.003         # -0.3% from open
SPY_BEAR_EXIT = -0.001          # -0.1% — hysteresis exit from bearish
SPY_DIRECTION_LOOKBACK_MIN = 15  # minutes
SPY_DIRECTION_FLAT_BAND = 0.0005  # ±0.05%

# --- Entry (Spec Section 4) ---
BREAKOUT_WINDOW_MIN = 15        # minutes
BREAKOUT_CUSHION_PCT = 0.001    # 0.1% above recent high
VOLUME_MULTIPLE = 1.5           # 1.5x median 1-minute volume
VOLUME_LOOKBACK_MIN = 15        # minutes (matches breakout window)
RS_FILTER_FLOOR = -0.005        # -0.5% over 15 minutes (filter)

# --- Exit (Spec Section 5) ---
STOP_LOSS_PCT = 0.005           # 0.5% below entry
TAKE_PROFIT_PCT = 0.010         # 1.0% above entry
TIME_LIMIT_SEC = 30 * 60        # 30 minutes
TRAILING_STOP_PCT = 0.004       # 0.4% below peak in runner mode

# --- Position sizing (Spec Section 6) ---
TIER1_SIZE_PCT = 0.03           # 3% (reduced)
TIER2_SIZE_PCT = 0.05           # 5% (normal)
TIER3_SIZE_PCT = 0.07           # 7% (strong)
TIER3_RS_THRESHOLD = 0.005      # +0.5% relative strength required
TIER3_DRAWDOWN_THRESHOLD = 0.005  # max 0.5% drawdown from session high
EXPOSURE_CAP_PCT = 0.20         # 20% total exposure
MAX_POSITIONS = 5               # max concurrent positions
PER_POSITION_HARD_CEILING = 0.10  # 10% safety backstop
TIER1_TRIGGER_LOSSES = 2        # consecutive losses to enter Tier 1

# --- Risk governors (Spec Section 7) ---
KILL_SWITCH_THRESHOLD = -0.02   # -2% of start-of-day equity
SYMBOL_BLOCK_LOSSES = 2         # consecutive losses to block symbol

# --- Session timing (Spec Section 7) ---
ET_TZ = ZoneInfo("America/New_York")
SESSION_OPEN_HOUR = 9           # 9:30 ET market open
SESSION_OPEN_MINUTE = 30
NO_ENTRIES_BEFORE_HOUR = 10     # 10:00 ET — entries enabled
NO_ENTRIES_BEFORE_MINUTE = 0
NO_ENTRIES_AFTER_HOUR = 15      # 15:30 ET — entries disabled
NO_ENTRIES_AFTER_MINUTE = 30
EOD_FORCE_CLOSE_HOUR = 15       # 15:55 ET — force close all positions
EOD_FORCE_CLOSE_MINUTE = 55
SESSION_END_HOUR = 16           # 16:00 ET — market close
SESSION_END_MINUTE = 0

# --- Operational ---
LOOP_SLEEP_SEC = 0.5            # main loop iteration sleep
TICK_BUFFER_SIZE = 1200         # 20 minutes of seconds — enough for all windows
MIN_TRADE_USD = 25              # don't submit orders below this notional


# ============================================================================
# SECTION 5: LOGGING
# ============================================================================
# Two output streams: file (everything DEBUG and above) and console (INFO+).
# Audit logging (CSV) is separate and handled in Section 9.

# All log and CSV timestamps display in Helsinki time so they match the
# investor's local clock. Strategy decisions internally use ET (market time)
# but never write ET timestamps to logs.
DISPLAY_TZ = ZoneInfo("Europe/Helsinki")

LOG_FORMAT = "%(asctime)s %(levelname)s %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_FILE = "scalper.log"


class HelsinkiFormatter(logging.Formatter):
    """Formatter that produces all log timestamps in Helsinki time,
    independent of the machine's local timezone setting."""

    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        dt_helsinki = dt.astimezone(DISPLAY_TZ)
        if datefmt:
            return dt_helsinki.strftime(datefmt)
        return dt_helsinki.strftime(LOG_DATE_FORMAT)


# Configure root logger — captures everything DEBUG and above.
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)

# Clear any default handlers that basicConfig() might have set up
# in earlier runs of the same Python process.
root_logger.handlers.clear()

# File handler — everything DEBUG and above, written to disk.
file_handler = logging.FileHandler(LOG_FILE, mode="a")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(HelsinkiFormatter(LOG_FORMAT, LOG_DATE_FORMAT))
root_logger.addHandler(file_handler)

# Console handler — INFO and above, so the terminal isn't flooded.
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(HelsinkiFormatter(LOG_FORMAT, LOG_DATE_FORMAT))
root_logger.addHandler(console_handler)

# Silence noisy third-party loggers. The Alpaca SDK uses urllib3 which logs
# every HTTP request and response at DEBUG level. Without this filter, the
# log file fills with thousands of lines per hour of API plumbing chatter
# that hides anything the strategy actually says.
for noisy in ("urllib3", "alpaca", "httpx", "httpcore"):
    logging.getLogger(noisy).setLevel(logging.WARNING)


def audit_timestamp(dt=None):
    """Returns a formatted timestamp string in display timezone (Helsinki),
    used for CSV audit rows."""
    t = dt or datetime.now(timezone.utc)
    return t.astimezone(DISPLAY_TZ).strftime(LOG_DATE_FORMAT)


logging.info("=" * 60)
logging.info("Scalper starting — strategy spec v1.0")
logging.info("Universe: %d tradable + 1 reference (%s)",
             len(TRADABLE_UNIVERSE), REFERENCE_SYMBOL)
logging.info("API base: %s", BASE_URL)
logging.info("=" * 60)


# ============================================================================
# SECTION 6: NYSE CALENDAR
# ============================================================================
# Trading calendar helpers. Used to detect non-trading days and compute
# "previous trading day" for stale-data checks.

NYSE_HOLIDAYS = {
    # 2025
    date(2025, 1, 1), date(2025, 1, 20), date(2025, 2, 17),
    date(2025, 4, 18), date(2025, 5, 26), date(2025, 6, 19),
    date(2025, 7, 4), date(2025, 9, 1), date(2025, 11, 27),
    date(2025, 12, 25),
    # 2026
    date(2026, 1, 1), date(2026, 1, 19), date(2026, 2, 16),
    date(2026, 4, 3), date(2026, 5, 25), date(2026, 6, 19),
    date(2026, 7, 3), date(2026, 9, 7), date(2026, 11, 26),
    date(2026, 12, 25),
    # 2027
    date(2027, 1, 1), date(2027, 1, 18), date(2027, 2, 15),
    date(2027, 3, 26), date(2027, 5, 31), date(2027, 6, 18),
    date(2027, 7, 5), date(2027, 9, 6), date(2027, 11, 25),
    date(2027, 12, 24),
}


def is_trading_day(d):
    """True if d is a NYSE trading day (weekday and not a holiday)."""
    return d.weekday() < 5 and d not in NYSE_HOLIDAYS


def previous_trading_day(d):
    """Return the most recent trading day strictly before d."""
    check = d - timedelta(days=1)
    while not is_trading_day(check):
        check -= timedelta(days=1)
    return check


# ============================================================================
# SECTION 7: TIME HELPERS
# ============================================================================
# Strategy decisions are anchored to ET (market time). These helpers convert
# between UTC, ET, and Helsinki, and answer questions like "are we in the
# trading window right now?"


def now_utc():
    """Current time in UTC. Single source of truth — every other timezone
    in the system is derived from this."""
    return datetime.now(timezone.utc)


def to_et(dt):
    """Convert a UTC datetime to ET (market time)."""
    return dt.astimezone(ET_TZ)


def to_helsinki(dt):
    """Convert a UTC datetime to Helsinki time (display)."""
    return dt.astimezone(DISPLAY_TZ)


def session_minutes_elapsed(dt=None):
    """How many minutes since 9:30 ET on the same calendar day.
    Returns 0 before market open, 390 after market close.
    Used for session-time-based logic."""
    t = dt or now_utc()
    et = to_et(t)
    open_et = et.replace(hour=SESSION_OPEN_HOUR,
                          minute=SESSION_OPEN_MINUTE,
                          second=0, microsecond=0)
    close_et = et.replace(hour=SESSION_END_HOUR,
                           minute=SESSION_END_MINUTE,
                           second=0, microsecond=0)
    if et < open_et:
        return 0
    if et > close_et:
        return 390  # 6.5-hour session
    return int((et - open_et).total_seconds() // 60)


def is_market_open(dt=None):
    """True if the market is currently open (9:30-16:00 ET on a trading day).
    Does NOT include any of our extra restrictions (no entries before 10:00,
    etc.) — those are applied separately."""
    t = dt or now_utc()
    et = to_et(t)
    if not is_trading_day(et.date()):
        return False
    open_et = et.replace(hour=SESSION_OPEN_HOUR,
                          minute=SESSION_OPEN_MINUTE,
                          second=0, microsecond=0)
    close_et = et.replace(hour=SESSION_END_HOUR,
                           minute=SESSION_END_MINUTE,
                           second=0, microsecond=0)
    return open_et <= et < close_et


def entries_allowed_now(dt=None):
    """True if new entries are allowed right now per the spec timing rules:
    - Market must be open
    - At or after 10:00 ET (skip first 30 minutes)
    - Strictly before 15:30 ET (no new entries in final 30 minutes)
    """
    t = dt or now_utc()
    et = to_et(t)
    if not is_market_open(t):
        return False
    minutes_since_open = (et.hour - SESSION_OPEN_HOUR) * 60 + \
                         (et.minute - SESSION_OPEN_MINUTE)
    no_entries_after_minutes = (NO_ENTRIES_AFTER_HOUR - SESSION_OPEN_HOUR) * 60 + \
                               (NO_ENTRIES_AFTER_MINUTE - SESSION_OPEN_MINUTE)
    no_entries_before_minutes = (NO_ENTRIES_BEFORE_HOUR - SESSION_OPEN_HOUR) * 60 + \
                                (NO_ENTRIES_BEFORE_MINUTE - SESSION_OPEN_MINUTE)
    return no_entries_before_minutes <= minutes_since_open < no_entries_after_minutes


def force_close_time_reached(dt=None):
    """True if the EOD forced-close time (15:55 ET) has been reached on
    a trading day. Once this returns True, all open positions must be closed."""
    t = dt or now_utc()
    et = to_et(t)
    if not is_trading_day(et.date()):
        return False
    force_close_et = et.replace(hour=EOD_FORCE_CLOSE_HOUR,
                                 minute=EOD_FORCE_CLOSE_MINUTE,
                                 second=0, microsecond=0)
    return et >= force_close_et


# ============================================================================
# SECTION 8: ALPACA INFRASTRUCTURE
# ============================================================================
# Pure plumbing — these functions wrap the Alpaca SDK and handle its quirks
# (status enum vs string, retries, polling). No strategy decisions live here.


def status_is(status, target):
    """Compare an Alpaca order status to a target string, handling
    Alpaca's habit of returning either an enum, a string, or a class."""
    try:
        val = getattr(status, "value", None)
        if isinstance(val, str):
            return val.lower() == target.lower()
        name = getattr(status, "name", None)
        if isinstance(name, str):
            return name.lower() == target.lower()
        return str(status).split(".")[-1].lower() == target.lower()
    except Exception:
        return False


def get_account_equity():
    """Total account equity right now, used for the daily kill switch.
    Returns float or None on failure."""
    try:
        account = trading_client.get_account()
        return float(account.equity)
    except Exception as e:
        logging.exception("Failed to read account equity: %s", e)
        return None


def get_buying_power():
    """Available buying power right now, used for position sizing.
    Returns float or None on failure."""
    try:
        account = trading_client.get_account()
        return float(account.buying_power)
    except Exception as e:
        logging.exception("Failed to read buying power: %s", e)
        return None


def get_all_positions():
    """All open positions as a dict {symbol: (qty, avg_entry_price)}.
    Returns empty dict on failure (caller decides what to do with that)."""
    try:
        positions = trading_client.get_all_positions()
        return {
            p.symbol: (int(float(p.qty)), float(p.avg_entry_price))
            for p in positions
        }
    except Exception as e:
        logging.warning("get_all_positions failed: %s", e)
        return {}


def get_position_qty(symbol):
    """Open quantity for a single symbol. Returns 0 if no position
    or on failure."""
    try:
        pos = trading_client.get_open_position(symbol)
        return int(float(pos.qty))
    except Exception:
        # Alpaca raises for "no position" — that's normal, not an error
        return 0


def fetch_latest_trade(symbols):
    """Fetch latest trade price and size for one or more symbols.
    Returns dict {symbol: (price, size)} with (None, None) for failures."""
    if isinstance(symbols, str):
        symbols = [symbols]
    out = {s: (None, None) for s in symbols}
    try:
        req = StockLatestTradeRequest(symbol_or_symbols=symbols)
        trades = stock_data_client.get_stock_latest_trade(req)
        for sym in symbols:
            t = trades.get(sym)
            if t is None:
                continue
            try:
                price = float(t.price)
                size = int(getattr(t, "size", 1) or 1)
                out[sym] = (price, size)
            except Exception:
                pass
    except Exception as e:
        logging.warning("fetch_latest_trade failed for %s: %s", symbols, e)
    return out


def fetch_recent_minute_bars(symbol, minutes_back):
    """Fetch the last N minutes of 1-minute bars for a symbol.
    Used at startup to populate price/volume windows.
    Returns a pandas DataFrame indexed by timestamp, or empty DataFrame on failure.

    Falls back gracefully if Alpaca's free tier blocks the request — the
    caller is responsible for handling the empty case (e.g., live observation
    until the window fills naturally).
    """
    try:
        end = now_utc()
        # Pad start by 5 minutes to ensure we cover the requested window even
        # with minor data delays.
        start = end - timedelta(minutes=minutes_back + 5)
        req = StockBarsRequest(
            symbol_or_symbols=symbol,
            start=start,
            end=end,
            timeframe=TimeFrame.Minute,
        )
        resp = stock_data_client.get_stock_bars(req)
        df = resp.df
        if df is None or df.empty:
            return pd.DataFrame()
        # Alpaca sometimes returns a multi-index (symbol, timestamp).
        # Flatten to single timestamp index.
        if isinstance(df.index, pd.MultiIndex):
            df = df.reset_index(level=0, drop=True)
        return df
    except Exception as e:
        logging.warning("fetch_recent_minute_bars failed for %s: %s",
                        symbol, e)
        return pd.DataFrame()


# --- Order submission ------------------------------------------------------
# Order submission is synchronous: each buy/sell call blocks until the fill
# is confirmed or the polling timeout is reached. Alpaca paper trading fills
# essentially instantly under normal conditions; the timeout exists for
# emergencies, not normal operation.

ORDER_POLL_TIMEOUT_SEC = 90      # max time to wait for fill confirmation
ORDER_POLL_INTERVAL_SEC = 0.5    # how often to check order status


def submit_buy(symbol, qty):
    """Submit a market buy order and poll for fill confirmation.

    Returns a dict on success:
        {
            "order_id": str,
            "filled_qty": float,
            "filled_price": float,
            "submit_time": datetime (UTC),
            "fill_time": datetime (UTC),
            "status": "filled" | "timeout",
        }

    Returns None if the order was rejected, cancelled, or the submission
    itself failed. In those cases an error is logged.

    Status of "timeout" means the order was submitted and we have an
    order_id, but it hasn't filled within the polling window. The caller
    should treat this as "position state unknown — manual review needed."
    """
    if qty <= 0:
        logging.warning("submit_buy: %s invalid qty %d", symbol, qty)
        return None

    submit_time = now_utc()

    try:
        order_request = MarketOrderRequest(
            symbol=symbol,
            qty=qty,
            side=OrderSide.BUY,
            type=OrderType.MARKET,
            time_in_force=TimeInForce.DAY,
        )
        submitted = trading_client.submit_order(order_request)
        order_id = getattr(submitted, "id", None)
        if order_id is None:
            logging.error("submit_buy: %s no order_id returned", symbol)
            return None
        logging.info("BUY submitted: %s qty=%d order_id=%s",
                     symbol, qty, order_id)
    except Exception as e:
        logging.exception("submit_buy: %s submission failed: %s", symbol, e)
        return None

    # Poll for fill confirmation.
    poll_start = now_utc()
    filled_qty = 0.0
    filled_price = None

    while (now_utc() - poll_start).total_seconds() < ORDER_POLL_TIMEOUT_SEC:
        try:
            current = trading_client.get_order_by_id(order_id)
            status = getattr(current, "status", None)

            if status_is(status, "filled"):
                filled_qty = float(getattr(current, "filled_qty", 0) or 0)
                avg_price = getattr(current, "filled_avg_price", None)
                if avg_price is not None:
                    filled_price = float(avg_price)
                if filled_qty > 0 and filled_price is not None:
                    fill_time = now_utc()
                    logging.info(
                        "BUY filled: %s qty=%.2f price=%.4f order_id=%s",
                        symbol, filled_qty, filled_price, order_id
                    )
                    return {
                        "order_id": order_id,
                        "filled_qty": filled_qty,
                        "filled_price": filled_price,
                        "submit_time": submit_time,
                        "fill_time": fill_time,
                        "status": "filled",
                    }

            if status_is(status, "canceled") or status_is(status, "rejected"):
                logging.warning(
                    "BUY %s order_id=%s status=%s — order did not fill",
                    symbol, order_id, status
                )
                return None

        except Exception as e:
            logging.debug("Polling error for %s order %s: %s",
                          symbol, order_id, e)

        time.sleep(ORDER_POLL_INTERVAL_SEC)

    # Polling timeout — order may still be working.
    logging.warning(
        "BUY %s order_id=%s polling timeout (%ds) — fill status unknown",
        symbol, order_id, ORDER_POLL_TIMEOUT_SEC
    )
    return {
        "order_id": order_id,
        "filled_qty": filled_qty,
        "filled_price": filled_price,
        "submit_time": submit_time,
        "fill_time": None,
        "status": "timeout",
    }


def submit_sell(symbol, qty):
    """Submit a market sell order and poll for fill confirmation.

    Returns a dict on success with the same shape as submit_buy.
    Returns None on rejection, cancellation, or submission failure.

    Important guards:
    - Verifies the symbol has at least `qty` shares to sell before submitting.
      Refuses to submit if the position is smaller (avoids accidental shorts).
    - If the position shows as 0, retries the position query once after a
      brief wait — Alpaca's position list can lag fills by a second or two.
    """
    if qty <= 0:
        logging.warning("submit_sell: %s invalid qty %d", symbol, qty)
        return None

    # Position safety check — never sell more than we have.
    available = get_position_qty(symbol)
    if available <= 0:
        time.sleep(1.0)  # brief wait for Alpaca position list to catch up
        available = get_position_qty(symbol)

    if available <= 0:
        logging.warning(
            "submit_sell: %s no position available (qty=%d) — skipping",
            symbol, available
        )
        return None

    qty_to_sell = min(int(qty), available)
    if qty_to_sell < int(qty):
        logging.warning(
            "submit_sell: %s reducing qty %d -> %d (only %d available)",
            symbol, qty, qty_to_sell, available
        )

    submit_time = now_utc()

    try:
        order_request = MarketOrderRequest(
            symbol=symbol,
            qty=qty_to_sell,
            side=OrderSide.SELL,
            type=OrderType.MARKET,
            time_in_force=TimeInForce.DAY,
        )
        submitted = trading_client.submit_order(order_request)
        order_id = getattr(submitted, "id", None)
        if order_id is None:
            logging.error("submit_sell: %s no order_id returned", symbol)
            return None
        logging.info("SELL submitted: %s qty=%d order_id=%s",
                     symbol, qty_to_sell, order_id)
    except Exception as e:
        logging.exception("submit_sell: %s submission failed: %s", symbol, e)
        return None

    # Poll for fill confirmation.
    poll_start = now_utc()
    filled_qty = 0.0
    filled_price = None

    while (now_utc() - poll_start).total_seconds() < ORDER_POLL_TIMEOUT_SEC:
        try:
            current = trading_client.get_order_by_id(order_id)
            status = getattr(current, "status", None)

            if status_is(status, "filled"):
                filled_qty = float(getattr(current, "filled_qty", 0) or 0)
                avg_price = getattr(current, "filled_avg_price", None)
                if avg_price is not None:
                    filled_price = float(avg_price)
                if filled_qty > 0 and filled_price is not None:
                    fill_time = now_utc()
                    logging.info(
                        "SELL filled: %s qty=%.2f price=%.4f order_id=%s",
                        symbol, filled_qty, filled_price, order_id
                    )
                    return {
                        "order_id": order_id,
                        "filled_qty": filled_qty,
                        "filled_price": filled_price,
                        "submit_time": submit_time,
                        "fill_time": fill_time,
                        "status": "filled",
                    }

            if status_is(status, "canceled") or status_is(status, "rejected"):
                logging.warning(
                    "SELL %s order_id=%s status=%s — order did not fill",
                    symbol, order_id, status
                )
                return None

        except Exception as e:
            logging.debug("Polling error for %s order %s: %s",
                          symbol, order_id, e)

        time.sleep(ORDER_POLL_INTERVAL_SEC)

    # Polling timeout.
    logging.warning(
        "SELL %s order_id=%s polling timeout (%ds) — fill status unknown",
        symbol, order_id, ORDER_POLL_TIMEOUT_SEC
    )
    return {
        "order_id": order_id,
        "filled_qty": filled_qty,
        "filled_price": filled_price,
        "submit_time": submit_time,
        "fill_time": None,
        "status": "timeout",
    }


# ============================================================================
# SECTION 9: AUDIT CSV LOGGING
# ============================================================================
# Two CSV files are maintained, one row per record:
#
#   trades.csv     — every BUY and SELL the system executes
#   decisions.csv  — every entry-evaluation moment (entered or blocked)
#
# CSVs use Helsinki time in their timestamps so they match the log files.
# Headers are written automatically on first row.

TRADES_CSV = "trades.csv"
DECISIONS_CSV = "decisions.csv"

TRADES_FIELDS = [
    "timestamp",      # Helsinki time
    "symbol",
    "action",         # BUY or SELL
    "qty",
    "price",          # filled price (or estimated if timeout)
    "fill_status",    # filled or timeout
    "tier",           # 1, 2, or 3 (BUY rows only)
    "market_state",   # bullish, neutral, bearish at moment
    "exit_reason",    # SELL rows only — stop / tp / trailing / time / eod
    "pnl",            # SELL rows only — realized P&L in dollars
    "order_id",
]

DECISIONS_FIELDS = [
    "timestamp",
    "symbol",
    "decision",       # entered, blocked
    "block_reason",   # if blocked: which filter rejected it
    "market_state",
    "rs_15min",       # symbol's 15-min relative strength vs SPY
    "above_vwap",     # symbol above VWAP at decision moment
    "drawdown",       # symbol drawdown from session high
    "vol_multiple",   # current 1-min volume vs 15-min median
    "tier_assigned",  # 1, 2, 3, or "n/a" if blocked
]


# Lock for CSV writes — prevents interleaved rows if two threads write
# at the same time. Currently the code is single-threaded but the lock
# is cheap insurance.
_csv_lock = threading.Lock()


def _write_csv_row(filename, fields, row):
    """Append a single row to a CSV file, writing the header if the file
    is new or empty. Idempotent — safe to call from anywhere."""
    try:
        with _csv_lock:
            file_is_new = (
                not os.path.exists(filename) or
                os.path.getsize(filename) == 0
            )
            with open(filename, "a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fields)
                if file_is_new:
                    writer.writeheader()
                writer.writerow(row)
    except Exception as e:
        logging.warning("Failed to write CSV row to %s: %s", filename, e)


def log_trade(symbol, action, qty, price, fill_status, market_state,
              order_id, tier=None, exit_reason=None, pnl=None,
              timestamp=None):
    """Record a BUY or SELL trade to trades.csv.

    For BUY: pass tier. exit_reason and pnl are None.
    For SELL: pass exit_reason and pnl. tier is None.
    """
    row = {
        "timestamp": audit_timestamp(timestamp),
        "symbol": symbol,
        "action": action,
        "qty": qty,
        "price": round(price, 4) if price is not None else None,
        "fill_status": fill_status,
        "tier": tier,
        "market_state": market_state,
        "exit_reason": exit_reason,
        "pnl": round(pnl, 2) if pnl is not None else None,
        "order_id": order_id,
    }
    _write_csv_row(TRADES_CSV, TRADES_FIELDS, row)


def log_decision(symbol, decision, market_state, rs_15min=None,
                 above_vwap=None, drawdown=None, vol_multiple=None,
                 tier_assigned=None, block_reason=None, timestamp=None):
    """Record an entry decision moment to decisions.csv.

    decision: "entered" or "blocked"
    If blocked, block_reason explains which filter rejected it.
    All numeric fields are optional and default to None — pass what's
    available; missing data is rendered as empty cells.
    """
    row = {
        "timestamp": audit_timestamp(timestamp),
        "symbol": symbol,
        "decision": decision,
        "block_reason": block_reason,
        "market_state": market_state,
        "rs_15min": round(rs_15min, 5) if rs_15min is not None else None,
        "above_vwap": above_vwap,
        "drawdown": round(drawdown, 5) if drawdown is not None else None,
        "vol_multiple": round(vol_multiple, 2) if vol_multiple is not None else None,
        "tier_assigned": tier_assigned if tier_assigned is not None else "n/a",
    }
    _write_csv_row(DECISIONS_CSV, DECISIONS_FIELDS, row)


# ============================================================================
# SECTION 10: MARKET DATA BUFFERS AND INDICATORS
# ============================================================================
# Rolling windows of price and volume per symbol, plus the indicator
# functions that compute strategy inputs from those windows.
#
# Buckets are 1-second resolution. Multiple ticks within the same second
# get averaged (price) and summed (volume), so the rate of the main loop
# doesn't affect indicator results.

# Sized at 20 minutes (1200 seconds) — longer than any lookback we need,
# with buffer for clean rolling computations.
BUFFER_SECONDS = 1200


class SymbolBuffer:
    """Rolling 1-second price/volume buffer for a single symbol.

    Used by the strategy to look back over the last N minutes for breakout
    detection, relative strength, VWAP, and drawdown calculations.

    Also tracks the session's open price and high price separately, since
    those are anchored to specific moments rather than rolling windows.
    """

    def __init__(self, symbol):
        self.symbol = symbol
        # Three parallel deques — same length, each index is one second.
        self.prices = deque(maxlen=BUFFER_SECONDS)
        self.volumes = deque(maxlen=BUFFER_SECONDS)
        self.timestamps = deque(maxlen=BUFFER_SECONDS)
        # Session anchors — set once per session, used for VWAP and drawdown.
        self.session_open_price = None
        self.session_high_price = None
        self.session_open_set_at = None  # datetime when session_open was set

    def add_tick(self, price, volume, timestamp):
        """Add a tick to the buffer.

        If the timestamp falls in the same second as the most recent tick,
        merge them: average the price, sum the volume. Otherwise append a
        new bucket.

        timestamp must be a UTC datetime.
        """
        bucket_ts = timestamp.replace(microsecond=0)

        if self.timestamps and self.timestamps[-1] == bucket_ts:
            # Same-second bucket — merge with previous tick.
            old_price = self.prices[-1]
            old_volume = self.volumes[-1]
            self.prices[-1] = (old_price + price) / 2.0
            self.volumes[-1] = old_volume + volume
        else:
            self.prices.append(price)
            self.volumes.append(volume)
            self.timestamps.append(bucket_ts)

        # Update session anchors.
        if self.session_open_price is None:
            self.session_open_price = price
            self.session_high_price = price
            self.session_open_set_at = timestamp
        else:
            if price > self.session_high_price:
                self.session_high_price = price

    def reset_session(self):
        """Clear session anchors at EOD. Buffer data stays — windows
        will naturally roll out as new data comes in tomorrow."""
        self.session_open_price = None
        self.session_high_price = None
        self.session_open_set_at = None

    def populate_from_bars(self, bars_df):
        """Fill the buffer from historical 1-minute bars at startup.

        Each minute bar gets expanded into 60 seconds at the bar's close
        price and 1/60 of the bar's volume. This isn't perfectly accurate
        (real ticks don't fill every second uniformly) but it's close
        enough to compute valid 15-minute indicators on the first loop
        iteration without waiting 15 live minutes.

        bars_df is a pandas DataFrame with columns 'close' and 'volume',
        indexed by timestamp.
        """
        if bars_df is None or bars_df.empty:
            return
        for ts, row in bars_df.iterrows():
            try:
                price = float(row["close"])
                volume = float(row.get("volume", 0))
                # Convert pandas Timestamp to UTC datetime.
                if hasattr(ts, "to_pydatetime"):
                    ts = ts.to_pydatetime()
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                # Expand the minute bar into 60 one-second buckets.
                second_volume = volume / 60.0
                for s in range(60):
                    bucket_ts = ts + timedelta(seconds=s)
                    self.prices.append(price)
                    self.volumes.append(second_volume)
                    self.timestamps.append(bucket_ts)
            except Exception as e:
                logging.debug("populate_from_bars skip row for %s: %s",
                              self.symbol, e)

        # Set session anchors from the first bar if not already set.
        if self.session_open_price is None and len(self.prices) > 0:
            self.session_open_price = self.prices[0]
            self.session_high_price = max(self.prices)

    def latest_price(self):
        """Most recent price, or None if buffer is empty."""
        return self.prices[-1] if self.prices else None

    def length(self):
        """Number of buckets currently in the buffer."""
        return len(self.prices)


# --- Indicator functions ---------------------------------------------------
# Each function takes a SymbolBuffer (plus parameters) and returns a number.
# Returns None when there isn't enough data to compute reliably.


def compute_recent_high(buffer, lookback_seconds):
    """Highest price in the last `lookback_seconds`.
    Returns None if buffer doesn't have enough data."""
    if buffer.length() < lookback_seconds:
        return None
    recent = list(buffer.prices)[-lookback_seconds:]
    return max(recent)


def compute_volume_median_1min(buffer, lookback_minutes):
    """Median of 1-minute volume totals over the last `lookback_minutes`.

    The strategy needs to compare current 1-minute volume against this median
    (for the volume confirmation filter). Returns None if not enough data.
    """
    needed_seconds = lookback_minutes * 60
    if buffer.length() < needed_seconds:
        return None
    # Take the last lookback_minutes worth of seconds, sum into 1-minute totals.
    recent_volumes = list(buffer.volumes)[-needed_seconds:]
    minute_totals = []
    for i in range(lookback_minutes):
        start = i * 60
        end = start + 60
        minute_totals.append(sum(recent_volumes[start:end]))
    if not minute_totals:
        return None
    sorted_totals = sorted(minute_totals)
    n = len(sorted_totals)
    if n % 2 == 1:
        return sorted_totals[n // 2]
    return (sorted_totals[n // 2 - 1] + sorted_totals[n // 2]) / 2.0


def compute_current_1min_volume(buffer):
    """Sum of volume over the last 60 seconds.
    Returns None if buffer has less than 60 seconds of data."""
    if buffer.length() < 60:
        return None
    return sum(list(buffer.volumes)[-60:])


def compute_vwap(buffer):
    """Volume-weighted average price over all buffered data.

    Strictly speaking VWAP should reset at session open. Since the buffer
    rolls at 20 minutes, this is effectively a 20-minute rolling VWAP for
    most of the session. That's actually closer to what we want for entry
    decisions than a since-open VWAP, which becomes stale by afternoon.
    Returns None on empty buffer or zero total volume.
    """
    if buffer.length() == 0:
        return None
    prices = list(buffer.prices)
    volumes = list(buffer.volumes)
    total_volume = sum(volumes)
    if total_volume <= 0:
        return None
    weighted_sum = sum(p * v for p, v in zip(prices, volumes))
    return weighted_sum / total_volume


def compute_relative_strength(symbol_buffer, spy_buffer, lookback_minutes):
    """Symbol's % change over `lookback_minutes` minus SPY's % change over
    the same period. Positive means the symbol outperformed SPY.

    Returns None if either buffer doesn't have enough data.
    """
    needed_seconds = lookback_minutes * 60
    if symbol_buffer.length() < needed_seconds:
        return None
    if spy_buffer.length() < needed_seconds:
        return None

    sym_then = list(symbol_buffer.prices)[-needed_seconds]
    sym_now = symbol_buffer.prices[-1]
    spy_then = list(spy_buffer.prices)[-needed_seconds]
    spy_now = spy_buffer.prices[-1]

    if sym_then <= 0 or spy_then <= 0:
        return None

    sym_change = (sym_now - sym_then) / sym_then
    spy_change = (spy_now - spy_then) / spy_then
    return sym_change - spy_change


def compute_drawdown_from_high(buffer):
    """How far below the session high the symbol currently is, as a fraction.

    Used by Tier 3 sizing — a "clean trend" symbol has small drawdown from
    its session high. A volatile symbol that pulled back sharply has large
    drawdown.

    Returns None if no session high is set.
    """
    if buffer.session_high_price is None or buffer.session_high_price <= 0:
        return None
    current = buffer.latest_price()
    if current is None:
        return None
    return (buffer.session_high_price - current) / buffer.session_high_price


def compute_spy_change_from_open(spy_buffer):
    """SPY's percentage change from session open.
    Used for market state level determination.
    Returns None if session open isn't set."""
    if spy_buffer.session_open_price is None or spy_buffer.session_open_price <= 0:
        return None
    current = spy_buffer.latest_price()
    if current is None:
        return None
    return (current - spy_buffer.session_open_price) / spy_buffer.session_open_price


def compute_spy_direction(spy_buffer, lookback_minutes, flat_band):
    """SPY direction over the last `lookback_minutes`.

    Returns "rising", "falling", or "flat".
    Returns None if buffer doesn't have enough data.

    The flat_band is the threshold below which a move counts as "flat"
    rather than directional.
    """
    needed_seconds = lookback_minutes * 60
    if spy_buffer.length() < needed_seconds:
        return None
    then_price = list(spy_buffer.prices)[-needed_seconds]
    now_price = spy_buffer.prices[-1]
    if then_price <= 0:
        return None
    pct_change = (now_price - then_price) / then_price
    if pct_change > flat_band:
        return "rising"
    if pct_change < -flat_band:
        return "falling"
    return "flat"


# ============================================================================
# SECTION 11: STATE MANAGEMENT
# ============================================================================
# All mutable strategy state lives in classes here. The main loop owns
# instances of these classes and passes them to decision functions.
# State is never accessed via module-level globals.


class Position:
    """A single open position. Created when an entry fills, modified by
    runner-mode transitions, deleted when the exit fills."""

    def __init__(self, symbol, qty, entry_price, entry_time, tier,
                 market_state_at_entry, order_id):
        self.symbol = symbol
        self.qty = qty
        self.entry_price = entry_price
        self.entry_time = entry_time          # UTC datetime
        self.tier = tier                      # 1, 2, or 3
        self.market_state_at_entry = market_state_at_entry
        self.entry_order_id = order_id

        # Mode tracking — starts in "normal", may transition to "runner".
        self.mode = "normal"
        self.peak_price = entry_price         # tracks max price for trailing stop

    def update_peak(self, current_price):
        """Track the highest price seen since entry. Used by runner mode
        to set the trailing stop."""
        if current_price > self.peak_price:
            self.peak_price = current_price

    def transition_to_runner(self):
        """Switch from normal mode to runner mode. Called when TP is hit
        in bullish market state."""
        self.mode = "runner"
        logging.info("%s transitioned to runner mode (peak=%.4f)",
                     self.symbol, self.peak_price)


class PositionBook:
    """All currently open positions, keyed by symbol.

    The strategy is long-only and one position per symbol, so symbol is
    a unique key. Caller is responsible for not opening duplicate positions.
    """

    def __init__(self):
        self._positions = {}  # symbol -> Position

    def add(self, position):
        """Record a new position. Logs a warning if one already exists
        for the symbol (shouldn't happen but worth detecting)."""
        if position.symbol in self._positions:
            logging.warning("Position book already has %s — overwriting",
                            position.symbol)
        self._positions[position.symbol] = position

    def remove(self, symbol):
        """Remove a position (after exit fill). Returns the removed
        Position object, or None if there wasn't one."""
        return self._positions.pop(symbol, None)

    def get(self, symbol):
        """Get the position for a symbol, or None if no position."""
        return self._positions.get(symbol)

    def has(self, symbol):
        """True if there's an open position for the symbol."""
        return symbol in self._positions

    def all_positions(self):
        """List of all current positions. Used for iteration in the main
        loop's exit-evaluation phase."""
        return list(self._positions.values())

    def count(self):
        """Number of open positions. Used for the MAX_POSITIONS check."""
        return len(self._positions)

    def total_exposure(self, latest_prices):
        """Total dollar value of open positions, used for the 20% exposure cap.

        latest_prices is a dict {symbol: price} — caller supplies current
        prices since the position book doesn't fetch them.
        """
        total = 0.0
        for pos in self._positions.values():
            price = latest_prices.get(pos.symbol, pos.entry_price)
            total += pos.qty * price
        return total


class TierTracker:
    """Tracks the sizing tier (1, 2, or 3) based on consecutive losses.

    Per spec section 6.1:
    - Default is Tier 2 (5%)
    - After 2 consecutive losses, drop to Tier 1 (3%)
    - On any winning trade, reset to Tier 2
    - Tier 3 (7%) is determined separately at entry time, not by this tracker

    This tracker only manages the Tier 1 / Tier 2 distinction. The Tier 3
    upgrade is computed at entry by the strategy logic.
    """

    def __init__(self):
        self._consecutive_losses = 0

    def record_outcome(self, pnl):
        """Record the outcome of a closed trade.
        pnl is the realized dollar P&L (negative = loss)."""
        if pnl < 0:
            self._consecutive_losses += 1
            if self._consecutive_losses >= TIER1_TRIGGER_LOSSES:
                logging.info("Consecutive losses: %d — Tier 1 active",
                             self._consecutive_losses)
        else:
            if self._consecutive_losses > 0:
                logging.info("Win recorded — Tier 1 cleared "
                             "(was %d consecutive losses)",
                             self._consecutive_losses)
            self._consecutive_losses = 0

    def is_tier1_active(self):
        """True if current default tier is reduced (Tier 1)."""
        return self._consecutive_losses >= TIER1_TRIGGER_LOSSES

    def reset_for_new_session(self):
        """Clear tier state at the start of a new trading day."""
        self._consecutive_losses = 0


class SymbolBlacklist:
    """Tracks which symbols are blocked from new entries due to consecutive
    losses on that symbol within the current session.

    Per spec section 7.2:
    - 2 consecutive losses on a symbol → blocked for the rest of the session
    - A win on the symbol resets the counter for that symbol
    - Block clears at session start the next day
    """

    def __init__(self):
        self._losses_per_symbol = defaultdict(int)
        self._blocked = set()

    def record_outcome(self, symbol, pnl):
        """Record the outcome of a closed trade on this symbol."""
        if pnl < 0:
            self._losses_per_symbol[symbol] += 1
            if self._losses_per_symbol[symbol] >= SYMBOL_BLOCK_LOSSES:
                if symbol not in self._blocked:
                    self._blocked.add(symbol)
                    logging.warning(
                        "%s blacklisted for session "
                        "(%d consecutive losses)",
                        symbol, self._losses_per_symbol[symbol]
                    )
        else:
            # Win on this symbol — clear its loss counter.
            if self._losses_per_symbol[symbol] > 0:
                logging.info("%s loss counter reset (won after %d losses)",
                             symbol, self._losses_per_symbol[symbol])
            self._losses_per_symbol[symbol] = 0

    def is_blocked(self, symbol):
        """True if the symbol is blacklisted for the current session."""
        return symbol in self._blocked

    def reset_for_new_session(self):
        """Clear all blacklist state at the start of a new trading day."""
        self._losses_per_symbol.clear()
        self._blocked.clear()


class KillSwitch:
    """Daily kill switch. Tracks whether the account has lost more than
    KILL_SWITCH_THRESHOLD from start-of-day equity.

    Per spec section 7.1:
    - Threshold: 2% of start-of-day equity (realized + unrealized)
    - On trigger: close all positions, block new entries until next session,
      manual restart required
    - Once tripped, stays tripped for the rest of the session
    """

    def __init__(self):
        self._start_of_day_equity = None
        self._tripped = False

    def initialize_for_session(self, start_equity):
        """Set the reference point at session start. Must be called before
        the kill switch can evaluate."""
        self._start_of_day_equity = start_equity
        self._tripped = False
        logging.info("Kill switch initialized: start equity=$%.2f, "
                     "threshold=$%.2f loss",
                     start_equity,
                     start_equity * abs(KILL_SWITCH_THRESHOLD))

    def evaluate(self, current_equity):
        """Check whether the kill switch should trip given current equity.

        Returns True the first time it trips (so the caller can take action).
        Returns False otherwise. Once tripped, returns False on subsequent
        calls — the caller already knows.
        """
        if self._tripped:
            return False
        if self._start_of_day_equity is None:
            logging.warning("Kill switch evaluated before initialization")
            return False

        loss_pct = (current_equity - self._start_of_day_equity) / \
                   self._start_of_day_equity

        if loss_pct <= KILL_SWITCH_THRESHOLD:
            self._tripped = True
            logging.warning(
                "KILL SWITCH TRIPPED: equity=$%.2f start=$%.2f "
                "loss=%.2f%% (threshold=%.2f%%)",
                current_equity,
                self._start_of_day_equity,
                loss_pct * 100,
                KILL_SWITCH_THRESHOLD * 100
            )
            return True
        return False

    def is_tripped(self):
        """True if the kill switch has tripped this session."""
        return self._tripped

    def reset_for_new_session(self):
        """Clear kill switch state at the start of a new trading day."""
        self._start_of_day_equity = None
        self._tripped = False


class StrategyState:
    """Container for all mutable strategy state. The main loop creates one
    instance and passes it to decision functions.

    This is just a holder — the actual logic lives in the contained objects
    and in the strategy functions in Section 12.
    """

    def __init__(self):
        self.positions = PositionBook()
        self.tier_tracker = TierTracker()
        self.blacklist = SymbolBlacklist()
        self.kill_switch = KillSwitch()
        self.buffers = {}  # symbol -> SymbolBuffer
        # Track session start so we can detect when a new day begins
        # (the system might run across multiple sessions).
        self.session_date = None

    def initialize_session(self, equity, symbols):
        """Set up state for a new trading session.

        Called once per day at startup or at session rollover.
        """
        self.kill_switch.initialize_for_session(equity)
        self.tier_tracker.reset_for_new_session()
        self.blacklist.reset_for_new_session()

        # Create or reset buffers for each symbol.
        for symbol in symbols:
            if symbol in self.buffers:
                self.buffers[symbol].reset_session()
            else:
                self.buffers[symbol] = SymbolBuffer(symbol)

        self.session_date = to_et(now_utc()).date()
        logging.info("Session initialized for %s with %d symbols, "
                     "equity=$%.2f",
                     self.session_date, len(symbols), equity)

    def get_buffer(self, symbol):
        """Get the buffer for a symbol, or None if not initialized."""
        return self.buffers.get(symbol)


# ============================================================================
# SECTION 12: STRATEGY LOGIC
# ============================================================================
# Decision functions. These read state and indicator data, produce a
# decision (state, tier, action), and return it. They don't modify state
# directly — the main loop applies decisions to state.
#
# Organized in three parts:
#   12.1  Market state computation
#   12.2  Entry evaluation (trigger, filters, tier)
#   12.3  Exit evaluation (normal mode, runner mode, transitions)


# --- 12.1 Market state -----------------------------------------------------


def compute_market_state(spy_buffer, previous_state):
    """Compute the current market state from SPY's level and direction.

    Returns one of: "bullish", "neutral", "bearish", or None if SPY data
    is insufficient.

    Per spec section 3:
    - Bullish requires LEVEL above +0.5% AND DIRECTION rising
    - Bearish requires LEVEL below -0.3% AND DIRECTION falling
    - Anything else is neutral
    - Hysteresis: once in bullish, exit only when level drops below +0.3%
    - Hysteresis: once in bearish, exit only when level rises above -0.1%

    `previous_state` is the state from the previous evaluation. Pass None
    or "neutral" on the first call. Used to apply hysteresis correctly.
    """
    spy_level = compute_spy_change_from_open(spy_buffer)
    spy_direction = compute_spy_direction(
        spy_buffer,
        SPY_DIRECTION_LOOKBACK_MIN,
        SPY_DIRECTION_FLAT_BAND
    )

    # Insufficient data — return None so caller can skip decision-making.
    if spy_level is None or spy_direction is None:
        return None

    # Apply hysteresis based on the previous state.
    if previous_state == "bullish":
        # Stay bullish unless level drops below the exit threshold.
        # Direction doesn't have to keep rising — only level loss exits.
        if spy_level < SPY_BULL_EXIT:
            new_state = _evaluate_fresh_state(spy_level, spy_direction)
            return new_state
        return "bullish"

    if previous_state == "bearish":
        # Stay bearish unless level rises above the exit threshold.
        if spy_level > SPY_BEAR_EXIT:
            new_state = _evaluate_fresh_state(spy_level, spy_direction)
            return new_state
        return "bearish"

    # Previous state was neutral (or unknown) — use entry thresholds.
    return _evaluate_fresh_state(spy_level, spy_direction)


def _evaluate_fresh_state(spy_level, spy_direction):
    """Determine state from level and direction without hysteresis.
    Used when we're in neutral or transitioning out of bullish/bearish."""
    if spy_level >= SPY_BULL_LEVEL and spy_direction == "rising":
        return "bullish"
    if spy_level <= SPY_BEAR_LEVEL and spy_direction == "falling":
        return "bearish"
    return "neutral"


# --- 12.2 Entry evaluation -------------------------------------------------


def evaluate_entry(symbol, state, market_state):
    """Decide whether to enter a position in `symbol` right now.

    Returns a tuple (tier, block_reason, indicators) where:
        - tier is 1, 2, or 3 if entry is approved; None if rejected
        - block_reason is a string explaining rejection, or None on approval
        - indicators is a dict snapshot of the values used in the decision,
          for audit logging

    All trigger conditions and filters per spec section 4.
    """
    indicators = {
        "rs_15min": None,
        "above_vwap": None,
        "drawdown": None,
        "vol_multiple": None,
        "breakout": None,
    }

    # --- Pre-checks ---
    if state.kill_switch.is_tripped():
        return None, "kill_switch_tripped", indicators

    if state.blacklist.is_blocked(symbol):
        return None, "symbol_blocked", indicators

    if state.positions.has(symbol):
        return None, "already_in_position", indicators

    if state.positions.count() >= MAX_POSITIONS:
        return None, "max_positions_reached", indicators

    if market_state is None:
        return None, "market_state_unknown", indicators

    if market_state == "bearish":
        return None, "market_bearish", indicators

    symbol_buffer = state.get_buffer(symbol)
    spy_buffer = state.get_buffer(REFERENCE_SYMBOL)
    if symbol_buffer is None or spy_buffer is None:
        return None, "no_buffer", indicators

    current_price = symbol_buffer.latest_price()
    if current_price is None:
        return None, "no_price", indicators

    # --- Trigger condition 1: price breakout ---
    recent_high = compute_recent_high(symbol_buffer,
                                      BREAKOUT_WINDOW_MIN * 60)
    if recent_high is None:
        return None, "insufficient_history", indicators

    breakout_level = recent_high * (1 + BREAKOUT_CUSHION_PCT)
    indicators["breakout"] = current_price >= breakout_level

    if not indicators["breakout"]:
        return None, "no_breakout", indicators

    # --- Trigger condition 2: volume confirmation ---
    current_volume = compute_current_1min_volume(symbol_buffer)
    median_volume = compute_volume_median_1min(symbol_buffer,
                                                VOLUME_LOOKBACK_MIN)
    if current_volume is None or median_volume is None or median_volume <= 0:
        return None, "insufficient_volume_data", indicators

    vol_multiple = current_volume / median_volume
    indicators["vol_multiple"] = vol_multiple

    if vol_multiple < VOLUME_MULTIPLE:
        return None, "low_volume", indicators

    # --- Filter 1: above VWAP ---
    vwap = compute_vwap(symbol_buffer)
    if vwap is None:
        return None, "no_vwap", indicators

    indicators["above_vwap"] = current_price > vwap

    if not indicators["above_vwap"]:
        return None, "below_vwap", indicators

    # --- Filter 2: relative strength not below floor ---
    rs = compute_relative_strength(symbol_buffer, spy_buffer,
                                   BREAKOUT_WINDOW_MIN)
    if rs is None:
        return None, "no_rs_data", indicators

    indicators["rs_15min"] = rs

    if rs < RS_FILTER_FLOOR:
        return None, "rs_too_weak", indicators

    # --- All filters passed — determine tier ---
    drawdown = compute_drawdown_from_high(symbol_buffer)
    indicators["drawdown"] = drawdown

    tier = _determine_entry_tier(state, market_state, rs, drawdown)
    return tier, None, indicators


def _determine_entry_tier(state, market_state, rs, drawdown):
    """Determine the sizing tier for an approved entry.

    Per spec section 6.1:
    - Tier 1 (3%): tier_tracker says we're in reduced mode after consecutive losses
    - Tier 3 (7%): bullish market AND rs > +0.5% AND drawdown < 0.5%
    - Tier 2 (5%): default

    Tier 1 takes precedence over Tier 3 — if we're in a losing streak,
    we're in reduced size regardless of how strong the setup looks.
    """
    if state.tier_tracker.is_tier1_active():
        return 1

    if (market_state == "bullish"
            and rs is not None
            and rs > TIER3_RS_THRESHOLD
            and drawdown is not None
            and drawdown < TIER3_DRAWDOWN_THRESHOLD):
        return 3

    return 2


def compute_position_size(tier, current_price, buying_power,
                          current_exposure):
    """Compute the share quantity for an entry given tier and account state.

    Applies all capacity constraints:
    - Tier-based sizing (3% / 5% / 7% of buying_power)
    - Per-position hard ceiling (10% of buying_power, defensive backstop)
    - Total exposure cap (current + new must not exceed 20% of buying_power)

    Returns 0 if the constraints would result in a position smaller than
    MIN_TRADE_USD or zero shares. Caller treats 0 as "skip this entry."
    """
    if buying_power <= 0 or current_price <= 0:
        return 0

    tier_pct = {1: TIER1_SIZE_PCT,
                2: TIER2_SIZE_PCT,
                3: TIER3_SIZE_PCT}.get(tier)
    if tier_pct is None:
        logging.error("Invalid tier %s in compute_position_size", tier)
        return 0

    target_dollars = buying_power * tier_pct

    # Apply per-position hard ceiling.
    hard_ceiling = buying_power * PER_POSITION_HARD_CEILING
    if target_dollars > hard_ceiling:
        logging.warning("Position size capped at hard ceiling: "
                        "target=$%.2f ceiling=$%.2f",
                        target_dollars, hard_ceiling)
        target_dollars = hard_ceiling

    # Apply total exposure cap.
    exposure_cap_dollars = buying_power * EXPOSURE_CAP_PCT
    available_exposure = exposure_cap_dollars - current_exposure
    if available_exposure <= 0:
        logging.info("Total exposure cap reached "
                     "(current=$%.2f cap=$%.2f) — skip entry",
                     current_exposure, exposure_cap_dollars)
        return 0

    if target_dollars > available_exposure:
        target_dollars = available_exposure

    # Don't submit tiny orders.
    if target_dollars < MIN_TRADE_USD:
        return 0

    qty = int(target_dollars // current_price)
    return qty


# --- 12.3 Exit evaluation --------------------------------------------------


def evaluate_exit(position, current_price, market_state, now=None):
    """Decide whether `position` should be closed right now.

    Returns a tuple (should_exit, reason) where reason is one of:
        "stop"          — stop-loss hit
        "take_profit"   — TP hit in normal mode (and not transitioning to runner)
        "trailing"      — trailing stop hit in runner mode
        "time_limit"    — 30-minute time limit elapsed in normal mode
        "eod"           — end-of-day forced close
        None            — no exit, position continues

    Per spec section 5.

    `now` defaults to current UTC time. Pass explicitly for testing.

    Note: this function only decides. The main loop is responsible for
    submitting the sell order, updating state, and recording the trade.
    Note: this function does NOT handle the runner-mode transition. The
    main loop calls evaluate_runner_transition separately when TP is hit.
    """
    if now is None:
        now = now_utc()

    # EOD forced close applies to all modes, all positions, no exceptions.
    if force_close_time_reached(now):
        return True, "eod"

    if current_price is None or current_price <= 0:
        return False, None

    if position.mode == "normal":
        # Stop-loss check (checked before TP — if both true, stop wins).
        stop_price = position.entry_price * (1 - STOP_LOSS_PCT)
        if current_price <= stop_price:
            return True, "stop"

        # Take-profit check.
        tp_price = position.entry_price * (1 + TAKE_PROFIT_PCT)
        if current_price >= tp_price:
            # Note: caller decides whether to transition to runner or exit.
            # Returning "take_profit" here means "TP touched"; the caller
            # then calls evaluate_runner_transition.
            return True, "take_profit"

        # Time limit check.
        elapsed = (now - position.entry_time).total_seconds()
        if elapsed >= TIME_LIMIT_SEC:
            return True, "time_limit"

        return False, None

    if position.mode == "runner":
        # Only the trailing stop matters in runner mode.
        # The peak should already be updated by the main loop before this
        # call — we trust position.peak_price as the current peak.
        trailing_stop_price = position.peak_price * (1 - TRAILING_STOP_PCT)
        if current_price <= trailing_stop_price:
            return True, "trailing"

        return False, None

    # Unknown mode — defensive.
    logging.error("Unknown position mode for %s: %s",
                  position.symbol, position.mode)
    return False, None


def evaluate_runner_transition(position, market_state):
    """Decide whether a position that just hit TP should transition to
    runner mode instead of exiting.

    Returns True if the position should switch to runner mode.
    Returns False if the position should exit normally at TP.

    Per spec section 5.2: runner mode activates when TP is hit AND market
    state is bullish at that moment. Anything else exits at TP.

    Only valid to call when position.mode == "normal" and TP has just been
    touched. The caller (main loop) is responsible for that precondition.
    """
    if position.mode != "normal":
        logging.error("evaluate_runner_transition called on non-normal "
                      "position for %s (mode=%s)",
                      position.symbol, position.mode)
        return False

    return market_state == "bullish"


def compute_realized_pnl(position, exit_price):
    """Compute realized P&L in dollars for a closing trade.
    Long position: (exit - entry) * qty.
    """
    return (exit_price - position.entry_price) * position.qty


# ============================================================================
# SECTION 13: MAIN LOOP
# ============================================================================
# The main loop is single-threaded and runs once every LOOP_SLEEP_SEC.
# Each iteration: time check, data fetch, buffer update, kill switch,
# exit evaluation, entry evaluation, sleep.


def _fetch_latest_prices_for_all(symbols):
    """Fetch latest trade prices for all symbols in one or more batched calls.

    Returns a dict {symbol: (price, size, timestamp)}.
    Symbols whose fetch failed are absent from the dict.
    """
    out = {}
    fetched = fetch_latest_trade(symbols)
    fetch_time = now_utc()
    for sym, (price, size) in fetched.items():
        if price is None:
            continue
        out[sym] = (price, size, fetch_time)
    return out


def _update_buffers(state, latest_prices):
    """Push the latest tick into each symbol's buffer.

    Symbols that don't have fresh data this iteration are simply skipped —
    their existing buffer remains and indicators continue computing against
    whatever data is there.
    """
    for symbol, (price, size, ts) in latest_prices.items():
        buffer = state.get_buffer(symbol)
        if buffer is None:
            continue
        buffer.add_tick(price, size, ts)


def _process_exits(state, latest_prices, market_state):
    """Evaluate every open position and exit those that should close.

    For each position:
    - Update peak_price for runner-mode trailing
    - Evaluate exit conditions
    - If exit fires, submit sell, record trade, update tier and blacklist

    Returns the number of positions exited this iteration.
    """
    exited = 0

    # Snapshot the position list — modifications during iteration would
    # break the loop. We may close positions during this method.
    positions_snapshot = state.positions.all_positions()

    for position in positions_snapshot:
        symbol = position.symbol
        price_data = latest_prices.get(symbol)

        if price_data is None:
            # No fresh price this tick — try buffer's latest price.
            buffer = state.get_buffer(symbol)
            current_price = buffer.latest_price() if buffer else None
        else:
            current_price = price_data[0]

        if current_price is None:
            continue

        # Update peak for runner-mode trailing.
        position.update_peak(current_price)

        should_exit, reason = evaluate_exit(position, current_price,
                                            market_state)

        if not should_exit:
            continue

        # If TP hit in normal mode, decide whether to transition or exit.
        if reason == "take_profit" and position.mode == "normal":
            if evaluate_runner_transition(position, market_state):
                position.transition_to_runner()
                # Don't exit — position continues in runner mode.
                continue

        # Exit the position.
        result = submit_sell(symbol, position.qty)
        if result is None:
            logging.warning("Sell submission failed for %s — position "
                            "remains open, will retry next loop", symbol)
            continue

        exit_price = result.get("filled_price") or current_price
        exit_status = result.get("status", "unknown")
        order_id = result.get("order_id", "")

        pnl = compute_realized_pnl(position, exit_price)

        log_trade(symbol=symbol, action="SELL",
                  qty=position.qty, price=exit_price,
                  fill_status=exit_status,
                  market_state=market_state,
                  order_id=order_id,
                  exit_reason=reason, pnl=pnl,
                  timestamp=result.get("fill_time"))

        # Update tier tracker and blacklist with the outcome.
        state.tier_tracker.record_outcome(pnl)
        state.blacklist.record_outcome(symbol, pnl)

        state.positions.remove(symbol)
        exited += 1

        logging.info("EXITED %s qty=%d entry=%.4f exit=%.4f pnl=$%.2f "
                     "reason=%s mode=%s",
                     symbol, position.qty, position.entry_price,
                     exit_price, pnl, reason, position.mode)

    return exited


def _process_entries(state, latest_prices, market_state):
    """Scan tradable universe for entry opportunities.

    For each symbol that's not already open and not blacklisted, evaluate
    entry. If approved, compute size and submit buy.

    Returns the number of entries submitted this iteration.
    """
    if not entries_allowed_now():
        return 0

    if state.kill_switch.is_tripped():
        return 0

    if state.positions.count() >= MAX_POSITIONS:
        return 0

    buying_power = get_buying_power()
    if buying_power is None or buying_power <= 0:
        return 0

    # Compute current exposure once for this iteration's entry decisions.
    prices_only = {sym: data[0] for sym, data in latest_prices.items()}
    current_exposure = state.positions.total_exposure(prices_only)

    entered = 0

    # Evaluate each tradable symbol. Order doesn't matter much — most
    # symbols won't qualify, and the capacity cap stops the iteration
    # if we hit MAX_POSITIONS or 20% exposure during the scan.
    for symbol in TRADABLE_UNIVERSE:
        if state.positions.count() >= MAX_POSITIONS:
            break

        tier, block_reason, indicators = evaluate_entry(
            symbol, state, market_state
        )

        if tier is None:
            # Log blocked decisions only when the trigger actually fired.
            # Avoid spamming the CSV with "no_breakout" rows for every
            # symbol on every tick.
            if block_reason not in (
                "no_breakout", "no_buffer", "no_price",
                "insufficient_history", "insufficient_volume_data",
                "already_in_position", "low_volume",
            ):
                log_decision(symbol=symbol, decision="blocked",
                             market_state=market_state,
                             rs_15min=indicators.get("rs_15min"),
                             above_vwap=indicators.get("above_vwap"),
                             drawdown=indicators.get("drawdown"),
                             vol_multiple=indicators.get("vol_multiple"),
                             tier_assigned=None,
                             block_reason=block_reason)
            continue

        # Entry approved. Compute size.
        buffer = state.get_buffer(symbol)
        current_price = buffer.latest_price() if buffer else None
        if current_price is None:
            continue

        qty = compute_position_size(tier, current_price, buying_power,
                                    current_exposure)
        if qty <= 0:
            log_decision(symbol=symbol, decision="blocked",
                         market_state=market_state,
                         rs_15min=indicators.get("rs_15min"),
                         above_vwap=indicators.get("above_vwap"),
                         drawdown=indicators.get("drawdown"),
                         vol_multiple=indicators.get("vol_multiple"),
                         tier_assigned=tier,
                         block_reason="size_zero")
            continue

        # Submit the buy.
        result = submit_buy(symbol, qty)
        if result is None:
            log_decision(symbol=symbol, decision="blocked",
                         market_state=market_state,
                         rs_15min=indicators.get("rs_15min"),
                         above_vwap=indicators.get("above_vwap"),
                         drawdown=indicators.get("drawdown"),
                         vol_multiple=indicators.get("vol_multiple"),
                         tier_assigned=tier,
                         block_reason="submit_failed")
            continue

        # Record the position.
        fill_price = result.get("filled_price") or current_price
        fill_status = result.get("status", "unknown")
        order_id = result.get("order_id", "")
        fill_time = result.get("fill_time") or now_utc()

        position = Position(
            symbol=symbol,
            qty=int(result.get("filled_qty") or qty),
            entry_price=fill_price,
            entry_time=fill_time,
            tier=tier,
            market_state_at_entry=market_state,
            order_id=order_id,
        )
        state.positions.add(position)

        # Update current_exposure so subsequent entries this iteration
        # see the new total.
        current_exposure += position.qty * fill_price

        log_trade(symbol=symbol, action="BUY",
                  qty=position.qty, price=fill_price,
                  fill_status=fill_status,
                  market_state=market_state,
                  order_id=order_id, tier=tier,
                  timestamp=fill_time)

        log_decision(symbol=symbol, decision="entered",
                     market_state=market_state,
                     rs_15min=indicators.get("rs_15min"),
                     above_vwap=indicators.get("above_vwap"),
                     drawdown=indicators.get("drawdown"),
                     vol_multiple=indicators.get("vol_multiple"),
                     tier_assigned=tier,
                     block_reason=None)

        logging.info("ENTERED %s qty=%d price=%.4f tier=%d state=%s",
                     symbol, position.qty, fill_price, tier, market_state)

        entered += 1

    return entered


def _diagnostic_snapshot(state, market_state):
    """Log a per-symbol diagnostic showing how close each symbol is to
    triggering an entry.

    For every symbol with enough data, computes the four entry inputs
    (breakout distance, volume ratio, VWAP position, relative strength)
    and reports the top 5 symbols closest to breaking out, plus any
    symbol whose breakout actually fired but failed a filter.

    This is operational visibility, not strategy state — strategy
    decisions ignore this output.
    """
    if market_state is None:
        logging.info("[diag] market_state=None — SPY buffer not ready yet")
        return

    spy_buffer = state.get_buffer(REFERENCE_SYMBOL)
    if spy_buffer is None:
        logging.info("[diag] no SPY buffer")
        return

    snapshots = []  # list of dicts, one per symbol with enough data

    for symbol in TRADABLE_UNIVERSE:
        buffer = state.get_buffer(symbol)
        if buffer is None:
            continue

        price = buffer.latest_price()
        if price is None:
            continue

        recent_high = compute_recent_high(buffer, BREAKOUT_WINDOW_MIN * 60)
        if recent_high is None:
            continue

        breakout_target = recent_high * (1 + BREAKOUT_CUSHION_PCT)
        # Negative distance = price already above the target (a triggered
        # breakout); positive distance = how much further price needs to rise.
        distance_to_breakout = (breakout_target - price) / price

        current_vol = compute_current_1min_volume(buffer)
        median_vol = compute_volume_median_1min(buffer, VOLUME_LOOKBACK_MIN)
        vol_ratio = None
        if current_vol is not None and median_vol is not None and median_vol > 0:
            vol_ratio = current_vol / median_vol

        vwap = compute_vwap(buffer)
        above_vwap = (price > vwap) if vwap is not None else None

        rs = compute_relative_strength(buffer, spy_buffer,
                                       BREAKOUT_WINDOW_MIN)

        snapshots.append({
            "symbol": symbol,
            "price": price,
            "high": recent_high,
            "dist": distance_to_breakout,
            "vol_ratio": vol_ratio,
            "above_vwap": above_vwap,
            "rs": rs,
            "buffer_len": buffer.length(),
        })

    if not snapshots:
        logging.info("[diag] no symbols have enough buffer data yet")
        return

    # Sort by distance — smallest (or most negative) first.
    snapshots.sort(key=lambda s: s["dist"])

    # Show top 5 nearest to (or past) breakout.
    logging.info("[diag] market_state=%s — top 5 closest to breakout:",
                 market_state)
    for s in snapshots[:5]:
        vol_str = f"{s['vol_ratio']:.2f}" if s['vol_ratio'] is not None else "n/a"
        vwap_str = (
            "above" if s["above_vwap"] is True else
            "below" if s["above_vwap"] is False else
            "n/a"
        )
        rs_str = f"{s['rs']*100:+.2f}%" if s['rs'] is not None else "n/a"
        logging.info(
            "  %s price=%.4f high15m=%.4f dist=%+.3f%% vol=%sx vwap=%s rs=%s buf=%ds",
            s["symbol"], s["price"], s["high"], s["dist"] * 100,
            vol_str, vwap_str, rs_str, s["buffer_len"]
        )

    # Flag any symbol that's actually past the breakout level but didn't
    # trigger — useful for spotting filter rejections.
    triggered = [s for s in snapshots if s["dist"] <= 0]
    if triggered:
        logging.info("[diag] %d symbol(s) past breakout level:",
                     len(triggered))
        for s in triggered:
            issues = []
            if s["vol_ratio"] is None or s["vol_ratio"] < VOLUME_MULTIPLE:
                issues.append(f"vol_low({s['vol_ratio']})")
            if s["above_vwap"] is False:
                issues.append("below_vwap")
            if s["rs"] is not None and s["rs"] < RS_FILTER_FLOOR:
                issues.append(f"rs_weak({s['rs']*100:+.2f}%)")
            issues_str = ",".join(issues) if issues else "should_enter"
            logging.info("  %s past_breakout: %s",
                         s["symbol"], issues_str)


def _check_kill_switch(state, latest_prices):
    """Check the daily kill switch. If tripped, close all positions.

    Returns True if the kill switch was just tripped (caller should stop
    further entries this iteration). Returns False otherwise.
    """
    equity = get_account_equity()
    if equity is None:
        return False

    just_tripped = state.kill_switch.evaluate(equity)
    if not just_tripped:
        return state.kill_switch.is_tripped()

    # Just tripped — close all positions immediately.
    logging.warning("Kill switch tripped — closing all open positions")
    for position in state.positions.all_positions():
        symbol = position.symbol
        price_data = latest_prices.get(symbol)
        current_price = price_data[0] if price_data else position.entry_price

        result = submit_sell(symbol, position.qty)
        if result is None:
            logging.error("Kill switch close failed for %s — manual review",
                          symbol)
            continue

        exit_price = result.get("filled_price") or current_price
        pnl = compute_realized_pnl(position, exit_price)

        log_trade(symbol=symbol, action="SELL",
                  qty=position.qty, price=exit_price,
                  fill_status=result.get("status", "unknown"),
                  market_state="kill_switch",
                  order_id=result.get("order_id", ""),
                  exit_reason="kill_switch", pnl=pnl,
                  timestamp=result.get("fill_time"))

        state.positions.remove(symbol)

    return True


def main_loop(state):
    """Run the main trading loop. Blocks until the program is interrupted
    (Ctrl-C) or the session ends.

    `state` is a fully initialized StrategyState. The caller is responsible
    for initializing it via initialize_session() and pre-populating buffers.
    """
    previous_market_state = "neutral"
    last_logged_state = None
    last_heartbeat = now_utc()
    last_diagnostic = now_utc() - timedelta(seconds=60)  # fire on first iter
    HEARTBEAT_INTERVAL_SEC = 300       # short status every 5 minutes
    DIAGNOSTIC_INTERVAL_SEC = 60       # per-symbol detail every 1 minute

    logging.info("Main loop starting")

    while True:
        try:
            now = now_utc()

            # If we're past EOD force-close time, close any remaining
            # positions and break out of the loop.
            if force_close_time_reached(now):
                if state.positions.count() > 0:
                    logging.info("EOD reached — force-closing %d positions",
                                 state.positions.count())
                    latest_prices = _fetch_latest_prices_for_all(ALL_SYMBOLS)
                    _eod_close_all(state, latest_prices)
                logging.info("EOD reached — main loop exiting")
                break

            # Outside market hours, sleep longer — no work to do.
            if not is_market_open(now):
                time.sleep(30)
                continue

            # Fetch latest data for all symbols (tradable + reference).
            latest_prices = _fetch_latest_prices_for_all(ALL_SYMBOLS)

            # Push ticks into buffers.
            _update_buffers(state, latest_prices)

            # Compute current market state from SPY data.
            spy_buffer = state.get_buffer(REFERENCE_SYMBOL)
            market_state = compute_market_state(spy_buffer,
                                                previous_market_state)
            if market_state is not None:
                if market_state != last_logged_state:
                    logging.info("Market state: %s (was %s)",
                                 market_state, last_logged_state)
                    last_logged_state = market_state
                previous_market_state = market_state

            # Heartbeat — confirm to the operator that the loop is alive
            # even when nothing interesting is happening.
            if (now - last_heartbeat).total_seconds() >= HEARTBEAT_INTERVAL_SEC:
                spy_len = spy_buffer.length() if spy_buffer else 0
                spy_price = spy_buffer.latest_price() if spy_buffer else None
                state_str = market_state if market_state else "computing"
                entries_ok = "yes" if entries_allowed_now(now) else "no"
                logging.info(
                    "[heartbeat] state=%s positions=%d entries_allowed=%s "
                    "spy_buffer=%ds spy_price=%s",
                    state_str, state.positions.count(), entries_ok,
                    spy_len,
                    f"{spy_price:.2f}" if spy_price else "n/a"
                )
                last_heartbeat = now

            # Diagnostic snapshot — per-symbol detail every minute so the
            # operator can see what the bot is seeing for the universe.
            if (now - last_diagnostic).total_seconds() >= DIAGNOSTIC_INTERVAL_SEC:
                _diagnostic_snapshot(state, market_state)
                last_diagnostic = now

            # Kill switch check — trips and closes everything if breached.
            ks_tripped = _check_kill_switch(state, latest_prices)

            if ks_tripped:
                # No more entries this iteration; existing positions are
                # already closed by _check_kill_switch.
                time.sleep(LOOP_SLEEP_SEC)
                continue

            # Process exits before entries so freed slots are available.
            _process_exits(state, latest_prices, market_state)

            # Process entries.
            if market_state is not None:
                _process_entries(state, latest_prices, market_state)

        except KeyboardInterrupt:
            logging.info("KeyboardInterrupt — shutting down main loop")
            break
        except Exception as e:
            logging.exception("Unhandled error in main loop: %s", e)
            # Don't break — keep trying. Single-iteration failures shouldn't
            # bring down the whole system.

        time.sleep(LOOP_SLEEP_SEC)


def _eod_close_all(state, latest_prices):
    """Close every open position at market price. Used at EOD."""
    for position in state.positions.all_positions():
        symbol = position.symbol
        price_data = latest_prices.get(symbol)
        current_price = price_data[0] if price_data else position.entry_price

        result = submit_sell(symbol, position.qty)
        if result is None:
            logging.error("EOD close failed for %s — manual review", symbol)
            continue

        exit_price = result.get("filled_price") or current_price
        pnl = compute_realized_pnl(position, exit_price)

        log_trade(symbol=symbol, action="SELL",
                  qty=position.qty, price=exit_price,
                  fill_status=result.get("status", "unknown"),
                  market_state="eod",
                  order_id=result.get("order_id", ""),
                  exit_reason="eod", pnl=pnl,
                  timestamp=result.get("fill_time"))

        state.positions.remove(symbol)
        logging.info("EOD closed %s qty=%d exit=%.4f pnl=$%.2f",
                     symbol, position.qty, exit_price, pnl)


# ============================================================================
# SECTION 14: STARTUP AND ENTRY POINT
# ============================================================================
# Startup runs once before the main loop and verifies the system is in a
# clean state to begin trading.


class StartupError(Exception):
    """Raised when startup checks fail. Prevents the system from running
    in an unsafe or unknown state."""
    pass


def startup_pre_flight_checks():
    """Verify basic preconditions before doing any work.

    Raises StartupError if the system shouldn't start.
    """
    now = now_utc()
    et = to_et(now)

    if not is_trading_day(et.date()):
        raise StartupError(
            f"Today ({et.date()}) is not a NYSE trading day. "
            f"System will not start."
        )

    # If we're past the EOD force-close time, there's nothing to do.
    if force_close_time_reached(now):
        raise StartupError(
            f"Current time {to_helsinki(now).strftime('%H:%M')} is past "
            f"EOD force-close. No new session can start today."
        )

    logging.info("Pre-flight: trading day OK, time OK")


def startup_account_check():
    """Read account equity and buying power. Returns (equity, buying_power).
    Raises StartupError on any failure or implausible value.
    """
    equity = get_account_equity()
    if equity is None:
        raise StartupError("Could not read account equity from Alpaca.")
    if equity <= 0:
        raise StartupError(f"Account equity is non-positive: ${equity:.2f}")

    buying_power = get_buying_power()
    if buying_power is None:
        raise StartupError("Could not read buying power from Alpaca.")
    if buying_power < 0:
        raise StartupError(
            f"Buying power is negative: ${buying_power:.2f}"
        )

    logging.info("Account: equity=$%.2f, buying_power=$%.2f",
                 equity, buying_power)
    return equity, buying_power


def startup_check_unexpected_positions():
    """Per spec section 7.4: if Alpaca shows any open positions when we
    start, refuse to run. The investor must manually close them or
    acknowledge before starting again.

    Raises StartupError if any positions exist.
    """
    positions = get_all_positions()
    if not positions:
        logging.info("Positions check: account is flat, OK to start")
        return

    msg_lines = ["Found unexpected open positions on Alpaca:"]
    for sym, (qty, avg_price) in positions.items():
        msg_lines.append(f"  {sym}: qty={qty} avg_entry=${avg_price:.4f}")
    msg_lines.append(
        "Per strategy spec, the system refuses to start with unexpected "
        "positions. Close them manually or review before restarting."
    )
    raise StartupError("\n".join(msg_lines))


def startup_populate_buffers(state):
    """Fetch the last BREAKOUT_WINDOW_MIN minutes of historical bars for
    each symbol to populate the rolling windows.

    Best-effort: symbols whose history can't be fetched will have empty
    buffers and won't be tradable until 15 minutes of live data accumulate.
    """
    logging.info("Pre-populating buffers from %d minutes of history...",
                 BREAKOUT_WINDOW_MIN)
    populated = 0
    skipped = 0

    for symbol in ALL_SYMBOLS:
        bars = fetch_recent_minute_bars(symbol, BREAKOUT_WINDOW_MIN)
        buffer = state.get_buffer(symbol)
        if buffer is None:
            logging.warning("No buffer for %s during pre-populate", symbol)
            skipped += 1
            continue
        if bars is None or bars.empty:
            logging.info("No history available for %s — buffer will fill "
                         "from live data", symbol)
            skipped += 1
            continue
        buffer.populate_from_bars(bars)
        populated += 1

    logging.info("Buffer pre-populate: %d populated, %d skipped",
                 populated, skipped)


def startup():
    """Full startup sequence. Returns a fully initialized StrategyState
    ready for main_loop. Raises StartupError on any failure."""
    logging.info("=" * 60)
    logging.info("Startup beginning")
    logging.info("=" * 60)

    startup_pre_flight_checks()
    equity, _ = startup_account_check()
    startup_check_unexpected_positions()

    state = StrategyState()
    state.initialize_session(equity, ALL_SYMBOLS)

    startup_populate_buffers(state)

    logging.info("Startup complete — handing off to main loop")
    return state


# ============================================================================
# ENTRY POINT
# ============================================================================
# This is what runs when you execute `python scalper.py`.

if __name__ == "__main__":
    try:
        state = startup()
        main_loop(state)
        logging.info("Main loop exited cleanly")
    except StartupError as e:
        logging.error("STARTUP ABORTED: %s", e)
        print(f"\nSTARTUP ABORTED:\n{e}\n")
        raise SystemExit(1)
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt at top level — shutting down")
    except Exception as e:
        logging.exception("Unhandled top-level error: %s", e)
        raise SystemExit(2)
    finally:
        logging.info("Scalper terminated")
