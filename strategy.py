#!/usr/bin/env python3
"""
Long-only tick scalper - safer, stricter entry, cooldowns, chunked market-data fetches,
defensive order handling to avoid oversells.

Requires: alpaca-data / alpaca-trade-api client compatible with used calls in the script,
and dotenv for environment variables.

Tweak parameters at top to taste.
"""

import logging
import os
import time
from datetime import datetime, timezone
from collections import deque, defaultdict
import threading

import pandas as pd
from dotenv import load_dotenv
from alpaca.data.historical.stock import StockHistoricalDataClient, StockLatestTradeRequest
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, OrderType, TimeInForce
from alpaca.trading.requests import MarketOrderRequest

# === CONFIG ===
SCALP = True
LOOP_SLEEP = 1.0                     # desired loop cadence (seconds)
TICKS_WINDOW = 300                   # rolling tick window (approx 5 minutes at 1s ticks)
EMA_FAST = 9
EMA_SLOW = 20
RSI_PERIOD = 7
VOL_SPIKE_MULT = 1.4                 # require stronger volume spike
TP_PCT = 0.006                       # 0.6% take profit
SL_PCT = 0.004                       # 0.4% stop loss
MAX_HOLD_SECONDS = 5 * 60            # max hold 5 minutes
BUY_POWER_LIMIT = 0.05               # fraction of buying power permitted per trade
BUY_CASH_BUFFER = 0.95               # 95% of computed cash to leave a buffer
COOLDOWN_SECONDS = 30                # no re-entry into the same symbol for 30s after exit
MIN_RSI_FOR_ENTRY = 52               # require modest momentum
MAX_RSI_FOR_ENTRY = 70
MIN_TRADE_USD = 25                   # don't attempt buys below $25 implied order
MARKET_DATA_CHUNK = 5                # number of symbols per batch request (reduces HTTP calls)
MAX_INFLIGHT_PER_SYMBOL = 1

# === LOGGING ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", filename="scalper_safe.log")
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger().addHandler(console)

# === helpers: indicators ===
def compute_ema_from_series(series, period):
    if len(series) < 2:
        return series.iloc[-1] if len(series) else float("nan")
    return series.ewm(span=period, adjust=False).mean()

def compute_rsi_from_series(series, period=14):
    if len(series) < period + 1:
        return pd.Series([float("nan")] * len(series))
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def compute_vwap_from_ticks(prices, sizes):
    if sizes.sum() == 0:
        return pd.Series([float("nan")] * len(prices))
    cumulative_pv = (prices * sizes).cumsum()
    cumulative_vol = sizes.cumsum()
    return cumulative_pv / cumulative_vol

# === Alpaca helpers: defensive ===
def fetch_latest_trade_price_and_size_batch(stock_data_client, symbols):
    """
    Batch fetch latest trades for a list of symbols.
    Returns dict: symbol -> (price, size) or None if missing.
    """
    try:
        req = StockLatestTradeRequest(symbol_or_symbols=symbols)
        trades = stock_data_client.get_stock_latest_trade(req)
        out = {}
        for sym in symbols:
            t = trades.get(sym)
            if t is None:
                out[sym] = (None, None)
            else:
                # adapt to how the client returns
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
        logging.debug("get_all_positions failed: %s", e)
        return {}

def get_position_qty(trade_client_local, symbol):
    try:
        pos = trade_client_local.get_position(symbol)
        return int(float(pos.qty))
    except Exception:
        return 0

def safe_market_buy(trade_client_local, symbol, cash_amount, order_lock, retries=3, wait_sec=0.5):
    """Place a conservative market buy. Only return after confirming shares filled."""
    with order_lock:
        try:
            # estimate price from latest trade
            est_price = None
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
                logging.debug(
                    "Computed buy qty too small for %s (qty=%s est_price=%s cash=%.2f)",
                    symbol, qty, est_price, cash_amount
                )
                return None

            order = MarketOrderRequest(
                symbol=symbol, qty=qty, side=OrderSide.BUY,
                type=OrderType.MARKET, time_in_force=TimeInForce.DAY
            )
            submitted = trade_client_local.submit_order(order)
            logging.info("%s - BUY submitted qty=%d (est_price=%s cash=%.2f)", symbol, qty, str(est_price), cash_amount)

            # Poll actual position until filled or max retries
            actual_qty = 0
            for attempt in range(retries):
                time.sleep(wait_sec)
                actual_qty = get_position_qty(trade_client_local, symbol)
                logging.debug("%s - poll attempt %d: position qty=%d", symbol, attempt+1, actual_qty)
                if actual_qty > 0:
                    logging.info("%s - BUY filled qty=%d", symbol, actual_qty)
                    return submitted  # filled order confirmed

            logging.warning("%s - BUY did not fill after %d attempts (qty=0)", symbol, retries)
            return None

        except Exception as e:
            logging.exception("safe_market_buy error for %s: %s", symbol, e)
            return None


def safe_market_sell(trade_client_local, symbol, intended_qty, order_lock):
    """Sell up to intended_qty but never more than current position qty. Uses lock."""
    with order_lock:
        available = get_position_qty(trade_client_local, symbol)
        qty_to_sell = min(int(intended_qty), available)
        if qty_to_sell <= 0:
            logging.info("safe_market_sell: nothing to sell for %s (available=%d intended=%s)", symbol, available, intended_qty)
            return None
        try:
            order = MarketOrderRequest(symbol=symbol, qty=qty_to_sell, side=OrderSide.SELL, type=OrderType.MARKET, time_in_force=TimeInForce.DAY)
            submitted = trade_client_local.submit_order(order)
            logging.info("%s - SELL submitted qty=%d (available=%d intended=%s)", symbol, qty_to_sell, available, intended_qty)
            return submitted
        except Exception as e:
            logging.exception("safe_market_sell error for %s: %s", symbol, e)
            return None

# === MAIN ===
def main():
    load_dotenv()
    global stock_data_client, trade_client
    stock_data_client = StockHistoricalDataClient(os.getenv("ALPACA_PAPER_API_KEY"), os.getenv("ALPACA_PAPER_SECRET_KEY"))
    trade_client = TradingClient(os.getenv("ALPACA_PAPER_API_KEY"), os.getenv("ALPACA_PAPER_SECRET_KEY"), paper=True)

    symbols = ["AAPL", "MSFT", "MU", "QCOM", "NVDA", "V", "AMD", "GOOG", "C", "EBAY", "OKTA", "TSLA", "AMZN", "ADSK", "DELL"]

    # rolling storage
    price_deques = {s: deque(maxlen=TICKS_WINDOW) for s in symbols}
    size_deques = {s: deque(maxlen=TICKS_WINDOW) for s in symbols}
    time_deques = {s: deque(maxlen=TICKS_WINDOW) for s in symbols}

    # state
    entry_times = {}          # symbol -> datetime
    entry_prices = {}         # symbol -> float
    entry_qty = {}            # symbol -> int
    inflight_orders = {}      # symbol -> order_id (or True)
    last_exit_time = {}       # symbol -> datetime when last exit occurred
    last_trade_attempt = defaultdict(lambda: datetime.min.replace(tzinfo=timezone.utc))

    order_lock = threading.Lock()
    stop_event = threading.Event()

    # background input thread for 'exit' command to liquidate
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
                    with order_lock_local:
                        try:
                            order = MarketOrderRequest(symbol=s, qty=q, side=OrderSide.SELL, type=OrderType.MARKET, time_in_force=TimeInForce.DAY)
                            trade_client_local.submit_order(order)
                            logging.info("EXIT SELL %s qty=%d", s, q)
                        except Exception as e:
                            logging.exception("EXIT SELL error for %s: %s", s, e)
        except Exception as e:
            logging.exception("Failed to fetch positions during EXIT: %s", e)

    thr = threading.Thread(target=input_listener, daemon=True)
    thr.start()

    # pacing helpers
    symbol_chunks = [symbols[i:i+MARKET_DATA_CHUNK] for i in range(0, len(symbols), MARKET_DATA_CHUNK)]
    last_account_fetch = 0
    account_fetch_interval = 10
    buying_power_limit_cached = None

    logging.info("Starting long-only scalper main loop.")

    try:
        while not stop_event.is_set():
            loop_start = datetime.now(timezone.utc)

            # refresh buying power occasionally
            if buying_power_limit_cached is None or (time.time() - last_account_fetch) > account_fetch_interval:
                buying_power_limit_cached = calculate_buying_power_limit(trade_client, BUY_POWER_LIMIT)
                last_account_fetch = time.time()

            # fetch positions snapshot (infrequent)
            try:
                positions_snapshot = get_positions_map(trade_client)
            except Exception:
                positions_snapshot = {}

            # batch fetch market data per chunk (reduces HTTP calls & rate pressure)
            for chunk in symbol_chunks:
                md = fetch_latest_trade_price_and_size_batch(stock_data_client, chunk)
                tnow = datetime.now(timezone.utc)

                for sym in chunk:
                    price, size = md.get(sym, (None, None))
                    if price is None:
                        continue

                    price_deques[sym].append(price)
                    size_deques[sym].append(size)
                    time_deques[sym].append(tnow)

                    # minimal history check
                    if len(price_deques[sym]) < max(EMA_SLOW, RSI_PERIOD, 3):
                        continue

                    prices = pd.Series(list(price_deques[sym]))
                    sizes = pd.Series(list(size_deques[sym]))
                    vwap_series = compute_vwap_from_ticks(prices, sizes)
                    ema_fast = compute_ema_from_series(prices, EMA_FAST)
                    ema_slow = compute_ema_from_series(prices, EMA_SLOW)
                    rsi_series = compute_rsi_from_series(prices, RSI_PERIOD)

                    ema_fast_val = float(ema_fast.iloc[-1])
                    ema_slow_val = float(ema_slow.iloc[-1])
                    rsi_val = float(rsi_series.iloc[-1]) if pd.notna(rsi_series.iloc[-1]) else None
                    vwap_val = float(vwap_series.iloc[-1]) if pd.notna(vwap_series.iloc[-1]) else None

                    # volume spike check - compare last tick size to 20-tick rolling mean
                    avg_size = sizes.rolling(window=min(len(sizes), 20)).mean().iloc[-1]
                    vol_ok = False
                    if pd.notna(avg_size) and avg_size > 0:
                        vol_ok = size > (avg_size * VOL_SPIKE_MULT)

                    price_above_vwap = (price > vwap_val) if vwap_val is not None else False
                    ema_trend_up = ema_fast_val > ema_slow_val
                    qty_open, avg_entry = positions_snapshot.get(sym, (0, 0.0))
                    
                 
                    # prefer authoritative API for qty
                    try:
                        api_qty = get_position_qty(trade_client, sym)
                        if api_qty != qty_open:
                            qty_open = api_qty
                    except Exception:
                        pass

                            # --- ENTRY CONDITIONS ---
                    now = datetime.now(timezone.utc)
                    last_exit = last_exit_time.get(sym, datetime.min.replace(tzinfo=timezone.utc))

                    if (qty_open == 0
                        and ema_trend_up
                        and price_above_vwap
                        and vol_ok
                        and rsi_val is not None
                        and MIN_RSI_FOR_ENTRY <= rsi_val <= MAX_RSI_FOR_ENTRY
                        and (now - last_exit).total_seconds() >= COOLDOWN_SECONDS
                        and inflight_orders.get(sym) is None):

                        # throttle per-symbol rapid attempts
                        if (now - last_trade_attempt[sym]).total_seconds() < 1.0:
                            continue
                        last_trade_attempt[sym] = now

                        # compute qty using cached buying power
                        limit_cash = buying_power_limit_cached or calculate_buying_power_limit(trade_client, BUY_POWER_LIMIT)
                        if limit_cash <= 0:
                            logging.debug("No buying power available; skipping buys.")
                        else:
                            cash_for_order = limit_cash * BUY_CASH_BUFFER
                            submitted = safe_market_buy(trade_client, sym, cash_for_order, order_lock)
                            if submitted:
                                # mark inflight
                                order_id = getattr(submitted, "id", None) or getattr(submitted, "order_id", None)
                                inflight_orders[sym] = order_id or True

                                # allow a brief moment for the order to process
                                time.sleep(1.0)

                                try:
                                    filled_order = trade_client.get_order(order_id)
                                    filled_qty = int(float(getattr(filled_order, "filled_qty", 0)))
                                    filled_avg_price = float(getattr(filled_order, "filled_avg_price", price))

                                    if filled_qty > 0:
                                        entry_qty[sym] = filled_qty
                                        entry_prices[sym] = filled_avg_price
                                        entry_times[sym] = now
                                        logging.info(
                                            "%s - ENTRY recorded qty=%d avg_price=%.4f rsi=%.2f avg_size=%.1f",
                                            sym, filled_qty, filled_avg_price, rsi_val or -1, avg_size or 0
                                        )
                                    else:
                                        logging.info("%s - ENTRY not yet filled, inflight remains", sym)

                                except Exception as e:
                                    logging.exception("Failed to reconcile buy order %s: %s", sym, e)

                                inflight_orders.pop(sym, None)

                    # --- EXIT CONDITIONS (only for longs) ---
                    if qty_open > 0:
                        # ensure we have an entry price
                        entry_price = entry_prices.get(sym, avg_entry or price)
                        tp_hit = price >= entry_price * (1 + TP_PCT)
                        sl_hit = price <= entry_price * (1 - SL_PCT)
                        time_exceeded = False
                        if sym in entry_times:
                            elapsed = (now - entry_times[sym]).total_seconds()
                            if elapsed >= MAX_HOLD_SECONDS:
                                time_exceeded = True

                        if tp_hit or sl_hit or time_exceeded:
                            intended_qty = entry_qty.get(sym, qty_open)
                            logging.info("%s - EXIT condition triggered (tp=%s sl=%s time_exceeded=%s) intended_qty=%s entry_price=%.4f last=%.4f",
                                         sym, tp_hit, sl_hit, time_exceeded, intended_qty, entry_price, price)
                            submitted = safe_market_sell(trade_client, sym, intended_qty, order_lock)
                            # brief reconcile
                            time.sleep(0.5)
                            post_qty = get_position_qty(trade_client, sym)
                            logging.info("%s - post-exit qty=%d", sym, post_qty)
                            # clear entry state and set cooldown if we closed
                            if post_qty == 0:
                                entry_times.pop(sym, None)
                                entry_prices.pop(sym, None)
                                entry_qty.pop(sym, None)
                                last_exit_time[sym] = datetime.now(timezone.utc)
                                positions_snapshot[sym] = (0, positions_snapshot.get(sym, (0,0))[1])

                # small sleep between batches to spread calls
                time.sleep(0.03)

            # keep loop cadence roughly LOOP_SLEEP
            elapsed_loop = (datetime.now(timezone.utc) - loop_start).total_seconds()
            to_sleep = max(0.0, LOOP_SLEEP - elapsed_loop)
            time.sleep(to_sleep)

    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt - shutting down and liquidating positions.")
        sell_all_positions(trade_client, order_lock)
    except Exception:
        logging.exception("Main loop crashed unexpectedly.")
        sell_all_positions(trade_client, order_lock)
    finally:
        stop_event.set()
        logging.info("Scalper stopped.")

if __name__ == "__main__":
    main()
