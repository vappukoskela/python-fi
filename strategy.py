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
LOOP_SLEEP = 1.0
TICKS_WINDOW = 300
EMA_FAST = 9
EMA_SLOW = 20
RSI_PERIOD = 7
VOL_SPIKE_MULT = 1.4
TP_PCT = 0.006
SL_PCT = 0.004
MAX_HOLD_SECONDS = 5 * 60
BUY_POWER_LIMIT = 0.05
BUY_CASH_BUFFER = 0.95
COOLDOWN_SECONDS = 30
MIN_RSI_FOR_ENTRY = 52
MAX_RSI_FOR_ENTRY = 70
MIN_TRADE_USD = 25
MARKET_DATA_CHUNK = 5
MAX_INFLIGHT_PER_SYMBOL = 1

# === LOGGING ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", filename="scalper_safe.log")
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger().addHandler(console)

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
        positions = trade_client_local.get_open_positions()
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
                logging.debug("Computed buy qty too small for %s (qty=%s est_price=%s cash=%.2f)", symbol, qty, est_price, cash_amount)
                return None
            order = MarketOrderRequest(
                symbol=symbol, qty=qty, side=OrderSide.BUY,
                type=OrderType.MARKET, time_in_force=TimeInForce.DAY
            )
            submitted = trade_client_local.submit_order(order)
            logging.info("%s - BUY submitted qty=%d (est_price=%s cash=%.2f)", symbol, qty, str(est_price), cash_amount)
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
            logging.info("safe_market_sell: nothing to sell for %s (available=%d intended=%s)", symbol, available, intended_qty)
            return None
        try:
            order = MarketOrderRequest(
                symbol=symbol, qty=qty_to_sell, side=OrderSide.SELL,
                type=OrderType.MARKET, time_in_force=TimeInForce.DAY
            )
            submitted = trade_client_local.submit_order(order)
            order_id = getattr(submitted, "id", None)
            logging.info("%s - SELL submitted qty=%d (available=%d intended=%s) order_id=%s", symbol, qty_to_sell, available, intended_qty, order_id)
            if order_id:
                try:
                    max_retries = 5
                    status = None
                    for attempt in range(max_retries):
                        confirmed = trade_client_local.get_order_by_id(order_id)
                        status = getattr(confirmed, "status", None)
                        logging.info("%s - SELL order %s status=%s (attempt %d/%d)", symbol, order_id, status, attempt+1, max_retries)
                        if status == "filled":
                            entry_times.pop(symbol, None)
                            entry_prices.pop(symbol, None)
                            entry_qty.pop(symbol, None)
                            last_exit_time[symbol] = datetime.now(timezone.utc)
                            logging.info("%s - EXIT state cleanup completed", symbol)
                            break
                        time.sleep(1.0)
                    else:
                        logging.warning("%s - SELL order %s not filled after %d retries (last status=%s)", symbol, order_id, max_retries, status)
                except Exception as e:
                    logging.warning("%s - Could not verify SELL order %s: %s", symbol, order_id, e)
            return submitted
        except Exception as e:
            logging.exception("safe_market_sell error for %s: %s", symbol, e)
            return None

# === MAIN ===
def main():
    # [unchanged setup code here...]

    # === SELL LOGIC ===
    if qty_open > 0:
        entry_time = entry_times.get(sym)
        if not entry_time:
            logging.warning(f"{sym} - Missing entry_time, skipping time-based exit check")
            continue

        entry_price = entry_prices.get(sym, avg_entry or price)
        elapsed = (datetime.now(timezone.utc) - entry_time).total_seconds()

        if elapsed >= MAX_HOLD_SECONDS:
            logging.info(f"{sym} - Time-based SELL triggered (held {elapsed:.1f}s ≥ {MAX_HOLD_SECONDS}s)")
            if not check_kill_switch():
                safe_market_sell(trade_client, sym, qty_open, order_lock)
            else:
                logging.warning(f"{sym} - Kill switch active, sell aborted")
            continue

        if (
            price >= entry_price * (1 + TP_PCT)
            or price <= entry_price * (1 - SL_PCT)
        ):
            reason = "TP" if price >= entry_price * (1 + TP_PCT) else "SL"
            logging.info(f"{sym} - Price-based SELL triggered ({reason}) price={price:.2f} entry={entry_price:.2f}")
            if not check_kill_switch():
                safe_market_sell(trade_client, sym, qty_open, order_lock)
            else:
                logging.warning(f"{sym} - Kill switch active, sell aborted")
      if __name__ == "__main__":
          main()
                      
