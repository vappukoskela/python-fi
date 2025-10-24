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
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s",
                    filename="scalper_safe.log")
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

# === MAIN ===
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
    last_exit_time = {}
    last_trade_attempt = defaultdict(lambda: datetime.min.replace(tzinfo=timezone.utc))
    order_lock = threading.Lock()
    stop_event = threading.Event()
    pending_entries = set()   # prevent duplicate buys

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

                    price_deques[sym].append(price)
                    size_deques[sym].append(size)
                    time_deques[sym].append(datetime.now(timezone.utc))

                    prices = pd.Series(price_deques[sym])
                    sizes = pd.Series(size_deques[sym])
                    ema_fast = compute_ema_from_series(prices, EMA_FAST).iloc[-1]
                    ema_slow = compute_ema_from_series(prices, EMA_SLOW).iloc[-1]
                    rsi_val = compute_rsi_from_series(prices, RSI_PERIOD).iloc[-1]
                    vwap_val = compute_vwap_from_ticks(prices, sizes).iloc[-1]

                    if pd.isna(ema_fast) or pd.isna(ema_slow) or pd.isna(rsi_val) or pd.isna(vwap_val):
                        continue

                    ema_trend_up = ema_fast > ema_slow
                    price_above_vwap = price > vwap_val
                    vol_ok = size > (sizes.mean() * VOL_SPIKE_MULT) if not pd.isna(sizes.mean()) else False

                    qty_open, avg_entry = positions_map.get(sym, (0, 0.0))
                    last_exit = last_exit_time.get(sym, datetime.min.replace(tzinfo=timezone.utc))

                    # clear pending once position is visible
                    if qty_open > 0 and sym in pending_entries:
                        pending_entries.discard(sym)

                    # === BUY LOGIC ===
                    if (
                        qty_open == 0 and
                        sym not in pending_entries and
                        ema_trend_up and
                        price_above_vwap and
                        vol_ok and
                        MIN_RSI_FOR_ENTRY <= rsi_val <= MAX_RSI_FOR_ENTRY and
                        (datetime.now(timezone.utc) - last_exit).total_seconds() >= COOLDOWN_SECONDS and
                        inflight_orders.get(sym) is None
                    ):
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
                                entry_qty[sym] = int((max_loop_budget * BUY_CASH_BUFFER) // price)
                                entry_prices[sym] = price
                                entry_times[sym] = datetime.now(timezone.utc)
                                logging.info(f"{sym} - ENTRY recorded qty={entry_qty[sym]} price={price:.2f} rsi={rsi_val:.2f}")
                        except Exception as e:
                            logging.exception("%s - BUY error: %s", sym, str(e))
                        finally:
                            inflight_orders.pop(sym, None)

                    # === SELL LOGIC ===
                   # === SELL LOGIC (scalping exits) ===
                            logging.debug(f"[TRACE] Pre-sell check: {sym} qty_open={qty_open}, entry_time={entry_times.get(sym)}")

                            if qty_open > 0:
                                  logging.debug(f"[TRACE] Entering sell block for {sym}, qty_open={qty_open}")
                            try:
                                last_price = float(price)
                                  logging.debug(f"[TRACE] Sell logic entered for {sym} at {datetime.now(timezone.utc)}")
                                  logging.debug(f"[TRACE] Current price={last_price:.4f}, avg_entry={avg_entry:.4f}, qty_open={qty_open}")

                            # Hard exits
                            tp_hit = last_price >= avg_entry * (1 + TP_PCT)
                            sl_hit = last_price <= avg_entry * (1 - SL_PCT)
                            logging.debug(f"[TRACE] TP hit: {tp_hit}, SL hit: {sl_hit}")
                            

                            # Build series from deques
                            prices_series = pd.Series(price_deques[sym])
                            sizes_series = pd.Series(size_deques[sym])
                    
                            # Compute indicators safely
                            vwap_series = compute_vwap_from_ticks(prices_series, sizes_series)
                            ema_fast_series = compute_ema_from_series(prices_series, EMA_FAST)
                            ema_slow_series = compute_ema_from_series(prices_series, EMA_SLOW)
                            rsi_series = compute_rsi_from_series(prices_series, RSI_PERIOD)
                    
                            # VWAP fail: last 3 bars below VWAP
                            vwap_fail = False
                            try:
                                if len(vwap_series) >= 3 and not pd.isna(vwap_series.iloc[-1]):
                                    vwap_fail = all(prices_series.iloc[-i] < vwap_series.iloc[-i] for i in range(1, 4))
                            except Exception:
                                vwap_fail = False
                    
                            # EMA trend fail: last 3 bars EMA_fast < EMA_slow
                            ema_fail = False
                            try:
                                if len(ema_fast_series) >= 3 and len(ema_slow_series) >= 3:
                                    ema_fail = all(ema_fast_series.iloc[-i] < ema_slow_series.iloc[-i] for i in range(1, 4))
                            except Exception:
                                ema_fail = False
                    
                            # RSI cooling below entry threshold
                            rsi_cool = False
                            try:
                                if len(rsi_series) >= 1 and not pd.isna(rsi_series.iloc[-1]):
                                    rsi_cool = rsi_series.iloc[-1] < MIN_RSI_FOR_ENTRY
                            except Exception:
                                rsi_cool = False
                            logging.debug(f"[TRACE] VWAP fail: {vwap_fail}, EMA fail: {ema_fail}, RSI cool: {rsi_cool}")

                            # Trailing stop ~0.3% from peak since entry
                            trailing_stop_hit = False
                            try:
                                if sym in entry_times and len(time_deques[sym]) == len(price_deques[sym]) and len(price_deques[sym]) >= 2:
                                    times_series = pd.Series(time_deques[sym])
                                    mask = times_series >= entry_times[sym]
                                    if mask.any():
                                        since_entry_prices = prices_series[mask]
                                        peak = float(since_entry_prices.max())
                                        if peak > 0:
                                            drawdown_pct = (peak - last_price) / peak
                                            trailing_stop_hit = drawdown_pct >= 0.003
                            except Exception:
                                trailing_stop_hit = False
                    
                            # Max hold time
                            time_exceeded = False
                            if sym in entry_times:
                                elapsed = (datetime.now(timezone.utc) - entry_times[sym]).total_seconds()
                                logging.debug("%s - Hold time check: elapsed=%.1f / max=%d", sym, elapsed, MAX_HOLD_SECONDS)
                                if elapsed >= MAX_HOLD_SECONDS:
                                   time_exceeded = elapsed >= MAX_HOLD_SECONDS
                            logging.debug(f"[TRACE] Trailing stop hit: {trailing_stop_hit}, Time exceeded: {time_exceeded}")

                            # Final decision
                            should_sell = (tp_hit or sl_hit or vwap_fail or ema_fail or rsi_cool or trailing_stop_hit or time_exceeded)
                            logging.debug(f"[TRACE] Final sell decision: should_sell={should_sell}, qty_open={qty_open}, entry_time={entry_time}")

                            if should_sell:
                                reason_parts = []
                                if tp_hit: reason_parts.append("TP")
                                if sl_hit: reason_parts.append("SL")
                                if vwap_fail: reason_parts.append("VWAP fail")
                                if ema_fail: reason_parts.append("EMA fail")
                                if rsi_cool: reason_parts.append("RSI cool")
                                if trailing_stop_hit: reason_parts.append("Trailing stop")
                                if time_exceeded: reason_parts.append("Max hold")
                                reason = ", ".join(reason_parts) if reason_parts else "Exit"
                    
                                submitted = safe_market_sell(trade_client, sym, qty_open, order_lock)
                                logging.info(
                                    "%s - SCALP SELL trigger qty=%d @ %.4f (%s) | Entry=%.4f",
                                    sym, qty_open, last_price, reason, avg_entry
                                )
                    
                        except Exception as e:
                            logging.exception("%s - SCALP SELL error: %s", sym, str(e))
        except Exception as e:
                logging.exception("Main loop error: %s", e)
                time.sleep(1.0)
if __name__ == "__main__":
    main()
