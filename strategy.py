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
RSI_PERIOD = 14
VOL_SPIKE_MULT = 1.4
TP_PCT = 0.006
SL_PCT = 0.004
# --- RUN MODE ---
# "SIM" = backtest on historical bars; "LIVE" = live/paper trading loop
RUN_MODE = "SIM"
TRAILING_STOP_PCT = 0.005  # 0.5% trailing stop
MAX_HOLD_SECONDS = 300   # example: 5 minutes
MIN_HOLD_SECONDS = 30    # example: 30 seconds grace period before indicators can trigger
TRAILING_STOP_PCT = 0.005   # 0.5% trailing stop
MAX_HOLD_SECONDS = 30 * 60
BUY_POWER_LIMIT = 0.05
BUY_CASH_BUFFER = 0.95
COOLDOWN_SECONDS = 30
MIN_RSI_FOR_ENTRY = 50
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

logging.debug("[TRACE] Logging system initialized")


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

def buy_conditions_met(sym, price, size, ema_fast, ema_slow, rsi_val, vwap_val,
                       sizes_series, last_exit, positions_map, inflight_orders, pending_entries):
    """
    Entry filter used by both SIM and LIVE loops.
    Mirrors your BUY block:
      - Cooldown
      - Trend (EMA fast > EMA slow)
      - Price above VWAP
      - RSI in allowed range
      - Volume spike vs recent mean
      - No open position / no pending order
    """
    try:
        # Cooldown
        if last_exit:
            since_last = (datetime.now(timezone.utc) - last_exit).total_seconds()
            if since_last < COOLDOWN_SECONDS:
                return False

        # Position/order checks
        no_position = positions_map.get(sym, (0, 0.0))[0] == 0
        inflight_none = inflight_orders.get(sym) is None
        not_pending = sym not in pending_entries
        if not (no_position and inflight_none and not_pending):
            return False

        # Trend filter
        if pd.isna(ema_fast) or pd.isna(ema_slow) or ema_fast <= ema_slow:
            return False

        # VWAP filter
        if pd.isna(vwap_val) or price <= vwap_val:
            return False

        # RSI filter
        if pd.isna(rsi_val) or not (MIN_RSI_FOR_ENTRY <= rsi_val <= MAX_RSI_FOR_ENTRY):
            return False

        # Volume spike filter (guard against NaN)
        mean_vol = sizes_series.mean() if len(sizes_series) > 0 else float('nan')
        vol_ok = (not pd.isna(mean_vol)) and (size > (mean_vol * VOL_SPIKE_MULT))
        if not vol_ok:
            return False

        return True

    except Exception as e:
        logging.error("[ERROR][%s] Buy evaluation failed: %s", sym, e)
        return False


def evaluate_sell(sym, last_price, ref_entry, price_deque, size_deque, entry_times,
                  ema_fast_period=EMA_FAST, ema_slow_period=EMA_SLOW, rsi_period=RSI_PERIOD):
    """
    Exit evaluation used by both SIM and LIVE loops.
    Returns (True, reason) or (False, None).
    """
    try:
        # --- Unpack entry_times ---
        entry_record = entry_times.get(sym)

        if entry_record:
            if isinstance(entry_record, tuple):
                entry_time, entry_price_at_entry = entry_record
            else:
                entry_time = entry_record
                entry_price_at_entry = None

            # Convert string to datetime if needed
            if isinstance(entry_time, str):
                from dateutil import parser
                entry_time = parser.parse(entry_time)

            if not isinstance(entry_time, datetime):
                logging.error("[%s] entry_time is not datetime after unpack: %s", sym, type(entry_time))
                return False, None

            elapsed = (datetime.now(timezone.utc) - entry_time).total_seconds()
        else:
            entry_time = None
            entry_price_at_entry = None
            elapsed = 0
        # --- End unpack ---

        # --- Hard exits ---
        tp_hit = last_price >= ref_entry * (1 + TP_PCT)
        sl_hit = last_price <= ref_entry * (1 - SL_PCT)

        prices_series = pd.Series(price_deque)
        sizes_series = pd.Series(size_deque)

        # Trailing stop
        trailing_stop_hit = False
        if len(prices_series) > 0:
            peak = prices_series.max()
            if peak > ref_entry * (1 + TP_PCT):
                trailing_stop_hit = last_price <= peak * (1 - TRAIL_PCT)

        # VWAP fail
        vwap_val = compute_vwap_from_ticks(prices_series, sizes_series).iloc[-1]
        vwap_fail = False
        if len(prices_series) >= 3 and not pd.isna(vwap_val):
            if all(prices_series.iloc[-i] < vwap_val * (1 - VWAP_DELTA) for i in range(1, 4)):
                vwap_fail = True

        # EMA fail
        ema_fast = compute_ema_from_series(prices_series, ema_fast_period).iloc[-1]
        ema_slow = compute_ema_from_series(prices_series, ema_slow_period).iloc[-1]
        ema_fail = False
        if len(prices_series) >= 2:
            ema_fast_prev = compute_ema_from_series(prices_series[:-1], ema_fast_period).iloc[-1]
            ema_slow_prev = compute_ema_from_series(prices_series[:-1], ema_slow_period).iloc[-1]
            if ema_fast < ema_slow and ema_fast_prev < ema_slow_prev and last_price < ema_slow * (1 - EMA_DELTA):
                ema_fail = True

        # RSI cooling
        rsi_val = compute_rsi_from_series(prices_series, rsi_period).iloc[-1]
        rsi_cooling = False
        if not pd.isna(rsi_val):
            rsi_peak = compute_rsi_from_series(prices_series, rsi_period).max()
            if rsi_peak - rsi_val >= RSI_COOL_THRESHOLD:
                rsi_cooling = True

        # Max hold
        max_hold_hit = elapsed >= MAX_HOLD_SEC if entry_time else False

        # --- Decision ---
        if tp_hit:
            return True, "Take-profit"
        if sl_hit:
            return True, "Stop-loss"
        if trailing_stop_hit:
            return True, "Trailing stop"
        if vwap_fail:
            return True, "VWAP fail"
        if ema_fail:
            return True, "EMA fail"
        if rsi_cooling:
            return True, "RSI cooling"
        if max_hold_hit:
            return True, "Max hold"

        return False, None

    except Exception as e:
        logging.error("[%s] Sell evaluation failed: %s", sym, str(e))
        return False, None



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

   # === SIMULATION BRANCH ===
    if RUN_MODE == "SIM":
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame
    
        # Choose symbol and day for replay
        symbol = "MSFT"
        start = datetime(2024, 10, 1, tzinfo=timezone.utc)
        end   = datetime(2024, 10, 2, tzinfo=timezone.utc)
    
        req = StockBarsRequest(
            symbol_or_symbols=[symbol],
            timeframe=TimeFrame.Minute,
            start=start,
            end=end
        )
        bars = stock_data_client.get_stock_bars(req).df
    
        logging.info("Starting SIM replay for %s from %s to %s", symbol, start, end)
    
        in_position = False
        entry_price = None
        entry_times = {}
        last_exit_time[symbol] = datetime.min.replace(tzinfo=timezone.utc)
    
        for idx, row in bars.iterrows():
            price = float(row["close"])
            size = int(row["volume"])
    
            # --- tee ts_val heti alussa ---
            ts_val = idx
            if isinstance(ts_val, tuple):
                ts_val = ts_val[0]
            try:
                ts_val = pd.to_datetime(ts_val).to_pydatetime()
            except Exception:
                logging.error("[SIM] Could not convert ts=%s to datetime", ts_val)
                continue
    
            # --- päivitä deques aina ---
            price_deques[symbol].append(price)
            size_deques[symbol].append(size)
            time_deques[symbol].append(ts_val)
    
            # --- laske indikaattorit ---
            prices = pd.Series(price_deques[symbol])
            sizes_series = pd.Series(size_deques[symbol])
            ema_fast = compute_ema_from_series(prices, EMA_FAST).iloc[-1]
            ema_slow = compute_ema_from_series(prices, EMA_SLOW).iloc[-1]
            rsi_val = compute_rsi_from_series(prices, RSI_PERIOD).iloc[-1]
            vwap_val = compute_vwap_from_ticks(prices, sizes_series).iloc[-1]
    
            if pd.isna(ema_fast) or pd.isna(ema_slow) or pd.isna(rsi_val) or pd.isna(vwap_val):
                continue
    
            # --- BUY / SELL logiikka ---
            if not in_position:
                if buy_conditions_met(symbol, price, size, ema_fast, ema_slow, rsi_val, vwap_val,
                                      sizes_series, last_exit_time[symbol], {}, {}, set()):
                    entry_price = price
                    entry_times[symbol] = (ts_val, price)
                    in_position = True
                    logging.info("%s [SIM] BUY @ %.4f | rsi=%.2f", symbol, price, rsi_val)
            else:
                sell, reason = evaluate_sell(symbol, price, entry_price,
                                             price_deques[symbol], size_deques[symbol], entry_times)
                if sell:
                    pnl = price - entry_price
                    logging.info("%s [SIM] SELL @ %.4f | Reason=%s | PnL=%.4f",
                                 symbol, price, reason, pnl)
                    in_position = False
                    entry_price = None
                    last_exit_time[symbol] = ts_val
     
        logging.info("SIM replay finished for %s", symbol)
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
                    
                    qty_open, avg_entry = positions_map.get(sym, (0, 0.0))
                    last_exit = last_exit_time.get(sym, datetime.min.replace(tzinfo=timezone.utc))
                    
                    # clear pending once position is visible
                    if qty_open > 0 and sym in pending_entries:
                        pending_entries.discard(sym)

                    # --- BUY / SELL using helpers ---
                    if qty_open == 0:  # not in position
                        if buy_conditions_met(sym, price, size, ema_fast, ema_slow, rsi_val, vwap_val,
                                              sizes, last_exit, positions_map, inflight_orders, pending_entries):
                            qty = calculate_buying_power_limit(trade_client, price)
                            if qty > 0:
                                submitted = safe_market_buy(trade_client, sym, qty, order_lock)
                                if submitted:
                                    entry_times[sym] = (datetime.now(timezone.utc), price)
                                    entry_prices[sym] = price
                                    entry_qty[sym] = qty
                                    inflight_orders[sym] = submitted
                                    pending_entries.add(sym)
                                    logging.info("%s LIVE BUY @ %.4f | qty=%d | rsi=%.2f",
                                                 sym, price, qty, rsi_val)
                    else:  # already in position
                        ref_entry = entry_prices.get(sym)
                        if ref_entry:
                            sell, reason = evaluate_sell(sym, price, ref_entry,
                                                         price_deques[sym], size_deques[sym], entry_times)
                            if sell:
                                qty = entry_qty.get(sym, 0)
                                submitted = safe_market_sell(trade_client, sym, qty, order_lock)
                                if submitted:
                                    pnl = (price - ref_entry) * qty
                                    logging.info("%s LIVE SELL @ %.4f | Reason=%s | PnL=%.4f | qty=%d",
                                                 sym, price, reason, pnl, qty)
                                    inflight_orders[sym] = submitted
                                    last_exit_time[sym] = datetime.now(timezone.utc)


                    # === SELL LOGIC (scalping exits) ===
                    if qty_open > 0:
                        try:
                            last_price = float(price)
                            ref_entry = entry_prices.get(sym, avg_entry)
                    
                            # --- Unpack entry_times ---
                            entry_record = entry_times.get(sym)
                            if isinstance(entry_record, tuple):
                                entry_time, entry_price_at_entry = entry_record
                            else:
                                entry_time = entry_record
                                entry_price_at_entry = None
                    
                            # --- Hard exits ---
                            tp_hit = last_price >= ref_entry * (1 + TP_PCT)
                            sl_hit = last_price <= ref_entry * (1 - SL_PCT)
                    
                            # --- Indicators ---
                            prices_series = pd.Series(price_deques[sym])
                            sizes_series = pd.Series(size_deques[sym])
                            vwap_series = compute_vwap_from_ticks(prices_series, sizes_series)
                            ema_fast_series = compute_ema_from_series(prices_series, EMA_FAST)
                            ema_slow_series = compute_ema_from_series(prices_series, EMA_SLOW)
                            rsi_series = compute_rsi_from_series(prices_series, RSI_PERIOD)
                    
                            # VWAP fail: last 3 bars below VWAP
                            VWAP_DELTA = 0.02
                            vwap_fail = False
                            try:
                                elapsed = (datetime.now(timezone.utc) - entry_time).total_seconds() if entry_time else 0
                                if elapsed >= MIN_HOLD_SECONDS and len(vwap_series) >= 3 and not pd.isna(vwap_series.iloc[-1]):
                                    bars_below = [prices_series.iloc[-i] < (vwap_series.iloc[-i] - VWAP_DELTA) for i in range(1, 4)]
                                    vwap_fail = all(bars_below)
                                    logging.debug(
                                        "[TRACE][%s] VWAP | last=%.4f | vwap=%.4f | bars_below=%s | Fail=%s",
                                        sym, prices_series.iloc[-1], vwap_series.iloc[-1], bars_below, vwap_fail
                                    )
                            except Exception as e:
                                logging.error("[ERROR][%s] VWAP evaluation failed: %s", sym, e)
                                vwap_fail = False
                    
                            # EMA fail
                            EMA_DELTA = 0.02
                            ema_fail = False
                            try:
                                if elapsed >= MIN_HOLD_SECONDS and len(ema_fast_series) >= 3 and len(ema_slow_series) >= 3:
                                    ema_fail = (
                                        ema_fast_series.iloc[-1] < (ema_slow_series.iloc[-1] - EMA_DELTA) and
                                        ema_fast_series.iloc[-2] < (ema_slow_series.iloc[-2] - EMA_DELTA) and
                                        last_price < (ema_slow_series.iloc[-1] - EMA_DELTA)
                                    )
                                    logging.debug(
                                        "[TRACE][%s] EMA | ema_fast_now=%.4f | ema_slow_now=%.4f | ema_fast_prev=%.4f | ema_slow_prev=%.4f | last=%.4f | Fail=%s",
                                        sym,
                                        ema_fast_series.iloc[-1], ema_slow_series.iloc[-1],
                                        ema_fast_series.iloc[-2], ema_slow_series.iloc[-2],
                                        last_price, ema_fail
                                    )
                            except Exception as e:
                                logging.error("[ERROR][%s] EMA evaluation failed: %s", sym, e)
                                ema_fail = False
                    
                            # RSI cooling
                            rsi_cool = False
                            try:
                                if entry_time and len(rsi_series) >= 3:
                                    elapsed = (datetime.now(timezone.utc) - entry_time).total_seconds()
                                    if elapsed >= MIN_HOLD_SECONDS:
                                        entry_time_norm = entry_time.replace(microsecond=0)
                                        times_series = pd.Series(time_deques[sym]).dt.tz_convert('UTC').dt.floor('s')
                                        entry_index = times_series[times_series >= entry_time_norm].index.min()
                                        if entry_index is not None and entry_index < len(rsi_series):
                                            rsi_entry = rsi_series.iloc[entry_index]
                                            rsi_now = rsi_series.iloc[-1]
                                            rsi_prev = rsi_series.iloc[-2]
                                            RSI_DROP = 5
                                            rsi_cool = (
                                                rsi_now < MIN_RSI_FOR_ENTRY and
                                                rsi_prev < MIN_RSI_FOR_ENTRY and
                                                rsi_entry > rsi_now and
                                                (rsi_entry - rsi_now) >= RSI_DROP
                                            )
                                            logging.debug(
                                                "[TRACE][%s] RSI | entry=%.2f | prev=%.2f | now=%.2f | drop=%.2f | threshold=%d | Cool=%s",
                                                sym, rsi_entry, rsi_prev, rsi_now,
                                                (rsi_entry - rsi_now), RSI_DROP, rsi_cool
                                            )
                            except Exception as e:
                                logging.error("[ERROR][%s] RSI evaluation failed: %s", sym, e)
                                rsi_cool = False
                    
                            # Trailing stop
                            trailing_stop_hit = False
                            try:
                                if entry_time and len(time_deques[sym]) == len(price_deques[sym]) and len(price_deques[sym]) >= 2:
                                    entry_time_norm = entry_time.replace(microsecond=0)
                                    times_series = pd.Series(time_deques[sym]).dt.tz_convert('UTC').dt.floor('s')
                                    prices_series = pd.Series(price_deques[sym])
                                    mask = times_series >= entry_time_norm
                                    if mask.any():
                                        since_entry_prices = prices_series[mask]
                                        peak = float(since_entry_prices.max())
                                        if peak > 0:
                                            drawdown_pct = (peak - last_price) / peak
                                            trailing_stop_hit = drawdown_pct >= TRAILING_STOP_PCT
                                            logging.debug(
                                                "[TRACE][%s] TS | Entry=%.4f | Last=%.4f | Peak=%.4f | Drawdown=%.4f%% | Th=%.4f%% | Hit=%s",
                                                sym, ref_entry, last_price, peak,
                                                drawdown_pct * 100, TRAILING_STOP_PCT * 100, trailing_stop_hit
                                            )
                            except Exception as e:
                                logging.error("[ERROR][%s] TS evaluation failed: %s", sym, e)
                                trailing_stop_hit = False
                    
                            # Max hold
                            time_exceeded = False
                            if entry_time:
                                elapsed = (datetime.now(timezone.utc) - entry_time).total_seconds()
                                if elapsed >= MAX_HOLD_SECONDS:
                                    time_exceeded = True
                    
                            # Exit reason priority
                            exit_reason = None
                            if trailing_stop_hit:
                                exit_reason = "Trailing stop"
                            elif sl_hit:
                                exit_reason = "Stop-loss"
                            elif tp_hit:
                                exit_reason = "Take-profit"
                            elif vwap_fail:
                                exit_reason = "VWAP fail"
                            elif ema_fail:
                                exit_reason = "EMA fail"
                            elif rsi_cool:
                                exit_reason = "RSI cooling"
                            elif time_exceeded:
                                exit_reason = "Max hold"
                            # Execute sell
                            if exit_reason:
                                # DEBUG: tarkista mitä entry_times sisältää juuri ennen myyntiä
                                if sym in entry_times:
                                    logging.debug("[DEBUG][LIVE] entry_times[%s] raw value: %s", sym, entry_times[sym])
                                    logging.debug("[DEBUG][LIVE] type(entry_times[%s]) = %s", sym, type(entry_times[sym]))
                                else:
                                    logging.debug("[DEBUG][LIVE] entry_times[%s] not set", sym)
                            
                                submitted = safe_market_sell(trade_client, sym, qty_open, order_lock)
                                logging.info(
                                    "%s - SCALP SELL qty=%d @ %.4f | Reason=%s | EntryRef=%.4f | EntryTuplePrice=%.4f",
                                    sym, qty_open, last_price, exit_reason, ref_entry,
                                    entry_price_at_entry if entry_price_at_entry is not None else float('nan')
                                )
                                in_position = False
                                entry_price = None
                                last_exit_time[sym] = datetime.now(timezone.utc)
                            
                        except Exception as e:
                            logging.error("[ERROR][%s] Sell logic failed: %s", sym, e)


                    
        except Exception as e:
                logging.exception("Main loop error: %s", e)
                time.sleep(1.0)
if __name__ == "__main__":
    main()
