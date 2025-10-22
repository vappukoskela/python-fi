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
def get_positions_map(trade_client_local):
    """Return dict: symbol -> (qty, avg_entry_price)."""
    try:
        positions = trade_client_local.get_open_positions()
        return {
            pos.symbol: (
                int(float(pos.qty)),
                float(pos.avg_entry_price) if pos.avg_entry_price else 0.0
            )
            for pos in positions
        }
    except Exception as e:
        logging.debug("get_open_positions failed: %s", e)
        return {}

def calculate_buying_power_limit(trade_client_local, fraction):
    """
    Return the maximum dollar amount allowed for trading this loop,
    based on a fraction of Alpaca's current buying power.
    """
    try:
        account = trade_client_local.get_account()
        buying_power = float(account.buying_power)
        return buying_power * fraction
    except Exception as e:
        logging.exception("Error fetching buying power: %s", e)
        return 0.0

def fetch_latest_trade_price_and_size_batch(data_client, symbols):
    """
    Fetch the latest trade price and size for a batch of symbols.
    Returns a dict: {symbol: (price, size)}.
    """
    results = {}
    try:
        req = StockLatestTradeRequest(symbol_or_symbols=symbols)
        trades = data_client.get_stock_latest_trade(req)
        for sym in symbols:
            trade = trades.get(sym)
            if trade and trade.price and trade.size:
                results[sym] = (float(trade.price), int(trade.size))
            else:
                results[sym] = (None, None)
    except Exception as e:
        logging.exception("Error fetching latest trades: %s", e)
        for sym in symbols:
            results[sym] = (None, None)
    return results

# (Other helpers like fetch_latest_trade_price_and_size_batch, calculate_buying_power_limit,
#  get_position_qty, safe_market_buy, safe_market_sell, check_kill_switch remain unchanged)

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
    pending_entries = set()

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
                            symbol=s,
                            qty=q,
                            side=OrderSide.SELL,
                            type=OrderType.MARKET,
                            time_in_force=TimeInForce.DAY
                        )
                        trade_client_local.submit_order(order)
                        logging.info("%s - Forced SELL qty=%d", s, q)
                    except Exception as e:
                        logging.exception("Forced sell error for %s: %s", s, e)
        except Exception as e:
            logging.exception("sell_all_positions error: %s", e)

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

                    if qty_open > 0 and sym in pending_entries:
                        pending_entries.discard(sym)

                    # === BUY LOGIC ===
                    if (
                        qty_open == 0
                        and sym not in pending_entries
                        and ema_trend_up
                        and price_above_vwap
                        and vol_ok
                        and MIN_RSI_FOR_ENTRY <= rsi_val <= MAX_RSI_FOR_ENTRY
                        and (datetime.now(timezone.utc) - last_exit).total_seconds() >= COOLDOWN_SECONDS
                        and inflight_orders.get(sym) is None
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
                            spent_this_loop += est_trade_cost
                            submitted = safe_market_buy(trade_client, sym, max_loop_budget * BUY_CASH_BUFFER, order_lock)
                            if submitted:
                                inflight_orders[sym] = getattr(submitted, "id", None) or True
                                entry_qty[sym] = int((max_loop_budget * BUY_CASH_BUFFER) // price)
                                entry_prices[sym] = price
                                entry_times[sym] = datetime.now(timezone.utc)
                                logging.info(f"{sym} - ENTRY recorded qty={entry_qty[sym]} price={price:.2f} rsi={rsi_val:.2f}")
                        finally:
                            inflight_orders.pop(sym, None)
                            pending_entries.discard(sym)
                            continue

                    # === SELL LOGIC ===
                    if qty_open > 0:
                        entry_time = entry_times.get(sym)
                        if not entry_time:
                            logging.warning(f"{sym} - Missing entry_time, skipping time-based exit check")
                            continue

                        # Ensure entry_time is a datetime
                        if isinstance(entry_time, str):
                            try:
                                entry_time = datetime.fromisoformat(entry_time)
                            except Exception:
                                logging.error(f"{sym} - Invalid entry_time format: {entry_time}")
                                continue

                        entry_price = entry_prices.get(sym, avg_entry or price)
                        elapsed = (datetime.now(timezone.utc) - entry_time).total_seconds()

                        if elapsed >= MAX_HOLD_SECONDS:
                            logging.info(f"{sym} - Time-based SELL triggered (held {elapsed:.1f}s ≥ {MAX_HOLD_SECONDS}s)")
                            if not check_kill_switch():
                                safe_market_sell(trade_client, sym, qty_open, order_lock)
                                last_exit_time[sym] = datetime.now(timezone.utc)
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
                                last_exit_time[sym] = datetime.now(timezone.utc)
                            else:
                                logging.warning(f"{sym} - Kill switch active, sell aborted")

            time.sleep(LOOP_SLEEP)

        except Exception as e:
            logging.exception("Main loop error: %s", e)
            time.sleep(5.0)

if __name__ == "__main__":
    main()

                                                      
