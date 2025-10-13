import logging
import os
import time
from datetime import datetime, timezone
from collections import deque, defaultdict

import pandas as pd
from dotenv import load_dotenv
from alpaca.data.historical.stock import StockHistoricalDataClient, StockLatestTradeRequest
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, OrderType, TimeInForce
from alpaca.trading.requests import MarketOrderRequest

import threading


# --- settings ---
SCALP = True
SCALP_SLEEP_SECONDS = 1  # poll every second
TICKS_WINDOW = 300  # number of ticks to keep per symbol (approx 5 minutes at 1s ticks)
EMA_FAST = 9
EMA_SLOW = 20
RSI_PERIOD = 7
VOL_SPIKE_MULT = 1.05
TP_PCT = 0.006
SL_PCT = 0.003
MAX_HOLD_SECONDS = 10 * 60  # fallback: max hold 10 minutes expressed in seconds
BUY_POWER_LIMIT = 0.05  # fraction of buying power

# --- helper functions ---
def compute_ema_from_series(series, period):
    # series is pandas Series of prices
    if len(series) < 2:
        return series.iloc[-1] if len(series) else float("nan")
    return series.ewm(span=period, adjust=False).mean()

def compute_rsi_from_series(series, period=14):
    if len(series) < period + 1:
        return pd.Series([float("nan")]*len(series))
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def compute_vwap_from_ticks(prices, sizes):
    # prices and sizes are pandas Series of equal length
    if sizes.sum() == 0:
        return pd.Series([float("nan")] * len(prices))
    cumulative_pv = (prices * sizes).cumsum()
    cumulative_vol = sizes.cumsum()
    vwap = cumulative_pv / cumulative_vol
    return vwap

def fetch_latest_trade_price_and_size(stock_data_client, symbol):
    try:
        req = StockLatestTradeRequest(symbol_or_symbols=symbol)
        trade = stock_data_client.get_stock_latest_trade(req)
        t = trade[symbol]
        # Some Alpaca clients provide .price and .size
        price = float(t.price)
        size = int(getattr(t, "size", 1) or 1)
        return price, size
    except Exception as e:
        logging.debug("Error fetching latest trade for %s: %s", symbol, str(e))
        return None, None

def calculate_buying_power_limit(trade_client, limit_fraction):
    try:
        account = trade_client.get_account()
        return float(account.buying_power) * limit_fraction
    except Exception as e:
        logging.exception("Failed to read account buying power: %s", e)
        return 0.0

def get_positions_map(trade_client):
    """Return dict: symbol -> (qty:int, avg_entry:float)"""
    try:
        positions = trade_client.get_all_positions()
        return {pos.symbol: (int(pos.qty), float(pos.avg_entry_price)) for pos in positions}
    except Exception as e:
        logging.debug("get_all_positions failed: %s", e)
        return {}
    
def sell_all_positions(trade_client_local):
    logging.info("User requested EXIT. Selling all open positions...")
    try:
        positions = trade_client_local.get_all_positions()
        for pos in positions:
            symbol = pos.symbol
            qty = int(pos.qty)
            if qty > 0:
                try:
                    order = MarketOrderRequest(
                        symbol=symbol,
                        qty=qty,
                        side=OrderSide.SELL,
                        type=OrderType.MARKET,
                        time_in_force=TimeInForce.DAY
                    )
                    trade_client_local.submit_order(order)
                    logging.info("EXIT SELL %s - Qty: %d", symbol, qty)
                except Exception as e:
                    logging.exception("EXIT SELL error for %s: %s", symbol, str(e))
    except Exception as e:
        logging.exception("Failed to fetch positions during EXIT: %s", str(e))
    logging.info("All EXIT orders (attempted) completed.")
def input_listener(stop_event, trade_client_local):
    """Blocking input loop running in a separate thread. Type 'exit' to stop and liquidate."""
    try:
        while not stop_event.is_set():
            try:
                user = input().strip().lower()
            except EOFError:
                # No stdin available (e.g., running as service). Sleep and continue.
                time.sleep(0.5)
                continue

            if user == "exit":
                logging.info("Input listener received 'exit' command.")
                # sell positions and set stop flag
                sell_all_positions(trade_client_local)
                stop_event.set()
                break
            # optional: support 'status' or other commands here
    except Exception as e:
        logging.exception("Input listener error: %s", e)
        stop_event.set()

# --- main loop ---
def main():
    load_dotenv()
    logging.basicConfig(filename="trade_log.txt", level=logging.DEBUG,
                        format="%(asctime)s %(levelname)s %(message)s")
    global stock_data_client, trade_client
    stock_data_client = StockHistoricalDataClient(
        os.getenv("ALPACA_PAPER_API_KEY"), os.getenv("ALPACA_PAPER_SECRET_KEY")
    )
    trade_client = TradingClient(
        os.getenv("ALPACA_PAPER_API_KEY"), os.getenv("ALPACA_PAPER_SECRET_KEY"), paper=True
    )
    stop_event = threading.Event()
    input_thread = threading.Thread(target=input_listener, args=(stop_event, trade_client), daemon=True)
    input_thread.start()

    try:
        clock = trade_client.get_clock()
        market_open = clock.is_open
        print(f"Market open: {market_open}")
    except Exception as e:
        logging.exception("Failed to fetch clock: %s", e)
        market_open = True  # let it run; error handled later

    symbols = ["AAPL", "MSFT", "MU", "QCOM", "NVDA", "V", "AMD", "GOOG", "C", "EBAY", "OKTA", "TSLA", "AMZN", "ADSK", "DELL"]

    # per-symbol rolling windows
    price_deques = {sym: deque(maxlen=TICKS_WINDOW) for sym in symbols}
    size_deques = {sym: deque(maxlen=TICKS_WINDOW) for sym in symbols}
    timestamp_deques = {sym: deque(maxlen=TICKS_WINDOW) for sym in symbols}

    entry_times = {}  # symbol -> timestamp when entry occurred (UTC)
    entry_prices = {}  # symbol -> avg_entry price

    last_positions_fetch = 0
    positions_fetch_interval = 1  # seconds; we fetch positions each loop (cheap-ish)
    last_account_fetch = 0
    account_fetch_interval = 10  # seconds to refresh buying power

    buying_power_limit_cached = None
    limit_fraction = BUY_POWER_LIMIT

    logging.info("Starting tick-scalper main loop (1s ticks).")

    while True:
        if stop_event.is_set():
            logging.info("Stop event set — exiting main loop.")
            break
        now = datetime.now(timezone.utc)

        # Optional: check market clock
        try:
            clock = trade_client.get_clock()
            if not clock.is_open:
                logging.info("Market closed. Exiting.")
                return
        except Exception as e:
            logging.debug("Clock check failed: %s", e)

        # refresh account buying power occasionally
        if buying_power_limit_cached is None or (now.timestamp() - last_account_fetch) > account_fetch_interval:
            buying_power_limit_cached = calculate_buying_power_limit(trade_client, limit_fraction)
            last_account_fetch = now.timestamp()

        # fetch positions once per loop (prevents per-symbol 404s)
        if (now.timestamp() - last_positions_fetch) >= positions_fetch_interval:
            positions_map = get_positions_map(trade_client)
            last_positions_fetch = now.timestamp()
        else:
            positions_map = {}

        # iterate symbols
        for sym in symbols:
            # fetch latest tick
            price, size = fetch_latest_trade_price_and_size(stock_data_client, sym)
            if price is None:
                continue

            # push tick into rolling storage
            price_deques[sym].append(price)
            size_deques[sym].append(size)
            timestamp_deques[sym].append(now)

            # need enough points for indicators
            if len(price_deques[sym]) < max(EMA_SLOW, RSI_PERIOD, 3):
                continue

            # build pandas series for calculations
            prices = pd.Series(list(price_deques[sym]))
            sizes = pd.Series(list(size_deques[sym]))
            vwap_series = compute_vwap_from_ticks(prices, sizes)

            # EMAs computed on tick-price series
            ema_fast_series = compute_ema_from_series(prices, EMA_FAST)
            ema_slow_series = compute_ema_from_series(prices, EMA_SLOW)

            # RSI computed on tick-price series
            rsi_series = compute_rsi_from_series(prices, RSI_PERIOD)

            # volume check: compare latest tick size to rolling mean of sizes
            avg_size = sizes.rolling(window=min(len(sizes), 20)).mean().iloc[-1]
            vol_ok = False
            if pd.notna(avg_size) and avg_size > 0:
                vol_ok = size > (avg_size * VOL_SPIKE_MULT)

            # signal conditions
            ema_cross_up = (ema_fast_series.iloc[-2] <= ema_slow_series.iloc[-2]) and (ema_fast_series.iloc[-1] > ema_slow_series.iloc[-1])
            ema_trend_up = ema_fast_series.iloc[-1] > ema_slow_series.iloc[-1]
            price_above_vwap = prices.iloc[-1] > vwap_series.iloc[-1] if pd.notna(vwap_series.iloc[-1]) else True
            rsi_val = float(rsi_series.iloc[-1]) if pd.notna(rsi_series.iloc[-1]) else None

            scalp_buy = (ema_cross_up or ema_trend_up) and price_above_vwap and vol_ok and (rsi_val is not None and 45 < rsi_val < 65)

            # current position for symbol from positions map
            qty_open, avg_entry = positions_map.get(sym, (0, 0.0))
            if qty_open == 0 and scalp_buy:
                try:
                    # compute qty using cached buying power limit
                    limit_cash = buying_power_limit_cached or calculate_buying_power_limit(trade_client, limit_fraction)
                    mkt_price = price
                    qty = int(limit_cash // mkt_price)
                    if qty > 0:
                        order = MarketOrderRequest(
                            symbol=sym,
                            qty=qty,
                            side=OrderSide.BUY,
                            type=OrderType.MARKET,
                            time_in_force=TimeInForce.DAY
                        )
                        trade_client.submit_order(order)
                        entry_times[sym] = now
                        entry_prices[sym] = mkt_price
                        logging.info("%s - SCALP BUY %d @ %.4f (rsi=%.2f avg_size=%.1f)", sym, qty, mkt_price, rsi_val or -1, avg_size or 0)
                except Exception as e:
                    logging.exception("%s - SCALP BUY error: %s", sym, str(e))

            # exit logic if we have an open position (either we just bought or existing)
            if qty_open > 0:
                try:
                    last_price = price
                    tp_hit = last_price >= avg_entry * (1 + TP_PCT)
                    sl_hit = last_price <= avg_entry * (1 - SL_PCT)

                    # vwap fail: last 3 ticks under vwap
                    vwap_fail = False
                    try:
                        if len(prices) >= 3 and pd.notna(vwap_series.iloc[-1]):
                            vwap_fail = all(prices.iloc[-i] < vwap_series.iloc[-i] for i in range(1, min(4, len(prices)+1)))
                    except Exception:
                        vwap_fail = False

                    # ema fail: last 3 ticks ema_fast < ema_slow
                    ema_fail = False
                    try:
                        if len(ema_fast_series) >= 3:
                            ema_fail = all(ema_fast_series.iloc[-i] < ema_slow_series.iloc[-i] for i in range(1, min(4, len(ema_fast_series)+1)))
                    except Exception:
                        ema_fail = False

                    time_exceeded = False
                    if sym in entry_times:
                        elapsed = (now - entry_times[sym]).total_seconds()
                        if elapsed >= MAX_HOLD_SECONDS:
                            time_exceeded = True

                    if tp_hit or sl_hit or vwap_fail or ema_fail or time_exceeded:
                        order = MarketOrderRequest(
                            symbol=sym,
                            qty=qty_open,
                            side=OrderSide.SELL,
                            type=OrderType.MARKET,
                            time_in_force=TimeInForce.DAY
                        )
                        trade_client.submit_order(order)
                        logging.info("%s - SCALP SELL %d @ %.4f (tp=%s sl=%s vwap_fail=%s ema_fail=%s time_exceeded=%s)",
                                     sym, qty_open, last_price, tp_hit, sl_hit, vwap_fail, ema_fail, time_exceeded)
                        entry_times.pop(sym, None)
                        entry_prices.pop(sym, None)
                except Exception as e:
                    logging.exception("%s - SCALP SELL error: %s", sym, str(e))

        # end for symbols

        # sleep til next second boundary to keep things rhythmic
        try:
            time_to_sleep = SCALP_SLEEP_SECONDS - (datetime.now(timezone.utc).microsecond / 1_000_000.0)
            if time_to_sleep > 0:
                time.sleep(time_to_sleep)
        except Exception:
            time.sleep(SCALP_SLEEP_SECONDS)
    try:
        if input_thread.is_alive():
            logging.debug("Waiting for input thread to finish...")
            input_thread.join(timeout=1.0)
    except Exception:
        pass

    logging.info("Main exiting.")
    return

if __name__ == "__main__":
    main()
