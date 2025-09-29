import logging
import os
import time
import argparse
import json
from datetime import datetime, timedelta, timezone
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from alpaca.data.historical.stock import StockHistoricalDataClient, StockLatestTradeRequest
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, OrderType, TimeInForce
from alpaca.trading.requests import MarketOrderRequest

# --- settings ---
SCALP = True
TIMEFRAME_SCALP = TimeFrame(1, TimeFrameUnit.Minute)
EMA_FAST = 9
EMA_SLOW = 20
RSI_PERIOD = 7
VOL_SPIKE_MULT = 1.2
MAX_HOLD_BARS = 10
SCALP_SLEEP_SECONDS = 10
BUY_POWER_LIMIT = 0.05
ENTRY_FILE = "entry_times.json"
HIGHEST_FILE = "highest_price.json"

# --- persistence helpers ---
def save_state(entry_times, highest_price):
    with open(ENTRY_FILE, "w") as f:
        json.dump({k: v.isoformat() for k, v in entry_times.items()}, f)
    with open(HIGHEST_FILE, "w") as f:
        json.dump(highest_price, f)

def load_state():
    try:
        with open(ENTRY_FILE, "r") as f:
            entry_times = {k: pd.to_datetime(v) for k, v in json.load(f).items()}
    except FileNotFoundError:
        entry_times = {}
    try:
        with open(HIGHEST_FILE, "r") as f:
            highest_price = {k: float(v) for k, v in json.load(f).items()}
    except FileNotFoundError:
        highest_price = {}
    return entry_times, highest_price

# --- helper functions ---
def compute_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def compute_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def compute_vwap(df):
    return (df['close'] * df['volume']).cumsum() / df['volume'].cumsum()

def compute_atr(df, period=14):
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = ranges.max(axis=1)
    return true_range.rolling(period).mean()

def fetch_bars(client, symbol, timeframe, days=1):
    try:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=days)
        req = StockBarsRequest(symbol_or_symbols=symbol, timeframe=timeframe, start=start, end=end, feed="iex")
        bars = client.get_stock_bars(req).df
        if symbol in bars.index.levels[0]:
            return bars.loc[symbol]
        return None
    except Exception as e:
        logging.exception("fetch_bars error: %s", str(e))
        return None

def get_underlying_price(symbol):
    req = StockLatestTradeRequest(symbol_or_symbols=symbol)
    trade = stock_data_client.get_stock_latest_trade(req)
    return trade[symbol].price

def calculate_buying_power_limit(limit_fraction):
    account = trade_client.get_account()
    return float(account.buying_power) * limit_fraction

def position_value(symbol):
    try:
        pos = trade_client.get_open_position(symbol)
        return int(pos.qty), float(pos.avg_entry_price)
    except:
        return 0, 0.0

# --- main loop ---
def main():
    parser = argparse.ArgumentParser(description="Scalping strategy runner")
    parser.add_argument("--fast", action="store_true", help="Enable fast scalp mode")
    args = parser.parse_args()
    FAST_SCALP_MODE = args.fast
    print(f"FAST_SCALP_MODE = {FAST_SCALP_MODE}")

    # --- Kill switch and market open guard ---
    if not os.path.exists("run.flag"):
        print("Kill switch active. Create 'run.flag' file to enable trading.")
        return

    response = input("Trading is enabled. Do you want to proceed? (yes/no): ").strip().lower()
    if response != "yes":
        print("Execution aborted by user.")
        return

    now = datetime.now().astimezone()
    if now.hour == 9 and now.minute < 35:
        print("Market just opened. Waiting period active.")
        return

    if FAST_SCALP_MODE:
        TP_PCT = 0.003
        SL_PCT = 0.002
        TRAIL_TRIGGER = 0.003
        TRAIL_OFFSET = 0.001
    else:
        TP_PCT = 0.006
        SL_PCT = 0.003
        TRAIL_TRIGGER = None
        TRAIL_OFFSET = None

    load_dotenv()
    logging.basicConfig(filename="trade_log.txt", level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")

    global stock_data_client, trade_client
    stock_data_client = StockHistoricalDataClient(os.getenv("ALPACA_PAPER_API_KEY"), os.getenv("ALPACA_PAPER_SECRET_KEY"))
    trade_client = TradingClient(os.getenv("ALPACA_PAPER_API_KEY"), os.getenv("ALPACA_PAPER_SECRET_KEY"), paper=True)

    symbols = ["AAPL", "MSFT", "MU", "QCOM", "NVDA", "V", "AMD", "GOOG", "C", "EBAY", "OKTA", "TSLA", "AMZN", "ADSK", "DELL"]

    entry_times, highest_price = load_state()

    # --- State inspection logging ---
    print("Loaded entry_times:", entry_times)
    print("Loaded highest_price:", highest_price)

    while True:
        for sym in symbols:
            # ... your existing trading logic remains unchanged ...
            pass  # Replace with your full loop logic

        time.sleep(SCALP_SLEEP_SECONDS if SCALP else 60)

if __name__ == "__main__":
    main()
