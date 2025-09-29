import logging, os, time, argparse, json
from datetime import datetime, timedelta, timezone
import numpy as np, pandas as pd
from dotenv import load_dotenv
from alpaca.data.historical.stock import StockHistoricalDataClient, StockLatestTradeRequest
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, OrderType, TimeInForce
from alpaca.trading.requests import MarketOrderRequest

# --- Strategy settings ---
SCALP = True
TIMEFRAME_SCALP = TimeFrame(1, TimeFrameUnit.Minute)
EMA_FAST, EMA_SLOW = 9, 20
RSI_PERIOD, VOL_SPIKE_MULT = 7, 1.2
MAX_HOLD_BARS, SCALP_SLEEP_SECONDS = 10, 10
BUY_POWER_LIMIT = 0.05
ENTRY_FILE, HIGHEST_FILE = "entry_times.json", "highest_price.json"

# --- Persistence helpers ---
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

# --- Indicator calculations ---
def compute_ema(series, period): return series.ewm(span=period, adjust=False).mean()
def compute_rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0).rolling(period).mean()
    loss = -delta.where(delta < 0, 0).rolling(period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))
def compute_vwap(df): return (df['close'] * df['volume']).cumsum() / df['volume'].cumsum()
def compute_atr(df, period=14):
    ranges = pd.concat([
        df['high'] - df['low'],
        (df['high'] - df['close'].shift()).abs(),
        (df['low'] - df['close'].shift()).abs()
    ], axis=1)
    return ranges.max(axis=1).rolling(period).mean()

# --- Alpaca helpers ---
def fetch_bars(client, symbol, timeframe, days=1):
    try:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=days)
        req = StockBarsRequest(symbol_or_symbols=symbol, timeframe=timeframe, start=start, end=end, feed="iex")
        bars = client.get_stock_bars(req).df
        return bars.loc[symbol] if symbol in bars.index.levels[0] else None
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

# --- Main loop ---
def main():
    parser = argparse.ArgumentParser(description="Scalping strategy runner")
    parser.add_argument("--fast", action="store_true", help="Enable fast scalp mode")
    args = parser.parse_args()
    FAST_SCALP_MODE = args.fast
    print(f"FAST_SCALP_MODE = {FAST_SCALP_MODE}")

    # --- Kill switch and confirmation ---
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

    TP_PCT, SL_PCT = (0.003, 0.002) if FAST_SCALP_MODE else (0.006, 0.003)
    TRAIL_TRIGGER, TRAIL_OFFSET = (0.003, 0.001) if FAST_SCALP_MODE else (None, None)

    load_dotenv()
    logging.basicConfig(filename="trade_log.txt", level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")
    global stock_data_client, trade_client
    stock_data_client = StockHistoricalDataClient(os.getenv("ALPACA_PAPER_API_KEY"), os.getenv("ALPACA_PAPER_SECRET_KEY"))
    trade_client = TradingClient(os.getenv("ALPACA_PAPER_API_KEY"), os.getenv("ALPACA_PAPER_SECRET_KEY"), paper=True)

    symbols = ["AAPL", "MSFT", "MU", "QCOM", "NVDA", "V", "AMD", "GOOG", "C", "EBAY", "OKTA", "TSLA", "AMZN", "ADSK", "DELL"]
    entry_times, highest_price = load_state()
    print("Loaded entry_times:", entry_times)
    print("Loaded highest_price:", highest_price)

    while True:
        for sym in symbols:
            bars = fetch_bars(stock_data_client, sym, TIMEFRAME_SCALP)
            if bars is None or len(bars) < EMA_SLOW + 2:
                continue

            close = bars['close']
            volume = bars['volume']
            ema_fast = compute_ema(close, EMA_FAST)
            ema_slow = compute_ema(close, EMA_SLOW)
            rsi = compute_rsi(close, RSI_PERIOD)
            vwap = compute_vwap(bars)
            avg_vol = volume.rolling(20).mean()

            price = close.iloc[-1]
            vol_spike = volume.iloc[-1] > avg_vol.iloc[-1] * VOL_SPIKE_MULT
            ema_cross = ema_fast.iloc[-2] > ema_slow.iloc[-2] and ema_fast.iloc[-1] > ema_slow.iloc[-1]
            vwap_check = price > vwap.iloc[-1] * 1.001
            rsi_check = 50 < rsi.iloc[-1] < 70

            qty, avg_entry = position_value(sym)
            now = datetime.now(timezone.utc)

            if qty == 0 and ema_cross and vwap_check and rsi_check and vol_spike:
                limit = calculate_buying_power_limit(BUY_POWER_LIMIT)
                qty_to_buy = int(limit // price)
                if qty_to_buy > 0:
                    order = MarketOrderRequest(
                        symbol=sym,
                        qty=qty_to_buy,
                        side=OrderSide.BUY,
                        type=OrderType.MARKET,
                        time_in_force=TimeInForce.DAY
                    )
                    trade_client.submit_order(order)
                    entry_times[sym] = now
                    highest_price[sym] = price
                    logging.info(f"BUY {sym} {qty_to_buy} @ {price}")

            elif qty > 0:
                hold_time = (now - entry_times[sym]).total_seconds() / 60
                highest_price[sym] = max(highest_price[sym], price)
                tp_price = avg_entry * (1 + TP_PCT)
                sl_price = avg_entry * (1 - SL_PCT)
                trail_price = highest_price[sym] * (1 - TRAIL_OFFSET) if TRAIL_TRIGGER and price > avg_entry * (1 + TRAIL_TRIGGER) else None

                if price >= tp_price or price <= sl_price or (trail_price and price <= trail_price) or hold_time > MAX_HOLD_BARS:
                    order = MarketOrderRequest(
                        symbol=sym,
                        qty=qty,
                        side=OrderSide.SELL,
                        type=OrderType.MARKET,
                        time_in_force=TimeInForce.DAY
                    )
                    trade_client.submit_order(order)
                    logging.info(f"SELL {sym} {qty} @ {price}")
                    entry_times.pop(sym, None)
                    highest_price.pop(sym, None)

        # Save state after each symbol loop
        save_state(entry_times, highest_price)
        time.sleep(SCALP_SLEEP_SECONDS if SCALP else 60)

if __name__ == "__main__":
    main()
