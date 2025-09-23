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
VOL_SPIKE_MULT = 1.05
MAX_HOLD_BARS = 10   # minutes
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
        req = StockBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=timeframe,
            start=start,
            end=end,
            feed="iex"
        )
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
    logging.basicConfig(filename="trade_log.txt", level=logging.DEBUG,
                        format="%(asctime)s %(levelname)s %(message)s")

    global stock_data_client, trade_client
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

    entry_times, highest_price = load_state()

    while True:
        for sym in symbols:
            if SCALP:
                df = fetch_bars(stock_data_client, sym, TIMEFRAME_SCALP, days=1)
                if df is None or df.empty:
                    continue

                close = df['close']
                vol = df['volume']
                ema_fast = compute_ema(close, EMA_FAST)
                ema_slow = compute_ema(close, EMA_SLOW)
                vwap = compute_vwap(df)
                rsi_s = compute_rsi(close, RSI_PERIOD)
                atr = compute_atr(df)

                if pd.isna(rsi_s.iloc[-1]) or len(df) < 3:
                    continue

                ema_cross_up = (ema_fast.iloc[-2] <= ema_slow.iloc[-2]) and (ema_fast.iloc[-1] > ema_slow.iloc[-1])
                ema_trend_up = ema_fast.iloc[-1] > ema_slow.iloc[-1]

                avg20 = vol.rolling(20).mean()
                if pd.isna(avg20.iloc[-1]):
                    continue
                vol_ok = vol.iloc[-1] > avg20.iloc[-1] * VOL_SPIKE_MULT

                scalp_buy = (ema_cross_up or ema_trend_up) and (close.iloc[-1] > vwap.iloc[-1]) and vol_ok and (45 < float(rsi_s.iloc[-1]) < 65)

                qty_open, avg_entry = position_value(sym)

                # --- BUY ---
                if scalp_buy and qty_open == 0:
                    try:
                        limit = calculate_buying_power_limit(BUY_POWER_LIMIT)
                        mkt_price = float(get_underlying_price(sym))
                        qty = int(limit // mkt_price)
                        if qty > 0:
                            order = MarketOrderRequest(
                                symbol=sym,
                                qty=qty,
                                side=OrderSide.BUY,
                                type=OrderType.MARKET,
                                time_in_force=TimeInForce.DAY
                            )
                            trade_client.submit_order(order)
                            entry_times[sym] = df.index[-1]
                            highest_price[sym] = mkt_price
                            save_state(entry_times, highest_price)
                            logging.info("%s - SCALP BUY %d @ %.2f", sym, qty, mkt_price)
                    except Exception as e:
                        logging.exception("%s - SCALP BUY error: %s", sym, str(e))

                # --- SELL ---
                if qty_open > 0:
                    try:
                        last = float(close.iloc[-1])
                        highest_price[sym] = max(highest_price.get(sym, last), last)

                        tp_hit = last >= avg_entry * (1 + TP_PCT)
                        sl_hit = last <= avg_entry * (1 - SL_PCT)

                        trail_hit = False
                        if TRAIL_TRIGGER and last >= avg_entry * (1 + TRAIL_TRIGGER):
                            trail_stop = highest_price[sym] * (1 - TRAIL_OFFSET)
                            if last < trail_stop:
                                trail_hit = True

                        vwap_fail = all(close.iloc[-i] < vwap.iloc[-i] for i in range(1, 3))
                        ema_fail = all(ema_fast.iloc[-i] < ema_slow.iloc[-i] for i in range(1, 3))

                        elapsed_minutes = (df.index[-1] - entry_times.get(sym, df.index[-1])).total_seconds() / 60
                        # --- Dynamic max hold (only in normal mode) ---
                        max_hold = False
                        if not FAST_SCALP_MODE and elapsed_minutes >= MAX_HOLD_BARS:
                            atr_val = atr.iloc[-1]
                            if not pd.isna(atr_val):
                                move = abs(last - avg_entry)
                                if move < 0.5 * atr_val:
                                    max_hold = True

                        # --- Decide exit reason ---
                        reason = None
                        if tp_hit:
                            reason = "TP_FAST" if FAST_SCALP_MODE else "TP_NORMAL"
                        elif sl_hit:
                            reason = "SL_TIGHT" if FAST_SCALP_MODE else "SL_NORMAL"
                        elif trail_hit:
                            reason = "TRAILING_STOP"
                        elif ema_fail or vwap_fail:
                            reason = "EMA/VWAP fail"
                        elif max_hold:
                            reason = "DYNAMIC_MAX_HOLD"

                        # --- Execute SELL if condition met ---
                        if reason:
                            order = MarketOrderRequest(
                                symbol=sym,
                                qty=qty_open,
                                side=OrderSide.SELL,
                                type=OrderType.MARKET,
                                time_in_force=TimeInForce.DAY
                            )
                            trade_client.submit_order(order)

                            pl_per_share = last - avg_entry
                            pl_total = pl_per_share * qty_open
                            logging.info(
                                "%s - SCALP SELL %d @ %.2f (%s) | Entry=%.2f Exit=%.2f "
                                "P/L per share=%.4f Total P/L=%.2f",
                                sym, qty_open, last, reason, avg_entry, last, pl_per_share, pl_total
                            )

                            # cleanup + persist
                            if sym in entry_times:
                                del entry_times[sym]
                            if sym in highest_price:
                                del highest_price[sym]
                            save_state(entry_times, highest_price)

                    except Exception as e:
                        logging.exception("%s - SCALP SELL error: %s", sym, str(e))

            else:
                # --- Trend strategy placeholder ---
                pass

        # wait before next loop
        time.sleep(SCALP_SLEEP_SECONDS if SCALP else 60)


if __name__ == "__main__":
    main()
