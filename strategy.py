import logging
import os
import time
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
TIMEFRAME_MAIN = TimeFrame(5, TimeFrameUnit.Minute)
EMA_FAST = 9
EMA_SLOW = 20
RSI_PERIOD = 7
VOL_SPIKE_MULT = 1.05
TP_PCT = 0.006  # widened to 0.6%
SL_PCT = 0.003  # widened to 0.3%
MAX_HOLD_BARS = 10  # minutes
SCALP_SLEEP_SECONDS = 10
BUY_POWER_LIMIT = 0.05

# --- helper functions ---
def sleep_until(target_time, chunk_seconds=30):
    """Pause until target_time (UTC) in small chunks for responsiveness."""
    if target_time.tzinfo is None:
        target_time = target_time.replace(tzinfo=timezone.utc)
    while True:
        now = datetime.now(timezone.utc)
        remaining = (target_time - now).total_seconds()
        if remaining <= 0:
            break
        time.sleep(min(remaining, chunk_seconds))

def compute_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def compute_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def compute_macd(series, fast=12, slow=26, signal=9):
    ema_fast = compute_ema(series, fast)
    ema_slow = compute_ema(series, slow)
    macd_line = ema_fast - ema_slow
    signal_line = compute_ema(macd_line, signal)
    return macd_line, signal_line

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

# --- EXIT command handler ---
def handle_exit_command():
    logging.info("EXIT command detected. Attempting to liquidate all open positions...")
    try:
        positions = trade_client.get_all_positions()
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
                    trade_client.submit_order(order)
                    logging.info("EXIT SELL %s - Qty: %d", symbol, qty)
                except Exception as e:
                    logging.exception("EXIT SELL error for %s: %s", symbol, str(e))
    except Exception as e:
        logging.exception("Failed to fetch positions during EXIT: %s", str(e))
    logging.info("All EXIT orders submitted. Exiting script.")
    exit(0)

# --- main loop ---
def main():
    load_dotenv()
    logging.basicConfig(filename="trade_log.txt", level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")
    global stock_data_client, trade_client
    stock_data_client = StockHistoricalDataClient(
        os.getenv("ALPACA_PAPER_API_KEY"), os.getenv("ALPACA_PAPER_SECRET_KEY")
    )
    trade_client = TradingClient(
        os.getenv("ALPACA_PAPER_API_KEY"), os.getenv("ALPACA_PAPER_SECRET_KEY"), paper=True
    )
    clock = trade_client.get_clock()
    market_open = clock.is_open
    print(f"Market open: {market_open}")
    symbols = ["AAPL", "MSFT", "MU", "QCOM", "NVDA", "V", "AMD", "GOOG", "C", "EBAY", "OKTA", "TSLA", "AMZN", "ADSK", "DELL"]
    entry_times = {}

    while True:
        if market_open and not clock.is_open:
            logging.info("Market closed. Sleeping until next open at %s", clock.next_open)
            market_open = False
            sleep_until(clock.next_open)
            continue

        if (not market_open) and clock.is_open:
            logging.info("Market opened. Resuming trading")
            market_open = True

        if not clock.is_open:
            logging.info("Market is closed. Exiting.")
            exit(0)

        # --- EXIT command check ---
        if os.path.exists("EXIT"):
            handle_exit_command()

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
                if pd.isna(rsi_s.iloc[-1]):
                    continue
                ema_cross_up = (ema_fast.iloc[-2] <= ema_slow.iloc[-2]) and (ema_fast.iloc[-1] > ema_slow.iloc[-1])
                ema_trend_up = ema_fast.iloc[-1] > ema_slow.iloc[-1]
                avg20 = vol.rolling(20).mean()
                if pd.isna(avg20.iloc[-1]):
                    continue
                vol_ok = vol.iloc[-1] > avg20.iloc[-1] * VOL_SPIKE_MULT
                scalp_buy = (ema_cross_up or ema_trend_up) \
                            and (close.iloc[-1] > vwap.iloc[-1]) \
                            and vol_ok \
                            and (45 < float(rsi_s.iloc[-1]) < 65)
                qty_open, avg_entry = position_value(sym)
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
                            logging.info("%s - SCALP BUY %d @ %.2f", sym, qty, mkt_price)
                    except Exception as e:
                        logging.exception("%s - SCALP BUY error: %s", sym, str(e))

                if qty_open > 0:
                    try:
                        last = float(close.iloc[-1])
                        tp_hit = last >= avg_entry * (1 + TP_PCT)
                        sl_hit = last <= avg_entry * (1 - SL_PCT)
                        vwap_fail = all(close.iloc[-i] < vwap.iloc[-i] for i in range(1, 4))
                        ema_fail = all(ema_fast.iloc[-i] < ema_slow.iloc[-i] for i in range(1, 4))
                        time_exceeded = False
                        if sym in entry_times:
                            bars_since_entry = len(df[df.index > entry_times[sym]])
                            time_exceeded = bars_since_entry >= MAX_HOLD_BARS

                        if tp_hit or sl_hit or vwap_fail or ema_fail or time_exceeded:
                            order = MarketOrderRequest(
                                symbol=sym,
                                qty=qty_open,
                                side=OrderSide.SELL,
                                type=OrderType.MARKET,
                                time_in_force=TimeInForce.DAY
                            )
                            trade_client.submit_order(order)
                            logging.info("%s - SCALP SELL %d @ %.2f", sym, qty_open, last)
                            entry_times.pop(sym, None)
                    except Exception as e:
                        logging.exception("%s - SCALP SELL error: %s", sym, str(e))

        time.sleep(SCALP_SLEEP_SECONDS)

if __name__ == "__main__":
    main()
        
