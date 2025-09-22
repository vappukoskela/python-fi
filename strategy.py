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

# --- asetukset ---
SCALP = True
TIMEFRAME_SCALP = TimeFrame(1, TimeFrameUnit.Minute)
TIMEFRAME_MAIN = TimeFrame(5, TimeFrameUnit.Minute)
EMA_FAST = 9
EMA_SLOW = 20
RSI_PERIOD = 7
VOL_SPIKE_MULT = 1.05
TP_PCT = 0.003
SL_PCT = 0.002
MAX_HOLD_BARS = 10   # now interpreted as minutes
SCALP_SLEEP_SECONDS = 10
BUY_POWER_LIMIT = 0.05

# --- apufunktiot ---
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

# --- pääohjelma ---
def main():
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
    entry_times = {}   # store entry timestamps instead of bar counts

    while True:
        for sym in symbols:
            if SCALP:
                # --- Scalping-strategia ---
                df = fetch_bars(stock_data_client, sym, TIMEFRAME_SCALP, days=1)
                if df is None or df.empty:
                    continue

                close = df['close']
                vol = df['volume']

                ema_fast = compute_ema(close, EMA_FAST)
                ema_slow = compute_ema(close, EMA_SLOW)
                vwap = compute_vwap(df)
                rsi_s = compute_rsi(close, RSI_PERIOD)

                if pd.isna(rsi_s.iloc[-1]):
                    continue

                ema_cross_up = (ema_fast.iloc[-2] <= ema_slow.iloc[-2]) and (ema_fast.iloc[-1] > ema_slow.iloc[-1])
                ema_cross_down = (ema_fast.iloc[-2] >= ema_slow.iloc[-2]) and (ema_fast.iloc[-1] < ema_slow.iloc[-1])
                ema_trend_up = ema_fast.iloc[-1] > ema_slow.iloc[-1]

                avg20 = vol.rolling(20).mean()
                if pd.isna(avg20.iloc[-1]):
                    continue
                vol_ok = vol.iloc[-1] > avg20.iloc[-1] * VOL_SPIKE_MULT

                scalp_buy = (ema_cross_up or ema_trend_up) \
                            and (close.iloc[-1] > vwap.iloc[-1]) \
                            and vol_ok \
                            and (40 < float(rsi_s.iloc[-1]) < 75)

                qty_open, avg_entry = position_value(sym)

                logging.debug(
                    "%s scalp chk | close=%.2f ema9=%.2f ema20=%.2f vwap=%.2f rsi=%.1f vol=%d avg20=%d "
                    "cross_up=%s cross_down=%s trend_up=%s scalp_buy=%s qty_open=%d",
                    sym, close.iloc[-1], ema_fast.iloc[-1], ema_slow.iloc[-1], vwap.iloc[-1],
                    float(rsi_s.iloc[-1]), vol.iloc[-1], avg20.iloc[-1],
                    ema_cross_up, ema_cross_down, ema_trend_up, scalp_buy, qty_open
                )

                if scalp_buy and qty_open == 0:
                    try:
                        limit = calculate_buying_power_limit(BUY_POWER_LIMIT)
                        mkt_price = float(get_underlying_price(sym))
                        qty = int(limit // mkt_price)
                        if qty == 0 and limit >= mkt_price:
                            qty = 1
                        if qty > 0:
                            order = MarketOrderRequest(
                                symbol=sym,
                                qty=qty,
                                side=OrderSide.BUY,
                                type=OrderType.MARKET,
                                time_in_force=TimeInForce.DAY
                            )
                            trade_client.submit_order(order)
                            entry_times[sym] = df.index[-1]   # store timestamp
                            logging.info("%s - SCALP BUY %d @ %.2f", sym, qty, mkt_price)
                    except Exception as e:
                        logging.exception("%s - SCALP BUY error: %s", sym, str(e))

                # --- SELL BLOCK ---
                if qty_open > 0:
                    try:
                        last = float(close.iloc[-1])
                        tp_hit = last >= avg_entry * (1 + TP_PCT)
                        sl_hit = last <= avg_entry * (1 - SL_PCT)

                        # Confirmation: use last 3 bars for EMA/VWAP
                        ema_fail = (ema_fast.iloc[-3:].mean() < ema_slow.iloc[-3:].mean())
                        vwap_fail = (close.iloc[-3:].mean() < vwap.iloc[-3:].mean())

                        # NEW: time-based max hold
                        elapsed_minutes = (df.index[-1] - entry_times.get(sym, df.index[-1])).total_seconds() / 60
                        max_hold = elapsed_minutes >= MAX_HOLD_BARS

                        reason = None
                        if tp_hit or sl_hit:
                            reason = "TP" if tp_hit else "SL"
                        elif ema_fail or vwap_fail:
                            reason = "EMA/VWAP fail"
                        elif max_hold:
                            reason = "MAX_HOLD"

                        if reason:
                            order = MarketOrderRequest(
                                symbol=sym,
                                qty=qty_open,
                                side=OrderSide.SELL,
                                type=OrderType.MARKET,
                                time_in_force=TimeInForce.DAY
                            )
                            trade_client.submit_order(order)
                            logging.info("%s - SCALP SELL %d @ market (%s)", sym, qty_open, reason)
                            if sym in entry_times:
                                del entry_times[sym]
                    except Exception as e:
                        logging.exception("%s - SCALP SELL error: %s", sym, str(e))

            else:
                # --- Trendistrategia ---
                pass

        time.sleep(SCALP_SLEEP_SECONDS if SCALP else 60)


if __name__ == "__main__":
    main()

