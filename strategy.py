import logging, os, time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import numpy as np, pandas as pd
from dotenv import load_dotenv
from alpaca.data.historical.stock import StockHistoricalDataClient, StockLatestTradeRequest
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.models import DataFeed
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, OrderType, TimeInForce
from alpaca.trading.requests import MarketOrderRequest

NY_TZ = ZoneInfo('America/New_York')
symbol_array = ['NVDA','AAPL','MSFT','GOOGL','AMZN','MU','QCOM','V','AMD','C','PLTR','EBAY','OKTA','IBM','ORCL','META']

# --- Parametrit ---
RSI_PERIOD = 14
MACD_FAST, MACD_SLOW, MACD_SIGNAL = 6, 13, 5
MA_FAST, MA_MID, MA_SLOW = 50, 100, 200
BUY_POWER_LIMIT = 0.05
TIMEFRAME_MAIN = TimeFrameUnit.Minute

SCALP = True
SCALP_TIMEFRAME = TimeFrameUnit.Minute
SCALP_LOOKBACK_MIN = 200
SCALP_SLEEP_SECONDS = 10
EMA_FAST_SCALP, EMA_SLOW_SCALP = 9, 20
RSI_SCALP_PERIOD = 7
VOL_SPIKE_MULT = 1.05
TP_PCT, SL_PCT = 0.004, 0.003
MAX_HOLD_BARS = 15

# --- API ---
load_dotenv()
API_KEY, API_SECRET = os.getenv("ALPACA_PAPER_API_KEY"), os.getenv("ALPACA_PAPER_SECRET_KEY")
ALPACA_PAPER_TRADE = (os.getenv("ALPACA_PAPER_TRADE","True")=="True")
trade_api_url = os.getenv("TRADE_API_URL")
trade_client = TradingClient(API_KEY, API_SECRET, paper=ALPACA_PAPER_TRADE, url_override=trade_api_url)
stock_data_client = StockHistoricalDataClient(API_KEY, API_SECRET)

# --- Helperit ---
def fetch_bars(client, symbol, timeframe_unit, days=90):
    today = datetime.now(NY_TZ).date()
    req = StockBarsRequest(
        symbol_or_symbols=[symbol],
        timeframe=TimeFrame(1, timeframe_unit),
        start=today - timedelta(days=days),
        feed=DataFeed.IEX
    )
    df = client.get_stock_bars(req).df
    if isinstance(df.index, pd.MultiIndex):
        df = df.xs(symbol, level='symbol')
    return df.sort_index()

def compute_rsi(prices, period):
    deltas = prices.diff()
    gains, losses = deltas.clip(lower=0), (-deltas).clip(lower=0)
    avg_gain, avg_loss = gains.rolling(period).mean(), losses.rolling(period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100/(1+rs))

def compute_macd(prices, fast, slow, signal):
    ema_fast, ema_slow = prices.ewm(span=fast).mean(), prices.ewm(span=slow).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal).mean()
    return macd_line, signal_line

def compute_ema(series, span): return series.ewm(span=span).mean()

def compute_intraday_vwap(df):
    df = df.copy()
    df['typical'] = (df['high']+df['low']+df['close'])/3.0
    day = df.index.tz_convert(NY_TZ).date if df.index.tzinfo else df.index.date
    vwap = []
    for d, sub in df.groupby(pd.Series(day, index=df.index)):
        cum_vol = sub['volume'].cumsum()
        cum_pv = (sub['typical']*sub['volume']).cumsum()
        vwap.extend(list(cum_pv/cum_vol))
    df['vwap'] = pd.Series(vwap, index=df.index)
    return df['vwap']

def pct_diff(a,b): return (a-b)/b if b!=0 else 0.0

def position_value(symbol):
    try:
        p = trade_client.get_open_position(symbol)
        return int(float(p.qty)), float(p.avg_entry_price)
    except Exception: return 0,0.0

def calculate_buying_power_limit(limit):
    return float(trade_client.get_account().buying_power)*limit

def get_underlying_price(symbol):
    req = StockLatestTradeRequest(symbol_or_symbols=symbol, feed=DataFeed.IEX)
    return stock_data_client.get_stock_latest_trade(req)[symbol].price

# --- Main loop ---
def main():
    logging.basicConfig(filename="trade_log.txt", level=logging.DEBUG,
                        format="%(asctime)s %(levelname)s: %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    logging.info("=== Strategy started ===")

    entry_bars = {}

    while True:
        for sym in symbol_array:
            if SCALP:
                # --- Scalping ---
                df = fetch_bars(stock_data_client, sym, SCALP_TIMEFRAME, days=2)
                if df is None or df.empty or len(df)<SCALP_LOOKBACK_MIN: continue
                close, vol = df['close'], df['volume']
                ema_fast, ema_slow = compute_ema(close, EMA_FAST_SCALP), compute_ema(close, EMA_SLOW_SCALP)
                rsi_s, vwap = compute_rsi(close, RSI_SCALP_PERIOD), compute_intraday_vwap(df)
                vol_ok = vol.iloc[-1] > VOL_SPIKE_MULT*vol.tail(20).mean()
                ema_cross_up = (ema_fast.iloc[-2]<=ema_slow.iloc[-2]) and (ema_fast.iloc[-1]>ema_slow.iloc[-1])
                ema_cross_down = (ema_fast.iloc[-2]>=ema_slow.iloc[-2]) and (ema_fast.iloc[-1]<ema_slow.iloc[-1])
                scalp_buy = ema_cross_up and (close.iloc[-1]>vwap.iloc[-1]) and vol_ok and (40<float(rsi_s.iloc[-1])<75)

                logging.debug(
                    "%s scalp chk | close=%.2f ema9=%.2f ema20=%.2f vwap=%.2f rsi=%.1f vol=%.0f avg20=%.0f "
                    "cross_up=%s cross_down=%s scalp_buy=%s qty_open=%d",
                    sym, close.iloc[-1], ema_fast.iloc[-1], ema_slow.iloc[-1], vwap.iloc[-1],
                    float(rsi_s.iloc[-1]), vol.iloc[-1], vol.tail(20).mean(),
                    ema_cross_up, ema_cross_down, scalp_buy, position_value(sym)[0]
                )

                qty_open, avg_entry = position_value(sym)
                last = float(close.iloc[-1])
                tp_hit = qty_open>0 and pct_diff(last,avg_entry)>=TP_PCT
                sl_hit = qty_open>0 and pct_diff(last,avg_entry)<=-SL_PCT
                ema_fail = qty_open>0 and ((last<ema_fast.iloc[-1]) or ema_cross_down)
                vwap_fail = qty_open>0 and (last<vwap.iloc[-1])
                hold_too_long = False
                if qty_open>0 and sym in entry_bars:
                    bars_held = len(df) - entry_bars[sym]
                    hold_too_long = bars_held >= MAX_HOLD_BARS

                if scalp_buy and qty_open==0:
                    try:
                        limit = calculate_buying_power_limit(BUY_POWER_LIMIT)
                        mkt_price = float(get_underlying_price(sym))
                        qty = int(limit//mkt_price)
                        if qty == 0 and limit >= mkt_price:
                            qty = 1
                        if qty>0:
                            order = MarketOrderRequest(symbol=sym, qty=qty, side=OrderSide.BUY,
                                                       type=OrderType.MARKET, time_in_force=TimeInForce.DAY)
                            trade_client.submit_order(order)
                            logging.info("%s - SCALP BUY %d @ %.2f", sym, qty, mkt_price)
                            entry_bars[sym] = len(df)
                    except Exception as e: logging.exception("%s - SCALP BUY error: %s", sym, str(e))

                if qty_open>0 and (tp_hit or sl_hit or ema_fail or vwap_fail or hold_too_long):
                    try:
                        order = MarketOrderRequest(symbol=sym, qty=qty_open, side=OrderSide.SELL,
                                                   type=OrderType.MARKET, time_in_force=TimeInForce.DAY)
                        trade_client.submit_order(order)
                        reason = "TP" if tp_hit else "SL" if sl_hit else "EMA/VWAP fail" if (ema_fail or vwap_fail) else
                                                reason = "TP" if tp_hit else "SL" if sl_hit else "EMA/VWAP fail" if (ema_fail or vwap_fail) else "MAX_HOLD"
                        logging.info("%s - SCALP SELL %d @ market (%s)", sym, qty_open, reason)
                        if sym in entry_bars:
                            del entry_bars[sym]
                    except Exception as e:
                        logging.exception("%s - SCALP SELL error: %s", sym, str(e))

            else:
                # --- Trendistrategia ---
                df = fetch_bars(stock_data_client, sym, TIMEFRAME_MAIN, days=90)
                if df is None or df.empty:
                    continue

                close = df['close']
                rsi = compute_rsi(close, RSI_PERIOD)
                macd_line, signal_line = compute_macd(close, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
                ema50, ema100, ema200 = (
                    compute_ema(close, MA_FAST),
                    compute_ema(close, MA_MID),
                    compute_ema(close, MA_SLOW)
                )

                uptrend = ema50.iloc[-1] > ema100.iloc[-1] > ema200.iloc[-1]
                rsi_now = float(rsi.iloc[-1]) if pd.notna(rsi.iloc[-1]) else 50
                macd_cross_up = (macd_line.iloc[-2] <= signal_line.iloc[-2]) and (macd_line.iloc[-1] > signal_line.iloc[-1])
                macd_cross_down = (macd_line.iloc[-2] >= signal_line.iloc[-2]) and (macd_line.iloc[-1] < signal_line.iloc[-1])

                buy_signal = uptrend and (rsi_now > 50) and macd_cross_up
                sell_signal = (rsi_now < 45) or macd_cross_down

                qty_open, avg_entry = position_value(sym)
                last = float(close.iloc[-1])

                logging.debug(
                    "%s trend chk | close=%.2f ema50=%.2f ema100=%.2f ema200=%.2f rsi=%.1f macd=%.3f sig=%.3f "
                    "uptrend=%s macd_up=%s macd_down=%s buy=%s sell=%s qty_open=%d",
                    sym, last, ema50.iloc[-1], ema100.iloc[-1], ema200.iloc[-1],
                    rsi_now, macd_line.iloc[-1], signal_line.iloc[-1],
                    uptrend, macd_cross_up, macd_cross_down, buy_signal, sell_signal, qty_open
                )

                if buy_signal and qty_open == 0:
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
                            logging.info("%s - TREND BUY %d @ %.2f", sym, qty, mkt_price)
                    except Exception as e:
                        logging.exception("%s - TREND BUY error: %s", sym, str(e))

                if qty_open > 0 and sell_signal:
                    try:
                        order = MarketOrderRequest(
                            symbol=sym,
                            qty=qty_open,
                            side=OrderSide.SELL,
                            type=OrderType.MARKET,
                            time_in_force=TimeInForce.DAY
                        )
                        trade_client.submit_order(order)
                        logging.info("%s - TREND SELL %d @ market", sym, qty_open)
                    except Exception as e:
                        logging.exception("%s - TREND SELL error: %s", sym, str(e))

        # odota seuraavaa kierrosta
        time.sleep(SCALP_SLEEP_SECONDS if SCALP else 60)


if __name__ == "__main__":
    main()
