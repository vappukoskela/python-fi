import logging
import os
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from dotenv import load_dotenv

from alpaca.data.historical.stock import StockHistoricalDataClient, StockLatestTradeRequest
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, OrderType, TimeInForce
from alpaca.trading.requests import MarketOrderRequest

NY_TZ = ZoneInfo('America/New_York')
symbol_array = ['NVDA', 'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'MU', 'QCOM', 'V', 'AMD', 'C', 'PLTR', 'EBAY', 'OKTA', 'IBM', 'ORCL', 'META']

# Tracking signal lags
rsi_bounce_bar = {symbol: None for symbol in symbol_array}
macd_cross_bar = {symbol: None for symbol in symbol_array}
rsi_retreat_bar = {symbol: None for symbol in symbol_array}
macd_death_cross_bar = {symbol: None for symbol in symbol_array}
macd_centerline_bar = {symbol: None for symbol in symbol_array}
current_bar_index = 0

# Strategy Parameters
RSI_PERIOD = 14
MACD_FAST = 6
MACD_SLOW = 13
MACD_SIGNAL = 5
MA_FAST = 50
MA_MID = 100
MA_SLOW = 200
BUY_POWER_LIMIT = 0.02
TIMEFRAME_MAIN = TimeFrameUnit.Minute
TIMEFRAME_TREND = TimeFrameUnit.Day

# Load environment variables
load_dotenv()
API_KEY = os.getenv("ALPACA_PAPER_API_KEY")
API_SECRET = os.getenv("ALPACA_PAPER_SECRET_KEY")
ALPACA_PAPER_TRADE = (os.getenv("ALPACA_PAPER_TRADE", "True") == "True")
trade_api_url = os.getenv("TRADE_API_URL")

if not API_KEY or not API_SECRET:
    raise RuntimeError("Missing Alpaca API credentials in environment variables.")

trade_client = TradingClient(api_key=API_KEY, secret_key=API_SECRET, paper=ALPACA_PAPER_TRADE, url_override=trade_api_url)
stock_data_client = StockHistoricalDataClient(api_key=API_KEY, secret_key=API_SECRET)

# --- Apufunktio yhdistettyyn debug-yhteenvetoon ---
def log_strategy_state(
    underlying_symbol,
    current_bar_index,
    last_fast, last_mid, last_slow, in_uptrend,
    volume_now, volume_avg, volume_ok,
    higher_low, breakout, price_action_ok,
    rsi_prev, rsi_now,
    macd_prev, macd_now, sig_prev, sig_now
):
    logging.info(
        "%s | Bar=%d | "
        "Trend: %s (MA50=%.2f, MA100=%.2f, MA200=%.2f) | "
        "Vol: now=%.0f avg20=%.0f ok=%s | "
        "PriceAction: HL=%s breakout=%s ok=%s | "
        "RSI: prev=%.2f now=%.2f bounce_ok=%s retreat_ok=%s | "
        "MACD: prev=%.4f now=%.4f sig_prev=%.4f sig_now=%.4f "
        "golden=%s death=%s centerline=%s",
        underlying_symbol,
        current_bar_index,
        in_uptrend,
        last_fast if pd.notna(last_fast) else float('nan'),
        last_mid if pd.notna(last_mid) else float('nan'),
        last_slow if pd.notna(last_slow) else float('nan'),
        volume_now, volume_avg, volume_ok,
        higher_low, breakout, price_action_ok,
        rsi_prev, rsi_now, (rsi_now > 30), ((rsi_prev > 70) and (rsi_now < 65)),
        macd_prev, macd_now, sig_prev, sig_now,
        ((macd_prev < sig_prev) and (macd_now > sig_now)),
        ((macd_prev > sig_prev) and (macd_now < sig_now)),
        ((macd_prev > 0) and (macd_now < 0))
    )

# --- Helper functions ---
def sleep_until(target_time, chunk_seconds=30):
    if target_time.tzinfo is None:
        target_time = target_time.replace(tzinfo=timezone.utc)
    while True:
        now = datetime.now(timezone.utc)
        remaining = (target_time - now).total_seconds()
        if remaining <= 0:
            break
        time.sleep(min(remaining, chunk_seconds))

def fetch_bars(client, underlying_symbol, timeframe_unit, days=90):
    today = datetime.now(NY_TZ).date()
    req = StockBarsRequest(
        symbol_or_symbols=[underlying_symbol],
        timeframe=TimeFrame(amount=1, unit=timeframe_unit),
        start=today - timedelta(days=days),
    )
    df = client.get_stock_bars(req).df
    # Varmista, että DataFrame on multiindex -> yksittäisen symbolin df
    if isinstance(df.index, pd.MultiIndex):
        df = df.xs(underlying_symbol, level='symbol')
    return df.sort_index()

def compute_rsi(prices, period):
    deltas = prices.diff()
    gains = deltas.clip(lower=0)
    losses = (-deltas).clip(lower=0)
    avg_gain = gains.rolling(period).mean()
    avg_loss = losses.rolling(period).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def compute_macd(prices, fast, slow, signal):
    ema_fast = prices.ewm(span=fast, adjust=False).mean()
    ema_slow = prices.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    return macd_line, signal_line

def calculate_buying_power_limit(buy_power_limit):
    buying_power = float(trade_client.get_account().buying_power)
    return buying_power * buy_power_limit

def get_underlying_price(symbol):
    req = StockLatestTradeRequest(symbol_or_symbols=symbol)
    resp = stock_data_client.get_stock_latest_trade(req)
    return resp[symbol].price

# --- Main trading loop ---
def main():
    logging.basicConfig(
        filename="trade_log.txt",
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    logging.info("=== Strategy started ===")

    clock = trade_client.get_clock()
    market_open = clock.is_open

    global rsi_bounce_bar, macd_cross_bar, rsi_retreat_bar, macd_death_cross_bar, macd_centerline_bar, current_bar_index

    while True:
        clock = trade_client.get_clock()

        for underlying_symbol in symbol_array:
            # Markkinaikkuna
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
                return

            # Hae data
            df_main = fetch_bars(stock_data_client, underlying_symbol, TIMEFRAME_MAIN, days=MA_SLOW + 100)
            df_trend = fetch_bars(stock_data_client, underlying_symbol, TIMEFRAME_TREND, days=MA_SLOW + 250)

            # Vähimmäispituustarkistukset
            if df_main is None or df_main.empty or 'close' not in df_main.columns or 'volume' not in df_main.columns:
                logging.warning("%s - Missing intraday data or columns.", underlying_symbol)
                continue
            if df_trend is None or df_trend.empty or 'close' not in df_trend.columns:
                logging.warning("%s - Missing daily trend data.", underlying_symbol)
                continue

            current_bar_index = len(df_main) - 1

            # Avoin positio
            try:
                position = trade_client.get_open_position(underlying_symbol)
                position_open = True
                current_qty = int(float(position.qty))
            except Exception as e:
                position_open = False
                current_qty = 0

            prices = df_main['close']
            if len(prices) < 50:
                logging.debug("%s - Not enough bars for indicators (%d < 50).", underlying_symbol, len(prices))
                continue

            # Indikaattorit
            rsi_series = compute_rsi(prices, RSI_PERIOD)
            macd_line, signal_line = compute_macd(prices, MACD_FAST, MACD_SLOW, MACD_SIGNAL)

            # Varmista arvoja on vähintään 2
            if rsi_series.notna().sum() < 2 or macd_line.notna().sum() < 2 or signal_line.notna().sum() < 2:
                logging.debug("%s - Indicators not ready (NaN head).", underlying_symbol)
                continue

            rsi_now = rsi_series.iloc[-1]
            rsi_prev = rsi_series.iloc[-2]
            macd_now = macd_line.iloc[-1]
            macd_prev = macd_line.iloc[-2]
            sig_now = signal_line.iloc[-1]
            sig_prev = signal_line.iloc[-2]

            # --- Trendisuodatin (päivädata) ---
            ma_fast = df_trend['close'].rolling(MA_FAST).mean()
            ma_mid  = df_trend['close'].rolling(MA_MID).mean()
            ma_slow = df_trend['close'].rolling(MA_SLOW).mean()
            last_fast = ma_fast.iloc[-1]
            last_mid  = ma_mid.iloc[-1]
            last_slow = ma_slow.iloc[-1]

            if pd.notna(last_fast) and pd.notna(last_mid) and pd.notna(last_slow):
                in_uptrend = (last_fast > last_mid) and (last_mid > last_slow)
            else:
                in_uptrend = False

            # --- Volyymisuodatin ---
            volume_now = float(df_main['volume'].iloc[-1])
            volume_avg = float(df_main['volume'].tail(20).mean()) if len(df_main) >= 20 else float('inf')
            volume_ok = volume_now > volume_avg

            # --- Price action ---
            price_action_ok = False
            higher_low = False
            breakout = False
            if len(prices) >= 6:
                recent = prices.tail(5)
                recent_lows = recent.rolling(window=2).min()
                recent_highs = recent.rolling(window=2).max()
                # varmistetaan, että saimme tarpeeksi arvoja
                if len(recent_lows.dropna()) >= 2 and len(recent_highs.dropna()) >= 2:
                    higher_low = recent_lows.iloc[-1] > recent_lows.iloc[-2]
                    breakout = prices.iloc[-1] > recent_highs.iloc[-2]
                    price_action_ok = higher_low and breakout

            # --- RSI / MACD signaalit (päivitetään viimeisin signaalibari) ---
            if pd.notna(rsi_now) and (rsi_now > 30):
                rsi_bounce_bar[underlying_symbol] = current_bar_index
            if pd.notna(macd_prev) and pd.notna(sig_prev) and pd.notna(macd_now) and pd.notna(sig_now):
                if (macd_prev < sig_prev) and (macd_now > sig_now):
                    macd_cross_bar[underlying_symbol] = current_bar_index
                if (macd_prev > sig_prev) and (macd_now < sig_now):
                    macd_death_cross_bar[underlying_symbol] = current_bar_index
            if pd.notna(rsi_prev) and pd.notna(rsi_now) and (rsi_prev > 70) and (rsi_now < 65):
                rsi_retreat_bar[underlying_symbol] = current_bar_index
            if pd.notna(macd_prev) and pd.notna(macd_now) and (macd_prev > 0) and (macd_now < 0):
                macd_centerline_bar[underlying_symbol] = current_bar_index

            # --- Yhdistetty debug-yhteenveto ---
            log_strategy_state(
                underlying_symbol,
                current_bar_index,
                last_fast, last_mid, last_slow, in_uptrend,
                volume_now, volume_avg, volume_ok,
                higher_low, breakout, price_action_ok,
                rsi_prev, rsi_now,
                macd_prev, macd_now, sig_prev, sig_now
            )

            # --- Signaalien yhdistäminen (voimassa 3 baria) ---
            bars_valid = 3

            buy_signal = (
                in_uptrend and volume_ok and price_action_ok and (
                    (rsi_bounce_bar[underlying_symbol] is not None and current_bar_index - rsi_bounce_bar[underlying_symbol] <= bars_valid) or
                    (macd_cross_bar[underlying_symbol] is not None and current_bar_index - macd_cross_bar[underlying_symbol] <= bars_valid)
                )
            )

            sell_signal = (
                (rsi_retreat_bar[underlying_symbol] is not None and current_bar_index - rsi_retreat_bar[underlying_symbol] <= bars_valid) or
                (macd_death_cross_bar[underlying_symbol] is not None and current_bar_index - macd_death_cross_bar[underlying_symbol] <= bars_valid) or
                (macd_centerline_bar[underlying_symbol] is not None and current_bar_index - macd_centerline_bar[underlying_symbol] <= bars_valid)
            )

            # --- Kaupankäyntilogiikka ---
            if buy_signal and not position_open:
                try:
                    limit = calculate_buying_power_limit(BUY_POWER_LIMIT)
                    price = float(get_underlying_price(underlying_symbol))
                    qty = int(limit // price)
                    if qty > 0:
                        order = MarketOrderRequest(
                            symbol=underlying_symbol,
                            qty=qty,
                            side=OrderSide.BUY,
                            type=OrderType.MARKET,
                            time_in_force=TimeInForce.DAY
                        )
                        trade_client.submit_order(order)
                        logging.info("%s - BUY %d @ %.2f", underlying_symbol, qty, price)
                    else:
                        logging.info("%s - BUY skipped: qty=0 (limit=%.2f, price=%.2f)", underlying_symbol, limit, price)
                except Exception as e:
                    logging.exception("%s - BUY error: %s", underlying_symbol, str(e))

            if sell_signal and position_open and current_qty > 0:
                try:
                    order = MarketOrderRequest(
                        symbol=underlying_symbol,
                        qty=current_qty,
                        side=OrderSide.SELL,
                        type=OrderType.MARKET,
                        time_in_force=TimeInForce.DAY
                    )
                    trade_client.submit_order(order)
                    logging.info("%s - SELL %d @ market", underlying_symbol, current_qty)
                except Exception as e:
                    logging.exception("%s - SELL error: %s", underlying_symbol, str(e))

        # odota seuraavaa kierrosta
        time.sleep(60)

if __name__ == "__main__":
    main()
