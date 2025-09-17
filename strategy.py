# Import standard library modules
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# Import third party modules
import numpy as np
import pandas as pd
from dotenv import load_dotenv

# Import Alpaca modules
from alpaca.data.historical.stock import (
    StockHistoricalDataClient, StockLatestTradeRequest,
)
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import (
    AssetClass, AssetStatus, OrderSide, OrderType, QueryOrderStatus, TimeInForce,
)
from alpaca.trading.requests import MarketOrderRequest

# Set the local timezone
NY_TZ = ZoneInfo('America/New_York')

# Symbols to trade
symbol_array = ['NVDA', 'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']

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
MAX_RISK_PCT = 0.03
TIMEFRAME_MAIN = TimeFrameUnit.Minute
TIMEFRAME_TREND = TimeFrameUnit.Day

# Load environment variables
load_dotenv()
API_KEY = os.getenv("ALPACA_PAPER_API_KEY")
API_SECRET = os.getenv("ALPACA_PAPER_SECRET_KEY")
ALPACA_PAPER_TRADE = os.getenv("ALPACA_PAPER_TRADE", "True")
trade_api_url = os.getenv("TRADE_API_URL")

if not API_KEY or not API_SECRET:
    raise RuntimeError("Missing Alpaca API credentials in environment variables.")

# Setup trading clients
trade_client = TradingClient(api_key=API_KEY, secret_key=API_SECRET, paper=ALPACA_PAPER_TRADE, url_override=trade_api_url)
stock_data_client = StockHistoricalDataClient(api_key=API_KEY, secret_key=API_SECRET)

# Helper functions
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
    return client.get_stock_bars(req).df

def compute_rsi(prices, period):
    deltas = prices.diff().dropna()
    gains = deltas.where(deltas > 0, 0)
    losses = (-deltas).where(deltas < 0, 0)
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

# Main trading loop
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
    stop_loss_price = {symbol: None for symbol in symbol_array}
    take_profit_price = {symbol: None for symbol in symbol_array}

    while True:
        clock = trade_client.get_clock()
        for underlying_symbol in symbol_array:
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

            df_main = fetch_bars(stock_data_client, underlying_symbol, TIMEFRAME_MAIN, days=MA_SLOW + 100)
            df_trend = fetch_bars(stock_data_client, underlying_symbol, TIMEFRAME_TREND, days=MA_SLOW + 10)
            current_bar_index = len(df_main) - 1

            try:
                position = trade_client.get_open_position(underlying_symbol)
                position_open = True
                current_qty = int(position.qty)
            except Exception:
                position_open = False
                current_qty = 0

            prices = df_main.close
            rsi_series = compute_rsi(prices, RSI_PERIOD)
            macd_line, signal_line = compute_macd(prices, MACD_FAST, MACD_SLOW, MACD_SIGNAL)

            rsi_now = rsi_series.iloc[-1]
            rsi_prev = rsi_series.iloc[-2]
            macd_now = macd_line.iloc[-1]
            macd_prev = macd_line.iloc[-2]
            sig_now = signal_line.iloc[-1]
            sig_prev = signal_line.iloc[-2]

            ma_fast = df_trend.close.rolling(MA_FAST).mean()
            ma_mid = df_trend.close.rolling(MA_MID).mean()
            ma_slow = df_trend.close.rolling(MA_SLOW).mean()
            if not (ma_fast.isna().any() or ma_mid.isna().any() or ma_slow.isna().any()):
                in_uptrend = (ma_fast.iloc[-1] > ma_mid.iloc[-1]) and (ma_mid.iloc[-1] > ma_slow.iloc[-1])
            else:
                in_uptrend = False

            buying_power_limit = calculate_buying_power_limit(BUY_POWER_LIMIT)
            current_price = get_underlying_price(underlying_symbol)
            if current_price <= 0 or buying_power_limit < current_price:
                position_size = 0
            else:
                position_size = int(buying_power_limit / current_price)

            # --- RSI yli 30 ---
            if rsi_now > 30:
                rsi_bounce_bar[underlying_symbol] = current_bar_index
            else:
                rsi_bounce_bar[underlying_symbol] = None

            # --- MACD golden cross ---
            if (macd_prev < sig_prev) and (macd_now > sig_now):
                macd_cross_bar[underlying_symbol] = current_bar_index
            else:
                macd_cross_bar[underlying_symbol] = None

            # --- Volyymisuodatin ---
            volume_series = df_main.volume
            volume_now = volume_series.iloc[-1]
            volume_avg = volume_series.rolling(window=20).mean().iloc[-1]
            volume_ok = volume_now > volume_avg

            # --- Price action ---
            recent_lows = prices.tail(5).rolling(window=2).min()
            recent_highs = prices.tail(5).rolling(window=2).max()
            higher_low = recent_lows.iloc[-1] > recent_lows.iloc[-2]
            breakout = prices.iloc[-1] > recent_highs.iloc[-2]
            price_action_ok = higher_low and breakout

            logging.info("%s - Price: $%.2f | RSI: %.2f | MACD: %.4f | Signal: %.4f", underlying_symbol, prices.iloc[-1], rsi_now, macd_now, sig_now)
            logging.info("%s - In uptrend: %s | Volume OK: %s | Price Action OK: %s", underlying_symbol, in_uptrend, volume_ok, price_action_ok)   
            logging.info("%s - rsi_bounce_bar: %s | macd_cross_bar: %s | position_size: %d | position_open: %s | current_qty: %d", underlying_symbol, rsi_bounce_bar[underlying_symbol], macd_cross_bar[underlying_symbol], position_size, position_open, current_qty)
            # --- Ostoehto ---
            if not position_open and position_size > 0 and in_uptrend and volume_ok and price_action_ok:
                if (rsi_bounce_bar[underlying_symbol] is not None and macd_cross_bar[underlying_symbol] is not None):
                
                    req = MarketOrderRequest(
                        symbol=underlying_symbol,
                        qty=position_size,
                        side=OrderSide.BUY,
                        type=OrderType.MARKET,
                        time_in_force=TimeInForce.DAY
                    )
                    res = trade_client.submit_order(req)

                    logging.info(
                        "BUY ORDER SUBMITTED - Symbol: %s | Qty: %d | Est.Price: $%.2f | OrderID: %s | ClientOrderID: %s | SubmittedAt: %s",
                        underlying_symbol, position_size, current_price,
                        res.id, res.client_order_id, res.submitted_at
                    )

                    # Stop loss ja take profit
                    stop_loss_price[underlying_symbol] = current_price * 0.97
                    take_profit_price[underlying_symbol] = current_price * 1.005

                    # Nollataan signaalit
                    rsi_bounce_bar[underlying_symbol] = None
                    macd_cross_bar[underlying_symbol] = None

            # --- Myyntisignaalit ---
            # RSI retreat
            if (rsi_prev > 70) and (rsi_now < 65):
                rsi_retreat_bar[underlying_symbol] = current_bar_index

            # MACD death cross tai centerline drop
            if (macd_prev > sig_prev) and (macd_now < sig_now):
                macd_death_cross_bar[underlying_symbol] = current_bar_index
            elif macd_prev > 0 and macd_now < 0:
                macd_centerline_bar[underlying_symbol] = current_bar_index
                
            logging.info("%s - RSI retreat bar: %s | MACD death cross bar: %s | MACD centerline bar: %s", underlying_symbol,
                         rsi_retreat_bar[underlying_symbol], macd_death_cross_bar[underlying_symbol], macd_centerline_bar[underlying_symbol])
            logging.info("%s - Stop loss price: %s | Take profit price: %s", underlying_symbol, stop_loss_price[underlying_symbol], take_profit_price[underlying_symbol])
            
            # --- Myyntiehto ---
            if position_open:
                exit_reason = None
                if macd_death_cross_bar[underlying_symbol] is not None:
                    exit_reason = "MACD death cross"
                elif macd_centerline_bar[underlying_symbol] is not None:
                    exit_reason = "MACD centerline drop"
                elif stop_loss_price[underlying_symbol] is not None and current_price <= stop_loss_price[underlying_symbol]:
                    exit_reason = "Stop loss"
                elif take_profit_price[underlying_symbol] is not None and current_price >= take_profit_price[underlying_symbol]:
                    exit_reason = "Take profit"

                if exit_reason:
                    req = MarketOrderRequest(
                        symbol=underlying_symbol,
                        qty=current_qty,
                        side=OrderSide.SELL,
                        type=OrderType.MARKET,
                        time_in_force=TimeInForce.DAY,
                    )
                    res = trade_client.submit_order(req)
                    logging.info(
                        "SELL ORDER SUBMITTED - Symbol: %s | Qty: %d | Est.Price: $%.2f | OrderID: %s | ClientOrderID: %s | SubmittedAt: %s",
                        underlying_symbol, current_qty, current_price,
                        res.id, res.client_order_id, res.submitted_at
                    )
                    logging.info("SELL triggered by: %s", exit_reason)

                    # Nollataan myyntisignaalit
                    rsi_retreat_bar[underlying_symbol] = None
                    macd_death_cross_bar[underlying_symbol] = None
                    macd_centerline_bar[underlying_symbol] = None
                    stop_loss_price[underlying_symbol] = None
                    take_profit_price[underlying_symbol] = None

        # --- Ajastus seuraavaan sykliin ---
        now = datetime.now(timezone.utc)
        next_run = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
        sleep_until(next_run, chunk_seconds=10)


if __name__ == "__main__":
    main()
