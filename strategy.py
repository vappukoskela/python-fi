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
    StockHistoricalDataClient,
    StockLatestTradeRequest,
)
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import (
    AssetClass,
    AssetStatus,
    OrderSide,
    OrderType,
    QueryOrderStatus,
    TimeInForce,
)
from alpaca.trading.requests import MarketOrderRequest


# Set the local timezone
NY_TZ = ZoneInfo('America/New_York')

# Select the stock (AAPL)
# underlying_symbol = 'AAPL'
symbol_array = ['NVDA', 'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']

# Set tracking signal lags over a predefined window
rsi_bounce_bar = {symbol: None for symbol in symbol_array}
macd_cross_bar = {symbol: None for symbol in symbol_array}
rsi_retreat_bar = {symbol: None for symbol in symbol_array}
macd_death_cross_bar = {symbol: None for symbol in symbol_array}
macd_centerline_bar = {symbol: None for symbol in symbol_array}
current_bar_index = 0

# Strategy Parameters
RSI_PERIOD       = 14                   # Standard medium‑term RSI
MACD_FAST        = 6                   # MACD fast EMA
MACD_SLOW        = 13                   # MACD slow EMA
MACD_SIGNAL      = 5                    # MACD signal line EMA
MA_FAST          = 50                   # Higher‑timeframe fast MA
MA_MID           = 100                  # Higher‑timeframe mid MA
MA_SLOW          = 200                  # Higher‑timeframe slow MA
BUY_POWER_LIMIT  = 0.02                 # Limit the amount of buying power to use for the trade
MAX_RISK_PCT     = 0.03                 # 1–3% position sizing
TIMEFRAME_MAIN   = TimeFrameUnit.Minute # Suggested trading timeframePpyho
TIMEFRAME_TREND  = TimeFrameUnit.Day    # Trend‑defining timeframe

# Load environment variables
# Please safely store your API keys and never commit them to the repository (use .gitignore)
load_dotenv()
API_KEY = os.getenv("ALPACA_PAPER_API_KEY")
API_SECRET = os.getenv("ALPACA_PAPER_SECRET_KEY") 
ALPACA_PAPER_TRADE = os.getenv("ALPACA_PAPER_TRADE", "True")  # Default to paper trading (Returns "True" if ALPACA_PAPER_TRADE not set)
trade_api_url = os.getenv("TRADE_API_URL")
print("API_KEY:", API_KEY) 
print("API_SECRET:", API_SECRET) 
print("TRADE_API_URL:", trade_api_url)

if not API_KEY or not API_SECRET:
    raise RuntimeError("Missing Alpaca API credentials in environment variables.")

# setup trading clients
trade_client = TradingClient(api_key=API_KEY, secret_key=API_SECRET, paper=ALPACA_PAPER_TRADE, url_override=trade_api_url)
stock_data_client = StockHistoricalDataClient(api_key=API_KEY, secret_key=API_SECRET)

# Helper: Pause execution until the specified UTC datetime.
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
        
# Helper: Fetch recent bar data  
def fetch_bars(client: StockHistoricalDataClient, underlying_symbol: str, timeframe_unit: TimeFrameUnit, days: int = 90) -> pd.DataFrame:
    today = datetime.now(NY_TZ).date()
    req = StockBarsRequest(
        symbol_or_symbols=[underlying_symbol],
        timeframe=TimeFrame(amount=1, unit=timeframe_unit),  # specify timeframe
        start=today - timedelta(days=days),             # specify start datetime, default=the beginning of the current day.
    )
    return client.get_stock_bars(req).df

# Helper: Compute RSI with Wilder's smoothing
def compute_rsi(prices, period):
    deltas = prices.diff().dropna()
    gains = deltas.where(deltas > 0, 0)
    losses = (-deltas).where(deltas < 0, 0)
    # Calculate initial average gain and loss
    avg_gain = gains.rolling(period).mean()
    avg_loss = losses.rolling(period).mean()
    # Calculate RS and RSI for all periods
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# Helper: Compute MACD and its signal line
def compute_macd(prices, fast, slow, signal):
    ema_fast = prices.ewm(span=fast, adjust=False).mean()
    ema_slow = prices.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    return macd_line, signal_line

# Helper: Calculate buying power limit based on account value and risk percentage
def calculate_buying_power_limit(buy_power_limit):
    # Check account buying power
    buying_power = float(trade_client.get_account().buying_power)
    # Calculate the limit amount of buying power to use for the trade
    buying_power_limit = buying_power * buy_power_limit
    return buying_power_limit

# Helper: Get the latest price of the underlying stock
def get_underlying_price(symbol):
    # Get the latest trade for the underlying stock
    underlying_trade_request = StockLatestTradeRequest(symbol_or_symbols=symbol)
    underlying_trade_response = stock_data_client.get_stock_latest_trade(underlying_trade_request)
    return underlying_trade_response[symbol].price

def get_next_bar_time(current_bar_time, timeframe):
    """Calculate the next bar time based on the current timeframe"""
    if timeframe == TimeFrameUnit.Hour:
        return current_bar_time + timedelta(hours=1)
    elif timeframe == TimeFrameUnit.Day:
        return current_bar_time + timedelta(days=1)

def main():
    """
    Main trading loop and setup.
    This function initializes logging, sets up per-symbol tracking variables for trading signals and risk management, 
    and enters an infinite loop to execute the trading strategy. For each symbol in the trading universe, it:
    - Checks market open/close status and sleeps or exits as appropriate.
    - Fetches historical price data for indicator calculation.
    - Computes technical indicators (RSI, MACD) and moving averages for trend filtering.
    - Determines position sizing based on available buying power.
    - Detects entry signals (RSI bounce and MACD golden cross) and submits buy orders if conditions are met.
    - Tracks stop loss and take profit levels per symbol.
    - Detects exit signals (MACD death cross, centerline drop, stop loss, or take profit) and submits sell orders.
    - Logs all trading actions and indicator values for audit and debugging.
    - Schedules the next iteration to run at the top of the next minute.
    The loop continues indefinitely, managing positions and responding to market conditions in real time.
    """
    """Main trading loop and setup."""
    # Configure logging
    logging.basicConfig(
        filename="trade_log.txt",          # file to write
        level=logging.INFO,                # log INFO and above
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    logging.info("=== Strategy started ===")

    # remembers whether the market was open in the previous iteration
    clock = trade_client.get_clock()
    market_open = clock.is_open

    # Set tracking signal flags over a predefined window
    global rsi_bounce_bar, macd_cross_bar, rsi_retreat_bar, macd_death_cross_bar, macd_centerline_bar, current_bar_index
    rsi_bounce_bar = {symbol: None for symbol in symbol_array}
    macd_cross_bar = {symbol: None for symbol in symbol_array}
    rsi_retreat_bar = {symbol: None for symbol in symbol_array}
    macd_death_cross_bar = {symbol: None for symbol in symbol_array}
    macd_centerline_bar = {symbol: None for symbol in symbol_array}
    current_bar_index = 0

    # Per-symbol stop loss and take profit
    stop_loss_price = {symbol: None for symbol in symbol_array}
    take_profit_price = {symbol: None for symbol in symbol_array}

    while True:
        clock = trade_client.get_clock()

        for underlying_symbol in symbol_array:
            # Detect if the market has just transitioned from open to closed.
            if market_open and not clock.is_open:
                logging.info("Market closed. Sleeping until next open at %s", clock.next_open)
                market_open = False
                sleep_until(clock.next_open)
                continue        # skip the rest of the loop while the market is shut

            # Detect if the market has just transitioned from closed to open.
            if (not market_open) and clock.is_open:
                logging.info("Market opened. Resuming trading")
                market_open = True           # fall through and run the trading logic

            # Detect if the market is closed (e.g., at script start or unexpected state), exit to prevent trading.
            if not clock.is_open:
                logging.info("Market is closed. Exiting.")
                exit(0)
            
            # Fetch data
            df_main = fetch_bars(stock_data_client, underlying_symbol, TIMEFRAME_MAIN, days=MA_SLOW + 100)
            df_trend = fetch_bars(stock_data_client, underlying_symbol, TIMEFRAME_TREND, days=MA_SLOW + 10)
            logging.info("Fetched %d main bars and %d trend bars for %s", len(df_main), len(df_trend), underlying_symbol)
            
            # Update current bar index
            current_bar_index = len(df_main) - 1

            # Check if we currently hold the underlying_symbol
            try:
                position = trade_client.get_open_position(underlying_symbol)
                position_open = True
                current_qty = int(position.qty)
            except Exception as e:
                position_open = False
                current_qty = 0

            # Compute indicators using helper functions
            prices = df_main.close
            rsi_series = compute_rsi(prices, RSI_PERIOD)
            macd_line, signal_line = compute_macd(prices, MACD_FAST, MACD_SLOW, MACD_SIGNAL)

            # Get latest values
            rsi_now = rsi_series.iloc[-1]
            rsi_prev = rsi_series.iloc[-2]
            macd_now = macd_line.iloc[-1]
            macd_prev = macd_line.iloc[-2]
            sig_now = signal_line.iloc[-1]
            sig_prev = signal_line.iloc[-2]


            logging.info("%s: Indicator values - rsi_now: %.2f | rsi_prev: %.2f | macd_now: %.4f | macd_prev: %.4f | sig_now: %.4f | sig_prev: %.4f",
                underlying_symbol,
                rsi_now,
                rsi_prev,
                macd_now,
                macd_prev,
                sig_now,
                sig_prev
                )
            # Trend filter on higher timeframe with NaN check
            ma_fast = df_trend.close.rolling(MA_FAST).mean()
            ma_mid = df_trend.close.rolling(MA_MID).mean()
            ma_slow = df_trend.close.rolling(MA_SLOW).mean()
            
            # Check if we have enough data for all MAs
            if not (ma_fast.isna().any() or ma_mid.isna().any() or ma_slow.isna().any()):
                in_uptrend = (ma_fast.iloc[-1] > ma_mid.iloc[-1]) and (ma_mid.iloc[-1] > ma_slow.iloc[-1])
            else:
                in_uptrend = False

            # Calculate position size based on buying power
            buying_power_limit = calculate_buying_power_limit(BUY_POWER_LIMIT)
            current_price = get_underlying_price(underlying_symbol)
            if current_price > 0:
                if buying_power_limit < current_price:
                    position_size = 0
                    logging.warning("Buying power limit ($%.2f) is less than current price ($%.2f) for %s. Skipping trade for this symbol.", buying_power_limit, current_price, underlying_symbol)
                    continue  # Skip trading for this symbol in this iteration
                position_size = int(buying_power_limit / current_price)
            else:
                position_size = 0
                logging.warning("Current price for %s is zero or negative, cannot calculate position size.", underlying_symbol)
                logging.warning("Current price for %s is zero or negative, cannot calculate position size.", underlying_symbol)

            # Detect RSI oversold bounce
            if (rsi_prev < 35) and (rsi_now > 35):
                rsi_bounce_bar[underlying_symbol] = current_bar_index
            # Detect MACD golden cross
            if (macd_prev < sig_prev) and (macd_now > sig_now):
                macd_cross_bar[underlying_symbol] = current_bar_index

            # Entry logic: Only RSI bounce over 30 is used 
            # Volyymisuodatin
            volume_series = df_main.volume
            volume_now = volume_series.iloc[-1]
            volume_avg = volume_series.rolling(window=20).mean().iloc[-1]
            volume_ok = volume_now > volume_avg

            # Hintakäyttäytyminen (price action)
            recent_lows = prices.tail(5).rolling(window=2).min()
            recent_highs = prices.tail(5).rolling(window=2).max()
            higher_low = recent_lows.iloc[-1] > recent_lows.iloc[-2]
            breakout = prices.iloc[-1] > recent_highs.iloc[-2]
            price_action_ok = higher_low and breakout

            if not position_open and position_size > 0 and in_uptrend and volume_ok and price_action_ok:
                if (rsi_bounce_bar[underlying_symbol] is not None and 
                    macd_cross_bar[underlying_symbol] is not None):
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
                        underlying_symbol,
                        position_size,
                        current_price,
                        res.id,
                        res.client_order_id,
                        res.submitted_at
                    )
                    # Set per-symbol stop loss and take profit
                    stop_loss_price[underlying_symbol] = current_price * 0.97
                    take_profit_price[underlying_symbol] = current_price * 1.005
                    # Reset entry signals
                    rsi_bounce_bar[underlying_symbol] = None
                    macd_cross_bar[underlying_symbol] = None

            # Detect RSI overbought retreat
            if (rsi_prev > 70) and (rsi_now < 65):
                rsi_retreat_bar[underlying_symbol] = current_bar_index

            # Detect MACD bearish signals
            if (macd_prev > sig_prev) and (macd_now < sig_now):  # death cross
                macd_death_cross_bar[underlying_symbol] = current_bar_index
            elif macd_prev > 0 and macd_now < 0:  # centerline drop
                macd_centerline_bar[underlying_symbol] = current_bar_index

            # Exit logic
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
                        underlying_symbol,
                        current_qty,
                        current_price,
                        res.id,
                        res.client_order_id,
                        res.submitted_at
                    )
                    logging.info("SELL triggered by: %s", exit_reason)

                    # Reset exit signals
                    rsi_retreat_bar[underlying_symbol] = None
                    macd_death_cross_bar[underlying_symbol] = None
                    macd_centerline_bar[underlying_symbol] = None
                    stop_loss_price[underlying_symbol] = None
                    take_profit_price[underlying_symbol] = None

        # Hourly scheduling
        # Compute the timestamp for the next top of hour
        now = datetime.now(timezone.utc)
        next_run = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
        sleep_until(next_run, chunk_seconds=10)
# The code below ensures that the main() function is called only when this script is executed directly.
# It prevents main() from running if the script is imported as a module in another script.
if __name__ == "__main__":
    main()
