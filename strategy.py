import os
import pandas as pd
from dotenv import load_dotenv
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from datetime import datetime
from zoneinfo import ZoneInfo

load_dotenv()
API_KEY = os.getenv("ALPACA_PAPER_API_KEY")
API_SECRET = os.getenv("ALPACA_PAPER_SECRET_KEY")

client = StockHistoricalDataClient(API_KEY, API_SECRET)

# Set the date you want — today is 2026-03-30
today = datetime(2026, 3, 30, tzinfo=ZoneInfo("America/New_York"))
market_open  = today.replace(hour=9,  minute=30)
market_close = today.replace(hour=16, minute=0)

req = StockBarsRequest(
    symbol_or_symbols="SPY",
    start=market_open,
    end=market_close,
    timeframe=TimeFrame(1, TimeFrameUnit.Minute)
)

bars = client.get_stock_bars(req).df
bars = bars.reset_index()
bars["et_time"] = bars["timestamp"].dt.tz_convert("America/New_York")
bars["helsinki_time"] = bars["timestamp"].dt.tz_convert("Europe/Helsinki")
bars["move_from_open_pct"] = (bars["close"] - bars["close"].iloc[0]) / bars["close"].iloc[0] * 100

bars.to_csv("spy_session_2026-03-30.csv", index=False)
print(bars[["et_time", "helsinki_time", "close", "move_from_open_pct"]].to_string())
           
