
import yfinance as yf
import pandas as pd

symbols = ['AMD', 'NVDA', 'GOOG']
for sym in symbols:
    ticker = yf.Ticker(sym)
    df = ticker.history(start='2026-04-07', end='2026-04-08', interval='1m')
    df.index = df.index.tz_convert('US/Eastern')
    mask = (df.index.time >= pd.Timestamp('11:00').time()) & \
           (df.index.time <= pd.Timestamp('11:50').time())
    filtered = df[mask][['Close']]
    print(f"\n=== {sym} ===")
    print(filtered.to_string())
