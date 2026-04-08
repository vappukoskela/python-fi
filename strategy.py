
import yfinance as yf
import pandas as pd
from zoneinfo import ZoneInfo

# === FETCH SPY 1-MINUTE BARS FOR TODAY ===
print("Fetching SPY 1-minute bars for 2026-04-02...")

spy = yf.download("SPY", start="2026-04-08", end="2026-04-09", interval="1m", progress=False)

if spy.empty:
    print("ERROR: No data returned. Market may still be open or yfinance issue.")
else:
    # Flatten multi-level columns if present
    if isinstance(spy.columns, pd.MultiIndex):
        spy.columns = spy.columns.get_level_values(0)

    # Convert index to proper timezone-aware timestamps
    spy.index = pd.to_datetime(spy.index)
    if spy.index.tz is None:
        spy.index = spy.index.tz_localize("UTC")

    # Add ET and Helsinki time columns
    spy["et_time"]       = spy.index.tz_convert("America/New_York")
    spy["helsinki_time"] = spy.index.tz_convert("Europe/Helsinki")

    # Only keep market hours 9:30 - 16:00 ET
    spy_et = spy["et_time"]
    spy = spy[(spy_et.dt.hour > 9) | ((spy_et.dt.hour == 9) & (spy_et.dt.minute >= 30))]
    spy = spy[spy_et.dt.hour < 16]

    # Calculate move from open
    open_price = spy["Close"].iloc[0]
    spy["move_from_open_pct"] = (spy["Close"] - open_price) / open_price * 100

    # Calculate rolling high watermark — tracks peak SPY reached
    spy["session_high_pct"] = spy["move_from_open_pct"].cummax()

    # Save to CSV
    output_file = "spy_session_2026-04-08.csv"
    spy[["et_time", "helsinki_time", "Close", "move_from_open_pct", "session_high_pct"]].to_csv(output_file, index=False)
    print(f"Saved to {output_file}")
    print(f"\nOpen price: {open_price:.2f}")
    print(f"Session high: {spy['Close'].max():.2f} ({spy['session_high_pct'].max():.2f}%)")
    print(f"Session low:  {spy['Close'].min():.2f} ({spy['move_from_open_pct'].min():.2f}%)")
    print(f"Close price:  {spy['Close'].iloc[-1]:.2f} ({spy['move_from_open_pct'].iloc[-1]:.2f}%)")
    print(f"\nFirst 5 rows:")
    print(spy[["et_time", "helsinki_time", "Close", "move_from_open_pct"]].head().to_string())
    print(f"\nLast 5 rows:")
    print(spy[["et_time", "helsinki_time", "Close", "move_from_open_pct"]].tail().to_string())
           
