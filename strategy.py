import yfinance as yf
import pandas as pd
from zoneinfo import ZoneInfo

# === FETCH AMD 1-MINUTE BARS FOR TODAY ===
print("Fetching AMD 1-minute bars for 2026-05-26...")
amd = yf.download("AMD", start="2026-05-26", end="2026-05-27", interval="1m", progress=False)

if amd.empty:
    print("ERROR: No data returned.")
else:
    if isinstance(amd.columns, pd.MultiIndex):
        amd.columns = amd.columns.get_level_values(0)

    amd.index = pd.to_datetime(amd.index)
    if amd.index.tz is None:
        amd.index = amd.index.tz_localize("UTC")

    amd["et_time"]       = amd.index.tz_convert("America/New_York")
    amd["helsinki_time"] = amd.index.tz_convert("Europe/Helsinki")

    amd_et = amd["et_time"]
    amd = amd[(amd_et.dt.hour > 9) | ((amd_et.dt.hour == 9) & (amd_et.dt.minute >= 30))]
    amd = amd[amd_et.dt.hour < 16]

    open_price = amd["Close"].iloc[0]
    amd["move_from_open_pct"] = (amd["Close"] - open_price) / open_price * 100
    amd["session_high_pct"] = amd["move_from_open_pct"].cummax()
    amd["minute_range_pct"] = (amd["High"] - amd["Low"]) / amd["Close"] * 100

    output_file = "amd_session_2026-05-26.csv"
    amd[["et_time", "helsinki_time", "Open", "High", "Low", "Close",
         "move_from_open_pct", "session_high_pct", "minute_range_pct"]].to_csv(output_file, index=False)

    print(f"Saved to {output_file}")
    print(f"\nOpen: {open_price:.2f}")
    print(f"Session high: {amd['High'].max():.2f}")
    print(f"Session low:  {amd['Low'].min():.2f}")
    print(f"Close: {amd['Close'].iloc[-1]:.2f} ({amd['move_from_open_pct'].iloc[-1]:.2f}%)")

    print(f"\nMax 1-minute range: {amd['minute_range_pct'].max():.3f}%")
    print(f"Avg 1-minute range: {amd['minute_range_pct'].mean():.3f}%")

    # AMD trade was 12:19 ET, stopped 12:20 ET (Helsinki 19:19-19:20)
    print(f"\n=== AMD around the trade window (12:18-12:25 ET) ===")
    window = amd[(amd_et.dt.hour == 12) & (amd_et.dt.minute >= 18) & (amd_et.dt.minute <= 25)]
    print(window[["et_time", "Open", "High", "Low", "Close", "minute_range_pct"]].to_string())
