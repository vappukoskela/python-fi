import yfinance as yf
import pandas as pd
from zoneinfo import ZoneInfo

# === FETCH QCOM 1-MINUTE BARS FOR TODAY ===
print("Fetching QCOM 1-minute bars for 2026-05-26...")
qcom = yf.download("QCOM", start="2026-05-26", end="2026-05-27", interval="1m", progress=False)

if qcom.empty:
    print("ERROR: No data returned. Market may still be open or yfinance issue.")
else:
    # Flatten multi-level columns if present
    if isinstance(qcom.columns, pd.MultiIndex):
        qcom.columns = qcom.columns.get_level_values(0)

    # Convert index to proper timezone-aware timestamps
    qcom.index = pd.to_datetime(qcom.index)
    if qcom.index.tz is None:
        qcom.index = qcom.index.tz_localize("UTC")

    # Add ET and Helsinki time columns
    qcom["et_time"]       = qcom.index.tz_convert("America/New_York")
    qcom["helsinki_time"] = qcom.index.tz_convert("Europe/Helsinki")

    # Only keep market hours 9:30 - 16:00 ET
    qcom_et = qcom["et_time"]
    qcom = qcom[(qcom_et.dt.hour > 9) | ((qcom_et.dt.hour == 9) & (qcom_et.dt.minute >= 30))]
    qcom = qcom[qcom_et.dt.hour < 16]

    # Calculate move from open
    open_price = qcom["Close"].iloc[0]
    qcom["move_from_open_pct"] = (qcom["Close"] - open_price) / open_price * 100

    # Calculate rolling high watermark
    qcom["session_high_pct"] = qcom["move_from_open_pct"].cummax()

    # Compute 1-minute range (high - low) as % — KEY for stop analysis
    qcom["minute_range_pct"] = (qcom["High"] - qcom["Low"]) / qcom["Close"] * 100

    # Save to CSV — include High, Low, Open for proper analysis
    output_file = "qcom_session_2026-05-26.csv"
    qcom[["et_time", "helsinki_time", "Open", "High", "Low", "Close",
          "move_from_open_pct", "session_high_pct", "minute_range_pct"]].to_csv(output_file, index=False)
    print(f"Saved to {output_file}")

    print(f"\nOpen price: {open_price:.2f}")
    print(f"Session high: {qcom['High'].max():.2f}")
    print(f"Session low:  {qcom['Low'].min():.2f}")
    print(f"Close price:  {qcom['Close'].iloc[-1]:.2f} ({qcom['move_from_open_pct'].iloc[-1]:.2f}%)")
    print(f"\nMax 1-minute range: {qcom['minute_range_pct'].max():.3f}%")
    print(f"Avg 1-minute range: {qcom['minute_range_pct'].mean():.3f}%")

    # Focus on the disaster window — 10:44 to 10:50 ET
    print(f"\n=== QCOM rapid-fire window (10:44-10:50 ET) ===")
    disaster = qcom[(qcom_et.dt.hour == 10) & (qcom_et.dt.minute >= 44) & (qcom_et.dt.minute <= 50)]
    print(disaster[["et_time", "Open", "High", "Low", "Close", "minute_range_pct"]].to_string())
           
