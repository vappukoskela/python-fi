"""
Fetch SPY 1-minute session data.

Run after market close (or whenever yfinance has the day's data).
Produces one CSV per day showing SPY's move from open at each minute.

Usage:
    Edit TARGET_DATE below, then run:
    python spy_session.py

Output:
    spy_session_YYYY-MM-DD.csv

Columns:
    - et_time, helsinki_time, Close
    - move_from_open_pct: % change from session open close
    - session_high_pct: peak move from open up to this point

Reliability:
    yfinance occasionally fails with a TypeError when Yahoo's servers
    return a malformed response. This script retries the fetch up to
    3 times with progressively longer delays.
"""

import yfinance as yf
import pandas as pd
import time
from zoneinfo import ZoneInfo

# === CONFIGURATION ===

TARGET_DATE = "2026-06-05"  # Edit this each day

# Reliability settings
MAX_RETRIES = 3
RETRY_DELAYS = [3, 8, 15]  # seconds between attempts


def fetch_spy(target_date):
    """Fetch SPY 1-minute bars. Returns DataFrame or None on failure.
    Does NOT raise - yfinance errors are caught and treated as empty.
    """
    end_date = (pd.Timestamp(target_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    try:
        df = yf.download(
            "SPY",
            start=target_date,
            end=end_date,
            interval="1m",
            progress=False,
            auto_adjust=False,
        )
    except Exception:
        return None
    if df is None or df.empty:
        return None
    return df


def fetch_spy_with_retry(target_date):
    """Fetch SPY with up to MAX_RETRIES+1 total attempts.
    Returns DataFrame on success, None if all attempts failed."""
    df = fetch_spy(target_date)
    if df is not None and not df.empty:
        return df

    for attempt in range(MAX_RETRIES):
        delay = RETRY_DELAYS[attempt]
        print(f"  retrying in {delay}s (attempt {attempt + 2}/{MAX_RETRIES + 1})...")
        time.sleep(delay)
        df = fetch_spy(target_date)
        if df is not None and not df.empty:
            return df

    return None


def main():
    print(f"Fetching SPY 1-minute bars for {TARGET_DATE}...")

    spy = fetch_spy_with_retry(TARGET_DATE)
    if spy is None or spy.empty:
        print("ERROR: No data returned after retries. yfinance may be down, or market is still open.")
        return

    # Flatten multi-level columns if present
    if isinstance(spy.columns, pd.MultiIndex):
        spy.columns = spy.columns.get_level_values(0)

    # Convert index to proper timezone-aware timestamps
    spy.index = pd.to_datetime(spy.index)
    if spy.index.tz is None:
        spy.index = spy.index.tz_localize("UTC")

    # Add ET and Helsinki time columns
    spy["et_time"] = spy.index.tz_convert("America/New_York")
    spy["helsinki_time"] = spy.index.tz_convert("Europe/Helsinki")

    # Only keep market hours 9:30 - 16:00 ET
    spy_et = spy["et_time"]
    spy = spy[(spy_et.dt.hour > 9) | ((spy_et.dt.hour == 9) & (spy_et.dt.minute >= 30))]
    spy = spy[spy["et_time"].dt.hour < 16]

    if spy.empty:
        print("ERROR: No bars in regular market hours.")
        return

    # Calculate move from open
    open_price = float(spy["Close"].iloc[0])
    spy["move_from_open_pct"] = (spy["Close"] - open_price) / open_price * 100

    # Calculate rolling high watermark
    spy["session_high_pct"] = spy["move_from_open_pct"].cummax()

    # Save to CSV
    output_file = f"spy_session_{TARGET_DATE}.csv"
    spy[["et_time", "helsinki_time", "Close", "move_from_open_pct", "session_high_pct"]].to_csv(output_file, index=False)
    print(f"Saved to {output_file}")
    print()
    print(f"Open price:   {open_price:.2f}")
    print(f"Session high: {spy['Close'].max():.2f} ({spy['session_high_pct'].max():.2f}%)")
    print(f"Session low:  {spy['Close'].min():.2f} ({spy['move_from_open_pct'].min():.2f}%)")
    print(f"Close price:  {spy['Close'].iloc[-1]:.2f} ({spy['move_from_open_pct'].iloc[-1]:.2f}%)")
    print()
    print("First 5 rows:")
    print(spy[["et_time", "helsinki_time", "Close", "move_from_open_pct"]].head().to_string())
    print()
    print("Last 5 rows:")
    print(spy[["et_time", "helsinki_time", "Close", "move_from_open_pct"]].tail().to_string())


if __name__ == "__main__":
    main()
           
