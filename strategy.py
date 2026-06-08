"""
Fetch 1-minute session data for all tradable symbols.

Run after market close (or whenever yfinance has the day's data).
Produces one CSV per day with one row per (symbol, minute) showing what
each symbol looked like at that moment.

Usage:
    Edit TARGET_DATE below, then run:
    python universe_session.py

Output:
    universe_session_YYYY-MM-DD.csv

Each row has:
    - timestamp columns (ET and Helsinki)
    - symbol
    - OHLC and volume for the minute
    - move_from_open_pct: % change from session open close
    - session_high_pct: peak move from open up to this point
    - minute_range_pct: intra-minute range (high-low)/close as %
    - vwap: rolling volume-weighted average price within session
    - dist_from_vwap_pct: how far above/below VWAP
    - high_5bar: highest close in prior 5 minutes (excluding current)
    - dist_to_breakout_pct: distance from breakout level (5bar_high * 1.001)

Reliability:
    yfinance occasionally fails with a TypeError on high-volume symbols
    (Yahoo's servers sometimes return malformed responses, especially for
    AAPL/AMD/NVDA/etc.). This script retries failed symbols up to 3 times
    with progressively longer delays.
"""

import yfinance as yf
import pandas as pd
import time
from zoneinfo import ZoneInfo

# === CONFIGURATION ===

TARGET_DATE = "2026-06-08"  # Edit this each day

# Universe - matches scalper.py TRADABLE_UNIVERSE
SYMBOLS = [
    "NVDA", "AMD", "TSLA", "AAPL", "MSFT", "AMZN", "GOOG", "META",
    "MU", "QCOM", "AVGO", "SMCI",
    "CRM", "ORCL", "ADSK", "NFLX", "PLTR", "SHOP",
    "V", "JPM", "C",
    "UBER", "XYZ",
]

BREAKOUT_BARS = 5  # matches bot - 5-minute lookback for breakout level

# Reliability settings (added to fix recurring fetch failures)
BASE_DELAY_SEC = 1.0       # delay between successful fetches (was 0.3)
MAX_RETRIES = 3            # how many times to retry a failed symbol
RETRY_DELAYS = [3, 8, 15]  # seconds to wait before each retry attempt


def fetch_symbol_session(symbol, target_date):
    """Fetch 1-minute bars for one symbol on target_date.

    Returns DataFrame on success, None if data is empty/missing.
    Does NOT raise - yfinance errors are caught and treated as empty.
    """
    end_date = (pd.Timestamp(target_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    try:
        df = yf.download(
            symbol,
            start=target_date,
            end=end_date,
            interval="1m",
            progress=False,
            auto_adjust=False,
        )
    except Exception:
        # Internal yfinance error - treat as empty
        return None

    if df is None or df.empty:
        return None

    # Flatten multi-level columns if present
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    # Timezone handling
    df.index = pd.to_datetime(df.index)
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")

    df["et_time"] = df.index.tz_convert("America/New_York")
    df["helsinki_time"] = df.index.tz_convert("Europe/Helsinki")

    # Restrict to regular market hours 9:30-16:00 ET
    et = df["et_time"]
    df = df[(et.dt.hour > 9) | ((et.dt.hour == 9) & (et.dt.minute >= 30))]
    df = df[df["et_time"].dt.hour < 16]

    if df.empty:
        return None

    df["symbol"] = symbol
    return df


def fetch_with_retry(symbol, target_date):
    """Fetch a symbol's data with retries on failure.

    Returns DataFrame on success after up to MAX_RETRIES+1 total attempts,
    or None if all attempts failed.
    """
    df = fetch_symbol_session(symbol, target_date)
    if df is not None and not df.empty:
        return df

    # First attempt failed - try retries
    for attempt in range(MAX_RETRIES):
        delay = RETRY_DELAYS[attempt]
        print(f"retrying in {delay}s...", end=" ", flush=True)
        time.sleep(delay)
        df = fetch_symbol_session(symbol, target_date)
        if df is not None and not df.empty:
            return df

    return None


def compute_session_metrics(df):
    """Add the analysis columns to a symbol's session DataFrame."""
    open_price = float(df["Close"].iloc[0])
    df["session_open"] = open_price

    # Move from open at each minute
    df["move_from_open_pct"] = (df["Close"] - open_price) / open_price * 100

    # Rolling session high watermark
    df["session_high_pct"] = df["move_from_open_pct"].cummax()

    # Intra-minute range
    df["minute_range_pct"] = (df["High"] - df["Low"]) / df["Close"] * 100

    # Session VWAP (cumulative volume-weighted)
    typical = (df["High"] + df["Low"] + df["Close"]) / 3.0
    cum_vol = df["Volume"].cumsum().replace(0, pd.NA)
    cum_pv = (typical * df["Volume"]).cumsum()
    df["vwap"] = cum_pv / cum_vol
    df["dist_from_vwap_pct"] = (df["Close"] - df["vwap"]) / df["vwap"] * 100

    # 5-bar high (excluding current minute) - matches bot's breakout reference
    df["high_5bar"] = df["High"].shift(1).rolling(BREAKOUT_BARS).max()
    breakout_level = df["high_5bar"] * 1.001
    df["dist_to_breakout_pct"] = (breakout_level - df["Close"]) / df["Close"] * 100

    return df


def main():
    print(f"=== Fetching universe session data for {TARGET_DATE} ===")
    print(f"Symbols: {len(SYMBOLS)}")
    print(f"Settings: base_delay={BASE_DELAY_SEC}s, max_retries={MAX_RETRIES}")
    print()

    all_frames = []
    success_count = 0
    failed_symbols = []

    for symbol in SYMBOLS:
        print(f"  {symbol}...", end=" ", flush=True)
        df = fetch_with_retry(symbol, TARGET_DATE)
        if df is None or df.empty:
            print("FAILED after all retries")
            failed_symbols.append(symbol)
            continue
        df = compute_session_metrics(df)
        all_frames.append(df)
        success_count += 1
        print(f"OK ({len(df)} bars)")
        # Polite delay between successful fetches
        time.sleep(BASE_DELAY_SEC)

    if not all_frames:
        print("\nERROR: No data fetched for any symbol.")
        return

    print()
    print(f"=== Combining {success_count}/{len(SYMBOLS)} symbols ===")

    if failed_symbols:
        print(f"WARNING - FAILED: {', '.join(failed_symbols)}")
        print(f"   {len(failed_symbols)} symbols missing from output")

    combined = pd.concat(all_frames, ignore_index=False)
    combined = combined.sort_values(["symbol", "et_time"])

    # Output columns in order
    output_cols = [
        "et_time", "helsinki_time", "symbol",
        "Open", "High", "Low", "Close", "Volume",
        "session_open",
        "move_from_open_pct", "session_high_pct", "minute_range_pct",
        "vwap", "dist_from_vwap_pct",
        "high_5bar", "dist_to_breakout_pct",
    ]

    out = combined[output_cols].copy()

    output_file = f"universe_session_{TARGET_DATE}.csv"
    out.to_csv(output_file, index=False)
    print(f"Saved {len(out)} rows to {output_file}")
    print()

    # Quick summary - show each symbol's day at a glance
    print("=== Per-symbol summary ===")
    print(f"{'Symbol':<6} {'Open':<10} {'High%':<8} {'Low%':<8} {'Close%':<8} {'Range%':<8} {'Bars':<5}")
    print("-" * 60)
    for sym in SYMBOLS:
        sub = out[out["symbol"] == sym]
        if sub.empty:
            continue
        open_p = sub["session_open"].iloc[0]
        high_pct = sub["session_high_pct"].max()
        low_pct = sub["move_from_open_pct"].min()
        close_pct = sub["move_from_open_pct"].iloc[-1]
        max_range = sub["minute_range_pct"].max()
        print(f"{sym:<6} ${open_p:<9.2f} {high_pct:+.2f}%   {low_pct:+.2f}%   "
              f"{close_pct:+.2f}%   {max_range:.2f}%   {len(sub)}")


if __name__ == "__main__":
    main()
           
