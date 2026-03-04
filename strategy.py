"""
Run this on your machine where alpaca-py is installed.
It fetches 1-minute bars around each losing trade entry
and shows whether price was already falling before entry.
"""

import os, time
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from dotenv import load_dotenv

load_dotenv()
API_KEY    = os.getenv("ALPACA_PAPER_API_KEY")
API_SECRET = os.getenv("ALPACA_PAPER_SECRET_KEY")
client = StockHistoricalDataClient(API_KEY, API_SECRET)
ET = ZoneInfo("America/New_York")

# Losing trades sampled from today's Alpaca log
# (symbol, buy_time_UTC, buy_price, sell_price, qty)
TRADES = [
    ("SHOP",  "2026-03-04 20:51:26", 129.850,  129.472, 84),
    ("SHOP",  "2026-03-04 20:33:54", 129.687,  129.410, 88),
    ("SHOP",  "2026-03-04 21:10:59", 128.527,  128.080, 88),
    ("PLTR",  "2026-03-04 22:00:16", 153.833,  152.710, 71),
    ("PLTR",  "2026-03-04 21:52:36", 154.344,  154.120, 74),
    ("PLTR",  "2026-03-04 22:23:18", 153.163,  153.031, 74),
    ("AVGO",  "2026-03-04 21:05:28", 321.390,  321.282, 35),
    ("AVGO",  "2026-03-04 20:37:59", 322.150,  320.573, 34),
    ("NVDA",  "2026-03-04 21:10:56", 184.274,  184.166, 62),
    ("TSLA",  "2026-03-04 18:41:11", 404.900,  404.290, 28),
    ("TSLA",  "2026-03-04 18:35:39", 405.230,  404.784, 28),
    ("TSLA",  "2026-03-04 18:17:55", 402.830,  402.630, 28),
    ("ADSK",  "2026-03-04 22:40:13", 256.570,  255.463, 47),
    ("EBAY",  "2026-03-04 23:39:08", 91.470,   91.140, 129),
]

LOOKBACK_MIN  = 10   # bars before entry to show
LOOKAHEAD_MIN = 10   # bars after entry to show

for sym, buy_utc_str, buy_px, sell_px, qty in TRADES:
    pnl = (sell_px - buy_px) * qty
    buy_dt = datetime.strptime(buy_utc_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    start  = buy_dt - timedelta(minutes=LOOKBACK_MIN)
    end    = buy_dt + timedelta(minutes=LOOKAHEAD_MIN + 5)

    try:
        req  = StockBarsRequest(
            symbol_or_symbols=sym, start=start, end=end,
            timeframe=TimeFrame(1, TimeFrameUnit.Minute)
        )
        df = client.get_stock_bars(req).df
        if df is None or df.empty:
            print(f"{sym}: no data\n"); continue

        df = df.reset_index()
        ts_col = "timestamp" if "timestamp" in df.columns else df.columns[0]
        df = df.set_index(ts_col)
        df.index = df.index.tz_convert(ET)

        entry_et = buy_dt.astimezone(ET)
        entry_min = entry_et.replace(second=0, microsecond=0)

        print(f"{'='*65}")
        print(f"{sym}  BUY={buy_px:.3f}  SELL={sell_px:.3f}  qty={qty}  PnL=${pnl:.1f}")
        print(f"Entry time ET: {entry_et.strftime('%H:%M:%S')}")
        print(f"{'Time':7s} {'O':8s} {'H':8s} {'L':8s} {'C':8s} {'Vol':7s}  {'Dir':3s}  Note")
        print("-"*65)

        for ts, row in df.iterrows():
            bar_min  = ts.replace(second=0, microsecond=0)
            delta    = int((bar_min - entry_min).total_seconds() // 60)
            o, h, l, c = float(row['open']), float(row['high']), float(row['low']), float(row['close'])
            vol      = int(row.get('volume', 0))
            dir_sym  = "↑" if c >= o else "↓"

            note = ""
            if delta == 0:   note = f"◄ ENTRY @ {buy_px:.3f}"
            elif delta == 1: note = "+1min"
            elif delta == 2: note = "+2min"
            elif delta == 3: note = "+3min"
            elif delta == -1: note = "-1min (pre-entry)"
            elif delta == -2: note = "-2min (pre-entry)"
            elif delta == -3: note = "-3min (pre-entry)"

            # Flag if bar before entry was already falling hard
            if delta == -1 and c < o:
                note += "  *** FALLING PRE-ENTRY"
            if delta == -2 and c < o:
                note += "  ** falling"

            print(f"{ts.strftime('%H:%M'):7s} {o:8.3f} {h:8.3f} {l:8.3f} {c:8.3f} {vol:7d}  {dir_sym}    {note}")

        # Summary verdict
        pre_bars  = [(ts, row) for ts, row in df.iterrows()
                     if -3 <= int((ts.replace(second=0,microsecond=0)-entry_min).total_seconds()//60) <= -1]
        post_bars = [(ts, row) for ts, row in df.iterrows()
                     if 1  <= int((ts.replace(second=0,microsecond=0)-entry_min).total_seconds()//60) <= 3]

        pre_falling  = sum(1 for _, r in pre_bars  if float(r['close']) < float(r['open']))
        post_falling = sum(1 for _, r in post_bars if float(r['close']) < float(r['open']))

        verdict = ""
        if pre_falling >= 2:
            verdict = "LATE ENTRY: price already falling 2+ bars before entry"
        elif pre_falling == 1 and post_falling >= 2:
            verdict = "BORDERLINE LATE: 1 pre-bar falling + continued down after"
        elif post_falling >= 2:
            verdict = "TIMING OK but momentum failed after entry"
        else:
            verdict = "TIMING MIXED - needs manual review"

        print(f"\n  → VERDICT: {verdict}")
        print(f"  → Pre-entry falling bars: {pre_falling}/3 | Post-entry falling: {post_falling}/3\n")
        time.sleep(0.25)

    except Exception as e:
        print(f"{sym}: ERROR {e}\n")
           
