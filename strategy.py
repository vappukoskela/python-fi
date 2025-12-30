# Save as analyze_buy_to_sell_by_buy_regime.py and run with Python 3.9+
import pandas as pd
import numpy as np

INFILE = "audit_trades_live.csv"
OUTFILE = "trade_summary_by_buy_regime_bias.csv"

# Read CSV and normalize
df = pd.read_csv(INFILE, parse_dates=["timestamp"], infer_datetime_format=True)
df = df.sort_values("timestamp").reset_index(drop=True)

# Coerce pnl to numeric, NaN for empty
df["pnl_num"] = pd.to_numeric(df["pnl"], errors="coerce")

# Build index of next SELL per symbol
# For speed: group by symbol and iterate
paired_rows = []  # list of dicts: buy_idx, buy_ts, buy_regime, buy_bias, sell_idx, sell_ts, sell_pnl
by_symbol = df.groupby("symbol", sort=False)

for sym, group in by_symbol:
    group = group.sort_values("timestamp").reset_index()
    # iterate rows in time order, track buys and match to next sell
    pending_buys = []
    for _, row in group.iterrows():
        action = str(row.get("action", "")).strip().upper()
        if action == "BUY":
            pending_buys.append(row)
        elif action == "SELL":
            # match this sell to the earliest pending buy (FIFO)
            if pending_buys:
                buy_row = pending_buys.pop(0)
                paired_rows.append({
                    "buy_index": int(buy_row["index"]),
                    "buy_ts": buy_row["timestamp"],
                    "buy_regime": buy_row.get("regime", ""),
                    "buy_bias": buy_row.get("bias", ""),
                    "sell_index": int(row["index"]),
                    "sell_ts": row["timestamp"],
                    "sell_pnl": row["pnl_num"]
                })
            else:
                # orphan sell, ignore for buy->sell pairing
                pass
    # any remaining pending_buys are unpaired; record with sell_pnl = NaN
    for buy_row in pending_buys:
        paired_rows.append({
            "buy_index": int(buy_row["index"]),
            "buy_ts": buy_row["timestamp"],
            "buy_regime": buy_row.get("regime", ""),
            "buy_bias": buy_row.get("bias", ""),
            "sell_index": np.nan,
            "sell_ts": pd.NaT,
            "sell_pnl": np.nan
        })

paired_df = pd.DataFrame(paired_rows)

# Classify outcomes
def classify(p):
    if pd.isna(p):
        return "neutral"
    if p > 0:
        return "win"
    if p < 0:
        return "loss"
    return "neutral"

paired_df["outcome"] = paired_df["sell_pnl"].apply(classify)

# Aggregate by buy_regime and buy_bias
agg = paired_df.groupby(["buy_regime", "buy_bias"], dropna=False).agg(
    count=("buy_index", "count"),
    wins=("outcome", lambda s: (s == "win").sum()),
    losses=("outcome", lambda s: (s == "loss").sum()),
    neutrals=("outcome", lambda s: (s == "neutral").sum()),
    total_pnl=("sell_pnl", lambda s: s.dropna().sum() if s.dropna().size > 0 else np.nan),
    avg_pnl=("sell_pnl", lambda s: s.dropna().mean() if s.dropna().size > 0 else np.nan)
).reset_index()

# Compute win_rate where wins+losses > 0
agg["win_rate"] = agg.apply(
    lambda r: (r["wins"] / (r["wins"] + r["losses"])) if (r["wins"] + r["losses"]) > 0 else np.nan,
    axis=1
)

# Formatting numeric columns
agg["win_rate_pct"] = agg["win_rate"].apply(lambda x: f"{x*100:.1f}%" if not pd.isna(x) else "")
agg["total_pnl"] = agg["total_pnl"].apply(lambda x: f"{x:.2f}" if not pd.isna(x) else "")
agg["avg_pnl"] = agg["avg_pnl"].apply(lambda x: f"{x:.2f}" if not pd.isna(x) else "")

# Save CSV with raw numeric values as well
agg_out = paired_df.groupby(["buy_regime", "buy_bias"], dropna=False).agg(
    count=("buy_index", "count"),
    wins=("outcome", lambda s: (s == "win").sum()),
    losses=("outcome", lambda s: (s == "loss").sum()),
    neutrals=("outcome", lambda s: (s == "neutral").sum()),
    win_rate=("outcome", lambda s: (s == "win").sum() / max(1, ((s == "win").sum() + (s == "loss").sum())) if ((s == "win").sum() + (s == "loss").sum()) > 0 else np.nan),
    total_pnl=("sell_pnl", lambda s: s.dropna().sum() if s.dropna().size > 0 else np.nan),
    avg_pnl=("sell_pnl", lambda s: s.dropna().mean() if s.dropna().size > 0 else np.nan)
).reset_index()

# Format numeric columns for CSV
agg_out["win_rate"] = agg_out["win_rate"].apply(lambda x: f"{x*100:.1f}%" if not pd.isna(x) else "")
agg_out["total_pnl"] = agg_out["total_pnl"].apply(lambda x: f"{x:.2f}" if not pd.isna(x) else "")
agg_out["avg_pnl"] = agg_out["avg_pnl"].apply(lambda x: f"{x:.2f}" if not pd.isna(x) else "")

agg_out = agg_out.rename(columns={"buy_regime": "buy_regime", "buy_bias": "buy_bias"})
agg_out.to_csv(OUTFILE, index=False)

# Print summary tables to console
print("\nSummary grouped by BUY regime and BUY bias\n")
print(agg_out.to_string(index=False))

# Also produce SELL-only summary aggregated by buy-side grouping
sell_only = paired_df[paired_df["sell_index"].notna()]
sell_agg = sell_only.groupby(["buy_regime", "buy_bias"], dropna=False).agg(
    count=("buy_index", "count"),
    wins=("outcome", lambda s: (s == "win").sum()),
    losses=("outcome", lambda s: (s == "loss").sum()),
    neutrals=("outcome", lambda s: (s == "neutral").sum()),
    win_rate=("outcome", lambda s: (s == "win").sum() / ((s == "win").sum() + (s == "loss").sum()) if ((s == "win").sum() + (s == "loss").sum()) > 0 else np.nan),
    total_pnl=("sell_pnl", lambda s: s.dropna().sum() if s.dropna().size > 0 else np.nan),
    avg_pnl=("sell_pnl", lambda s: s.dropna().mean() if s.dropna().size > 0 else np.nan)
).reset_index()

sell_agg["win_rate"] = sell_agg["win_rate"].apply(lambda x: f"{x*100:.1f}%" if not pd.isna(x) else "")
sell_agg["total_pnl"] = sell_agg["total_pnl"].apply(lambda x: f"{x:.2f}" if not pd.isna(x) else "")
sell_agg["avg_pnl"] = sell_agg["avg_pnl"].apply(lambda x: f"{x:.2f}" if not pd.isna(x) else "")

print("\nSELL outcomes grouped by BUY regime and BUY bias\n")
print(sell_agg.to_string(index=False))

print(f"\nSaved summary CSV to {OUTFILE}")

