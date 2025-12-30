# analysis_by_regime_bias.py
import pandas as pd

# Load CSV (adjust filename if needed)
df = pd.read_csv("audit_trades_live.csv", parse_dates=["timestamp"], keep_default_na=True)

# Normalize columns
df["action"] = df["action"].str.upper().str.strip()
df["regime"] = df["regime"].fillna("UNKNOWN").astype(str)
df["bias"] = df["bias"].fillna("unknown").str.lower().str.strip()
# Convert pnl to numeric; empty -> NaN
df["pnl"] = pd.to_numeric(df["pnl"], errors="coerce")

# Focus on SELL rows for outcome metrics (exits have pnl)
sells = df[df["action"] == "SELL"].copy()

# Define win/lose: treat pnl > 0 as win, pnl <= 0 as loss (treat NaN as unknown)
sells["outcome"] = sells["pnl"].apply(lambda x: "win" if pd.notna(x) and x > 0 else ("loss" if pd.notna(x) and x <= 0 else "unknown"))

# Grouping: regime x bias
group = sells.groupby(["regime", "bias"])

summary = group.agg(
    sell_count = ("action", "count"),
    wins = ("outcome", lambda s: (s == "win").sum()),
    losses = ("outcome", lambda s: (s == "loss").sum()),
    unknown_outcomes = ("outcome", lambda s: (s == "unknown").sum()),
    win_rate = ("outcome", lambda s: (s == "win").sum() / max(1, ((s == "win") | (s == "loss")).sum())),
    pnl_sum = ("pnl", "sum"),
    pnl_mean = ("pnl", "mean")
).reset_index()

# Format win_rate as percent
summary["win_rate_pct"] = (summary["win_rate"] * 100).round(1)

# Optional: also show BUY counts per regime/bias for context
buys = df[df["action"] == "BUY"].groupby(["regime", "bias"]).size().reset_index(name="buy_count")

# Merge buys into summary (left join)
report = summary.merge(buys, on=["regime", "bias"], how="left").fillna({"buy_count": 0})

# Sort for readability
report = report.sort_values(["regime", "bias"])

# Print clean table
pd.set_option("display.max_rows", None)
print(report[[
    "regime", "bias", "buy_count", "sell_count", "wins", "losses", "unknown_outcomes",
    "win_rate_pct", "pnl_sum", "pnl_mean"
]])
