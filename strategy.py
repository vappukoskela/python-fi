import pandas as pd

# Load the log file (adjust filename)
df = pd.read_csv("NVDA_AGG_SIM_trades.csv", header=None)

# Inspect first row to see column count
print("Columns detected:", df.shape[1])

# Assign column names (based on your sample structure)
df.columns = [
    "timestamp", "symbol", "regime_info", "price", "val1", "val2", "val3",
    "val4", "val5", "val6", "trend", "trend_label",
    "tp_pct", "sl_multiplier", "outcome", "extra"
]

# --- Basic summaries ---
# Count of good vs bad blocks
outcome_counts = df["outcome"].value_counts()

# Count per symbol
symbol_counts = df.groupby("symbol")["outcome"].value_counts()

# Average score per regime (extract numeric score from 'regime_info')
df["score"] = df["regime_info"].str.extract(r"score=(\d+\.\d+)").astype(float)
avg_scores = df.groupby("symbol")["score"].mean()

# Summary table
summary = pd.DataFrame({
    "total_rows": df.groupby("symbol").size(),
    "good_blocks": df[df["outcome"]=="good_block"].groupby("symbol").size(),
    "bad_blocks": df[df["outcome"]=="bad_block"].groupby("symbol").size(),
    "avg_score": avg_scores
}).fillna(0)

print("\nOutcome counts:\n", outcome_counts)
print("\nSymbol outcome counts:\n", symbol_counts)
print("\nSummary by symbol:\n", summary)

