import pandas as pd

# Load the log file (adjust filename)
df = pd.read_csv("2025-11-26.txt", header=None)

# Assign column names (based on your structure)
df.columns = [
    "timestamp", "symbol", "regime_info", "price", "val1", "val2", "val3",
    "val4", "val5", "val6", "trend", "trend_label",
    "tp_pct", "sl_multiplier", "outcome", "extra"
]

# --- Extract regime and score ---
df["regime"] = df["regime_info"].str.extract(r"Regime=(\w+)")
df["score"] = df["regime_info"].str.extract(r"score=(\d+\.\d+)").astype(float)

# --- Outcome counts overall ---
outcome_counts = df["outcome"].value_counts()
print("\nOutcome counts:\n", outcome_counts)

# --- Outcome counts per regime ---
regime_counts = df.groupby(["regime", "outcome"]).size()
print("\nRegime outcome counts:\n", regime_counts)

# --- Average score per regime ---
avg_scores = df.groupby("regime")["score"].mean()

# --- Summary table by regime ---
summary = pd.DataFrame({
    "total_rows": df.groupby("regime").size(),
    "good_blocks": df[df["outcome"]=="good_block"].groupby("regime").size(),
    "bad_blocks": df[df["outcome"]=="bad_block"].groupby("regime").size(),
    "neutral_blocks": df[df["outcome"]=="neutral_block"].groupby("regime").size(),
    "avg_score": avg_scores
}).fillna(0)

print("\nSummary by regime:\n", summary)
