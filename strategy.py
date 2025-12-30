# analyze_audit.py
import pandas as pd
import numpy as np

# Adjust filename if needed
FNAME = "audit_trades_live.csv"

def load_data(fname):
    df = pd.read_csv(fname, dtype=str)
    # Normalize columns
    df.columns = [c.strip() for c in df.columns]
    # Ensure numeric columns
    if "pnl" in df.columns:
        df["pnl"] = pd.to_numeric(df["pnl"], errors="coerce")
    else:
        df["pnl"] = np.nan
    df["action"] = df["action"].str.strip().str.upper()
    df["regime"] = df.get("regime", "").fillna("UNKNOWN").astype(str).str.strip()
    df["bias"] = df.get("bias", "").fillna("unknown").astype(str).str.strip()
    return df

def summary_counts(df):
    buys = df[df["action"] == "BUY"].copy()
    sells = df[df["action"] == "SELL"].copy()
    print("Total rows:", len(df))
    print("Total BUY rows:", len(buys))
    print("Total SELL rows:", len(sells))
    print()

    print("Buy counts by regime:")
    print(buys["regime"].value_counts(dropna=False).to_string())
    print()
    print("Buy counts by bias:")
    print(buys["bias"].value_counts(dropna=False).to_string())
    print()
    print("Buy counts by regime x bias (cross-tab):")
    print(pd.crosstab(buys["regime"], buys["bias"]))
    print("\n" + "-"*60 + "\n")

    print("Sell counts by regime:")
    print(sells["regime"].value_counts(dropna=False).to_string())
    print()
    print("Sell counts by bias:")
    print(sells["bias"].value_counts(dropna=False).to_string())
    print()
    print("Sell counts by regime x bias (cross-tab):")
    print(pd.crosstab(sells["regime"], sells["bias"]))
    print("\n" + "-"*60 + "\n")

    # Win rate and PnL on sells (closed trades)
    closed = sells.copy()
    closed["is_win"] = closed["pnl"].apply(lambda x: True if pd.notna(x) and x > 0 else False)
    total_closed = len(closed)
    wins = closed["is_win"].sum()
    winrate = wins / total_closed if total_closed else float("nan")
    print(f"Closed trades (SELL rows): {total_closed}, Wins: {wins}, Win rate: {winrate:.2%}")
    print()

    # PnL totals
    total_pnl = closed["pnl"].sum(skipna=True)
    avg_pnl = closed["pnl"].mean(skipna=True)
    median_pnl = closed["pnl"].median(skipna=True)
    print(f"Total net PnL (closed trades): {total_pnl:.2f}")
    print(f"Average PnL per closed trade: {avg_pnl:.2f}")
    print(f"Median PnL per closed trade: {median_pnl:.2f}")
    print()

    # Breakdown by regime and bias
    print("Win rate and PnL by regime:")
    by_regime = closed.groupby("regime").agg(
        closed_count=("pnl","count"),
        wins=("is_win","sum"),
        winrate=("is_win", lambda s: s.sum()/len(s) if len(s) else float("nan")),
        net_pnl=("pnl","sum"),
        avg_pnl=("pnl","mean")
    ).sort_values("closed_count", ascending=False)
    print(by_regime.to_string())
    print("\n" + "-"*60 + "\n")

    print("Win rate and PnL by bias:")
    by_bias = closed.groupby("bias").agg(
        closed_count=("pnl","count"),
        wins=("is_win","sum"),
        winrate=("is_win", lambda s: s.sum()/len(s) if len(s) else float("nan")),
        net_pnl=("pnl","sum"),
        avg_pnl=("pnl","mean")
    ).sort_values("closed_count", ascending=False)
    print(by_bias.to_string())
    print("\n" + "-"*60 + "\n")

    print("Win rate and PnL by regime x bias:")
    by_combo = closed.groupby(["regime","bias"]).agg(
        closed_count=("pnl","count"),
        wins=("is_win","sum"),
        winrate=("is_win", lambda s: s.sum()/len(s) if len(s) else float("nan")),
        net_pnl=("pnl","sum"),
        avg_pnl=("pnl","mean")
    ).sort_values("closed_count", ascending=False)
    print(by_combo.to_string())
    print("\n" + "-"*60 + "\n")

    # Save CSV summaries
    by_regime.to_csv("summary_by_regime.csv")
    by_bias.to_csv("summary_by_bias.csv")
    by_combo.to_csv("summary_by_regime_bias.csv")
    print("Saved summary_by_regime.csv, summary_by_bias.csv, summary_by_regime_bias.csv")

if __name__ == "__main__":
    df = load_data(FNAME)
    summary_counts(df)
