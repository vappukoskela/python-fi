import re
import pandas as pd

# Lähdetiedosto
filename = "jarkko_14112025.txt"

# Kohdetiedosto
output_file = "parsed_trades.csv"

rows = []
with open(filename, "r", encoding="utf-8") as f:
    for line in f:
        # BUY rivit: INFO SYMBOL - BUY ... qty=xx ... price=yy
        m_buy = re.search(r"INFO\s+(\w+)\s+-\s+BUY.*qty=(\d+).*price=(\d+\.\d+)", line)
        if m_buy:
            symbol = m_buy.group(1)
            qty = int(m_buy.group(2))
            price = float(m_buy.group(3))
            rows.append({"symbol": symbol, "action": "BUY", "quantity": qty, "price": price})
            continue

        # SELL rivit: INFO SYMBOL - SELL ... qty=xx ... (price voi olla eri kohdassa)
        m_sell = re.search(r"INFO\s+(\w+)\s+-\s+SELL.*qty=(\d+)", line)
        if m_sell:
            symbol = m_sell.group(1)
            qty = int(m_sell.group(2))
            # Hinta voi löytyä erikseen
            m_price = re.search(r"price=(\d+\.\d+)", line)
            price = float(m_price.group(1)) if m_price else None

            # Poimitaan myös reason ja pnl jos ne löytyvät riviltä
            m_reason = re.search(r"(Stop-loss|Take-profit|EMA fail|RSI fail|Trailing stop)", line)
            reason = m_reason.group(1) if m_reason else None

            m_pnl = re.search(r"pnl=(-?\d+\.\d+)", line)
            pnl = float(m_pnl.group(1)) if m_pnl else None

            rows.append({
                "symbol": symbol,
                "action": "SELL",
                "quantity": qty,
                "price": price,
                "reason": reason,
                "pnl": pnl
            })

# Muodosta DataFrame
df = pd.DataFrame(rows)

# Tallenna CSV-tiedostoon
df.to_csv(output_file, index=False)

print(df.head())
print(f"✅ Poimittuja rivejä: {len(df)}")
print(f"📂 Tallennettu tiedostoon: {output_file}")

# --- Analyysi myynneistä indikaattoreittain ---
if "reason" in df.columns and "pnl" in df.columns:
    sell_df = df[df["action"] == "SELL"].copy()

    if not sell_df.empty:
        summary = sell_df.groupby("reason").agg(
            count=("reason", "size"),
            pnl_sum=("pnl", "sum"),
            wins=("pnl", lambda x: (x > 0).sum()),
            losses=("pnl", lambda x: (x <= 0).sum())
        )
        summary["share_pct"] = summary["count"] / len(sell_df) * 100
        summary["win_pct"] = summary["wins"] / summary["count"] * 100

        print("\n=== Myyntien tilastot indikaattoreittain ===")
        print(summary)

        total_pnl = sell_df["pnl"].sum()
        total_win_pct = (sell_df["pnl"] > 0).mean() * 100

        print("\n=== Kokonaistulos ===")
        print(f"Kokonaissumma (PnL): {total_pnl:.4f}")
        print(f"Voittoprosentti: {total_win_pct:.2f} %")
    else:
        print("\n⚠️ Ei myyntirivejä analysoitavaksi.")
