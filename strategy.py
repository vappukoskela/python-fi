import re
import pandas as pd

# Lähdetiedosto
filename = "jarkko_14112025.txt"

# Kohdetiedosto
output_file = "parsed_trades.csv"

rows = []
with open(filename, "r", encoding="utf-8") as f:
    for line in f:
        # --- Uusi: ENTRY recorded BUY ---
        # Esim: "INFO MSFT - ENTRY recorded qty=27 price=505.03 ..."
        m_entry = re.search(r"INFO\s+(\w+)\s+-\s+ENTRY recorded\s+qty=(\d+)\s+price=(\d+\.\d+)", line)
        if m_entry:
            symbol = m_entry.group(1)
            qty = int(m_entry.group(2))
            price = float(m_entry.group(3))
            rows.append({"symbol": symbol, "action": "BUY", "quantity": qty, "price": price})
            continue

        # BUY rivit: INFO SYMBOL - BUY ... qty=xx ... price=yy
        m_buy = re.search(r"INFO\s+(\w+)\s+-\s+BUY.*qty=(\d+).*price=(\d+\.\d+)", line)
        if m_buy:
            symbol = m_buy.group(1)
            qty = int(m_buy.group(2))
            price = float(m_buy.group(3))
            rows.append({"symbol": symbol, "action": "BUY", "quantity": qty, "price": price})
            continue

        # --- Uusi: SCALP SELL ---
        # Esim: "INFO MSFT - SCALP SELL qty=27 @ 504.7300 | Reason=Stop-loss ..."
        m_scalp = re.search(r"INFO\s+(\w+)\s+-\s+SCALP SELL\s+qty=(\d+)\s+@\s+(\d+\.\d+).*?Reason=([A-Za-z\-]+)", line)
        if m_scalp:
            symbol = m_scalp.group(1)
            qty = int(m_scalp.group(2))
            price = float(m_scalp.group(3))
            reason = m_scalp.group(4)
            # PnL voi olla rivillä, yritetään poimia jos löytyy
            m_pnl = re.search(r"(?:pnl=|PnL[:=]\s?)(-?\d+\.\d+)", line, re.IGNORECASE)
            pnl = float(m_pnl.group(1)) if m_pnl else None
            rows.append({
                "symbol": symbol,
                "action": "SELL",
                "quantity": qty,
                "price": price,
                "reason": reason,
                "pnl": pnl
            })
            continue

        # SELL rivit: INFO SYMBOL - SELL ... qty=xx ...
        m_sell = re.search(r"INFO\s+(\w+)\s+-\s+SELL.*qty=(\d+)", line)
        if m_sell:
            symbol = m_sell.group(1)
            qty = int(m_sell.group(2))
            # Hinta voi löytyä eri muodossa
            m_price = re.search(r"price=(\d+\.\d+)", line)
            if not m_price:
                m_price = re.search(r"@\s+(\d+\.\d+)", line)
            price = float(m_price.group(1)) if m_price else None

            m_reason = re.search(r"(Stop-loss|Take-profit|EMA fail|RSI fail|Trailing stop)", line, re.IGNORECASE)
            reason = m_reason.group(1) if m_reason else None

            m_pnl = re.search(r"(?:pnl=|PnL[:=]\s?)(-?\d+\.\d+)", line, re.IGNORECASE)
            pnl = float(m_pnl.group(1)) if m_pnl else None

            rows.append({
                "symbol": symbol,
                "action": "SELL",
                "quantity": qty,
                "price": price,
                "reason": reason,
                "pnl": pnl
            })
            continue

# Muodosta DataFrame
df = pd.DataFrame(rows)
df.to_csv(output_file, index=False)

print(df.head())
print(f"✅ Poimittuja rivejä: {len(df)}")
print(f"📂 Tallennettu tiedostoon: {output_file}")

# --- Analyysi myynneistä indikaattoreittain ---
if "reason" in df.columns and "pnl" in df.columns:
    sell_df = df[df["action"] == "SELL"].copy()

    if not sell_df.empty:
        summary = sell_df.groupby(["symbol","reason"]).agg(
            count=("reason", "size"),
            pnl_sum=("pnl", "sum"),
            wins=("pnl", lambda x: (x > 0).sum()),
            losses=("pnl", lambda x: (x <= 0).sum())
        )
        summary["share_pct"] = summary["count"] / len(sell_df) * 100
        summary["win_pct"] = summary["wins"] / summary["count"] * 100

        print("\n=== Myyntien tilastot indikaattoreittain ja osakekohtaisesti ===")
        print(summary)

        total_pnl = sell_df["pnl"].sum(skipna=True)
        total_win_pct = (sell_df["pnl"] > 0).mean() * 100

        print("\n=== Kokonaistulos ===")
        print(f"Kokonaissumma (PnL): {total_pnl:.4f}")
        print(f"Voittoprosentti: {total_win_pct:.2f} %")
    else:
        print("\n⚠️ Ei myyntirivejä analysoitavaksi.")
