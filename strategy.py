import re
import pandas as pd

# Tiedoston nimi
filename = "jarkko_14112025.txt"

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
            rows.append({"symbol": symbol, "action": "SELL", "quantity": qty, "price": price})

# Muodosta DataFrame
df = pd.DataFrame(rows)

print(df.head())
print(f"✅ Poimittuja rivejä: {len(df)}")
