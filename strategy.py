import re

# vaihda tähän oman tiedostosi nimi
jarkkobackup8 = "strategy.py"

with open(filename, "r", encoding="utf-8") as f:
    for lineno, line in enumerate(f, start=1):
        # etsitään kaikki rivit joissa esiintyy ts
        if re.search(r"\bts\b", line) or re.search(r"idx

\[0\]

", line):
            print(f"{lineno:4d}: {line.strip()}")
