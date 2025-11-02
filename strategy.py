import requests
import re

url = "https://raw.githubusercontent.com/vappukoskela/python-fi/jarkkobackup8/strategy.py"
resp = requests.get(url)
resp.raise_for_status()

for lineno, line in enumerate(resp.text.splitlines(), start=1):
    # etsitään kaikki rivit joissa esiintyy ts tai ts_val
    if re.search(r"\bts\b", line) or re.search(r"\bts_val\b", line):
        print(f"{lineno:4d}: {line.strip()}")
