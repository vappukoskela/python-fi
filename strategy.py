import requests
import re

url = "https://raw.githubusercontent.com/vappukoskela/python-fi/jarkkobackup8/strategy.py"
resp = requests.get(url)
resp.raise_for_status()

for lineno, line in enumerate(resp.text.splitlines(), start=1):
    # etsitään kaikki rivit joissa esiintyy idx[0]
    if re.search(r"idx

\[0\]

", line):
        print(f"{lineno:4d}: {line.strip()}")
