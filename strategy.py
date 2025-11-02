import requests

url = "https://raw.githubusercontent.com/vappukoskela/python-fi/jarkkobackup8/strategy.py"
resp = requests.get(url)
resp.raise_for_status()

for lineno, line in enumerate(resp.text.splitlines(), start=1):
    if "entry_times" in line or "last_exit_time" in line:
        print(f"{lineno:4d}: {line.strip()}")

