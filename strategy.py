import re

jarkkobackup8 = "strategy.py"   # tämä on se tiedosto jota haluat tutkia

with open(jarkkobackup8, "r", encoding="utf-8") as f:
    for lineno, line in enumerate(f, start=1):
        if "entry_times" in line or "last_exit_time" in line:
            print(f"{lineno:4d}: {line.strip()}")
