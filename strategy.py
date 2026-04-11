
import re
import sys
from collections import defaultdict

LOG_FILE = "scalper_safe.log"
OUTPUT_FILE = "log_summary.txt"

# Patterns to extract
PATTERNS = {
    "exit":        re.compile(r"EXIT evaluate_sell \| reason=([^\|]+)"),
    "entry_block": re.compile(r"\[BLOCK\] (\w+) (?:rejected|blocked)[^\|]*\| Reason=(.+)"),
    "entry_accept":re.compile(r"\[ENTRY_STACK\]\[(\w+)\] regime=(\w+) score=([\d.]+) threshold=([\d.]+)"),
    "buy":         re.compile(r"\[BUY_SUBMITTED\] (\w+) order_id=\S+ qty=(\d+) est_price=([\d.]+)"),
    "sell_fill":   re.compile(r"\[TRADE\] (\w+) \[LIVE\] SELL @ ([\d.]+)"),
    "regime":      re.compile(r"\[DETECT_REGIME|detect_regime|regime=(\w+)"),
    "spy_block":   re.compile(r"SPY.*(bearish override|direction FALLING|FALLING|ELEVATED|EXTREME)"),
    "patch16_sl":  re.compile(r"\[PATCH16\]\[SL\] (\w+) using tight stop"),
    "session":     re.compile(r"\[SESSION_STATE\] state=(\w+)"),
    "market_char": re.compile(r"\[PATCH15\] Market character: (\w+) -> (\w+)"),
    "recon_sell":  re.compile(r"RECON SELL submitted (\w+) qty=\d+ @ last=([\d.]+) \| Reason=(.+)"),
}

exit_reasons = defaultdict(int)
block_reasons = defaultdict(int)
entry_accepts = []
buy_list = []
sell_list = []
spy_blocks = []
session_changes = []
market_char_changes = []
recon_sells = []
patch16_sl_symbols = []

lines_read = 0

try:
    with open(LOG_FILE, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            lines_read += 1

            # Exit reasons
            m = PATTERNS["exit"].search(line)
            if m:
                exit_reasons[m.group(1).strip()] += 1

            # Entry blocks
            m = PATTERNS["entry_block"].search(line)
            if m:
                block_reasons[m.group(2).strip()] += 1

            # Entry accepts (scored entries that passed)
            m = PATTERNS["entry_accept"].search(line)
            if m:
                entry_accepts.append({
                    "sym": m.group(1),
                    "regime": m.group(2),
                    "score": m.group(3),
                    "threshold": m.group(4),
                    "line": line.strip()
                })

            # Buys submitted
            m = PATTERNS["buy"].search(line)
            if m:
                buy_list.append(f"{m.group(1)} qty={m.group(2)} @ {m.group(3)}")

            # SPY blocks
            m = PATTERNS["spy_block"].search(line)
            if m:
                spy_blocks.append(line.strip())

            # PATCH16 adaptive SL
            m = PATTERNS["patch16_sl"].search(line)
            if m:
                patch16_sl_symbols.append(m.group(1))

            # Session state changes
            m = PATTERNS["session"].search(line)
            if m and "STATE CHANGED" in line:
                session_changes.append(line.strip())

            # Market character changes
            m = PATTERNS["market_char"].search(line)
            if m:
                market_char_changes.append(
                    f"{m.group(1)} -> {m.group(2)} | {line[:19]}"
                )

            # Reconcile sells
            m = PATTERNS["recon_sell"].search(line)
            if m:
                recon_sells.append(
                    f"{m.group(1)} @ {m.group(2)} reason={m.group(3).strip()}"
                )

except FileNotFoundError:
    print(f"ERROR: {LOG_FILE} not found. Run this script from the same folder.")
    sys.exit(1)

# Write output
with open(OUTPUT_FILE, "w", encoding="utf-8") as out:

    out.write(f"LOG SUMMARY — {LOG_FILE}\n")
    out.write(f"Total lines read: {lines_read}\n\n")

    out.write("=" * 60 + "\n")
    out.write("EXIT REASONS (all sessions)\n")
    out.write("=" * 60 + "\n")
    for reason, count in sorted(exit_reasons.items(), key=lambda x: -x[1]):
        out.write(f"  {count:4d}  {reason}\n")

    out.write("\n" + "=" * 60 + "\n")
    out.write("ENTRY BLOCK REASONS (top 30)\n")
    out.write("=" * 60 + "\n")
    for reason, count in sorted(block_reasons.items(), key=lambda x: -x[1])[:30]:
        out.write(f"  {count:4d}  {reason}\n")

    out.write("\n" + "=" * 60 + "\n")
    out.write(f"ENTRY ACCEPTS — {len(entry_accepts)} scored entries that passed\n")
    out.write("=" * 60 + "\n")
    for e in entry_accepts:
        out.write(f"  {e['sym']:6s} regime={e['regime']:8s} score={e['score']} threshold={e['threshold']}\n")

    out.write("\n" + "=" * 60 + "\n")
    out.write(f"BUY ORDERS SUBMITTED — {len(buy_list)}\n")
    out.write("=" * 60 + "\n")
    for b in buy_list:
        out.write(f"  {b}\n")

    out.write("\n" + "=" * 60 + "\n")
    out.write(f"RECONCILE SELLS — {len(recon_sells)}\n")
    out.write("=" * 60 + "\n")
    for r in recon_sells:
        out.write(f"  {r}\n")

    out.write("\n" + "=" * 60 + "\n")
    out.write(f"SPY BLOCK LINES — {len(spy_blocks)} (sample, max 50)\n")
    out.write("=" * 60 + "\n")
    for s in spy_blocks[:50]:
        out.write(f"  {s}\n")

    out.write("\n" + "=" * 60 + "\n")
    out.write(f"PATCH16 ADAPTIVE SL ACTIVATIONS — {len(patch16_sl_symbols)} times\n")
    out.write("=" * 60 + "\n")
    for s in set(patch16_sl_symbols):
        out.write(f"  {s} ({patch16_sl_symbols.count(s)} times)\n")

    out.write("\n" + "=" * 60 + "\n")
    out.write(f"SESSION STATE CHANGES\n")
    out.write("=" * 60 + "\n")
    for s in session_changes:
        out.write(f"  {s}\n")

    out.write("\n" + "=" * 60 + "\n")
    out.write(f"MARKET CHARACTER TRANSITIONS\n")
    out.write("=" * 60 + "\n")
    for s in market_char_changes:
        out.write(f"  {s}\n")

print(f"Done. Read {lines_read} lines.")
print(f"Output written to: {OUTPUT_FILE}")
print(f"  Exit reasons found:    {sum(exit_reasons.values())}")
print(f"  Entry blocks found:    {sum(block_reasons.values())}")
print(f"  Entry accepts found:   {len(entry_accepts)}")
print(f"  Buy orders found:      {len(buy_list)}")
print(f"  Recon sells found:     {len(recon_sells)}")
print(f"  SPY block lines found: {len(spy_blocks)}")
           
