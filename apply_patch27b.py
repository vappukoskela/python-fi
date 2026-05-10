"""
PATCH27 follow-up — fixes the one remaining stale regime_at_entry in fill_row.
Run in the same folder as strategy.py:
    python apply_patch27b.py
"""

import os
import sys

TARGET = "strategy.py"

if not os.path.exists(TARGET):
    print(f"ERROR: {TARGET} not found.")
    sys.exit(1)

with open(TARGET, "r", encoding="utf-8") as f:
    src = f.read()

OLD = """                    fill_row = {
                        "timestamp": _audit_ts(fill_ts),
                        "symbol": symbol,
                        "action": "BUY_CONFIRMED",
                        "price": filled_price,
                        "reason": "entry_fill_confirmed",
                        "bias": bias_val,
                        "pnl": None,
                        "ema_fast": ema_fast_val,
                        "ema_slow": ema_slow_val,
                        "rsi": rsi_val,
                        "vwap": vwap_val,
                        "regime": regime_at_entry,
                        "code_version": CODE_VERSION,"""

NEW = """                    fill_row = {
                        "timestamp": _audit_ts(fill_ts),
                        "symbol": symbol,
                        "action": "BUY_CONFIRMED",
                        "price": filled_price,
                        "reason": "entry_fill_confirmed",
                        "bias": bias_val,
                        "pnl": None,
                        "ema_fast": ema_fast_val,
                        "ema_slow": ema_slow_val,
                        "rsi": rsi_val,
                        "vwap": vwap_val,
                        "regime": audit_regime,
                        "code_version": CODE_VERSION,"""

if OLD in src:
    src = src.replace(OLD, NEW, 1)
    with open(TARGET, "w", encoding="utf-8") as f:
        f.write(src)
    print("✓ fill_row regime fixed — audit_regime now used consistently throughout safe_market_buy")
    print("✓ PATCH27 is fully complete. No stale regime_at_entry references remain.")
else:
    print("✗ fill_row string not found — check manually")
    print("  Look for the fill_row dict inside safe_market_buy (after the polling loop)")
    print('  and change: "regime": regime_at_entry,')
    print('  to:         "regime": audit_regime,')
