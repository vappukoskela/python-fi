"""
PATCH27 — Regime Mismatch Bug Fix
Run this script in the same folder as strategy.py:
    python apply_patch27.py

It makes 7 targeted changes and writes strategy.py back.
A backup is saved as strategy.py.bak before any changes.
"""

import os
import sys
import shutil

TARGET = "strategy.py"
BACKUP = "strategy.py.bak"

if not os.path.exists(TARGET):
    print(f"ERROR: {TARGET} not found in current directory.")
    sys.exit(1)

with open(TARGET, "r", encoding="utf-8") as f:
    src = f.read()

original = src
changes_applied = 0

# ── CHANGE 1 ── safe_market_buy signature: add smoothed_regime parameter
OLD1 = """    bias=None,
    config_session=None
):
    import logging, time, csv, os"""

NEW1 = """    bias=None,
    config_session=None,
    smoothed_regime=None  # PATCH27: smoothed regime from main loop for mismatch detection
):
    import logging, time, csv, os"""

if OLD1 in src:
    src = src.replace(OLD1, NEW1, 1)
    changes_applied += 1
    print("✓ Change 1 applied: safe_market_buy signature")
else:
    print("✗ Change 1 NOT found — check manually")

# ── CHANGE 2 ── regime mismatch guard after regime_at_entry is computed
OLD2 = """                regime_at_entry = detect_regime(prices_series, sizes_series)
                bias_val = bias if bias is not None else globals().get("day_bias", "unknown")"""

NEW2 = """                regime_at_entry = detect_regime(prices_series, sizes_series)

                # PATCH27: execution-time regime mismatch guard.
                # If raw re-detection at execution time is HIGH_VOL, block the entry
                # even if _smooth_regime passed TREND to evaluate_entry due to stickiness.
                if regime_at_entry == "HIGH_VOL":
                    logging.warning(
                        "[BUY_BLOCK][PATCH27] %s blocked at execution — raw regime=HIGH_VOL "
                        "(smoothed was %s) — regime flipped between gating and execution",
                        symbol, smoothed_regime or "unknown"
                    )
                    return None

                # Use smoothed_regime for audit so it reflects what evaluate_entry used for gating.
                # Falls back to regime_at_entry if smoothed_regime was not passed.
                audit_regime = smoothed_regime if smoothed_regime is not None else regime_at_entry

                bias_val = bias if bias is not None else globals().get("day_bias", "unknown")"""

if OLD2 in src:
    src = src.replace(OLD2, NEW2, 1)
    changes_applied += 1
    print("✓ Change 2 applied: regime mismatch guard")
else:
    print("✗ Change 2 NOT found — check manually")

# ── CHANGE 3 ── entry_config_dict: use audit_regime
OLD3 = """                entry_config_dict = {
                    "regime": regime_at_entry,
                    "bias": bias_val,"""

NEW3 = """                entry_config_dict = {
                    "regime": audit_regime,
                    "bias": bias_val,"""

if OLD3 in src:
    src = src.replace(OLD3, NEW3, 1)
    changes_applied += 1
    print("✓ Change 3 applied: entry_config_dict regime")
else:
    print("✗ Change 3 NOT found — check manually")

# ── CHANGE 4 ── buy_row: use audit_regime
OLD4 = """                    "regime": regime_at_entry,
                    "code_version": CODE_VERSION,
                    "dist_from_session_high": entry_config_dict["dist_from_session_high"],
                    "move_from_open": entry_config_dict["move_from_open"],
                    "range_position": entry_config_dict["range_position"],
                }
                exec_rows.append(buy_row)"""

NEW4 = """                    "regime": audit_regime,
                    "code_version": CODE_VERSION,
                    "dist_from_session_high": entry_config_dict["dist_from_session_high"],
                    "move_from_open": entry_config_dict["move_from_open"],
                    "range_position": entry_config_dict["range_position"],
                }
                exec_rows.append(buy_row)"""

if OLD4 in src:
    src = src.replace(OLD4, NEW4, 1)
    changes_applied += 1
    print("✓ Change 4 applied: buy_row regime")
else:
    print("✗ Change 4 NOT found — check manually")

# ── CHANGE 5 ── EXEC_AUDIT_FILE writer: use audit_regime
OLD5 = """                                "regime": regime_at_entry,
                                "code_version": CODE_VERSION,
                                "dist_from_session_high": entry_config_dict["dist_from_session_high"],
                                "move_from_open": entry_config_dict["move_from_open"],
                                "range_position": entry_config_dict["range_position"],
                                "session_minutes": _session_minutes_from_ts(submit_ts),"""

NEW5 = """                                "regime": audit_regime,
                                "code_version": CODE_VERSION,
                                "dist_from_session_high": entry_config_dict["dist_from_session_high"],
                                "move_from_open": entry_config_dict["move_from_open"],
                                "range_position": entry_config_dict["range_position"],
                                "session_minutes": _session_minutes_from_ts(submit_ts),"""

if OLD5 in src:
    src = src.replace(OLD5, NEW5, 1)
    changes_applied += 1
    print("✓ Change 5 applied: EXEC_AUDIT_FILE writer regime")
else:
    print("✗ Change 5 NOT found — check manually")

# ── CHANGE 6 ── PATCH23 candidates: store regime in tuple
OLD6 = """                        _patch23_candidates.append((score, symbol, day_bias, CONFIG_SESSION, price))"""

NEW6 = """                        _patch23_candidates.append((score, symbol, day_bias, CONFIG_SESSION, price, regime))"""

if OLD6 in src:
    src = src.replace(OLD6, NEW6, 1)
    changes_applied += 1
    print("✓ Change 6 applied: _patch23_candidates tuple includes regime")
else:
    print("✗ Change 6 NOT found — check manually")

# ── CHANGE 7 ── PATCH23 loop: unpack regime and pass to safe_market_buy
OLD7 = """                for _cand_score, _cand_sym, _cand_bias, _cand_cfg, _cand_price in _patch23_candidates:"""

NEW7 = """                for _cand_score, _cand_sym, _cand_bias, _cand_cfg, _cand_price, _cand_regime in _patch23_candidates:"""

if OLD7 in src:
    src = src.replace(OLD7, NEW7, 1)
    changes_applied += 1
    print("✓ Change 7a applied: PATCH23 loop unpack")
else:
    print("✗ Change 7a NOT found — check manually")

# ── CHANGE 7b ── PATCH23 safe_market_buy call: pass smoothed_regime
OLD7B = """                            bias=_cand_bias,
                            config_session=_cand_cfg
                        )
                    finally:
                        pending_entries.discard(_cand_sym)"""

NEW7B = """                            bias=_cand_bias,
                            config_session=_cand_cfg,
                            smoothed_regime=_cand_regime  # PATCH27
                        )
                    finally:
                        pending_entries.discard(_cand_sym)"""

if OLD7B in src:
    src = src.replace(OLD7B, NEW7B, 1)
    changes_applied += 1
    print("✓ Change 7b applied: PATCH23 safe_market_buy call passes smoothed_regime")
else:
    print("✗ Change 7b NOT found — check manually")

# ── Update CODE_VERSION ──
OLD_VER = 'CODE_VERSION = "PATCH26_2026-04-30"'
NEW_VER = 'CODE_VERSION = "PATCH27_2026-05-07"'

if OLD_VER in src:
    src = src.replace(OLD_VER, NEW_VER, 1)
    print("✓ CODE_VERSION updated to PATCH27_2026-05-07")
else:
    print("✗ CODE_VERSION not updated — not found (may already be updated)")

# ── Verify no stale regime_at_entry references remain in safe_market_buy ──
# Find safe_market_buy and check
smb_start = src.find("def safe_market_buy(")
smb_end = src.find("\ndef safe_market_short(", smb_start)
smb_body = src[smb_start:smb_end]
remaining = smb_body.count('"regime": regime_at_entry')
if remaining == 0:
    print("✓ Verification: no stale regime_at_entry in safe_market_buy")
else:
    print(f"⚠ Verification: {remaining} stale regime_at_entry reference(s) remain in safe_market_buy — check manually")

print()
print(f"Total changes applied: {changes_applied}/8")

if src == original:
    print("WARNING: No changes were made. The source strings may not match.")
    sys.exit(1)

# Write backup then patched file
shutil.copy2(TARGET, BACKUP)
print(f"Backup written: {BACKUP}")

with open(TARGET, "w", encoding="utf-8") as f:
    f.write(src)

print(f"Patched file written: {TARGET}")
print()
if changes_applied == 8:
    print("All 8 changes applied successfully. PATCH27 is complete.")
else:
    print(f"WARNING: Only {changes_applied}/8 changes applied. Review the ✗ lines above.")
