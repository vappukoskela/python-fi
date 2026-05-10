"""
PATCH28 — Fix2/Fix3 execution-time guards in safe_market_buy
=============================================================
ROOT CAUSE:
  Fix2 and Fix3 live in evaluate_entry and check `if regime == "TREND"`.
  The regime passed to evaluate_entry is the SMOOTHED regime from _smooth_regime().
  When smoothing stickiness returns DRIFT or RANGE (not TREND), Fix2/Fix3 never run.
  safe_market_buy then re-detects the raw regime, finds TREND, and submits the order.

  This caused:
    - GOOG SM70 May 8: RSI=80.53 entered despite Fix2 (smoothed was not TREND)
    - JPM SM166 May 8: mfo=-1.21% entered despite Fix3 (smoothed was not TREND)
    - TSLA SM53 May 7, QQQ SM100 May 7: same pattern

FIX:
  Add Fix2 and Fix3 checks inside safe_market_buy, immediately after PATCH27's
  HIGH_VOL guard. These checks run on regime_at_entry (raw re-detection), which
  is the same value logged in the audit. If regime_at_entry == "TREND" at execution
  time, the same RSI and mfo quality gates that evaluate_entry intended to enforce
  are now enforced regardless of what the smoothed regime was during gating.

Run in the same folder as strategy.py:
    python apply_patch28.py
"""

import os
import sys

TARGET = "strategy.py"

if not os.path.exists(TARGET):
    print(f"ERROR: {TARGET} not found.")
    sys.exit(1)

with open(TARGET, "r", encoding="utf-8") as f:
    src = f.read()

# ── CHANGE 1 ── Add Fix2/Fix3 execution-time guards after PATCH27 HIGH_VOL block
OLD = """                # PATCH27: execution-time regime mismatch guard.
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
                audit_regime = smoothed_regime if smoothed_regime is not None else regime_at_entry"""

NEW = """                # PATCH27: execution-time regime mismatch guard.
                # If raw re-detection at execution time is HIGH_VOL, block the entry
                # even if _smooth_regime passed TREND to evaluate_entry due to stickiness.
                if regime_at_entry == "HIGH_VOL":
                    logging.warning(
                        "[BUY_BLOCK][PATCH27] %s blocked at execution — raw regime=HIGH_VOL "
                        "(smoothed was %s) — regime flipped between gating and execution",
                        symbol, smoothed_regime or "unknown"
                    )
                    return None

                # PATCH28: Fix2/Fix3 execution-time guards for TREND entries.
                # Mirrors the checks in evaluate_entry but runs on regime_at_entry (raw).
                # Prevents Fix2/Fix3 bypass when smoothed regime != TREND at gating time
                # (e.g. smoothing returned DRIFT/RANGE due to stickiness, so Fix2/Fix3
                # never evaluated in evaluate_entry, but raw re-detection finds TREND here).
                if regime_at_entry == "TREND":
                    _exec_rsi = _safe_last(compute_rsi_from_series(prices_series, RSI_PERIOD))
                    _exec_spy_high_ts = globals().get("_spy_session_high_ts")
                    _exec_spy_high_age = (
                        (datetime.now(timezone.utc) - _exec_spy_high_ts).total_seconds()
                        if isinstance(_exec_spy_high_ts, datetime) else float("inf")
                    )
                    _exec_spy_advancing = _exec_spy_high_age < 900  # new high within last 15 min

                    # PATCH28 Fix2: RSI ceiling when SPY stalling
                    if not pd.isna(_exec_rsi) and _exec_rsi > 75 and not _exec_spy_advancing:
                        logging.warning(
                            "[BUY_BLOCK][PATCH28][FIX2] %s blocked at execution — "
                            "TREND + RSI=%.1f > 75 while SPY stalling (high %.0fs old, "
                            "smoothed_regime was %s)",
                            symbol, _exec_rsi, _exec_spy_high_age, smoothed_regime or "unknown"
                        )
                        return None

                    # PATCH28 Fix3: mfo floor when SPY stalling
                    _exec_sym_open = globals().get(f"today_open_{symbol}")
                    _exec_mfo = (
                        (est_price - _exec_sym_open) / _exec_sym_open
                        if _exec_sym_open and _exec_sym_open > 0 else float("nan")
                    )
                    if not pd.isna(_exec_mfo) and _exec_mfo < 0.0015 and not _exec_spy_advancing:
                        logging.warning(
                            "[BUY_BLOCK][PATCH28][FIX3] %s blocked at execution — "
                            "TREND + mfo=%.2f%% < 0.15%% while SPY stalling "
                            "(smoothed_regime was %s)",
                            symbol, _exec_mfo * 100, smoothed_regime or "unknown"
                        )
                        return None

                # Use smoothed_regime for audit so it reflects what evaluate_entry used for gating.
                # Falls back to regime_at_entry if smoothed_regime was not passed.
                audit_regime = smoothed_regime if smoothed_regime is not None else regime_at_entry"""

if OLD in src:
    src = src.replace(OLD, NEW, 1)
    print("✓ PATCH28 applied: Fix2/Fix3 execution-time guards added to safe_market_buy")
else:
    print("✗ PATCH28 string not found — check that PATCH27 is correctly applied first")
    sys.exit(1)

# ── CHANGE 2 ── Update CODE_VERSION
OLD_VER = 'CODE_VERSION = "PATCH27_2026-05-07"'
NEW_VER = 'CODE_VERSION = "PATCH28_2026-05-08"'

if OLD_VER in src:
    src = src.replace(OLD_VER, NEW_VER, 1)
    print("✓ CODE_VERSION updated to PATCH28_2026-05-08")
else:
    print("✗ CODE_VERSION not updated — may already have been changed")

with open(TARGET, "w", encoding="utf-8") as f:
    f.write(src)

print()
print("PATCH28 complete.")
print()
print("What this fixes:")
print("  Fix2 and Fix3 now run at ORDER SUBMISSION time on the raw detected regime.")
print("  Even if smoothed regime was DRIFT/RANGE at evaluate_entry, these checks")
print("  fire when safe_market_buy re-detects TREND at execution.")
print()
print("Expected log messages when blocks fire:")
print("  [BUY_BLOCK][PATCH28][FIX2] GOOG blocked — TREND+RSI=80.5>75 while SPY stalling")
print("  [BUY_BLOCK][PATCH28][FIX3] JPM blocked — TREND+mfo=-1.21%<0.15% while SPY stalling")
