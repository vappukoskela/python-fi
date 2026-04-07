# Check if rocket mode fired
grep "ROCKET" scalper_safe.log

# Check session state classifications through the day
grep "SESSION_STATE" scalper_safe.log

# Check what SMCI entry looked like - why did move_from_open filter not block it
grep "SMCI" scalper_safe.log | grep -E "BUY|BLOCK|BUY_BLOCK"

# Check ENTRY_DIAG snapshots through the day
grep "ENTRY_DIAG" scalper_safe.log

# Check DAY_REGIME classification at startup
grep "DAY_REGIME" scalper_safe.log | head -20
           
