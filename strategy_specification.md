# Intraday Breakout Scalper — Strategy Specification

**Status**: Approved — Version 1.0
**Author**: Private investor
**Date**: May 2026
**Purpose**: Reference document for all development and operational decisions

---

## 1. Purpose and Investor Profile

This is a private individual investor's automated intraday trading system designed to generate consistent positive returns from US equity markets. Starting capital is in the $70,000–$100,000 range, with profits reinvested rather than withdrawn during the early phase. As the account grows, absolute returns grow proportionally without requiring the strategy to take more risk.

The system operates under active human supervision at all times. The investor is present during all trading sessions and available to intervene. This is a tool the investor controls, not an autonomous system.

**Return target**: 20–40% annual return on capital. This target is set deliberately above the 10–12% that passive index investing provides. An active system that cannot reliably outperform passive investing does not justify the operational effort required to run it. The 20–40% range is ambitious but achievable for a disciplined retail system with sound execution.

The investor accepts that some days will be negative, some flat. Weekly and monthly consistency matters more than individual daily results. The measure of success is the trend over time.

---

## 2. Core Design Principles

Three principles guide every design decision in this strategy.

**Simplicity over sophistication.** Complex signals look intelligent but hide overfitting. Every parameter and rule in the system must have a clear, defensible reason. A strategy with fewer parameters generalizes better to future market conditions than one with many. Total system parameter count is approximately 25 numbers, all listed in Section 8.

**Discipline over optimization.** The temptation to add rules in response to bad days is what destroys retail trading systems. New rules require evidence from multiple sessions, not reactions to single events. When in doubt, the simpler version wins.

**Risk management over return chasing.** A bad day must never become a serious setback. The daily kill switch caps single-day damage at 2% of account equity. Position sizing is calibrated so even cascading losses cannot escape this cap. Returns are a consequence of repeated good decisions; they are not pursued at the cost of risk control.

---

## 3. Market Context — How the System Reads the Market

The system reads the broader market continuously and adjusts its behavior based on what it observes. The reference instrument is SPY (the S&P 500 ETF). SPY's behavior defines the day's character, and that character flows through the rest of the system.

**Three market states**: bullish, neutral, bearish.

The state is determined by two measurements that must agree:

- **Level**: SPY's percentage change from today's open
- **Direction**: SPY's price now versus SPY's price 15 minutes ago

A state is reached only when both level and direction agree. If they disagree, the state is neutral.

**State definitions**:

- **Bullish**: SPY level is above +0.5% from open AND direction is rising (price now higher than 15 min ago by at least 0.05%)
- **Bearish**: SPY level is below -0.3% from open AND direction is falling (price now lower than 15 min ago by at least 0.05%)
- **Neutral**: Anything else

**Hysteresis** prevents flickering when SPY oscillates near a boundary. Once in a state, exiting requires a 0.2% buffer:

- Bullish exits to neutral when SPY drops below +0.3%
- Bearish exits to neutral when SPY rises above -0.1%

**Why these thresholds**: SPY moves of 0.5% from open are meaningful but not rare; this threshold captures genuine bullish character without requiring exceptional days. The bearish threshold is asymmetric (-0.3% rather than -0.5%) because the strategy is long-only and benefits from stepping out of weakness quickly. The 0.2% hysteresis buffer is wide enough to ignore normal noise around the boundaries while still catching real reversals.

**Why level AND direction together**: SPY can be at a high level while currently fading, or at a low level while currently recovering. Requiring both to agree means the state only triggers when current conditions are unambiguous. This produces more time in neutral but cleaner signals when bullish or bearish.

The market state is computed continuously throughout the session. New entries and exits respond to the current state at the moment of decision.

---

## 4. Entry Logic — When to Buy

The system trades long-only. An entry is allowed when a breakout trigger fires AND all filters pass simultaneously.

### 4.1 The Breakout Trigger

Both conditions must be true at the same moment for a candidate to be considered:

**Condition 1 — Price breakout**: The symbol's current price exceeds the highest price reached in the last 15 minutes by at least 0.1%.

**Condition 2 — Volume confirmation**: The current 1-minute volume is at least 1.5× the median 1-minute volume over the last 15 minutes.

**Why a breakout signal**: Relative-strength signals are lagging by definition — they only register after the move has happened. Breakout signals are leading; they fire at the moment a move begins. For a scalping strategy targeting quick captures of intraday momentum, leading signals are essential.

**Why 15 minutes**: Long enough that the "recent high" represents a real consolidation level, not just minutes of drift. Short enough that fresh moves register quickly. Matches the same 15-minute timeframe used elsewhere in the system for conceptual consistency.

**Why 0.1% cushion**: Filters out single-tick noise where price might exceed the prior high by a single penny. On a $200 stock, this is 20 cents — meaningful enough to indicate real movement, small enough not to delay entry materially.

**Why 1.5× volume**: Distinguishes "someone is actively buying" from "price drifted upward in quiet trading." Real breakouts virtually always show 2× or higher volume; 1.5× catches them while filtering out drift. Below this threshold, breakouts are usually false signals.

### 4.2 Entry Filters

All three must be true at the moment of the trigger:

1. **Market state is bullish or neutral** (not bearish)
2. **Symbol is above its VWAP** (volume-weighted average price for the day)
3. **Symbol's 15-minute relative strength is not below -0.5%** (symbol is not actively weak versus SPY)

**Why these filters**: The trigger by itself catches breakouts. The filters block setups where the broader context contradicts the trigger. A symbol breaking out into a falling market, below its own VWAP, or actively underperforming SPY is a higher-risk setup than the same breakout under supportive conditions.

**Why these specific levels**: VWAP is binary (above or below). The relative strength threshold of -0.5% is a "not actively weak" floor — symbols only need to be neutral or strong on that dimension; they don't need to be exceptionally strong. This keeps the filter from being so restrictive that it blocks most breakouts.

### 4.3 Startup Behavior

At system startup, the breakout window must be populated with price and volume data before entries can be evaluated. The system fetches the last 15 minutes of historical bars from Alpaca to populate the window. If historical data is unavailable, the system observes live data for 15 minutes before enabling entries.

This applies whether the system starts at the normal session opening or at a late start (e.g., 11:00 AM). The window is always populated relative to the current moment.

### 4.4 Multiple Candidate Handling

When multiple symbols meet entry criteria simultaneously, the system enters those that fit within the position and capital limits (Section 6). If more candidates qualify than slots are available, candidates are ranked by entry trigger quality (volume multiple above the 1.5× minimum) and the highest-quality candidates are taken first.

---

## 5. Exit Logic — When to Sell

Exits use a two-mode system. Every entry begins in normal mode. Trades that succeed strongly in bullish conditions transition to runner mode.

### 5.1 Normal Mode (default)

Three exit conditions, whichever fires first:

- **Stop-loss**: Position closes if price falls 0.5% below entry
- **Take-profit**: Position closes at 1.0% above entry, OR transitions to runner mode if market state is bullish at the moment TP is reached
- **Time limit**: Position closes at market price if neither stop nor TP has triggered after 30 minutes

**Why 0.5% stop**: Wide enough that normal price wiggles after the breakout do not trigger false stops. Tight enough that real breakout failures trigger quickly at acceptable cost. Combined with the 1.0% TP, produces a 2:1 reward-to-risk ratio that allows the strategy to be profitable at realistic breakout win rates (40-50%).

**Why 1.0% TP**: Creates the 2:1 ratio with the 0.5% stop. In neutral market conditions, this is the actual exit price. In bullish conditions, it's the activation threshold for runner mode. Lower targets would compress the ratio toward break-even after costs; higher targets would be reached too rarely.

**Why 30-minute time limit**: Most real breakouts resolve (either succeed or fail) within 15 minutes. Trades that have neither hit TP nor stop after 30 minutes have lost their momentum and are unlikely to develop further. The time limit closes them and frees the slot.

### 5.2 Runner Mode

When a position in normal mode hits TP and market state is bullish, the position transitions to runner mode rather than closing.

In runner mode:

- The fixed take-profit is cancelled
- A trailing stop is set at 0.4% below the peak price
- The trailing stop updates upward as the peak rises
- The position exits when the trailing stop triggers, OR at end-of-day forced close

**Why runner mode**: Most breakouts give a small move and reverse. Some breakouts develop into sustained runs. A fixed TP captures the small moves but cuts off the sustained ones too early. Runner mode lets winners run when conditions support continued strength.

**Why activate only on bullish market state**: Sustained moves require supportive market conditions. Activating runner mode in neutral conditions invites holding through reversals. The bullish state is a single, clean activation gate.

**Why 0.4% trailing distance**: Wide enough that normal tick-by-tick noise does not trigger the trail. Narrow enough that real reversals trigger quickly without giving back excessive profit. On a 5% run, the position would exit at approximately 4.6%, capturing the bulk of the move.

**No additional exit conditions in runner mode**: The trailing stop is the exit mechanism. No RSI checks, no SPY-direction overrides, no momentum filters. Adding more conditions would create the layered complexity that destroys system reliability.

### 5.3 End-of-Day Forced Close

All open positions, regardless of mode or P&L, are closed at 15:55 ET (5 minutes before market close). This is non-negotiable. The strategy does not hold positions overnight.

---

## 6. Position Sizing

The system uses three sizing tiers, with capacity constraints that limit total exposure.

### 6.1 The Three Tiers

**Tier 1 — Reduced (3% of available buying power)**: Activates when the last two completed trades on the strategy were both losses. Reverts to Tier 2 on the next winning trade.

**Tier 2 — Normal (5% of available buying power)**: Default tier for any entry not qualifying for Tier 3 and not downgraded to Tier 1.

**Tier 3 — Strong (7% of available buying power)**: Activates when ALL of these are true at entry:
- Market state is bullish (not just neutral)
- Symbol's 15-minute relative strength is above +0.5% (not just non-negative)
- Symbol's drawdown from session high is less than 0.5% (clean trend, not volatile up-and-down)

**Why three tiers**: Different conditions warrant different risk levels. A breakout in a clean bullish trend with a strong outperforming symbol is a higher-quality setup than a breakout in a neutral market on a symbol just barely qualifying. The tiers translate this quality difference into appropriate capital allocation.

**Why these specific percentages**: The 5% normal size is conservative enough that even multiple losses don't threaten the account. The 7% strong size is meaningfully larger (40% increase) without approaching risk limits. The 3% reduced size is half-default — small enough to limit damage during losing streaks while still keeping the system engaged.

**Why these Tier 3 conditions specifically**: The drawdown-from-session-high condition captures "clean trend" — the symbol isn't just up, it's up smoothly without significant pullbacks. The relative strength condition (above +0.5%, not just "not below -0.5%") requires actual outperformance, not just non-weakness. The bullish market state requires supportive broader conditions. All three together identify the best setups; missing any one drops to Tier 2.

### 6.2 Capacity Constraints

The system can run multiple concurrent positions, subject to two constraints (whichever binds first stops new entries):

- **Total exposure cap**: Combined position values cannot exceed 20% of available buying power
- **Position count cap**: Maximum 5 concurrent positions

**Per-position hard ceiling**: No single position ever exceeds 10% of available buying power, regardless of tier. This is a defensive backstop that should never trigger in normal operation but exists to catch bugs or misconfigurations.

**Why hybrid (capital + count)**: Capital-based exposure alone could allow many small positions in correlated names, fragmenting attention without reducing real risk. Count alone could force passing on a fourth strong setup when capital is available. The hybrid limits both fragmentation and over-concentration.

**Why 20% / 5 positions**: At normal Tier 2 sizing, 5% × 5 positions = 25% — slightly above the capital cap, so the capital cap binds first and prevents fragmentation. At Tier 3 strong sizing, 7% × 3 = 21% — also approximately at the cap. The capital cap dominates; the count cap is a backstop.

---

## 7. Risk Governors

Risk governors are the global safety nets that protect against bad days, runaway losses, and edge cases not handled by trade-level logic.

### 7.1 Daily Kill Switch

If the account loses 2% of start-of-day equity (realized losses + unrealized losses on open positions), the kill switch triggers. On trigger:

- All open positions close immediately at market price
- New entries are blocked for the rest of the session
- The event is logged clearly
- Manual restart is required the next session

**Why 2%**: A single Tier 2 trade hitting full stop loses about 0.025% of account. Five back-to-back full stops at Tier 2 lose about 0.125%. Reaching 2% requires a substantially worse-than-normal day — many losses, gap moves, or correlated drawdowns. By that point, the day is broken and stopping is correct.

**Why include unrealized losses**: A kill switch that only counts closed losses waits for damage to be locked in before reacting. Including open positions reflects current real damage and triggers when the day is going badly, not after.

**Why manual restart**: Automated restart removes the human assessment of "what just happened today and is tomorrow safe to trade." That assessment is exactly what should happen after a kill-switch event. The investor reviews, decides, and restarts deliberately.

### 7.2 Per-Symbol Session Loss Limit

A symbol that produces 2 consecutive losing trades within a single session is blocked from further entries that day. A winning trade on the symbol resets the counter. The block clears at the next session's opening.

**Why 2 consecutive**: One loss is normal variance. Three losses means three full-sized losing trades have already occurred. Two losses is the right point to pause — early enough to limit damage, late enough not to overreact to a single bad outcome.

**Why session-only block, not conditional unblock**: Conditional unblocks (e.g., "if SPY recovers by X% and Y minutes pass") add complexity without clear evidence that re-entering blocked symbols mid-session is profitable. The conservative response is to leave the symbol alone for the day and look at it fresh tomorrow.

### 7.3 EOD Timing

- **No new entries after 15:30 ET** (30 minutes before market close)
- **All positions force-closed at 15:55 ET** (5 minutes before market close)

**Why these times**: A trade entered at 15:50 has only 5 minutes to develop before force-close. Stopping new entries at 15:30 gives entered positions reasonable time to play out. The 15:55 close gives 5 minutes of buffer for order fills before the bell, enough to handle normal execution while staying close to the close to capture late-session moves.

### 7.4 Session Start Rules

- The system can start at any time during the session
- Entries are blocked before 10:00 ET (the first 30 minutes after market open)
- If unexpected positions exist on Alpaca at startup, the system refuses to start and logs clearly. Manual review is required before trading begins.

**Why block first 30 minutes**: Opening volatility produces erratic price action and unreliable signals. Sitting out the first 30 minutes is a deliberate choice to wait for the market to settle.

**Why refuse to start with unexpected positions**: If positions exist that the system did not open in this session, something went wrong — either an EOD close failed, or positions were opened manually outside the system. Either case requires human review, not automated handling. The system should not guess what to do with positions whose context it doesn't have.

### 7.5 Per-Trade Risk Discipline

Beyond the global governors, every trade has its own risk discipline already specified in earlier sections:

- Stop-loss on every position (Section 5.1)
- Take-profit and time limits prevent runaway holds (Section 5.1)
- Trailing stop in runner mode protects accumulated profit (Section 5.2)
- Position sizing reduces after losses (Section 6.1, Tier 1)

These trade-level controls plus the global governors form a layered defense.

---

## 8. Complete Parameter Reference

All numerical parameters in the strategy, consolidated for reference. Every parameter listed has a justification in the relevant section above.

### Market State (Section 3)
- Bullish level threshold: +0.5%
- Bullish exit threshold (hysteresis): +0.3%
- Bearish level threshold: -0.3%
- Bearish exit threshold (hysteresis): -0.1%
- Direction lookback: 15 minutes
- Direction flat band: ±0.05%

### Entry (Section 4)
- Breakout window: 15 minutes
- Breakout cushion: 0.1% above recent high
- Volume multiple: 1.5× median 1-minute volume
- Volume comparison window: 15 minutes
- Relative strength floor (filter): -0.5% over 15 minutes

### Exit (Section 5)
- Stop-loss: 0.5% below entry
- Take-profit: 1.0% above entry
- Time limit: 30 minutes
- Trailing stop (runner mode): 0.4% below peak
- EOD forced close: 15:55 ET

### Position Sizing (Section 6)
- Tier 1 size: 3% of buying power
- Tier 2 size: 5% of buying power
- Tier 3 size: 7% of buying power
- Tier 3 RS threshold: +0.5%
- Tier 3 drawdown threshold: 0.5%
- Total exposure cap: 20%
- Position count cap: 5
- Per-position hard ceiling: 10%
- Tier 1 trigger: 2 consecutive losses

### Risk Governors (Section 7)
- Daily kill switch threshold: 2% of start-of-day equity
- Per-symbol session block: 2 consecutive losses
- No new entries before: 10:00 ET
- No new entries after: 15:30 ET

**Total: approximately 25 numerical parameters across the entire strategy.**

---

## 9. Tradable Universe

### 9.1 Current Universe (24 Symbols)

**Mega-cap tech**: NVDA, AMD, TSLA, AAPL, MSFT, AMZN, GOOG, META

**Semiconductors**: MU, QCOM, AVGO, SMCI

**Software / cloud**: CRM, ORCL, ADSK, NFLX, PLTR, SHOP

**Financials**: V, JPM, C

**Other high-beta**: UBER, SQ

### 9.2 Reference-Only Instruments

**SPY**: Used for market state determination (Section 3) but never traded.

### 9.3 Universe Selection Criteria

All universe members are highly liquid US large-cap equities listed on NYSE or NASDAQ. Daily volumes are in millions of shares. Bid-ask spreads are typically narrow (one cent or less). These characteristics ensure that breakout signals are reliable and execution is predictable.

The universe is tech-heavy. This is intentional — tech stocks have provided most of the meaningful intraday breakouts in recent years. Concentration risk is managed at the account level through the daily kill switch and capital exposure cap, not by diversifying the universe into less suitable names.

### 9.4 Future: Pre-Market Scanner

Long-term roadmap includes a pre-market scanner that selects daily candidates from a larger universe (e.g., S&P 500 or NASDAQ 100) based on:
- Premarket volume relative to typical
- Premarket price change (gap up or down)
- Overnight news catalysts
- Premarket relative strength versus market futures

The scanner outputs a daily watchlist of 10-20 stocks, replacing the fixed universe for that session.

The scanner is **deferred**. It activates only after the fixed-universe strategy demonstrates consistent edge across multiple market conditions. Building it earlier introduces a moving target that makes evaluating the core strategy harder.

---

## 10. What This Strategy Does Not Do

Explicit boundaries on the system's behavior:

- **No shorting.** Long-only.
- **No options or leverage.** Cash equity positions only.
- **No overnight holds.** All positions close before market close.
- **No trading in the first 30 minutes** after market open (9:30-10:00 ET).
- **No trading in the final 30 minutes** before market close for entries (15:30-16:00 ET) — though existing positions are managed and force-closed at 15:55.
- **No reliance on news feeds or analyst ratings as entry signals.** The system reads market structure, price, and volume.
- **No automated operation without human supervision.** The investor is present during all trading sessions.
- **No size increases after losses.** Position sizing decreases after consecutive losses (Tier 1); it never increases as a recovery attempt.
- **No re-entry into blocked symbols within the same session.**
- **No automated restart after kill-switch trigger.** Restart requires manual decision the next session.

---

## 11. Path to Live Trading

The system is currently in paper trading on the Alpaca paper trading platform. The transition to real capital happens when the strategy demonstrates consistent positive edge across multiple market conditions — strong bull days, flat days, and weak or bearish days — over a sufficient number of sessions.

**Specific evidence required before considering live trading**:

- Positive cumulative P&L over a meaningful sample (minimum 30 trading sessions)
- Performance distributed across different market conditions, not concentrated in one regime
- Daily kill switch has either not triggered or triggered only on demonstrably bad market days
- Maximum daily drawdown observed in paper trading remains below 2%
- The investor can articulate, after observing the strategy in operation, the cases where it works and the cases where it does not

**Insufficient evidence**:

- Profitable performance limited to strong bull days
- Single-session results, even very strong ones
- Confidence based on intuition rather than observed outcomes

The decision to go live is the investor's judgment based on the paper trading record. No automated criterion triggers the transition. When live trading begins, position sizes start small (well below the specified percentages) and scale up gradually only after live results confirm paper-trading expectations.

---

## 12. Maintenance Discipline

This strategy has the structure it does because complexity was actively resisted during design. Maintaining that structure requires ongoing discipline.

**Rules for any future change**:

1. **No additions based on a single session.** A failure observed once is not evidence; it's variance. Changes require the failure to be observed across multiple sessions with similar character.

2. **Every addition must come with a removal candidate.** Adding a rule requires identifying an existing rule that would be removed if the new one proves to work. This forces examination of why existing rules were not sufficient.

3. **Specification first, code second.** Any proposed change is first written into this document, in plain language, before any code change is made. If the change cannot be expressed cleanly in prose, it is the wrong change.

4. **Parameter count is a budget.** The current count is approximately 25. Adding a parameter requires removing one or strong evidence that the addition is essential.

5. **Performance disappointment is not a valid reason to add rules.** Losing money within expected bounds is variance, not a failure requiring code change. Only structural failures — behavior that is actually wrong, not unlucky — justify changes.

These rules exist because the previous version of the system grew from a similar starting point into a system with 100+ parameters across overlapping regime configurations, ultimately becoming impossible to reason about. Avoiding that outcome requires saying no to most additions.

---

## 13. Open Questions and Roadmap

Items deliberately deferred from this version of the specification:

**Pre-market scanner** — daily dynamic universe selection. Deferred until the fixed-universe strategy demonstrates consistent edge.

**Sizing tier refinement** — whether the current 3/5/7 tiers should be expanded or modified. Deferred until paper trading data shows whether the tier triggers correctly identify higher-quality setups.

**Stop-loss tiering by market state** — whether stops should differ in neutral versus bullish conditions. The investor expressed a hypothesis that this might help. Deferred until paper trading data shows whether neutral-condition losers would have recovered at wider stops at a different rate than bullish-condition losers.

**Symbol-level volatility adjustments** — whether high-volatility symbols (e.g., TSLA, SMCI) should have different stop or TP percentages than lower-volatility ones (e.g., V, JPM). Deferred until paper trading shows whether one-size-fits-all parameters cause different success rates across the universe.

**Earnings-day filtering** — whether to filter out symbols on their earnings reporting day, even though the strategy never holds overnight. Currently the system makes no special allowance; symbols are traded normally on their earnings day. Deferred unless paper trading shows earnings-day trades systematically underperform.

These items are recorded so they are not forgotten and so any future revisitation has a clear starting context.

---

**This document is the source of truth for the strategy. Code implements the document. Where they conflict, the document is correct and the code must be updated. Changes to the strategy follow the maintenance rules in Section 12.**
