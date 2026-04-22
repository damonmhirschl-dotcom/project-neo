# OPERATIONAL_NOTES.md
# Project Neo — EC2 Operational Reference
# Last updated: 2026-04-21 (final session close)
# EC2 path: /root/Project_Neo_Damon/OPERATIONAL_NOTES.md

---

## Quick Reference

| Item | Value |
|---|---|
| EC2 instance | i-02d767a507a0301c0 (ip-10-50-1-4) |
| Agent root | /root/Project_Neo_Damon/ |
| Virtualenv | /root/algodesk/algodesk |
| User | root |
| AWS region | eu-west-2 |
| RDS endpoint | algodesk-postgres-dev.chubomleuerz.eu-west-2.rds.amazonaws.com |
| DB name | postgres |
| DB search_path | forex_network |
| Kill switch param | /platform/config/kill-switch |
| Admin S3 bucket | algodesk-admin-dashboard-dev |
| Trading S3 bucket | algodesk-trading-dashboard-dev |
| Admin CF dist | E2HV8N8YKHB6WD |
| NAT EIP | 3.11.151.103 |

---

## Agent Services

| Agent | Service | Cycle |
|---|---|---|
| Macro | neo-macro-agent | 15–60 min (interruptible sleep) |
| Technical | neo-technical-agent | 5 min |
| Regime | neo-regime-agent | 15–60 min (interruptible sleep) |
| Orchestrator | neo-orchestrator-agent | ~5 min |
| Risk Guardian | neo-risk-guardian-agent | 15s poll |
| Execution | neo-execution-agent | 10s poll |
| Learning Module | neo-learning-module | 30s poll |

```bash
# Status all 7
sudo systemctl status neo-macro-agent neo-technical-agent neo-regime-agent \
  neo-orchestrator-agent neo-risk-guardian-agent neo-execution-agent neo-learning-module

# Errors last 30 min
sudo journalctl -u neo-macro-agent -u neo-technical-agent -u neo-regime-agent \
  -u neo-orchestrator-agent -u neo-risk-guardian-agent -u neo-execution-agent \
  -u neo-learning-module --since "30 minutes ago" --no-pager | \
  grep -iE "error|exception|traceback|critical|failed"

# Detailed logs in /var/log/neo/*.error.log
```

---

## Paper Trading Users

| Short ID | Profile | Risk/trade | Threshold | Max positions |
|---|---|---|---|---|
| e61202e4 | Conservative | 1.0% | 0.3645 | 4 |
| 76829264 | Balanced | 2.0% | 0.3148 | 5 |
| d6c272e4 | Aggressive | 3.0% | 0.2923 | 6 |

Thresholds recalibrated 2026-04-21 from active-session percentiles (London 07–12 UTC, NY 13–17 UTC).
Monday priority: recalibrate using weekday data only — weekend data skews percentiles downward.

---

## DATA_QUALITY_CUTOFF

File: /root/Project_Neo_Damon/Learning_Module/learning_module.py line ~112
Current value: '2026-04-21 18:15:00+00'

IMPORTANT: Always use STRING format 'YYYY-MM-DD HH:MM:SS+00' — NOT datetime(...) form.
datetime(...) causes NameError on startup (module uses import datetime not from datetime import).

Advanced multiple times on 2026-04-21 to exclude:
- Pre-normalisation era trades
- Duplicate execution bug trades (14:18–14:25)
- Stress test breach trades (14:49–15:00)
- Ghost close autopsies (17:12–17:27)

Restart learning module after any change.

---

## Kill Switch

```bash
# Check
aws ssm get-parameter --name /platform/config/kill-switch \
  --region eu-west-2 --query Parameter.Value --output text

# Activate
aws ssm put-parameter --name /platform/config/kill-switch \
  --value active --type String --overwrite --region eu-west-2

# Deactivate
aws ssm put-parameter --name /platform/config/kill-switch \
  --value inactive --type String --overwrite --region eu-west-2
```

Checked by orchestrator, risk guardian, execution agent run_cycle() (D1 fix 2026-04-21).
Never auto-resets. Must be manually deactivated.

---

## Circuit Breaker

Set proactively by risk guardian _check_drawdown_circuit_breaker() (D2 fix 2026-04-21).
Triggers when: (peak_account_value - account_value) / peak_account_value * 100 >= daily_loss_limit_pct
Execution agent re-checks circuit_breaker_active before every execute_trade() call (D3 fix 2026-04-21).

```sql
-- Clear manually
UPDATE forex_network.risk_parameters
SET circuit_breaker_active = FALSE,
    circuit_breaker_reason = NULL,
    circuit_breaker_at = NULL
WHERE user_id = '<full-uuid>';
```

---

## Ghost Close Protection (2026-04-21)

Two failure paths, both now covered:

PATH A — ConnectionError/RemoteDisconnected:
  ig_broker.py _force_reconnect() — closes dead socket pool, creates fresh requests.Session,
  forces re-auth bypassing 6h TTL check. Applied to all 5 outbound request methods.

PATH B — 200 response with empty positions list:
  manage_open_positions() line ~1861 — if get_positions() returns [] while DB has open trades,
  ibkr_instruments is pre-populated from DB rows. Close detection never fires. WARNING logged.
  Root cause: IG demo drops idle TCP connections; retry succeeds but returns empty response body.

Ghost closes now require BOTH fixes to fail simultaneously — not possible.

---

## Safety Defects Fixed (2026-04-21)

D1 — Kill switch was in dead code run_continuous(). Fixed: check_kill_switch() at top of
     execution agent run_cycle() before any other action.

D2 — No proactive drawdown monitor. Fixed: _check_drawdown_circuit_breaker() in risk guardian
     run_cycle(), called before poll_and_validate() every cycle.

D3 — Stale approvals could fire after circuit breaker set. Fixed: inline circuit_breaker_active
     DB check immediately before every execute_trade() call.

---

## Learning Module — Reset Procedure

The learning module reads from FOUR tables. All four must be wiped:

  1. forex_network.proposals
  2. forex_network.rejection_patterns
  3. forex_network.trade_autopsies     <-- commonly missed, causes stale signals on restart
  4. forex_network.pattern_memory

```bash
sudo systemctl stop neo-learning-module

python3 -c "
import psycopg2
conn = psycopg2.connect(host='algodesk-postgres-dev.chubomleuerz.eu-west-2.rds.amazonaws.com',
  dbname='postgres', options='-c search_path=forex_network')
cur = conn.cursor()
cur.execute('DELETE FROM proposals'); print('proposals:', cur.rowcount)
cur.execute('DELETE FROM rejection_patterns'); print('rejection_patterns:', cur.rowcount)
cur.execute('DELETE FROM trade_autopsies'); print('trade_autopsies:', cur.rowcount)
cur.execute('DELETE FROM pattern_memory'); print('pattern_memory:', cur.rowcount)
cur.execute(\"UPDATE agent_heartbeats SET cycle_count=0, error_count=0 WHERE agent_name='learning_module'\")
conn.commit(); conn.close()
"

# Edit DATA_QUALITY_CUTOFF in learning_module.py line ~112
# Use STRING format: '2026-04-21 17:30:00+00' NOT datetime(...) form
sudo systemctl start neo-learning-module
```

Schema fixes applied 2026-04-21:
- pattern_memory UNIQUE (user_id, pattern_key) — ON CONFLICT now works
- pattern_memory.pattern_type DEFAULT 'rejection' — INSERT no longer fails
- cursor_factory kwarg removed from _get_recent_context() — no more TypeError

Do not approve learning module proposals until 20+ legitimate closed trades exist per profile.

---

## Dashboard Patch Pattern

```bash
aws s3 cp s3://<bucket>/index.html /tmp/fix.html --no-progress 2>/dev/null
# ... edit /tmp/fix.html ...
aws s3 cp /tmp/fix.html s3://<bucket>/index.html \
  --content-type "text/html" --no-progress 2>/dev/null
aws cloudfront create-invalidation --distribution-id <DIST_ID> \
  --paths "/index.html" --region eu-west-2
# ~30-60s to propagate
```

---

## Common DB Queries

```sql
-- Open positions
SELECT user_id, instrument, deal_id, position_size, entry_price, opened_at
FROM trades WHERE exit_time IS NULL ORDER BY opened_at;

-- Agent heartbeats
SELECT agent_name, last_seen,
       EXTRACT(EPOCH FROM (NOW()-last_seen))/60 AS age_min,
       cycle_count, error_count
FROM agent_heartbeats ORDER BY agent_name, user_id;

-- Risk parameters
SELECT user_id, account_value, peak_account_value,
       convergence_threshold, circuit_breaker_active,
       daily_loss_limit_pct, updated_at
FROM risk_parameters ORDER BY user_id;

-- Duplicate open positions check
SELECT user_id, instrument, COUNT(*)
FROM trades WHERE exit_time IS NULL
GROUP BY user_id, instrument HAVING COUNT(*) > 1;
```

---

## exit_reason Constraint Values

Valid as of 2026-04-21:
  stop_loss, stop_hit, stop_failed, trailing_stop, take_profit, regime_change,
  manual, pre_normalisation, circuit_breaker, time, time_exit, signal_exit,
  duplicate_close, max_hold_exceeded

---

## Known Traps

Trap 13 (strengthened) — IG FIFO closes ignore dealId. API close hits wrong position and may
open accidental position on different instrument if epic lookup fails. Only reliable selective
close = IG web portal. If epic lookup fails during close, ABORT — never fall back to default.

Trap 20 — AWS CLI progress text prepends to HTML if S3 download piped incorrectly.
Always use --no-progress 2>/dev/null when downloading S3 files.

Trap 21 — React useEffect fires before browser paints DOM. getElementById returns null if called
synchronously. Always use setTimeout(fn, 100-150ms) for DOM elements in same state change.

Trap 22 — CDN scripts in <head> load asynchronously. Inline the library to eliminate race
condition. QR code on trading dashboard replaced with inline pure-JS encoder.

Trap 23 — Silent catch(e) { console.error(e) } masks broken API contracts. Always surface
visible error state. Was cause of proposals buttons silently failing (HTTP 400 swallowed).

Trap 24 — IG epic lookup failure causes fallback to wrong epic, opening accidental position
on different instrument. FIXED 2026-04-22: RuntimeError raised in close_position() if epic
not found (was silently falling back to CS.D.EURUSD.MINI.IP). _close_position() catches it.

Trap 25 — IG demo drops idle TCP keep-alive connections. RemoteDisconnected fires on retry.
The 6h session TTL check passes (session not "expired") but socket is dead. _force_reconnect()
now handles this — creates fresh socket + session, bypasses TTL. Also: IG can return HTTP 200
with empty positions [] on reconnect before session fully loads. manage_open_positions() now
guards against this — never treat empty response as "all positions closed" if DB has open trades.

---

## Pre-Live Gate Checklist

- [ ] Stress tests re-run clean (kill switch, drawdown, circuit breaker — all PASS)
- [ ] Trap 24: epic lookup abort on failure in execution_agent.py
- [ ] Git repository init and initial commit
- [ ] Dev/staging/prod environment separation
- [ ] Walk-forward validation (50+ closed trades, WFO pass rate >60%)
- [ ] IG swap rates wired (RG2) — Notion 3484d2ece67681e59308e9d0f1813c35
- [ ] Alpha Vantage key rotation
- [ ] Sharpe >1.0, Sortino >1.5, max drawdown <8%, stress score <50

---

## Next Session Priority

1. ~~Trap 24 — epic lookup abort hardening~~ DONE 2026-04-22
2. Stress test re-run (all three — expect PASS)
3. Git repository init
4. Monitor trade sizing on new trades (~3.9 lots Balanced, ~6.6 lots Aggressive)
5. Monday threshold recalibration (weekday active-session percentiles only)
6. ~~neo-ingest-ig-sentiment-dev — add 13 cross pairs~~ DONE 2026-04-22
7. ~~neo-ingest-prices-dev — add 13 cross pairs~~ DONE 2026-04-22
8. Technical agent interruptible sleep pre-open wake (macro/regime agents still pending)
9. Regime snapshot max age 30→55 min
10. Same-cycle correlation race condition (pre-live)

---

## Dashboard Updates — April 21 Evening (Post-Session)

### QR Code Fix (trading dashboard) — Final Resolution
After multiple attempts, the working solution is qrcodejs@1.0.0 from unpkg.com.

Script tag order in <head> (trading dashboard):
  <script src="https://unpkg.com/@babel/standalone/babel.min.js"></script>
  <script src="https://unpkg.com/chart.js/dist/chart.umd.js"></script>
  <script src="https://unpkg.com/qrcodejs@1.0.0/qrcode.min.js"></script>
  <script>window.QRCodeReady=true;</script>

History of failed approaches:
1. cdnjs qrcode.min.js — async load race condition (Trap 22)
2. Inline pure-JS QR encoder (custom written) — rendered visually but not scannable by
   authenticator apps. Canvas output unreadable by mobile camera/TOTP apps.
3. qrcodejs@1.0.0 from unpkg — WORKING. Correct QR, scannable, authenticator adds account.

Additional fixes in same session:
- chart.js CDN swapped from cdn.jsdelivr.net → unpkg.com/chart.js/dist/chart.umd.js
  (jsdelivr.net blocked by CloudFront CSP script-src directive)
- mfaSecret URL-encoded with encodeURIComponent() in otpauth:// URI (harmless on base32
  but correct practice)
- Staleness alert threshold raised from 15 → 30 minutes in risk_guardian_agent.py
  _check_risk_parameters_staleness() line ~1198 (STALENESS_THRESHOLD_MIN = 30)

### Trap 26
otpauth:// URI fields must be URL-encoded. Cognito base32 secrets can contain + and =
padding — unencoded, + becomes a space and = is misinterpreted by authenticator apps,
causing silent QR scan failures with no error message.

### Trap 27
Canvas-rendered QR codes (HTML5 canvas via custom JS encoder) may render visually but
fail to scan on mobile. Use a battle-tested QR library (qrcodejs, qrcode.js) rather than
a custom canvas implementation. Visual rendering is not proof of scannability.

### CSP Note
CloudFront CSP script-src allowlist: unpkg.com is allowed, cdn.jsdelivr.net is NOT.
Always load external scripts from unpkg.com. Verify against CSP before deploying any
new external script tag.

---

## Learning Module — Proposal Gate (applied 2026-04-21)

Two gates added to prevent proposals being generated from insufficient data:

1. _generate_proposals() line ~1537 — MIN_TRADES_FOR_PROPOSALS = 20
   Counts trades WHERE exit_time > DATA_QUALITY_CUTOFF. If < 20, skips entirely.

2. check_sortino_threshold() line ~896 — same gate on closed trade count.
   Returns empty proposals list, _write_proposals() never called.

Current state: 0 clean closed trades post-cutoff. Proposals suppressed.
Gate will lift automatically once 20 legitimate trades close after 18:15 UTC.

Do not approve any proposals until this gate has lifted naturally.

DATA_QUALITY_CUTOFF final value: '2026-04-21 18:15:00+00'
Excludes: pre-normalisation trades, duplicate bug trades, stress test trades,
          ghost-closed stop losses (last exit 18:12 UTC).

---

## Execution Agent — Cycle Interval Fix (2026-04-22)

CYCLE_INTERVAL_MINUTES was effectively 15 minutes (900s default) — the 10s
run_continuous() loop was dead code never called in the multi-user production path.

Fixed: CYCLE_INTERVAL_MINUTES = 0.5 (30 seconds) on the class.

Off-hours sleep extension added to main loop (lines 3177–3180):
- Active hours 07:00–22:00 UTC: 30s cycle
- Off-hours 22:00–07:00 UTC: 300s cycle (5 min)

Approvals are now picked up within ~30s of risk guardian writing them.

## Technical Agent — Interruptible Sleep Gap (2026-04-22)

Technical agent has no interruptible sleep — uses plain time.sleep(1800) in quiet
hours. At the 07:00 UTC London open transition, the technical agent may be mid-sleep
and signals go stale (20-min TTL) before it wakes.

Observed: technical agent woke at 07:09 UTC, orchestrator missed the 07:13 window,
execution agent didn't pick up approvals until manually restarted at 08:23 UTC.

Fix pending: Add interruptible sleep to technical agent (same pattern as macro agent
lines 1483–1513) so it wakes at the 07:00 UTC market state transition.

## P&L — DB Now Matches IG (2026-04-22)

- DB pnl backfilled from IG transaction CSV for all post-normalisation trades
- execution agent _close_position() now writes IG-confirmed P&L via /confirms endpoint
  (exit_price from fill, pnl directly from IG profit field, pnl_pips computed locally)
- account_value_currency now populated from IG accounts response (was always NULL,
  dashboard was falling back to hardcoded "GBP")
- Dashboard fmt() fixed: negative P&L now shows -£18.00 not £18.00, $ replaced with £

## Currency Detection (2026-04-22)

ig_broker.get_netliquidation() now reads acct["currency"] from IG response instead
of hardcoding "GBP". get_account_value() returns (float, currency) tuple.
execution_agent.update_account_value() writes account_value_currency to risk_parameters.
Dashboard reads it automatically — works for any currency account.

---

## Correlation Threshold Fix (2026-04-22)

CORRELATION_THRESHOLDS in risk_guardian_agent.py was keyed by username
(neo_user_001/002/003) not UUIDs. All profiles silently fell through to
default 0.75. Fixed to use actual UUIDs:

  Conservative (e61202e4...): 0.70  -- blocks at >= 0.70 (catches GBPUSD/NZDUSD exactly)
  Balanced     (76829264...): 0.75
  Aggressive   (d6c272e4...): 0.80

## Same-Cycle Race Condition (known issue — fix before go-live)

Orchestrator approves multiple pairs in one cycle. Risk guardian checks
open positions at cycle start, not per-approval. Result: correlated pairs
can open simultaneously in the same cycle before either is in the DB.

Observed: AUDUSD+NZDUSD (corr 0.88) opened simultaneously for Balanced and
Aggressive. EURUSD+GBPUSD (corr 0.87) also opened in same cycle.

Fix needed: risk guardian must re-check open positions after each approval
in a batch, not just at cycle start.

## Execution Agent Cycle Intervals (2026-04-22)

Active hours (07:00-22:00 UTC): 30s
Off-hours (22:00-07:00 UTC): 300s (5 min)

Orchestrator: 300s active, 600s off-hours
Risk guardian: 15s active, 300s off-hours

## Admin Open Positions Panel (2026-04-22)

Lambda: neo-admin-open-positions-dev
Route: GET /admin/open-positions (JWT auth, API k24gcypjyh, integration hprbtck)
Location: Trades tab, above AdminPerformancePanel
Shows: profile, instrument, direction, size (lots), entry, current price,
       unrealised P&L (GBP), stop, target, time open. 30s auto-refresh.

## Technical Agent Interruptible Sleep (PENDING)

Technical agent uses plain time.sleep(1800) in quiet hours — no market-state
wake-up on transition. At 07:00 UTC London open, signals go stale before agent
wakes, causing 9-minute delay to first trades.

Fix: add interruptible sleep pattern matching macro agent (lines 1483-1513).
Check market state every 30s during sleep, break on state transition.

## Regime Snapshot Max Age (PENDING)

REGIME_SNAPSHOT_MAX_AGE_MINUTES = 30 but max regime sleep = 60 min.
Orchestrator falls back to session=unknown and stress=50 for up to 30 min
mid-sleep. Raise to 55 to match sleep cycle.

---

## Admin Open Positions Panel (2026-04-22 afternoon)

Lambda: neo-admin-open-positions-dev
Route: GET /admin/open-positions (JWT auth, API k24gcypjyh, integration hprbtck)
Location: Trades tab, above AdminPerformancePanel
Columns: Profile, Instrument, Dir, Size (lots), Entry, Current, Pips, Stop, Target, Held
Mobile (<768px): Stop/Target/Held hidden, 7 columns shown
Auto-refresh: 30s with rest of dashboard data

## Dashboard P&L Display (confirmed correct 2026-04-22)

Trades Lambda (neo-admin-trades-dev) aliases:
  convergence_score → conv   (t.conv in dashboard)
  agents_agreed     → agents (t.agents in dashboard)
  regime_at_entry   → regime (t.regime in dashboard)

These fields will be NULL on trades opened before 2026-04-22 execution_agent fix.
Will populate naturally on all future trades. No code change needed.

## Cycle Intervals (as of 2026-04-22)

Execution agent:  30s active  / 300s off-hours (22:00-07:00 UTC)
Orchestrator:    300s active  / 600s off-hours
Risk guardian:    15s active  / 300s off-hours

## Correlation Thresholds (fixed 2026-04-22)

Now keyed by actual UUIDs (was neo_user_001/002/003 — never matched):
  e61202e4 (Conservative): 0.70  -- blocks at >= 0.70
  76829264 (Balanced):     0.75
  d6c272e4 (Aggressive):   0.80

Boundary: >= means 0.70 correlation blocks Conservative (catches GBPUSD/NZDUSD pair).

## Same-Cycle Correlation Race (known issue — pre-live gate)

Orchestrator approves multiple pairs in one cycle. Risk guardian checks open
positions at cycle start only. Correlated pairs can open simultaneously before
either appears in the DB.

Observed 2026-04-22: AUDUSD+NZDUSD (corr 0.88) opened together for Balanced
and Aggressive. EURUSD+GBPUSD (corr 0.87) also opened in same cycle.

Fix: risk guardian must re-check open positions after each approval in a batch.

## P&L Architecture (confirmed correct 2026-04-22)

Source of truth: IG account balance (execution agent polls every 30s)
DB pnl column: written from IG /confirms response on every close (not computed locally)
Dashboard: reads risk_parameters.account_value for balance, account.daily_pnl for today

DB backfill: 49 historical trades updated from IG transaction CSV 2026-04-22.
Gap: -£2.67 vs IG (12 pre-normalisation trades zeroed intentionally).

## Pending Pre-Live Items

1. Technical agent interruptible sleep — 9-min London open delay observed 2026-04-22
2. Regime snapshot max age 30→55 min
3. Same-cycle correlation race condition
4. Stress test formal re-run (D1/D2/D3 confirmed live, re-run pending)
5. Git repository init
6. Trap 24 — epic lookup abort on failure

---

## C13 Take-Profit Implementation (2026-04-22)

Three exit mechanisms now live in manage_open_positions():

1. TARGET PRICE MONITOR (new — line ~1963)
   Each cycle: reads mktPrice from ibkr_instruments, compares against
   target_price. Long: current >= target → close. Short: current <= target → close.
   exit_reason = 'take_profit'. Skips time-exit check after closing.
   Target prices are NOT decorative — they are now actively monitored.

2. TRAILING STOP ACTIVE FLAG (fixed — line ~2171)
   trailing_stop_active = TRUE written to DB on every trailing stop move.
   Was never written before. Activates at 1R profit, ratchets with price.
   Flag stays TRUE for life of trade once set.

3. TIME-BASED EXIT (pre-existing — confirmed working)
   _get_max_hold_days(): stress>50 → 2 days, trending → 5 days, else → 3 days
   regime_at_entry NULL → 3 days (current open trades all NULL)
   exit_reason = 'max_hold_exceeded'

Exit priority order in manage_open_positions():
  1. Close detection (position gone from IG → stop_hit)
  2. Trailing stop update
  3. Target price check → take_profit
  4. Time-based exit → max_hold_exceeded
  5. Circuit breaker Tier 2

## C13 Exit Architecture — Regime-Aware (updated 2026-04-22)

Regime check added to target price monitor:

  trending regime   → skip fixed target, trailing stop manages exit (5-day max hold)
  ranging/other     → close at structure target → exit_reason='take_profit' (3-day max hold)
  NULL regime       → treated as ranging (3-day max hold, target respected)

Current open trades backfilled with regime_at_entry from closest preceding regime signal:
  AUDUSD ×2, GBPUSD ×3, EURUSD ×2 → trending → trailing stop only, 5-day window
  NZDUSD ×3 → ranging → target price active, 3-day window

Future trades: regime_at_entry written at entry via execution_agent FIX 3 (2026-04-22).

---

## Pre-Open Wake — Technical Agent (2026-04-22)

Quiet hours sleep replaced with interruptible loop in technical_agent.py.
Wakes at 06:40 UTC (London pre-open) and 12:40 UTC (NY pre-open).
Also wakes on any quiet→active state transition.
Polls every 30s during quiet sleep.

Macro and regime agents: still use flat sleep with state-transition detection
but no pre-open time wake. Add 06:40/12:40 check to both — pending.

## C13 Exit Architecture (2026-04-22)

Exit priority in manage_open_positions():
  1. Close detection (position gone from IG → stop_hit)
  2. Trailing stop update + trailing_stop_active = TRUE written to DB
  3. Target price check:
     - Trending regime: SKIP (trailing stop handles exit)
     - Ranging/unknown: close at target → exit_reason='take_profit'
  4. Time-based exit: trending=5d, ranging=3d, stress>50=2d → max_hold_exceeded
  5. Circuit breaker Tier 2

regime_at_entry must be populated for correct max_hold_days.
All trades opened after 2026-04-22 execution_agent restart will have it set.
Backfill query available for any trades with NULL regime_at_entry.

## Trajectory Code Status (confirmed 2026-04-22)

All three signal agents have trajectory code live and firing:
- Macro: last 5 cycles of own scores injected into LLM prompt
- Technical: post-LLM confidence modifier from trajectory features  
- Regime: ADX transitional counts persisted via agent_state

Agents deliberately do NOT see open positions, stops, targets or P&L.
Clean separation prevents anchoring bias. This is by design.

---

## Portfolio Architecture Redesign (2026-04-22 afternoon)

### Problem
All 7 instruments USD-denominated. USD factor concentration causes all positions to
lose simultaneously when USD direction reverses. Pairwise correlation blocks insufficient.

### Decision: 20 instruments
7 USD pairs (existing) + 13 cross pairs:
Tier 1: EURGBP, EURJPY, GBPJPY
Tier 2: EURCHF, GBPCHF, EURAUD, GBPAUD, EURCAD, GBPCAD
Tier 3: AUDNZD, AUDJPY, CADJPY, NZDJPY

### Dynamic Allocation: Cross-Sectional Signal Ranking
All 20 instruments compete on convergence score each cycle.
USD factor cap (2/3/4 units) = safety net ceiling, not quota.
Top K by score take position slots.
Research: ~3x Sharpe improvement over fixed allocation (PMR 2021).

### Per-Currency Scoring (macro agent refactor required)
Current: macro agent scores pairs directly (per-pair).
Required: macro agent scores 8 currencies independently.
All 20 pair signals derived: pair_signal = base_score - quote_score.
Adjustments: JPY/CHF safe-haven (VIX), AUD commodity (iron ore), CAD oil.
Cross pairs: apply 1.5x amplifier to compensate score compression.

### Key Bug Risks
- B9:  USD unit count sign error (USDCHF long = USD LONG not short)
- B10: Cross-pair scores compressed near zero without amplifier
- B14: Pip value wrong for cross pair quote currencies
- B15: JPY pip size 0.01 not 0.0001 (100x sizing error)
- B16: Macro derived score direction inverted if convention wrong

### Phase 0 Prerequisites (before any code)
1. Confirm macro agent scoring architecture (per-pair vs per-currency)
2. Verify all 13 cross pair epics exist on IG demo
3. Confirm Polygon snapshot covers cross pairs
4. Audit regime agent: batch vs individual LLM calls
5. Confirm pip size assumption in execution agent

### Full Reset Plan
After all changes implemented and debugged, full DB reset and fresh start.
DATA_QUALITY_CUTOFF removed post-reset.
Estimated 7-10 working days to implementation + debug.

### Notion Pages
- Portfolio Architecture v2:    34a4d2ece67681bb9e63d066bb4485b0
- Dynamic Allocation spec:      34a4d2ece67681c098fdd63a7c17a3bb
- Cross-Pair Implementation:    34a4d2ece67681aaa000f3218d6cec78
- Full Reset Plan:              34a4d2ece67681bf8840ec42fc3b9b65
- Macro Per-Currency Scoring:   (see Cross-Pair Implementation spec)

---

## Phase 0 Findings — April 22 (pre cross-pair implementation)

### Macro agent scoring: CONFIRMED PER-PAIR
PAIRS = ["EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD", "NZDUSD"]
Each pair has its own primary_cbs and key_releases config.
Score: -1.0 to +1.0 per pair. LLM prompt: claude-sonnet-4-6, max_tokens 6000.
Signal payload: reasoning, data_sources, confidence_adjustments (full adversarial defence).
CONCLUSION: Full refactor to per-currency scoring is required. Phase 2 is a major
rewrite of macro agent scoring logic and LLM prompt.

### Historical prices: CONFIRMED CLEAN (USD pairs only)
15M: ~50k candles from 2024-04-14 | 1H: ~62k from 2016-04-14 | 1D: ~5200 from 2009-09-25
Cross pairs: NOT INGESTED. All 13 cross pairs need historical data via EODHD before
technical indicators can be calculated. Phase 1.4 task.

### Still unconfirmed (remaining Phase 0 items)
- IG demo cross pair epics: all 13 need verifying before EPIC_MAP update
- Polygon snapshot cross pair coverage: unconfirmed
- Regime agent: batch vs individual LLM calls per instrument
- Execution agent: pip size assumption (0.0001 hardcoded or lookup table)

---

## Cross-Pair Lambda Deployments (2026-04-22)

### neo-ingest-ig-sentiment-dev
`FX_MARKET_IDS` expanded from 7 → 20 pairs. Smoke test: all 20 pairs returned
live sentiment data (0 errors). Lambda uses live IG account, handler.handler entry.
Cross pairs confirmed working: EURGBP, EURJPY, GBPJPY, EURCHF, GBPCHF, EURAUD,
GBPAUD, EURCAD, GBPCAD, AUDNZD, AUDJPY, CADJPY, NZDJPY.
Note: handler is in handler.py (not lambda_function.py — old file, ignored).

### neo-ingest-prices-dev
`PAIRS` list expanded from 7 → 20 pairs. TraderMade nightly backfill will now
include all 13 cross pairs starting tonight (22:30 UTC).
Note: IG allowance exhausted ~2026-04-22 (resets ~Wednesday). Historical backfill
for 11 remaining cross pairs will happen via TraderMade nightly Lambda.
