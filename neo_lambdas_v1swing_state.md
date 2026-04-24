# Project Neo — Lambda V1 Swing State

**Date:** 2026-04-24  
**Change:** Regime agent decommission + V1 Swing alignment

---

## Lambda State Summary

| Lambda | Status | Notes |
|--------|--------|-------|
| `neo-admin-signals-dev` | ENABLED, UPDATED | stress_score omitted when None (post-V1-Swing) |
| `neo-critical-alerter-dev` | **DISABLED** | Reads system_stress_alerts; no new writes post-decommission |
| `neo-daily-summary-dev` | ENABLED, UPDATED | Replaced stress query with V1 Swing open-positions health |
| `neo-admin-open-positions-dev` | ENABLED | No regime/stress reads — no change needed |
| `neo-admin-trades-dev` | ENABLED | No regime/stress reads — no change needed |
| `neo-dash-positions-dev` | ENABLED | No regime/stress reads — no change needed |
| `neo-ingest-*` | ENABLED | Data ingest — unaffected |

---

## Fix Details

### neo-admin-signals-dev (Fix 1)

**Problem:** `stress_score` is now `None` in orchestrator decision payloads
post-regime-decommission. Handler previously spread this `null` into
`card_payload` via the ctx dict comprehension.

**Change (handler.py):**
- `signals_full` ctx dict: now filters out `None` values so `stress_score=None`
  is not included in card_payload (dashboard safety)
- `decisions` list: `stress_score` key omitted entirely when `None` (rather
  than returning `null`) so the decisions table column naturally hides for
  V1 Swing cycles

**Commit:** `46b345b`  
**Deploy:** 2026-04-24

---

### neo-critical-alerter-dev (Fix 2)

**Problem:** Lambda reads `forex_network.system_stress_alerts` which no longer
receives writes (regime agent decommissioned). The table has historical data
but V1 Swing has no stress-alert concept.

**Decision:** DISABLED (not deleted). Lambda and function code preserved.

**EventBridge Rule:** `neo-critical-alerter-schedule` → **DISABLED**  
Schedule was: `rate(5 minutes)`

**Re-enable if needed:**
```bash
aws events enable-rule --name neo-critical-alerter-schedule --region eu-west-2
```

**Note:** `system_stress_alerts` table is preserved with historical data.
If a future strategy reintroduces stress monitoring, re-enable the rule
and update the Lambda to handle new alert format.

---

### neo-daily-summary-dev (Fix 3)

**Problem:** `compute_daily_summary` queried `shared.market_context_snapshots`
for daily stress min/max/avg. Table no longer populated post-decommission.

**Change (neo_daily_summary.py):**
- Replaced stress query with `COUNT(*) FROM forex_network.trades WHERE exit_time IS NULL`
  (current open positions count)
- `stress_score_min/max/avg` fields preserved in DB schema as `0` (no schema change)
- `open_positions_count` added to summary dict
- Email `SYSTEM` section now shows: `Open positions: N  (V1 Swing)`

**Commit:** `46b345b`  
**Deploy:** 2026-04-24

---

## Flagged for Future Follow-up

These references to stress/regime exist in Lambda code but were NOT changed in
this dispatch (read-only or harmless with empty table):

- `neo-daily-summary-dev/neo_daily_summary.py:311` — weekly email still has
  stress_state line from `detail['stress_state']` (now returns "V1 Swing")
- `neo-critical-alerter-dev` — entire Lambda disabled; code unchanged

---

## DB Schema Preserved

The following tables/columns are **NOT dropped** — they hold historical data
and are needed if stress monitoring is reintroduced:

- `shared.market_context_snapshots` — historical snapshots
- `forex_network.system_stress_alerts` — historical stress alerts
- `forex_network.agent_signals.stress_score` — historical scores (now NULL for new rows)
- `forex_network.daily_summaries.stress_score_min/max/avg` — now stored as 0
- `forex_network.weekly_reports.stress_score_avg/peak` — now stored as 0
