# 🚀 Deployment Log — April 17, 2026

Source: https://www.notion.so/3454d2ece67681159f35e20956b90a15

## Deployment Status
- EC2 instance: `i-02d767a507a0301c0` (`ip-10-50-1-4`)
- Virtualenv: `/root/algodesk/algodesk`
- Agent root: `/root/Project_Neo_Damon/`
- Run user: `root`
- Database: `postgres` (NOT `algodesk`)
- S3 deployments: `algodesk-agent-deployments-dev`

| Agent | Service | Status |
|---|---|---|
| Macro | neo-macro-agent | ✅ Running |
| Technical | neo-technical-agent | ✅ Running |
| Regime | neo-regime-agent | 🛠️ Deploying |
| Orchestrator | neo-orchestrator-agent | ✅ Deployed |
| Risk Guardian | neo-risk-guardian-agent | 🚧 Next |
| Execution | neo-execution-agent | Pending |
| Learning Module | neo-learning-module | Pending |

## User ID Mapping
- neo_user_001 → `e61202e4-30d1-70f8-9927-30b8a439e042`
- neo_user_002 → `76829264-20e1-7023-1e31-37b7a37a1274`
- neo_user_003 → `d6c272e4-a031-7053-af8e-ade000f0d0d5`

## Bugs Already Fixed During Deployment

### All agents
- Database name: `postgres` not `algodesk`
- UUID resolution: `_resolve_user_id()` converts Cognito username → UUID at startup
- `search_path=forex_network,shared,public` on DB connections
- Deploy scripts: virtualenv `/root/algodesk/algodesk`, agent dir `/root/Project_Neo_Damon/`, user `root`, no `StartLimitIntervalSec`, safe `cp` for same source/dest

### MCP agents (macro, technical, regime)
- `client.beta.messages.create()` with `extra_headers={"anthropic-beta": "mcp-client-2025-04-04"}`
- EODHD MCP URL: `https://mcpv2.eodhd.dev/v2/mcp` (no authorization_token)
- EODHD MCP auth still returning 400 — needs EODHD-side OAuth config (non-blocking)

### Macro + Technical (Sonnet-built)
- Heartbeat: `self.user_id` not `'system'::uuid`
- Signal writes: missing `user_id` column added
- CLI: `--user` (required) and `--test` flags added
- Transaction rollback on all DB error handlers
- `importance` column removed from `economic_releases` query

### Technical agent
- `decimal.Decimal` → `float` conversion for OHLCV and price metrics
- ATR values cast to `float()` before arithmetic

### Regime agent
- Dynamic column detection for `market_context_snapshots`
- All 8 fetch functions wrapped with try/except + rollback
- `user_label` removed (doesn't exist on `risk_parameters`)
- `pair_a` column auto-detection on `portfolio_correlation`
- f-string formatting fix for ADX display
- `stress_score_confidence` converted from string to numeric
- `session` and `day_of_week` made optional in INSERT

### Execution agent
- Trailing stop protocol: `manage_open_positions()` every cycle
- Circuit breaker Tier 2: close losers → tighten winners → close winners if still in breach
- Swap cost on trade close
- IBKR account ID queried at startup, no hardcoded `DU_PAPER`

### Learning module
- Recovery gate: 50% recovery before step-up, tracks `step_down_drawdown_pct`

## Migrations Required
- `migrate_regime_columns_v1.sql`
- `migrate_recovery_gate_v1.sql`

## Known Non-Blocking Issues
- EODHD MCP OAuth: 400 auth error — EODHD-side config needed
- Polygon API: 0 live prices outside market hours (falls back to RDS)
- GDELT API: 429 rate limiting (handled gracefully)
- ADX values appear very high (297, 545) — possible Decimal/float in regime price metrics

---

# REMAINING WORK FOR THIS SESSION

1. **Technical agent** — still running old single-user service file; needs redeployment so it runs for all 3 users (same pattern as Macro/Orchestrator/Execution).
2. **Risk Guardian** — exits immediately; poll interval fix needs deploying.
3. **Kelly values** — update `max_position_size_pct` from `0.5 / 1.5` to `1.0 / 3.0`.
4. **Verify all 7 heartbeats reporting for all 3 users.**
5. **Run E2E test** (`/root/algodesk/e2e_test.py`) and fix any remaining failures.

Current services on box:
- neo-execution-agent.service (all users) — running
- neo-macro-agent.service (all users) — running
- neo-orchestrator-agent.service (all users) — running
- neo-regime-agent.service (neo_user_002) — running [single-user, may also need multi-user]
- neo-technical-agent.service (neo_user_002) — running [NEEDS multi-user redeploy]

Unit files: `/etc/systemd/system/neo-*.service`
Logs: `/var/log/neo/`
