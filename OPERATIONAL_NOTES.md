# Project Neo — Operational Notes
**Last updated:** 2026-04-23 (morning session)
**EC2 instance:** i-02d767a507a0301c0 (ip-10-50-1-4, algodesk-vpc-dev, eu-west-2)
**Virtualenv:** `/root/algodesk/algodesk`
**Agent root:** `/root/Project_Neo_Damon/`
**User:** root
**GitHub:** https://github.com/damonmhirschl-dotcom/project-neo (private)
**Git:** push with `cd /root/Project_Neo_Damon && git add -A && git commit -m "msg" && git push`

---

## How to Start a Session

1. Run `bash /root/Project_Neo_Damon/check_integrity.sh`
2. Fetch Notion session reference: `3434d2ec-e676-81ef-9b5d-f2b466bfc54e`
3. Fetch Session Continuity page: `34a4d2ec-e676-8143-9d23-f52de39a6216`
4. Read this file
5. Confirm ready — state system status, open positions, and next priority

---

## DB Connection — Copy-Paste Ready

**Database name is `postgres` (NOT `algodesk`)**

```bash
# One-liner for queries
sudo bash << 'EOF'
PW=$(aws secretsmanager get-secret-value --secret-id platform/rds/credentials --region eu-west-2 --query SecretString --output text | python3 -c "import sys,json; print(json.load(sys.stdin)['password'])")
USER=$(aws secretsmanager get-secret-value --secret-id platform/rds/credentials --region eu-west-2 --query SecretString --output text | python3 -c "import sys,json; print(json.load(sys.stdin)['username'])")
HOST=$(aws ssm get-parameter --name /platform/config/rds-endpoint --query Parameter.Value --output text --region eu-west-2)
PGPASSWORD=$PW psql -h $HOST -U $USER -d postgres -c "YOUR QUERY HERE"
EOF
```

---

## Key Table Names

| Table | Purpose |
|---|---|
| `forex_network.trades` | All trades — open and closed |
| `forex_network.agent_signals` | All agent signals per cycle |
| `forex_network.convergence_history` | Orchestrator convergence scores |
| `forex_network.agent_heartbeats` | Agent liveness tracking |
| `forex_network.risk_parameters` | Per-profile risk config |
| `forex_network.price_metrics` | ATR + realised vol per pair/timeframe |
| `forex_network.historical_prices` | OHLCV candles — all pairs/timeframes |
| `forex_network.swap_rates` | Overnight financing (manual populate) |
| `forex_network.proposals` | Learning module proposals |
| `forex_network.rejection_patterns` | Learning module rejection patterns |
| `forex_network.trade_autopsies` | Learning module trade autopsies |
| `shared.portfolio_correlation` | Pairwise correlation (user_id=NULL all rows) |

## Key Column Names — forex_network.trades

| Column | Notes |
|---|---|
| `id` | Trade ID |
| `user_id` | UUID |
| `instrument` | e.g. EURUSD (no slash) |
| `direction` | long / short |
| `entry_price` | Fill price |
| `entry_time` | Open timestamp (NOT open_time) |
| `exit_price` | NULL if open |
| `exit_time` | NULL if open |
| `exit_reason` | stop_hit / take_profit / manual_close / time_exit |
| `pnl` | GBP P&L — NULL if open or snapshot missed |
| `position_size` | Lots |
| `stop_price` | Stop level |
| `target_price` | Take profit level |
| `convergence_score` | Score at entry |
| `session_at_entry` | London / NY / Asian / Overlap |
| `entry_rank_position` | 1-based rank by conviction |

---

## System Status (as of 2026-04-23 morning)

- All 7 agents running
- Architecture: 20-pair cross-sectional
- Kill switch: inactive | Circuit breakers: all FALSE
- DATA_QUALITY_CUTOFF: 2026-04-21 18:15:00 UTC
- Learning module: proposals suppressed until 20 clean closed trades

---

## Agent Services

| Agent | Service | Active cycle | Off-hours cycle |
|---|---|---|---|
| Macro | neo-macro-agent | 900s | 3600s |
| Technical | neo-technical-agent | 900s | 1800s |
| Regime | neo-regime-agent | 900s | 3600s |
| Orchestrator | neo-orchestrator-agent | 300s | 600s |
| Risk Guardian | neo-risk-guardian-agent | 15s | 300s |
| Execution | neo-execution-agent | 30s | 300s |
| Learning Module | neo-learning-module | — | — |

Pre-open wake: 06:40 UTC (London), 12:40 UTC (NY) — macro, technical, regime
Heartbeat: every 30s inside quiet-hours loop — stale threshold 30 min

---

## Agent File Locations

```
/root/Project_Neo_Damon/Macro_Agent/macro_agent.py
/root/Project_Neo_Damon/Technical_Agent/technical_agent.py
/root/Project_Neo_Damon/Regime_Agent/regime_agent.py
/root/Project_Neo_Damon/orchestrator_agent.py
/root/Project_Neo_Damon/risk_guardian_agent.py
/root/Project_Neo_Damon/execution_agent.py
/root/Project_Neo_Damon/learning_module.py
/root/Project_Neo_Damon/ig_broker.py
/root/Project_Neo_Damon/shared/warn_log.py
```

## Dashboard Files

```
/root/Project_Neo_Damon/Dashboard/admin-index.html
/root/Project_Neo_Damon/Dashboard/trading-index.html
/root/Project_Neo_Damon/Dashboard/deploy.sh
```

Always pull fresh from S3 before editing:
```bash
aws s3 cp s3://algodesk-admin-dashboard-dev/index.html /root/Project_Neo_Damon/Dashboard/admin-index.html --no-progress 2>/dev/null
```

Deploy: `cd /root/Project_Neo_Damon/Dashboard && bash deploy.sh`

---

## AWS Configuration

### Secrets Manager (NO leading slash)
| Path | Format |
|---|---|
| `platform/polygon/api-key` | `{"api_key": "KEY"}` |
| `platform/finnhub/api-key` | `{"api_key": "KEY"}` |
| `platform/eodhd/api-key` | `{"api_key": "KEY"}` |
| `platform/alphaVantage/api-key` | `{"api_key": "KEY"}` — rotate before go-live |
| `platform/anthropic/api-key` | `{"api_key": "KEY"}` |
| `platform/rds/credentials` | `{"username": "X", "password": "Y"}` |
| `dev/ibkr/credentials` | `{"username": "X", "password": "Y"}` |
| `platform/github/pat` | `{"token": "X"}` |

### Parameter Store (WITH leading slash)
| Path | Value |
|---|---|
| `/platform/config/rds-endpoint` | algodesk-postgres-dev.chubomleuerz.eu-west-2.rds.amazonaws.com |
| `/platform/config/aws-region` | eu-west-2 |
| `/platform/config/kill-switch` | inactive |
| `/platform/config/state-store-table` | agent_signals |
| `/platform/config/heartbeat-table` | agent_heartbeats |

---

## 20-Pair Architecture

### Instrument List
USD pairs (7): EURUSD, GBPUSD, USDJPY, USDCHF, AUDUSD, USDCAD, NZDUSD
Cross Tier 1 (3): EURGBP, EURJPY, GBPJPY
Cross Tier 2 (6): EURCHF, GBPCHF, EURAUD, GBPAUD, EURCAD, GBPCAD
Cross Tier 3 (4): AUDNZD, AUDJPY, CADJPY, NZDJPY

### Risk Parameters
| Profile | Risk/trade | max_positions | max_usd_units | Threshold |
|---|---|---|---|---|
| Conservative | 1.0% | 4 | 2 | 0.3645 |
| Balanced | 2.0% | 5 | 3 | 0.3148 |
| Aggressive | 3.0% | 6 | 4 | 0.2923 |

### LLM Configuration
- Macro: claude-sonnet-4-6, max_tokens=8000
- Technical: claude-haiku-4-5-20251001, max_tokens=12000
- Both produce 20/20 signals per cycle after schema slim (2026-04-22)
- Gap-fill present as safety net

### Orchestrator
Two-pass: score all 20 → sort by abs(convergence) desc → evaluate in ranked order.

---

## Critical Implementation Notes

### portfolio_correlation
- Table: `shared.portfolio_correlation` (190 rows)
- user_id=NULL on ALL rows — risk guardian must NOT filter by user_id
- Columns: instrument_a, instrument_b, correlation_30d, updated_at

### CHECK 4 + 4.5 — Direction-Aware
- CHECK 4 pre-check: shared_currency_concentration blocks same-direction stacking
- CHECK 4.5: signed net exposure — abs(net + delta) > cap
- long pair = +1 base/-1 quote, short = -1 base/+1 quote
- _curr_cap defined once, shared by both checks

### PIP_SIZE_MAP
- JPY pairs: 0.01, all others: 0.0001
- All pip-sensitive conversion in ig_broker.py only

### IG Swap Rates
- get_swap_rates() always returns None — IG REST doesn't expose financing
- TODO at ig_broker.py:220
- Option A: populate swap_rates manually from IG website quarterly

### Trap 24
- RuntimeError in ig_broker.py lines 619-621 when epic not found
- Both execute_trade call sites wrapped in per-approval try/except

### Learning Module Reset
Wipe ALL THREE tables: proposals, rejection_patterns, trade_autopsies
Then advance DATA_QUALITY_CUTOFF in learning_module.py line ~112.

### Same-Cycle Race Fix
- Orchestrator appends synthetic position after each approval
- Risk guardian passes cycle_positions into each validate_trade call

### ATR Lookback (fixed 2026-04-22)
- 4 x 1H bars = 4 hours (was 16)
- 120 x 1H bars = 5 days (was 96)

### JPY Spread Baseline (fixed 2026-04-23)
- JPY pairs: ~0.010 (1.0 pip)
- Non-JPY: 0.00020 (0.2 pips)

---

## Warning Logs (added 2026-04-23)

Location: `/var/log/neo/{agent}.warnings.log`
Format: `2026-04-23 07:15:32 UTC | AGENT | CATEGORY | message | key=value`

Categories: GAP_FILL, API_FAIL, STALE_DATA, FALLBACK, NULL_DATA, MISSING_SIGNAL,
THRESHOLD, DB_FAIL, PARSE_FAIL, SKIP, TIMEOUT, RETRY, RATE_LIMIT, INVALID_DATA,
LOOP_BREAK, EXCEPTION, SIZE_WARN, MISSING_CONFIG, POSITION_MISMATCH, SIGNAL_EXPIRED,
HEARTBEAT_FAIL, CYCLE_SLOW, CONSTRAINT_FAIL, PARTIAL_FILL, PRICE_STALE,
CORRELATION_SKIP, CAP_BLOCK, RECONNECT, DIVERGENCE, SNAPSHOT_MISS

Check all: `for f in /var/log/neo/*.warnings.log; do echo "=== $f ==="; tail -10 "$f"; done`
Logrotate: 14-day rotation — `/etc/logrotate.d/neo-warnings`

---

## Data State

- All 39 pair combinations in historical_prices
- Nightly Lambda (neo-ingest-prices-dev, 22:30 UTC): all 20 pairs
- price_metrics backfilled for all 13 cross pairs (200 rows each, 2026-04-10 onwards)
- TraderMade allowance resets ~2026-05-01
- Backfill scripts: /tmp/backfill_15m.py, /tmp/backfill_price_metrics.py

---

## Dashboard

### API
- Base URL: `https://k24gcypjyh.execute-api.eu-west-2.amazonaws.com/admin`
- Auth: JWT Bearer (Cognito eu-west-2_3CtTzmJbA) — NOT API key
- HTTP API v2 — NO /dev/ prefix

### Lambda Endpoints
| Lambda | Route |
|---|---|
| neo-admin-signals-dev | GET /signals |
| neo-admin-open-positions-dev | GET /open-positions |
| neo-dash-positions-dev | GET /positions |
| neo-daily-summary-dev | fires 22:00 UTC |
| neo-admin-agents-dev | GET /agents |

---

## Pre-Live Gates

| Gate | Status |
|---|---|
| Git + GitHub | Done — damonmhirschl-dotcom/project-neo |
| Stress test D1/D2/D3 | Done |
| Pre-open wake | Done |
| Heartbeat in quiet-hours | Done |
| Warning logs | Done |
| Walk-forward validation (50+ trades) | Pending — time-dependent |
| IG swap rates (RG2) | Pending — manual populate |
| Alpha Vantage key rotation | Pending — before go-live |
| Macro per-currency scoring refactor | Pending — critical path |
| Full system reset | Pending — after all gates clear |

---

## Session Close Checklist

```bash
cd /root/Project_Neo_Damon && git add -A && git commit -m "Session close — $(date +%Y-%m-%d)" && git push
```

Then update Notion continuity page and this file.

---

## Division of Labour

- This Claude session: strategy, Notion updates, OPERATIONAL_NOTES, dispatch prompts
- Claude Code (EC2): all file edits, greps, restarts, investigations
- Never relay SSH commands manually — write a dispatch prompt
- Dispatch prompts are task-only — no SSH instructions, no preamble
- Work in parallel — dispatch multiple prompts simultaneously
- check_integrity.sh runs first every session
