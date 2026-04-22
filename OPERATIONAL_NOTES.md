# Project Neo — Operational Notes
**Last updated:** 2026-04-22 (late session)
**EC2 instance:** i-02d767a507a0301c0 (ip-10-50-1-4, algodesk-vpc-dev, eu-west-2)
**Virtualenv:** `/root/algodesk/algodesk`
**Agent root:** `/root/Project_Neo_Damon/`
**User:** root
**Git:** commit f7b864e, branch main, local only (no remote yet)

---

## How to Start a Session

1. Run `bash /root/Project_Neo_Damon/check_integrity.sh` first — flags stale files, service status, git state
2. Fetch Notion session reference: `3434d2ec-e676-81ef-9b5d-f2b466bfc54e`
3. Fetch Session Continuity page: `34a4d2ec-e676-8143-9d23-f52de39a6216`
4. Read this file
5. Confirm ready — state system status, open positions, and next priority

---

## System Status (as of 2026-04-22 late session)

- **All 7 agents running, no errors**
- **Architecture:** 20-pair cross-sectional, fully deployed
- **Open positions:** mixed USD + cross pairs
- **Kill switch:** inactive
- **Circuit breakers:** all FALSE
- **DATA_QUALITY_CUTOFF:** 2026-04-21 18:15:00 UTC
- **Learning module:** proposals suppressed until 20 clean closed trades post-cutoff
- **Paper trading account:** ~£149,688

---

## Agent Services

| Agent | Service | Status |
|---|---|---|
| Macro | neo-macro-agent | ✅ Running |
| Technical | neo-technical-agent | ✅ Running |
| Regime | neo-regime-agent | ✅ Running |
| Orchestrator | neo-orchestrator-agent | ✅ Running |
| Risk Guardian | neo-risk-guardian-agent | ✅ Running |
| Execution | neo-execution-agent | ✅ Running |
| Learning Module | neo-learning-module | ✅ Running |

**Cycle intervals:**
- Execution: 30s active, 300s off-hours
- Orchestrator: 300s active, 600s off-hours
- Risk Guardian: 15s active, 300s off-hours
- Macro/Technical/Regime: 900s active, 3600s off-hours (pre-open wake at 06:40 + 12:40 UTC)

**Heartbeat fix (2026-04-22):** All agents now write heartbeat inside quiet-hours sleep loop every 30s. Stale threshold is 30 min — agents will always show active during off-hours.

---

## Agent File Locations

**Deploy command:** `cd /root/Project_Neo_Damon/Dashboard && bash deploy.sh`
**Never edit HTML files outside this directory.** S3 is the live version — always pull fresh before editing.

---

## AWS Configuration

### Secrets Manager (NO leading slash)
| Path | Format |
|---|---|
| `platform/polygon/api-key` | `{"api_key": "KEY"}` |
| `platform/finnhub/api-key` | `{"api_key": "KEY"}` |
| `platform/eodhd/api-key` | `{"api_key": "KEY"}` |
| `platform/alphaVantage/api-key` | `{"api_key": "KEY"}` ⚠️ rotate before go-live |
| `platform/anthropic/api-key` | `{"api_key": "KEY"}` |
| `platform/rds/credentials` | `{"username": "X", "password": "Y"}` |
| `dev/ibkr/credentials` | `{"username": "X", "password": "Y"}` |

### Parameter Store (WITH leading slash)
| Path | Value |
|---|---|
| `/platform/config/rds-endpoint` | algodesk-postgres-dev.chubomleuerz.eu-west-2.rds.amazonaws.com |
| `/platform/config/aws-region` | eu-west-2 |
| `/platform/config/ibkr-environment` | paper |
| `/platform/config/kill-switch` | inactive |
| `/platform/config/state-store-table` | agent_signals |
| `/platform/config/heartbeat-table` | agent_heartbeats |

---

## 20-Pair Architecture

### Instrument List
**USD pairs (7):** EURUSD, GBPUSD, USDJPY, USDCHF, AUDUSD, USDCAD, NZDUSD
**Cross pairs — Tier 1 (3):** EURGBP, EURJPY, GBPJPY
**Cross pairs — Tier 2 (6):** EURCHF, GBPCHF, EURAUD, GBPAUD, EURCAD, GBPCAD
**Cross pairs — Tier 3 (4):** AUDNZD, AUDJPY, CADJPY, NZDJPY

### Risk Parameters
| Profile | Risk/trade | max_positions | max_usd_units | Convergence threshold |
|---|---|---|---|---|
| Conservative (neo_user_001) | 1.0% | 4 | 2 | 0.3645 |
| Balanced (neo_user_002) | 2.0% | 5 | 3 | 0.3148 |
| Aggressive (neo_user_003) | 3.0% | 6 | 4 | 0.2923 |

### Agent Weights
- Macro: 35%, Technical: 45%, Regime: 20%
- Signal expiry: 20 minutes
- Pre-open wake: 06:40 UTC (London), 12:40 UTC (NY)

### Orchestrator — Cross-Sectional Ranking
Two-pass approach:
1. Score all 20 pairs, write convergence history
2. Sort by abs(convergence) descending → evaluate in ranked order

### Macro + Technical Agent — Gap-Fill
LLM returns 7-9/20 pairs despite 20-pair prompt. Gap-fill pads to 20 signals with neutral fallback (score=0.0, confidence=0.2). This is confirmed working — not a token limit issue (max_tokens=6000, 20 blocks ~3000 tokens).

---

## Critical Implementation Notes

### portfolio_correlation
- Table: `shared.portfolio_correlation` (190 rows, 19 distinct pair_1 values)
- **user_id=NULL on ALL rows** — risk guardian must NOT filter by user_id
- Columns: instrument_a, instrument_b, correlation_30d, updated_at

### CHECK 4 + 4.5 — Direction-Aware (updated 2026-04-22)
- CHECK 4 pre-check: shared_currency_concentration blocks same-direction currency stacking before correlation lookup
- CHECK 4.5: direction-aware signed net exposure — long pair = +1 base/-1 quote, short = -1 base/+1 quote
- Cap fires when abs(net + delta) > cap, not on unsigned count
- _curr_cap defined once at top of CHECK 4, shared by both checks

### PIP_SIZE_MAP
- JPY pairs: 0.01, all others: 0.0001
- Lives in ig_broker.py — all pip-sensitive conversion goes through here

### IG Swap Rates
- `get_swap_rates()` always returns None — IG REST does not expose overnight financing
- TODO at ig_broker.py:220. Option A: populate swap_rates manually from IG website (20 pairs, quarterly)

### Trap 24
- RuntimeError in ig_broker.py lines 619–621 when epic not found on close
- Both execute_trade call sites wrapped in per-approval try/except

### Learning Module Reset
Wipe ALL THREE tables together:
1. `forex_network.proposals`
2. `forex_network.rejection_patterns`
3. `forex_network.trade_autopsies`

Then advance DATA_QUALITY_CUTOFF in learning_module.py line ~112 before restart.

### Same-Cycle Race Fix
- Orchestrator appends synthetic `{"instrument": inst, "direction": dir_}` after each approval
- Risk guardian passes cycle_positions into each validate_trade call
- All 3 gates read cycle_positions — NOT the DB

### entry_rank_position
- Orchestrator sorts approved decisions by convergence desc, stamps 1-based rank
- Flows through payload into execution agent _write_trade INSERT

### price_metrics — Cross Pairs
- Backfilled 2026-04-22: all 13 cross pairs, 200 rows each, ATR valid on every row
- Date range: 2026-04-10 → 2026-04-22 (200 × 1H bars)
- Nightly Lambda extends this forward automatically

### ATR Lookback Fix (2026-04-22)
- technical_agent.py line 843: 16→4 (4 × 1H bars = 4 hours)
- technical_agent.py line 848: 96→120 (120 × 1H bars = 5 days)
- NaN ADX guard added at line 725

---

## Data State

### Historical Prices
- All 39 pair combinations present
- 15M: all 13 cross pairs current through 2026-04-22
- Nightly Lambda (neo-ingest-prices-dev, 22:30 UTC): confirmed on all 20 pairs
- TraderMade allowance exhausted for April — resets ~2026-05-01

### Backfill Scripts
- `/tmp/backfill_15m.py` — dynamic 30-day window, per-pair resume, safe to re-run
- `/tmp/backfill_price_metrics.py` — one-time cross pair price_metrics seed (already run)

---

## Dashboard

### API
- Base URL: `https://k24gcypjyh.execute-api.eu-west-2.amazonaws.com/admin`
- Auth: JWT Bearer token (Cognito, not API key)
- No /convergence endpoint — convergence data embedded in /signals response

### Signals Lambda
- Response size: ~34KB compressed (gzip), ~1.1MB decompressed
- Payload: convergence(20) + signals(20) + signals_full(81) + decisions(1000)
- _PAYLOAD_KEEP includes: reasoning, key_factors, key_levels, signal_scores, upcoming_events, pre_event_active, pre_event_reason, decision, plus all regime fields

### Lambda Endpoints
| Lambda | Route | Notes |
|---|---|---|
| neo-admin-signals-dev | GET /signals | gzip, DISTINCT ON dedup, 20 pairs |
| neo-admin-open-positions-dev | GET /open-positions | All 20 pairs slash format |
| neo-dash-positions-dev | GET /positions | All 20 pairs slash format |
| neo-daily-summary-dev | POST /daily-summary | Fires 22:00 UTC daily |

### Known Issues
- LLM returns 7-9/20 signals per cycle — gap-fill handles this correctly
- FRED VIX API unreliable — regime falls back to cached value gracefully
- RSI divergence on 4 cross-pairs at 22:47 cycle — monitoring

---

## Pre-Live Gates Remaining

| Gate | Status |
|---|---|
| Git repository init | ✅ Done — commit 76ae1b0 |
| Stress test D1/D2/D3 | ✅ Done — all PASS |
| Macro/regime pre-open wake | ✅ Done |
| Heartbeat in quiet-hours loops | ✅ Done — all agents |
| Walk-forward validation (50+ trades) | ⏳ Time-dependent |
| IG swap rates (RG2) | ⏳ Option A: manual populate swap_rates |
| Alpha Vantage key rotation | ⏳ Before go-live |
| Macro per-currency scoring refactor | ⏳ Critical path — dedicated session |
| Full system reset | ⏳ After all gates clear |

---

## Logrotate
Fixed 2026-04-22 — all 6 configs rewritten multi-line. Logs rotate at midnight. technical_agent.error.log was 634KB — will reduce as cross pair price_metrics fills in.

---

## Division of Labour

- **This Claude session:** strategy, Notion updates, OPERATIONAL_NOTES updates, dispatch prompt authoring
- **Claude Code (EC2):** all file edits, greps, restarts, investigations
- **Never relay SSH commands manually** — write a Claude Code dispatch prompt instead
- **Dispatch prompts are task-only** — no SSH instructions, no preamble
- **Work in parallel** — dispatch multiple prompts simultaneously
- **Never repeat a dispatched prompt** — write a new one or refer by description
- **check_integrity.sh runs first every session** — before any other work
