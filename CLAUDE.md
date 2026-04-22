# Project Neo — Claude Code Instructions

## Dashboard files — ALWAYS pull before editing

Before touching either dashboard HTML file, fetch the live S3 version first:

```bash
aws s3 cp s3://algodesk-trading-dashboard-dev/index.html /home/ubuntu/trading-index.html --region eu-west-2
aws s3 cp s3://algodesk-admin-dashboard-dev/index.html /home/ubuntu/admin-index-live.html --region eu-west-2
```

Canonical working copies live at:
- /home/ubuntu/trading-index.html   → Trading dashboard (S3: algodesk-trading-dashboard-dev)
- /home/ubuntu/admin-index-live.html → Admin dashboard  (S3: algodesk-admin-dashboard-dev)

Never edit /tmp/ copies of these files as the source of truth — /tmp/ files are session-specific and will be stale.

## Deploying

After editing, always deploy back to S3 + invalidate CloudFront:

```bash
# Trading dashboard
aws s3 cp /home/ubuntu/trading-index.html s3://algodesk-trading-dashboard-dev/index.html --cache-control 'no-cache, no-store, must-revalidate' --content-type 'text/html' --region eu-west-2
aws cloudfront create-invalidation --distribution-id E322UMK78GHG4D --paths '/*' --region eu-west-2

# Admin dashboard
aws s3 cp /home/ubuntu/admin-index-live.html s3://algodesk-admin-dashboard-dev/index.html --cache-control 'no-cache, no-store, must-revalidate' --content-type 'text/html' --region eu-west-2
aws cloudfront create-invalidation --distribution-id E2HV8N8YKHB6WD --paths '/*' --region eu-west-2
```

## Infrastructure

- EC2 private IP: 10.50.1.4 (ubuntu user)
- AWS region: eu-west-2
- RDS endpoint: SSM /platform/config/rds-endpoint
- RDS credentials: Secrets Manager platform/rds/credentials
- DB: postgres, search_path=forex_network,shared,public
- Trading CloudFront: E322UMK78GHG4D
- Admin CloudFront: E2HV8N8YKHB6WD
- Admin Cognito client: 7ri5i3fa50q4baf7ko0e3uoef

## Active orchestrator path

/root/Project_Neo_Damon/Orchestrator/orchestrator_agent.py


## Agent file map — READ BEFORE EDITING ANY AGENT

Full canonical paths in FILE_MAP.md. Key facts:

- The orchestrator has TWO directories — only ONE is live:
  - LIVE:  /root/Project_Neo_Damon/Orchestrator/orchestrator_agent.py
  - STALE: /root/Project_Neo_Damon/_archive/Orchestrator_Agent/  (DO NOT EDIT)
- All other agents: live file is in the *_Agent or *_Module directory matching the service name
- Backup files are in _bak/ subdirectories — never in the parent agent dir
- Session working files go in /home/ubuntu/_session_<date>/ not in /root/Project_Neo_Damon/

## Systemd service restart commands

```bash
sudo systemctl restart neo-orchestrator-agent
sudo systemctl restart neo-macro-agent
sudo systemctl restart neo-technical-agent
sudo systemctl restart neo-regime-agent
sudo systemctl restart neo-execution-agent
sudo systemctl restart neo-risk-guardian-agent
sudo systemctl restart neo-learning-module
```



## IG Markets — added 2026-04-19

**Secret:** `platform/ig-markets/credentials` (api_key, username, password, account_type=live)

**Lambda:** `neo-ingest-ig-sentiment-dev`
- Schedule: `cron(30 * ? * MON-FRI *)` — hourly at :30, weekdays only
- Fetches: FX client sentiment (7 pairs), cross-asset sentiment (SPX500/GER30/UK100), DXY daily price
- Writes FX + cross-asset rows to `forex_network.ig_client_sentiment`
- Writes DXY close to `forex_network.cross_asset_prices` (instrument='DXY', timeframe='1D')
- Logs to `forex_network.api_call_log` with provider='ig_markets'

**API notes:**
- Sentiment market IDs: bare pair names (EURUSD, GBPUSD, etc.); cross-asset: US500, DE30, FT100
- Prices: do NOT send `Version: 3` header — silently returns empty body
- FX prices scaled ×10,000 (11760.7 = 1.17607); DXY scaled ×100 (9789.8 = 97.898)
- Price allowance: 10,000 data points per rolling 7-day window
- Cross-asset sentiment that returns 0%/0%: GOLD, SILVER, OIL, VIX, DXY — not useful

**Macro agent integration:**
- `get_multi_broker_sentiment()` — reads MyFXBook SSI + IG sentiment + DXY close
- Prompt sections: `## MULTI-BROKER RETAIL SENTIMENT CONSENSUS`, `## IG CROSS-ASSET RETAIL SENTIMENT`, `## DXY (US Dollar Index)`

**Regime agent integration (added 2026-04-19):**
- `order_flow_anomaly`: reads both `sentiment_ssi` (MyFXBook) AND `ig_client_sentiment` (IG) — averages extremity across both brokers for stronger signal
- `cross_asset_risk`: `fetch_cross_asset_prices()` now fetches DXY from `cross_asset_prices` DB; weight 0.15 added to `_compute_cross_asset_stress()` (rising DXY = risk-off). Weights rebalanced: SPX500 0.30, GER30 0.20, XAUUSD 0.20, DXY 0.15, UK100 0.10, UKOIL 0.05
- `cross_asset_risk`: `set_cross_asset_data()` applies IG equity sentiment overlay (SPX500/GER30/UK100) — contrarian ±3pt adjustment when crowd diverges from price direction

**DXY historical backfill (2026-04-19):**
- 194 daily bars loaded into `forex_network.cross_asset_prices` (instrument='DXY', timeframe='1D')
- Date range: 2025-08-26 to 2026-04-17. Ongoing bars written hourly by `neo-ingest-ig-sentiment-dev`

## Active ingest Lambdas

| Lambda | Schedule | Writes to | Notes |
|--------|----------|-----------|-------|
| `neo-ingest-ig-sentiment-dev` | cron(30 * ? * MON-FRI *) | `ig_client_sentiment`, `cross_asset_prices` (DXY) | FX sentiment 7 pairs + SPX500/GER30/UK100 + DXY daily |
| `neo-ingest-ssi-dev` | hourly, MON-FRI | `sentiment_ssi` | MyFXBook SSI — source for macro + regime order_flow_anomaly |
| `neo-ingest-bond-yields-dev` | daily | `cross_asset_prices` (US10Y/US2Y/UK10Y/DE10Y/JP10Y) | EODHD — source for regime yield_curve_slope |
| `neo-api-health-probe-dev` | every 5 min | `api_health` | Pings all active providers; IG probe uses POST /session |

## Disabled providers (2026-04-19)

### GDELT — disabled
- Reason: Persistent HTTP 429 rate-limit errors; tone signal unreliable for trading use.
- Effect: Geopolitical stress component (weight 0.10) uses fixed neutral value (50) each cycle.
- Location: Regime_Agent/regime_agent.py — fetch_gdelt_geopolitical_tone() returns None immediately; stress block sets normalised_score=50 directly.
- Planned replacement: NewsAPI.ai (pending integration).

### Polygon — live price feed disabled; technical indicators ACTIVE
- Live price feed disabled: REST API returned 0 for all live FX pairs. `get_polygon_live_prices()` removed.
- Effect on prices: hierarchy is now TraderMade (primary) -> RDS last bar (fallback).
- Technical indicators still active: `get_polygon_technical_indicators()` (RSI/EMA/SMA/MACD cross-validation) runs every technical agent cycle. Secret: `platform/polygon/api-key`. Do NOT remove this.

### Alpha Vantage — disabled
- Reason: Free tier (25 calls/day) not viable for production. All data covered by Finnhub + EODHD.
- Effect: No data loss. alphavantage_key removed from macro, technical, and regime agents. av_key parameter removed from regime agent function signatures. Health probe no longer checks alphavantage_mcp. Admin dashboard provider list no longer shows Alpha Vantage.
- Secret: platform/alphaVantage/api-key left dormant in Secrets Manager (do not delete — may upgrade tier later).
- Locations removed: Macro_Agent/macro_agent.py, Technical_Agent/technical_agent.py, Regime_Agent/regime_agent.py, neo-api-health-probe-dev Lambda, admin dashboard.

## File Locking (MANDATORY)
Before editing ANY agent file, acquire an exclusive lock:
    source /root/Project_Neo_Damon/lock_agent.sh regime   # or macro, technical, orchestrator
After completing all edits and restarting the agent:
    unlock_agent
If the lock fails, STOP. Another CLI instance is editing the file. Do NOT proceed.

## Agent Cycle Intervals
- Technical agent: 5 minutes (LLM call gated by 10-pip price-change threshold — skips during quiet markets)
- Orchestrator: 5 minutes (reads pre-computed signals from DB, no LLM cost)
- Macro agent: 15 minutes active hours / 60 minutes quiet hours (Asian session) — intentional, not a bug
- Regime agent: 15 minutes (stress components are daily/hourly data)

## Process Management — Orphan Prevention

### Background
On 2026-04-20, two orphaned technical_agent.py processes (PIDs 25186, 26348) were found racing
against the systemd-managed instance, writing old-code signals to the DB. Root cause: previous
manual runs using `python agent.py 2>&1 | head -30` — when `head` exits the pipe breaks, but the
Python daemon loop continues running indefinitely, reparented to init.

### Fix Applied
All 7 systemd unit files have an `ExecStartPre` directive that kills any existing instance of the
agent script before launching the new one:

    ExecStartPre=-/bin/bash -c 'pkill -9 -f "python.*<agent_name>.py" || true'

The `-` prefix makes this non-fatal if nothing matches. This fires on every `systemctl start` and
`systemctl restart`, regardless of how the orphan was created.

### Rules
1. NEVER run an agent script directly: `python agent.py` or `python agent.py | head -N`
   - These create orphans that systemd cannot see or manage
   - Use `sudo journalctl -u neo-<agent-name> -f` to follow live logs instead
2. ALWAYS use `systemctl restart neo-<agent-name>` to restart an agent
3. If you must test a one-off script invocation, use a DIFFERENT script name (e.g. diag_*.py),
   not the live agent filename — the ExecStartPre pkill matches on filename
4. After any deployment, verify PID count with:
   `pgrep -f "python.*<agent_name>.py" | wc -l`   # must return 1

### Architecture Note
All 7 agents are single-instance services (one systemd unit per agent). Each process handles all
3 users internally. There are no -001/-002/-003 per-user variants.

## Architectural Changes — 2026-04-20

### Stage 1: Agent State Persistence
- **Table**: `forex_network.agent_state` (JSONB, PRIMARY KEY agent_name+state_key)
- **Module**: `/root/Project_Neo_Damon/shared/agent_state.py` — save_state/load_state/delete_state/log_loaded_state_summary
- **Regime**: adx_transitional_counts persisted per restart — seeded GBPUSD=15 (active cycles survive restarts)
- **Macro**: cycle_count saved/restored per restart
- **Technical**: _last_llm_prices saved/restored (max_age=30m) — avoids spurious LLM call on restart
- **Orchestrator**: signal_writer.cycle_count saved/restored per restart

### Stage 2: order_flow_anomaly removed from regime stress
- Removed `order_flow_anomaly` component (was 0.10 weight)
- New STRESS_WEIGHTS: vix_level_trend=0.17, yield_curve_slope=0.17, realised_vol_divergence=0.22, correlation_breakdown=0.10, cot_extremes=0.06, geopolitical_index=0.11, cross_asset_risk=0.17

### Stage 3: Regime heartbeat fixes (M1+M2)
- **M1**: `degradation_mode` now written correctly: 'halted' if kill_switch, 'degraded' if stress_confidence=='low', else NULL
- **M2**: `convergence_boost` now written from actual computed value (was hardcoded 0.00)
- Both fixes in `write_heartbeat()` signature: now accepts `degradation_mode` and `convergence_boost` params

### Stage 4: Learning module schema migration
- `trade_autopsies`: added user_id, pattern_context, exclude_from_patterns columns
- `pattern_memory`: added pattern_key, direction, decay_factor columns + unique index idx_pattern_memory_user_key

### Stage 6: Market-state-aware signal freshness (orchestrator)
- `SIGNAL_MAX_AGE_MINUTES` dict replaces scalar `MAX_SIGNAL_AGE_MINUTES = 20`
- active session: macro=25min, technical=20min, regime=20min
- quiet session: macro=65min (60-min quiet cycle), technical=20min, regime=20min
- Prevents false-absent flags during Asian session for macro signals
- Log message now includes market state: "stale (Xmin old, quiet)"

### Stage 5: NOT YET IMPLEMENTED
- historical_prices is populated (821K rows, last 1H bar ~2.5h stale)
- No live bar ingest process exists — needs a separate implementation task
- Agents read from the table successfully; gap only affects very recent bar calculations
