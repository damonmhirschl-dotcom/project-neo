# Project Neo — OPERATIONAL NOTES
## Last Updated: 2026-04-24 ~12:00 UTC

---

## System State

- All 7 agents running clean on EC2 i-02d767a507a0301c0
- 0 open trades
- IG balance: £147,618.98 (paper mode)
- DATA_QUALITY_CUTOFF: 2026-04-23 21:55:00 UTC
- Architecture freeze lifted: 2026-04-24
- System is a learning environment — changes are made based on evidence from shadow trades, diagnostics, and closed-trade outcomes

---

## EC2 / Infrastructure

- **Instance:** i-02d767a507a0301c0 (ip-10-50-1-4)
- **Virtualenv:** /root/algodesk/algodesk
- **Agent root:** /root/Project_Neo_Damon/
- **User:** root
- **AWS Region:** eu-west-2
- **RDS Endpoint:** algodesk-postgres-dev.chubomleuerz.eu-west-2.rds.amazonaws.com
- **CloudFront:** d28ozra6f51mhh.cloudfront.net
- **S3 Dashboard:** algodesk-admin-dashboard-dev

---

## DB Connection

```bash
sudo bash << 'DBEOF'
PW=$(aws secretsmanager get-secret-value --secret-id platform/rds/credentials --region eu-west-2 --query SecretString --output text | python3 -c "import sys,json; print(json.load(sys.stdin)['password'])")
USER=$(aws secretsmanager get-secret-value --secret-id platform/rds/credentials --region eu-west-2 --query SecretString --output text | python3 -c "import sys,json; print(json.load(sys.stdin)['username'])")
HOST=$(aws ssm get-parameter --name /platform/config/rds-endpoint --query Parameter.Value --output text --region eu-west-2)
PGPASSWORD=$PW psql -h $HOST -U $USER -d postgres -c "YOUR QUERY HERE"
DBEOF
```

---

## Agent Services

| Service | Command |
|---|---|
| Macro | neo-macro-agent |
| Technical | neo-technical-agent |
| Regime | neo-regime-agent |
| Orchestrator | neo-orchestrator-agent |
| Risk Guardian | neo-risk-guardian-agent |
| Execution | neo-execution-agent |
| Learning Module | neo-learning-module |

```bash
# Check all statuses
for svc in neo-macro-agent neo-technical-agent neo-regime-agent neo-orchestrator-agent neo-risk-guardian-agent neo-execution-agent neo-learning-module; do
  echo "$svc: $(systemctl is-active $svc)"
done
```

---

## Key File Paths

- Macro agent: /root/Project_Neo_Damon/Macro_Agent/macro_agent.py
- Technical agent: /root/Project_Neo_Damon/Technical_Agent/technical_agent.py
- Regime agent: /root/Project_Neo_Damon/Regime_Agent/regime_agent.py
- Orchestrator: /root/Project_Neo_Damon/orchestrator_agent.py
- Risk Guardian: /root/Project_Neo_Damon/risk_guardian_agent.py
- Learning Module: /root/Project_Neo_Damon/Learning_Module/learning_module.py
- Execution Agent: /root/Project_Neo_Damon/execution_agent.py
- Scripts: /root/Project_Neo_Damon/scripts/
- Dashboard: /root/Project_Neo_Damon/Dashboard/admin-index.html

---

## Architecture State

### Signal Flow
1. Macro agent → macro_signals_history (FRED/Finnhub, daily data)
2. Technical agent → RSI(14) pullback + ADX(14) trend filter
3. Regime agent → stress score, session classification, ADX regime
4. Orchestrator → hierarchical gating (macro → technical → regime)
5. Risk Guardian → position sizing, daily loss gate, correlation check
6. Execution agent → IG paper trades

### Gate Stack (Orchestrator)
- Layer 1: Macro gate — abs(macro_score) >= max(fixed_threshold, p50)
- Layer 2: Technical gate — abs(tech_score) >= 0.10 (TECH_MIN_THRESHOLD)
- Layer 3: Directional agreement (except MACRO_ONLY_PAIRS)
- Layer 4: Convergence threshold check
- Layer 5: Regime checks (session, stress, R:R)

### MACRO_ONLY_PAIRS (no directional agreement required)
AUDUSD, EURUSD, USDJPY, GBPAUD, CADJPY, GBPJPY

### Allowed Instruments (12 pairs)
AUDJPY, EURJPY, GBPJPY, CADJPY, EURAUD, USDJPY, EURUSD, GBPUSD, AUDUSD, EURGBP, NZDJPY, GBPCAD

### Stop / Target Calculation
- Stop: 1D ATR × 2.0 (from price_metrics, timeframe='1D')
- Fallback stop: 1H ATR × 3.0
- Target: entry ± (stop_distance × min_rr)
- Sanity check: warn if target outside 20-day high/low

### Conviction Sizing (Power Curve)
- conviction_score = abs(macro_score)
- normalised = (conviction - threshold) / (max_ref - threshold)
- risk_pct = min_risk + (max_risk - min_risk) × normalised^exponent
- COT multiplier applied at RG sizing stage (not macro scoring)

### Risk Parameters (current)
| Profile | Threshold | Min Risk | Max Risk | Max Ref |
|---|---|---|---|---|
| Conservative | 0.3281 | 0.25% | 1.00% | 0.800 |
| Balanced | 0.2833 | 0.40% | 2.00% | 0.724 |
| Aggressive | 0.2631 | 0.50% | 3.00% | 1.032 |

### Percentile Gates (p50, tanh space)
| Pair | p75 |
|---|---|
| AUDJPY | 0.894 |
| NZDJPY | 0.937 |
| USDJPY | 0.695 |
| CADJPY | 0.677 |
| GBPJPY | 0.656 |
| EURJPY | 0.612 |
| EURAUD | 0.372 |
| AUDUSD | 0.330 |
| GBPCAD | TBD |
| EURUSD | 0.297 |
| GBPUSD | 0.228 |
| EURGBP | 0.189 |

---

## Learning Module

### Poll Sequence
1. analyse_contrarian_performance()
2. analyse_entry_timing()
3. compute_per_agent_accuracy()
4. compute_per_source_accuracy()
5. compute_macro_direction_accuracy()
6. analyse_shadow_trades()
7. compute_kelly_fraction() — silent until 50 trades
8. diagnose_architecture()
9. analyse_rg_rejections()
10. compute_entry_timing_quality() — silent until 20 trades

### Shadow Trade Hypotheses
| Hypothesis | Tests |
|---|---|
| full_cycle | Every pair, every gate exit point |
| macro_only | Macro passed, tech too weak |
| tech_threshold_0.10 | Tech in 0.10-0.20 range |
| macro_regime_no_tech | Macro+regime agree, tech absent |
| p75_relaxed | Macro above p50, below p75 |
| directional_disagreement | Both gates passed, directions oppose |

Shadow trades: one per pair per hypothesis per day
Outcomes: 4H populated ~14:00 UTC same day, 1D next day, 5D five days later
Backfill cron: 22:45 UTC weekdays

### Gates for Proposals
- Shadow trade win rate > 58% on 20+ samples
- Kelly fraction: silent until 50 closed trades
- Architecture diagnostics: fire every cycle regardless

---

## Crontab (EC2)

```
0 23 * * 1-5   compute_1d_price_metrics.py     — 1D ATR refresh
0 6  * * 1     compute_macro_percentiles.py    — weekly p75 refresh (tanh space)
45 22 * * 1-5  backfill_shadow_trades.py       — shadow trade outcomes
30 22 * * 1-5  backfill_rejected_signals.py    — rejected signal outcomes
0 22 * * 1-5   send_daily_lm_summary.py        — daily LM summary via SNS
```

---

## Key Commits (2026-04-23 / 2026-04-24)

| Hash | Description |
|---|---|
| 6165ab1 | Macro Agent v2 — deterministic |
| 2bb95fb | Technical Agent v2 — RSI/ADX |
| 3d9433b | Regime Agent v2 — LLM removed |
| 2ce7157 | Orchestrator hierarchical gating |
| e9e8f24 | Percentile macro gate p75 |
| c3f4e87 | Conviction sizing from macro_score |
| 7b01799 | RG max_convergence_reference updated |
| ab39d1a | Orchestrator approval payload fields |
| 52a5186 | All agents wired to full ingested data |
| 77e6c11 | Swing detection 5-bar rule on 1D |
| 56c0046 | 1D ATR x2.0 for swing stops |
| 5e10a79 | compute_1d_price_metrics.py + nightly cron |
| 905b567 | BoE GBP 10Y yield 6,645 rows |
| e0a1d08 | Eurostat EUR unemployment 314 rows |
| 4415f9f | Stop/target: 1D ATR x2.0 fixed |
| e70cb8a | missing_signal MACRO_ONLY fix |
| c395982 | MACRO_ONLY_PAIRS directional fix |
| bf607e3 | _compute_pnl formula fixed |
| 2345e96 | Shadow trade deduplication |
| f4d3114 | tanh percentiles + VIX cache |
| 515b589 | RG daily loss gate fixed |
| 4973bce | Stress hysteresis fixed |

---

## Outstanding Backlog

- price_at_signal NULL in shadow trades — fix in flight
- 13 pairs with 1D history starting 2016 (Check 5b)
- FRED yield retry for EUR/GBP/JPY/CAD/NZD
- VARCHAR(100) truncation in regime agent write_market_context_snapshot
- Walk-forward validation rolling windows (spec doc 08)
- Kelly fraction + Calmar ratio (post-50-trade gate)
- Learning module weight feedback loop
- IBKR Gateway — not needed until go-live
- Admin dashboard mobile/responsive polish: tablet breakpoint (700-1100px), header-right button overflow on narrow screens, font scaling
- Eurostat + Stats NZ unemployment gaps

---

## Reset Procedure

If full reset needed:
1. Close all IG positions
2. Truncate: trades, trade_autopsies, agent_signals, convergence_history, rejected_signals, proposals, rejection_patterns, shadow_trades, system_events, rg_rejections
3. Reset risk_parameters: account_value = peak_account_value = £147,618.98, drawdown_step_level = 0, circuit_breaker_active = FALSE
4. Advance DATA_QUALITY_CUTOFF to current UTC time
5. Restart all agents in order: macro → technical → regime → orchestrator → risk_guardian → execution → learning_module

