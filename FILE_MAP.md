# Project Neo — Canonical File Map
# Last updated: 2026-04-18
#
# DO NOT edit files outside the LIVE FILE column.
# Dev/deploy source is synced FROM live, not the other way around.
# After editing live, run: cp <live> <dev_source> to keep them in sync.
#
# To force a deploy when source < production lines: FORCE_DEPLOY=1 ./deploy_<agent>.sh

| Agent          | Service                    | Live File (edit here)                                                   | Dev/Deploy Source                              | Lines |
|----------------|----------------------------|-------------------------------------------------------------------------|------------------------------------------------|-------|
| Orchestrator   | neo-orchestrator-agent     | /root/Project_Neo_Damon/Orchestrator/orchestrator_agent.py              | Orchestrator_Agent/orchestrator_agent.py       | 2181  |
| Macro          | neo-macro-agent            | /root/Project_Neo_Damon/Macro_Agent/macro_agent.py                      | Macro_Agent/macro_agent.py (same dir)          | 1075  |
| Technical      | neo-technical-agent        | /root/Project_Neo_Damon/Technical_Agent/technical_agent.py              | Technical_Agent/technical_agent.py (same dir)  | 1433  |
| Regime         | neo-regime-agent           | /root/Project_Neo_Damon/Regime_Agent/regime_agent.py                    | Regime_Agent/regime_agent.py (same dir)        | 2392  |
| Execution      | neo-execution-agent        | /root/Project_Neo_Damon/Execution_Agent/execution_agent.py              | Execution_Agent/execution_agent.py (same dir)  | 1999  |
| Risk Guardian  | neo-risk-guardian-agent    | /root/Project_Neo_Damon/Risk_Guardian_Agent/risk_guardian_agent.py      | Risk_Guardian_Agent/risk_guardian_agent.py (same dir) | 1178 |
| Learning       | neo-learning-module        | /root/Project_Neo_Damon/Learning_Module/learning_module.py              | Learning_Agent/learning_module.py              | 1559  |

## Cross-directory deployments (src != live dir)

Two agents deploy FROM a different directory than where they run:

- **Orchestrator**: dev source in `Orchestrator_Agent/`, service runs from `Orchestrator/`
  - After editing live (`Orchestrator/`), sync back:
    `cp Orchestrator/orchestrator_agent.py Orchestrator_Agent/orchestrator_agent.py`

- **Learning**: dev source in `Learning_Agent/`, service runs from `Learning_Module/`
  - After editing live (`Learning_Module/`), sync back:
    `cp Learning_Module/learning_module.py Learning_Agent/learning_module.py`

## Orphaned / stale directories

Moved to `_stale_backup_20260418/` on 2026-04-18. Do not restore.

- `New_Files/` — old copies of all agents (pre-2026-04-17)
- `Risk_Guardian/` — duplicate of Risk_Guardian_Agent/ (stale, different path)

## Deploy scripts

Each agent has a `deploy_<agent>.sh` in its dev/deploy source directory.
All scripts include a safety check that aborts if source is SMALLER than production.
Override with `FORCE_DEPLOY=1 ./deploy_<agent>.sh`.

## Systemd services

All services run as root. Config in `/etc/systemd/system/neo-<agent>.service`.
Logs in `/var/log/neo/<agent>.log` and `/var/log/neo/<agent>.error.log`.

## Monitoring scripts

Read-only diagnostic scripts. Do not edit agent live files from here.
Directory: `/root/Project_Neo_Damon/monitoring/`
Logs:       `/root/Project_Neo_Damon/monitoring/logs/`

| Script | Launcher | Mode | Purpose |
|---|---|---|---|
| `monday_open_monitor.py` | `start_monday_monitor.sh` | tmux loop (5 min × 3 hr) | London open health: agents, convergence, trades |
| `convergence_monitor.py` | `start_convergence.sh` | single run | Full 7-stage pipeline trace, rejection breakdown |
| `trade_health_check.py` | `start_trade_check.sh` | single run | Open trades, circuit breakers, IBKR events |
| `eod_summary.py` | `start_eod_summary.sh` | single run | Daily recap: PnL, API performance, account status |
| `agent_drift_monitor.py` | `start_drift.sh` | single run | Score distributions, stuck signals, convergence trends |

Usage:
```bash
# Monday open (persists after SSH disconnect):
sudo bash /root/Project_Neo_Damon/monitoring/start_monday_monitor.sh
tmux attach -t neo-monitor   # re-attach later; Ctrl-B D to detach

# Single-run scripts:
sudo bash /root/Project_Neo_Damon/monitoring/start_convergence.sh
sudo bash /root/Project_Neo_Damon/monitoring/start_trade_check.sh
sudo bash /root/Project_Neo_Damon/monitoring/start_eod_summary.sh
sudo bash /root/Project_Neo_Damon/monitoring/start_drift.sh
```
