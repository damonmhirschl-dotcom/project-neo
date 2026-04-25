# Project Neo — Claude Session Reference

## Read this before doing anything else

### Source of truth
EC2: /root/Project_Neo_Damon (pushed to GitHub after every commit)
GitHub: github.com/damonmhirschl-dotcom/project-neo (origin + backup remotes)

### Before editing any file
1. Check current git status: `cd /root/Project_Neo_Damon && git status`
2. Check file is not locked: `./scripts/neo_lock.sh check <file>`
3. Acquire lock: `./scripts/neo_lock.sh acquire <file> <your-cli-id> <purpose>`
4. Make changes
5. Release lock and commit: `./scripts/neo_lock.sh release <file> <your-cli-id>`
6. `git add <file> && git commit -m "<message>" && git push origin main && git push backup main`

### Never
- Edit a file without checking the lock first
- Leave a lock acquired after your session ends
- Commit without pushing to both remotes
- Hardcode values that belong in v1_swing_parameters.py

### Key file locations
| Component | File |
|---|---|
| Strategy parameters (single source of truth) | v1_swing_parameters.py |
| Pydantic payload schemas | shared/schemas/v1_swing_payloads.py |
| Technical Agent | Technical_Agent/technical_agent.py |
| Macro Agent | Macro_Agent/macro_agent.py |
| Orchestrator | Orchestrator_Agent/orchestrator_agent.py |
| Risk Guardian | Risk_Guardian_Agent/risk_guardian_agent.py |
| Execution Agent | Execution_Agent/execution_agent.py |
| Learning Module | Learning_Module/learning_module.py |
| Correlation writer | scripts/update_portfolio_correlation.py |
| Economic calendar ingest | scripts/ingest_economic_calendar.py |
| Price metrics compute | scripts/compute_price_metrics.py |

### Current state (as of 2026-04-25)
- V1 Swing fully deployed, DB clean, ready for go-live
- All 22 pairs: price_metrics populated (1H/4H/1D), swap_rates 22/22, correlations live
- Economic calendar: Finnhub, refreshed daily 06:00 UTC
- Correlation matrix: refreshed daily 22:30 UTC
- Last commit: bb5c4f9

### Services (EC2 systemd)
- neo-technical-agent
- neo-macro-agent
- neo-orchestrator
- neo-risk-guardian
- neo-execution-agent
- neo-learning-module

Restart any: `sudo systemctl restart <service-name>`
Logs: `journalctl -u <service-name> -n 50 --no-pager`

### Architecture reference
Notion: https://www.notion.so/34d4d2ece67681e199eed6d4b54a9cfb

### Hard rules — never bypass
- All constants in v1_swing_parameters.py — no hardcoding in agent files
- All payload changes must update shared/schemas/v1_swing_payloads.py
- Smoke test must pass before any service restart: `python3 tests/v1_swing_smoke_test.py`
- Push to both origin and backup after every commit

### EC2 Connection
Host: i-02d767a507a0301c0
Region: eu-west-2
User: ubuntu
SSH alias: project-neo (configured in ~/.ssh/config via VPN → ProxyJump)

Connect: ssh project-neo
If alias not available: ssh ubuntu@<ec2-ip> -i ~/.ssh/github_project_neo

VPN must be active before connecting.
All AWS CLI, CDK, rsync, and infrastructure commands run on EC2 — not locally.
Local Mac AWS credentials (algodesk-dev profile) expire; EC2 IAM role does not.
