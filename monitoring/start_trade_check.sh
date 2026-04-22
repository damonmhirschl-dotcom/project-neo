#!/bin/bash
PYBIN=/root/algodesk/algodesk/bin/python3
SCRIPT_PATH=/root/Project_Neo_Damon/monitoring/trade_health_check.py
LOG=/root/Project_Neo_Damon/monitoring/logs/trade_check_$(date +%Y%m%d_%H%M).log
echo "Running ${SCRIPT_PATH##*/} — log: $LOG"
sudo $PYBIN $SCRIPT_PATH 2>&1 | tee "$LOG"
echo ""
echo "Log saved to: $LOG"
