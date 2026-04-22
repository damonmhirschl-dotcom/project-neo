#!/bin/bash
# Monday Market Open Monitor — runs every 5 min until 10:00 UTC or first trade
# Start:     sudo bash /root/Project_Neo_Damon/monitoring/start_monday_monitor.sh
# Watch live: tmux attach -t neo-monitor    (Ctrl-B D to detach)
# Check log:  tail -100 /root/Project_Neo_Damon/monitoring/logs/monday_*.log | less

PYBIN=/root/algodesk/algodesk/bin/python3
SCRIPT=/root/Project_Neo_Damon/monitoring/monday_open_monitor.py
LOG=/root/Project_Neo_Damon/monitoring/logs/monday_$(date +%Y%m%d_%H%M).log
SESSION=neo-monitor

if tmux has-session -t "$SESSION" 2>/dev/null; then
    echo "Session  already running. Attach with: tmux attach -t $SESSION"
    exit 1
fi

tmux new-session -d -s "$SESSION" "$PYBIN $SCRIPT 2>&1 | tee $LOG"
echo "Monday monitor started in tmux session "
echo "Log: $LOG"
echo ""
echo "  Watch live:   tmux attach -t $SESSION    (Ctrl-B D to detach)"
echo "  Check later:  tail -100 $LOG"
