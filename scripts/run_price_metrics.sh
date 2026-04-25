#!/bin/bash
# Wrapper: compute_price_metrics.py with failure alerting
set -euo pipefail
PYTHON=/root/algodesk/algodesk/bin/python3
SCRIPT=/root/Project_Neo_Damon/scripts/compute_price_metrics.py
LOG=/var/log/neo/price_metrics.log

cd /root/Project_Neo_Damon

if ! $PYTHON $SCRIPT >> $LOG 2>&1; then
    $PYTHON - << 'PYEOF'
import sys
sys.path.insert(0, '/root/Project_Neo_Damon')
from shared.alerting import send_alert
send_alert('CRITICAL', 'compute_price_metrics cron failed', {'log': '/var/log/neo/price_metrics.log'}, 'cron')
PYEOF
    exit 1
fi
