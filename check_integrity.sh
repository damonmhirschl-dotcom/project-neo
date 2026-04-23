#!/bin/bash
echo "=== Project Neo Integrity Check ==="
echo ""
echo "--- Services ---"
for svc in neo-macro-agent neo-technical-agent neo-regime-agent neo-orchestrator-agent neo-risk-guardian-agent neo-execution-agent neo-learning-module; do
  status=$(systemctl is-active $svc)
  [ "$status" = "active" ] && echo "OK  $svc" || echo "FAIL $svc: $status"
done
echo ""
echo "--- Git Status ---"
cd /root/Project_Neo_Damon && git log --oneline -3 && git status --short
echo ""
echo "--- Stale Staging Files ---"
find /home/ubuntu -maxdepth 1 \( -name "*.py" -o -name "*.html" \) 2>/dev/null | grep -v ".cache" | head -10
echo ""
echo "--- Dashboard Sync ---"
LOCAL=$(md5sum /root/Project_Neo_Damon/Dashboard/admin-index.html 2>/dev/null | cut -d' ' -f1)
S3=$(aws s3 cp s3://algodesk-admin-dashboard-dev/index.html - --no-progress 2>/dev/null | md5sum | cut -d' ' -f1)
[ "$LOCAL" = "$S3" ] && echo "OK  admin-index.html in sync with S3" || echo "DIFF admin-index.html DIFFERS from S3 (local=$LOCAL s3=$S3)"
echo ""
echo "--- Log Sizes ---"
find /var/log/neo -name "*.log" -size +10M 2>/dev/null | while read f; do echo "LARGE: $f $(du -sh $f | cut -f1)"; done
echo "Log check complete"
echo ""
echo "--- Kill Switch ---"
aws ssm get-parameter --name /platform/config/kill-switch --query Parameter.Value --output text --region eu-west-2 2>/dev/null || echo "SSM read failed"
