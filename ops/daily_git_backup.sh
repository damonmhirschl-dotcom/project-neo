#!/usr/bin/env bash
# /root/Project_Neo_Damon/ops/daily_git_backup.sh
# Daily git backup: commit working-directory changes and push to origin.
# Logs to /var/log/neo/git_backup.log (also appended by cron redirect).

set -euo pipefail

REPO="/root/Project_Neo_Damon"
LOG="/var/log/neo/git_backup.log"
TIMESTAMP="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

log() {
    echo "[${TIMESTAMP}] $*" | tee -a "$LOG"
}

log "=== daily_git_backup start ==="

cd "$REPO"

# Pull latest (non-destructive; skip on failure — don't block push)
log "Pulling origin/main..."
if git pull origin main --ff-only 2>&1 | tee -a "$LOG"; then
    log "Pull OK"
else
    log "WARN: pull failed or nothing to pull — continuing"
fi

# Stage all changes (tracked modifications + new untracked files,
# excluding .gitignored paths)
git add -A

# Only commit if there is something staged
if git diff --cached --quiet; then
    log "No changes to commit — skipping commit"
else
    COMMIT_MSG="auto: daily backup ${TIMESTAMP}"
    git commit -m "${COMMIT_MSG}" 2>&1 | tee -a "$LOG"
    log "Committed: ${COMMIT_MSG}"
fi

# Push to origin
log "Pushing to origin/main..."
if git push origin main 2>&1 | tee -a "$LOG"; then
    log "Push OK"
    EXIT_STATUS=0
else
    log "ERROR: push failed"
    EXIT_STATUS=1
fi

log "=== daily_git_backup end (exit=${EXIT_STATUS}) ==="
exit $EXIT_STATUS
