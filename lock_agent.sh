#!/bin/bash
# Usage: source lock_agent.sh <agent_name>
# Example: source lock_agent.sh regime
# This acquires an exclusive lock. If another CLI has the lock, it fails immediately.
# Release with: unlock_agent

AGENT_NAME="${1:?Usage: source lock_agent.sh <agent_name>}"
LOCK_FILE="/tmp/neo_lock_${AGENT_NAME}"

lock_agent() {
    exec 200>"$LOCK_FILE"
    if ! flock -n 200; then
        echo "❌ LOCKED — another CLI instance is currently editing the $AGENT_NAME agent."
        echo "   Lock file: $LOCK_FILE"
        echo "   Wait for the other instance to finish, or manually release: rm $LOCK_FILE"
        return 1
    fi
    echo "🔒 Lock acquired for $AGENT_NAME agent. No other CLI can edit this file."
    echo "   Release when done: unlock_agent"
}

unlock_agent() {
    flock -u 200 2>/dev/null
    rm -f "$LOCK_FILE" 2>/dev/null
    echo "🔓 Lock released for $AGENT_NAME agent."
}

lock_agent
