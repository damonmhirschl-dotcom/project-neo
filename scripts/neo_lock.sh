#!/bin/bash
# Usage: neo_lock.sh acquire <file> <cli_id> <purpose>
#        neo_lock.sh release <file> <cli_id>
#        neo_lock.sh check <file>

ACTION=$1
FILE=$2
CLI_ID=$3
PURPOSE=$4
LOCK_DIR="/root/Project_Neo_Damon/.neo_locks"
LOCK_FILE="$LOCK_DIR/$(echo $FILE | tr '/' '_').lock"

case $ACTION in
  acquire)
    if [ -f "$LOCK_FILE" ]; then
      echo "LOCKED: $FILE is locked by $(cat $LOCK_FILE)"
      exit 1
    fi
    echo "{\"cli_id\": \"$CLI_ID\", \"file\": \"$FILE\", \"purpose\": \"$PURPOSE\", \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" > "$LOCK_FILE"
    echo "ACQUIRED: lock on $FILE"
    ;;
  release)
    if [ -f "$LOCK_FILE" ]; then
      OWNER=$(python3 -c "import json; print(json.load(open('$LOCK_FILE'))['cli_id'])")
      if [ "$OWNER" = "$CLI_ID" ]; then
        rm "$LOCK_FILE"
        echo "RELEASED: lock on $FILE"
      else
        echo "ERROR: lock owned by $OWNER, not $CLI_ID"
        exit 1
      fi
    else
      echo "NO LOCK: $FILE is not locked"
    fi
    ;;
  check)
    if [ -f "$LOCK_FILE" ]; then
      echo "LOCKED: $(cat $LOCK_FILE)"
    else
      echo "FREE: $FILE"
    fi
    ;;
esac
