#!/usr/bin/env bash
set -euo pipefail

COLONY_DIR="/home/clawadmin/.openclaw/workspace/ioi-colony"
LOG_DIR="$COLONY_DIR/LOGS"
RUN_LOG="$LOG_DIR/cron_runner.log"

mkdir -p "$LOG_DIR"

cd "$COLONY_DIR"

echo "RUN $(date -Is)" >> "$RUN_LOG"
/usr/bin/python3 colony_cycle.py >> "$RUN_LOG" 2>&1
echo "END $(date -Is)" >> "$RUN_LOG"
echo "" >> "$RUN_LOG"
