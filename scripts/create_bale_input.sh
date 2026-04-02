#!/usr/bin/env bash

set -e

if [ $# -lt 2 ]; then
  echo "Usage: $0 <branch_slug> <YYYY-MM-DD> [--open]"
  exit 1
fi

BRANCH="$1"
DATE="$2"
OPEN_EDITOR="${3:-}"

BASE_DIR="$HOME/.openclaw/workspace/ioi-colony"
RAW_DIR="$BASE_DIR/RAW_INPUT/bale_release/$BRANCH/$DATE"
INPUTS_DIR="$BASE_DIR/INPUTS/bale_release"

FILENAME="daily_bale_summary_released_to_rail.txt"
RAW_FILE="$RAW_DIR/$FILENAME"
INPUT_FILE="$INPUTS_DIR/${BRANCH}_${DATE}_daily_bale_summary.txt"

# Create directories
mkdir -p "$RAW_DIR"
mkdir -p "$INPUTS_DIR"

# Create empty files if they don’t exist
touch "$RAW_FILE"
touch "$INPUT_FILE"

# Create symlink (INPUTS → RAW)
ln -sf "$RAW_FILE" "$INPUT_FILE"

echo "[OK] Directory created: $RAW_DIR"
echo "[OK] Raw file: $RAW_FILE"
echo "[OK] Input link: $INPUT_FILE"

# Optional: open editor
if [ "$OPEN_EDITOR" == "--open" ]; then
  nano "$RAW_FILE"
fi
