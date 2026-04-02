#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

WATCH_ROOT="${1:-RAW_INPUT/whatsapp}"
STRICT_FLAG="${STRICT_FLAG:---strict}"
PRINT_JSON_FLAG="${PRINT_JSON_FLAG:---print-json}"

mkdir -p "$WATCH_ROOT"

processed_count=0
skipped_count=0

while IFS= read -r -d '' file; do
  if [[ "$(basename "$file")" == .* ]]; then
    ((skipped_count+=1))
    continue
  fi

  echo "[run_whatsapp_batch] processing: $file"
  python3 scripts/whatsapp_gatekeeper.py "$file" "$STRICT_FLAG" "$PRINT_JSON_FLAG"
  ((processed_count+=1))
done < <(find "$WATCH_ROOT" -type f -name '*.txt' -print0 | sort -z)

echo "[run_whatsapp_batch] done"
echo "[run_whatsapp_batch] processed=$processed_count skipped=$skipped_count root=$WATCH_ROOT"
