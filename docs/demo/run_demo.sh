#!/usr/bin/env bash
set -euo pipefail

run() {
  echo
  printf "$ %s\n" "$*"
  bash -lc "$*"
}

show_config() {
  local path="$1"
  local lines="${2:-20}"
  echo
  printf "$ cat %s\n" "$path"
  sed -n "1,${lines}p" "$path" || true
}

# 1) CSV (batch) with verbose summary
CFG_CSV=examples/config/rules.json
IN_CSV=examples/data/iris_nulls.csv
OUT_CSV=examples/data/iris_cleaned.csv

show_config "$CFG_CSV" 18
echo
echo "# Before (CSV head):"
sed -n '1,6p' "$IN_CSV" || true
run go run ./cmd/janitor --config "$CFG_CSV" --verbose
echo
echo "# After (CSV head):"
sed -n '1,6p' "$OUT_CSV" || true

# 2) JSONL (batch) with verbose summary
CFG_JSONL=examples/config/rules_jsonl.json
IN_JSONL=examples/data/sample.jsonl
OUT_JSONL=examples/data/sample_clean.jsonl

show_config "$CFG_JSONL" 18
echo
echo "# Before (JSONL head):"
sed -n '1,4p' "$IN_JSONL" || true
run go run ./cmd/janitor --config "$CFG_JSONL" --verbose
echo
echo "# After (JSONL head):"
sed -n '1,4p' "$OUT_JSONL" || true

# 3) CSV (streaming) with verbose summary
run go run ./cmd/janitor --config examples/config/rules.json --chunk-size 2000 --verbose

echo
echo "(demo complete)"
