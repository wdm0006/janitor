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
show_config examples/config/rules.json 20
run go run ./cmd/janitor --config examples/config/rules.json --verbose

# 2) JSONL (batch) with verbose summary
show_config examples/config/rules_jsonl.json 20
run go run ./cmd/janitor --config examples/config/rules_jsonl.json --verbose

# 3) CSV (streaming) with verbose summary
run go run ./cmd/janitor --config examples/config/rules.json --chunk-size 2000 --verbose

echo
echo "(demo complete)"
