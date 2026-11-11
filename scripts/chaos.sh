#!/usr/bin/env bash
# Usage: scripts/chaos.sh [runs] [-- extra chaos flags]
# Example: scripts/chaos.sh 3 -- -clock-jump-every 15s
# Every run's raw output is kept in results/ (passes AND failures).
set -uo pipefail
cd "$(dirname "$0")/.."
runs=${1:-1}; shift || true
[[ "${1:-}" == "--" ]] && shift
mkdir -p results
export HOST_UID=$(id -u) HOST_GID=$(id -g)
docker compose up -d --build --wait db api
docker compose stop worker >/dev/null 2>&1 || true   # the harness brings its own workers
pass=0; fail=0
for i in $(seq 1 "$runs"); do
  echo "########## run $i/$runs ##########"
  ts=$(date -u +%Y%m%dT%H%M%SZ)
  if docker compose run --rm chaos "$@" 2>&1 | tee "results/console-$ts.txt"; then
    pass=$((pass+1))
  else
    fail=$((fail+1))
  fi
done
echo "runs=$runs pass=$pass fail=$fail" | tee -a results/summary.txt
