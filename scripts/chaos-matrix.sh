#!/usr/bin/env bash
# The variants used for the report. Each writes its own results/run-*/ directory.
set -uo pipefail
cd "$(dirname "$0")/.."
export HOST_UID=$(id -u) HOST_GID=$(id -g)
v() { echo; echo "################ VARIANT: $1 ################"; }
v "default (read committed)";          scripts/chaos.sh 1
v "repeatable read";                   PG_ISOLATION="repeatable read" scripts/chaos.sh 1
v "serializable";                      PG_ISOLATION="serializable" scripts/chaos.sh 1
v "clock jumps every ~10s";            scripts/chaos.sh 1 -- -clock-jump-every 10s
v "high load: unlimited rate, 8x4 slots"
docker compose up -d --wait api >/dev/null 2>&1
docker compose run --rm -e WORKER_CONCURRENCY=4 chaos -rate 0 -clients 64 -dup-rate 0.3 2>&1 \
  | tee "results/console-highload-$(date -u +%Y%m%dT%H%M%SZ).txt"
