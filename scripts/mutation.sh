#!/usr/bin/env bash
# Mutation check: break one safety mechanism at a time in a scratch copy of the repo and
# confirm the tests (and, for worker-side mutations, the chaos harness) FAIL.
# A check that still passes on broken code proves nothing.
#
# Usage: scripts/mutation.sh [--chaos]     (needs `docker compose up -d db api`)
set -uo pipefail
cd "$(dirname "$0")/.."
root=$PWD
out=$root/results/mutations
mkdir -p "$out"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
chaos=${1:-}

# name | test to run | python replacement (old -> new) applied to internal/store/store.go
mutate() {
  local name=$1 test=$2 old=$3 new=$4
  local dir=$work/$name
  rsync -a --exclude results --exclude .git "$root/" "$dir/"
  python3 - "$dir/internal/store/store.go" "$old" "$new" <<'PY' || { echo "$name: replacement not found"; return 1; }
import sys
p, olds, news = sys.argv[1:]
s = open(p).read()
for old, new in zip(olds.split("|||"), news.split("|||")):  # several edits per mutant
    if old not in s: sys.exit(1)
    s = s.replace(old, new, 1)
open(p, "w").write(s)
PY
  echo "=== mutation $name: '$old' -> '$new'" | tee "$out/$name.txt"
  if (cd "$dir" && go test -count=1 -run "$test" -v ./internal/store) >>"$out/$name.txt" 2>&1; then
    echo "SURVIVED: tests still pass (a test gap, or an equivalent mutant — see docs/SILENT_FAILURES.md)" | tee -a "$out/$name.txt"
  else
    echo "KILLED: tests fail with the mutation (good)" | tee -a "$out/$name.txt"
    grep -E -- '--- FAIL|silent_test.go' "$out/$name.txt" | head -8
  fi
  if [[ "$chaos" == "--chaos" && "${5:-}" == "chaos" ]]; then
    (cd "$dir" && go build -o "$work/soupq-$name" ./cmd/soupq)
    echo "=== chaos with mutant workers ($name)" | tee -a "$out/$name.txt"
    DATABASE_URL="postgres://soup:soup@localhost:5432/soup?sslmode=disable" \
      "$work/soupq-$name" chaos -jobs 2000 -rate 200 -out "$out/chaos-$name" 2>&1 \
      | grep -v '^t=' | tee -a "$out/$name.txt" | grep -E 'RESULT|violations by kind'
  fi
}

mutate no-fencing-on-complete TestZombieCannotWriteStaleResult \
  "WHERE id = \$1 AND attempt = \$2 AND status = 'running'
		RETURNING id, attempt" \
  "WHERE id = \$1 AND \$2::int IS NOT NULL AND status = 'running'
		RETURNING id, attempt" chaos

mutate no-row-lock-on-claim 'TestNoDoubleClaim$' \
  "ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED" "ORDER BY id LIMIT 1"

mutate no-row-lock-no-recheck-on-claim 'TestNoDoubleClaim$' \
  "FROM c WHERE j.id = c.id AND j.status = 'queued'" "FROM c WHERE j.id = c.id"

mutate no-row-lock-and-no-recheck-on-claim 'TestNoDoubleClaim$' \
  "ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED
	)
	UPDATE jobs j SET status = 'running', attempt = j.attempt + 1, worker_id = \$1,
	       lease_until = now() + make_interval(secs => \$2),
	       started_at = now(), updated_at = now()
	FROM c WHERE j.id = c.id AND j.status = 'queued'" \
  "ORDER BY id LIMIT 1
	)
	UPDATE jobs j SET status = 'running', attempt = j.attempt + 1, worker_id = \$1,
	       lease_until = now() + make_interval(secs => \$2),
	       started_at = now(), updated_at = now()
	FROM c WHERE j.id = c.id"

mutate cancel-ignores-state TestCancelCompleteRace \
  "WHERE id = \$1 AND status IN ('queued', 'running')" "WHERE id = \$1"

# both lease checks: the CTE one alone is an equivalent mutant (the outer UPDATE re-checks)
mutate reaper-ignores-lease-in-cte-only TestReaperLeavesLiveLeases \
  "WHERE status = 'running' AND lease_until < now()" "WHERE status = 'running'"

mutate reaper-ignores-lease TestReaperLeavesLiveLeases \
  "WHERE status = 'running' AND lease_until < now()|||AND j.status = 'running' AND j.lease_until < now()" \
  "WHERE status = 'running'|||AND j.status = 'running'" chaos
