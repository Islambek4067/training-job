# soupq — a training-job queue that is tested by killing it

Go + Postgres. One binary (`soupq`) with subcommands `migrate`, `api`, `worker`, `chaos`.

```bash
docker compose up                  # db + migrate + api (:8080) + 4 workers
```

The first run builds the image automatically. After changing the code, use
`docker compose up --build` to rebuild it.

- Design (claim, dead-worker detection, cancel, guarantee): [docs/DESIGN.md](docs/DESIGN.md)
- Chaos harness: [cmd/soupq/chaos.go](cmd/soupq/chaos.go), raw output in [results/](results/)
- Silent failures and how each was checked: [docs/SILENT_FAILURES.md](docs/SILENT_FAILURES.md)
- All runs, what the harness misses, verdict: [docs/REPORT.md](docs/REPORT.md)
- AI usage log: [AI_USAGE.md](AI_USAGE.md)

## API

| Method | Path | Notes |
|---|---|---|
| `POST` | `/jobs` | header `Idempotency-Key` required; body `{"payload": {...}}`. `201` new, `200` replay (`Idempotent-Replayed: true`), `409` same key + different payload |
| `GET` | `/jobs/{id}` | |
| `GET` | `/jobs?status=&limit=&cursor=` | keyset pagination, newest first; pass `next_cursor` back as `cursor` |
| `POST` | `/jobs/{id}/cancel` | `200` cancelled (idempotent), `409` already succeeded/failed |

Emulated job payload: `{"duration_ms": 150, "fail": false}`.

```bash
curl -s -XPOST localhost:8080/jobs -H 'Idempotency-Key: k1' -d '{"payload":{"duration_ms":3000}}'
curl -s localhost:8080/jobs/1
curl -s -XPOST localhost:8080/jobs/1/cancel
curl -s 'localhost:8080/jobs?status=succeeded&limit=20'
```

"Execution" writes a row to `job_runs` when a worker starts a job (at-least-once, can be > 1)
and a row to `job_effects` atomically with `running → succeeded` (must be exactly 1 for
succeeded jobs, 0 otherwise).

## Chaos harness

```bash
scripts/chaos.sh 3                              # 3 runs with the task's defaults
scripts/chaos.sh 1 -- -clock-jump-every 15s     # extra flags go after --
PG_ISOLATION="repeatable read" scripts/chaos.sh 1
```

The script stops the compose workers; the harness then spawns its **own** 8 worker processes
so that it can send them real signals. Defaults: 10,000 jobs submitted at 100/s, `kill -9` of a
random worker every ~2 s (respawned), `SIGSTOP` of a busy worker for 8 s (> 5 s lease, i.e.
a zombie) every ~5 s, 30 % of keys POSTed 2–4× simultaneously plus a late replay, 1 % reused
with a different payload, 2 % of jobs last 4 s (so a zombie wakes while the new owner is
still running), 10 % of jobs cancelled at a random moment (20 % of those twice
concurrently), 2 % of jobs fail. Client retries on errors with the same key.

Checked at the end, directly against the DB:
no lost / phantom jobs · one id per key, ≤ 1 `201` per key · ≤ 1 effect per job, succeeded ⇔ 1
effect written by the final attempt *and* by the job's owner · nothing `queued`/`running` after drain + 2 leases of grace
· every `200` cancel ends `cancelled`, every `409` cancel ends in the state it reported ·
`finished_at` ⇔ terminal · failing payloads never succeed.

It also prints **coverage** (how many stale commits fencing actually rejected, how many jobs
were re-executed) and warns when a chaos path was never exercised — the first version of
the harness had exactly that bug (see `results/debug/`).

Every run writes `results/run-<ts>/report.txt`, `workers.log` (all worker output + every
signal sent) and, on failure, `violations.txt`. Exit code ≠ 0 on any violation.

## Silent-failure tests and mutation check

```bash
docker compose up -d db && go test -count=1 -v ./internal/store
scripts/mutation.sh --chaos      # break each safety mechanism, expect the checks to fail
```

## Configuration (env)

`DATABASE_URL`, `LEASE_MS` (5000), `HEARTBEAT_MS` (1000), `REAP_MS` (1000), `POLL_MS` (100),
`WORKER_CONCURRENCY` (1), `MAX_ATTEMPTS` (5), `PG_ISOLATION` (empty = read committed).

## Layout

```
cmd/soupq/        main.go (subcommands), chaos.go (harness)
internal/store/   schema.sql + every SQL statement
internal/api/     HTTP handlers
internal/worker/  claim / heartbeat / fenced commit / reaper
docs/DESIGN.md    one-page design note
results/          raw harness output, all runs
```
