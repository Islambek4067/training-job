# Design note — soupq

One Postgres database is the single source of truth. API and workers are stateless
processes; every state transition is one conditional `UPDATE` (compare-and-set) on the
`jobs` row. No in-memory state is trusted after a crash.

## Why Postgres (not SQLite)
8 worker *processes* (so that `kill -9` is real) + an API all write concurrently.
SQLite serialises writers on one file lock and has no row locks, so "claim one job without
blocking others" degrades into retry loops on `SQLITE_BUSY`. Postgres gives row-level locks,
`FOR UPDATE SKIP LOCKED`, a server-side clock (`now()`), and MVCC — exactly the primitives a
queue needs.

## States
`queued → running → succeeded | failed | cancelled`, plus `running → queued` when a lease
expires (retry). Terminal states are never left. Enforced by the `WHERE status = …` clause of
every transition and backed by `CHECK` constraints (running ⇔ has lease, terminal ⇔ has
`finished_at`).

## How a worker claims a job
```sql
WITH c AS (SELECT id FROM jobs WHERE status='queued' ORDER BY id
           LIMIT 1 FOR UPDATE SKIP LOCKED)
UPDATE jobs SET status='running', attempt=attempt+1, worker_id=$w,
                lease_until=now()+$lease
FROM c WHERE jobs.id=c.id AND jobs.status='queued' RETURNING id, attempt, payload;
```
`SKIP LOCKED` lets 8 workers claim in parallel without waiting on each other; the row lock
makes a double claim impossible. **`attempt` is a fencing token**: it increases on every
claim, and every later write by that worker must present it.

## How a dead worker is detected
Leases, measured **only on the database clock**. A running worker heartbeats every 1 s:
`UPDATE … SET lease_until = now()+5s WHERE id=$id AND attempt=$a AND status='running'`.
Every worker also runs a reaper every 1 s that sends `running` rows with `lease_until < now()`
back to `queued` (or to `failed` once `attempt >= max_attempts`). Worst-case detection is
lease + reaper interval ≈ 6 s. Workers never compare their own clocks to anything, so clock
skew between machines cannot cause false expiry. A false positive (a slow but alive worker)
is safe: it only causes a duplicate *execution*, never a duplicate *effect* (below).

A heartbeat that updates 0 rows means "you no longer own this job" (reaped or cancelled):
the worker aborts the work and drops the result.

## Completion and the side effect
Completion is one statement (so no client ever holds a row lock across a round-trip — a
worker frozen between `UPDATE` and `COMMIT` would block reaper and cancel for that row):
```sql
WITH done AS (UPDATE jobs SET status='succeeded', finished_at=now(), lease_until=NULL
              WHERE id=$id AND attempt=$a AND status='running' RETURNING id, attempt)
INSERT INTO job_effects(job_id, attempt, worker_id) SELECT id, attempt, $w FROM done;
-- 0 rows inserted ⇒ the result was stale, nothing written
```
A zombie (a worker that froze past its lease and came back) holds an old `attempt`, so its
`UPDATE` matches 0 rows and its effect is rolled back. The lease being expired *by itself*
does not block a commit: if nobody reclaimed the job, the old owner's result is still valid.
Correctness rests on the fencing token, not on wall-clock time.

`job_effects` deliberately has **no** `UNIQUE(job_id)`: the chaos harness must measure the
protocol, not a constraint that would hide its bugs. In production I would add it as a
second line of defence.

## What happens when a running job is cancelled
`POST /jobs/{id}/cancel` runs `UPDATE … SET status='cancelled' WHERE status IN
('queued','running')`. It wins or loses against completion on the row lock — exactly one of
the two commits. The worker learns about it at its next heartbeat (≤ 1 s), stops, and its
fenced completion cannot commit. So `cancelled` means **"no effect will ever be
committed"**. It does **not** mean "computation stopped instantly": up to one heartbeat of
work is wasted (longer if the worker is frozen, but then it cannot commit either). Cancelling
a terminal job returns 409; cancelling an already-cancelled job is a no-op 200.

## Idempotent create
`INSERT … ON CONFLICT (idempotency_key) DO NOTHING RETURNING …`; on conflict, read the
existing row. Concurrent duplicates are serialised by the unique index. The same key with a
different payload returns 409 (compared as `jsonb`, so key order doesn't matter).

## Guarantee
**At-least-once execution, exactly-once committed effect.**
- Execution is at-least-once: a worker can be killed after doing the work but before
  committing, so the work is redone. `job_runs` records every execution start to make this
  visible.
- The effect is exactly-once *because it lives in the same database as the state* and is
  committed atomically with the state transition under the fencing token.
- At-most-once was rejected: it means losing jobs on every crash. True exactly-once
  *execution* is impossible with crashing workers.
- If the effect were external (a checkpoint in S3, a webhook), this design degrades to
  at-least-once and the external system must deduplicate on `(job_id)`; the fencing token
  would have to be passed to and checked by that system.

## Transaction isolation
READ COMMITTED (Postgres default). Correctness comes from row locks + conditional `UPDATE`s,
not from snapshots: when two transactions race on one row, the second one waits and then
re-evaluates its `WHERE` against the committed version. Under REPEATABLE READ / SERIALIZABLE
the same race raises `40001` instead; all writes are wrapped in a retry for `40001`/`40P01`.
