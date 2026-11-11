# Report: will it survive production?

## Every run, including the failed and the misleading ones

The harness changed three times, each time because I found a way it was lying to me.
Runs are grouped by harness version; nothing was deleted.

| # | What | Harness | Result | Note |
|---|---|---|---|---|
| 1 | 1k jobs (`results/debug/run-20261111T101019Z`) | v1 | PASS | **vacuous**: 4 freezes, 0 zombie writes. The kill loop killed frozen workers before they woke |
| 2 | 1k jobs (`results/debug/run-20261111T101247Z`) | v1.1 | PASS | after fix: 5 stale commits rejected |
| 3–5 | 10k jobs × 3, read committed | v1.1 | PASS ×3 | 79–88 kill -9, 30–37 freezes each |
| 6–10 | 10k: RC, RR, SERIALIZABLE, clock jumps, high load | v1.1 | PASS ×5 | |
| — | `go test` silent-failure suite, run 1 (`results/tests/01-FAIL-*`) | — | **FAIL** | SERIALIZABLE: 18 claims exhausted 20 retries (unjittered backoff) |
| — | `go test`, run 2 (`results/tests/02-*`) | — | PASS | exponential backoff + jitter |
| 11 | mutation chaos, 2k jobs (`results/mutations-v1-harness-blind`) | v1.1 | **PASS on broken code** | fencing removed and the harness still passed: jobs ≤ 200 ms, so the new owner had always finished before the zombie woke |
| 12 | mutation chaos (`results/mutations-v2-tautological-invariant`) | v2 (+2 % 4 s jobs) | **PASS on broken code** | zombies *did* commit stale results (job 259: zombie w7.g0 committed, the real owner w2.g1 was rejected), but the effect stored `RETURNING attempt` of the row, so "effect attempt == final attempt" was a tautology |
| 13 | mutation chaos (`results/mutations/`) | v3 (+ writer ≠ owner check) | **FAIL as it should** | no-fencing mutant: 4 FENCING violations / 2k jobs; reaper mutant: PASS + `EFFICIENCY WARNING` (2,059 re-executions vs 63 causes) |
| — | `go test`, final (`results/tests/03-final.txt`) | — | PASS | 10 tests × 3 isolation levels where relevant |
| 14–16 | 10k jobs × 3, read committed (`run-20261111T112750Z`, `…113332Z`, `…113907Z`) | v3 | PASS ×3 | 154–165 kill -9, 62–65 freezes, 62–64 stale commits rejected, 0 violations each |
| 17 | 10k jobs, clock jump every ~10 s (`run-20261111T114443Z`) | v3 | PASS | 34 forced expiries of every live lease + 157 kill -9 + 65 freezes: 310 re-executions, 170 owners stopped by fencing (53 via heartbeat), 6 jobs lease-exhausted, 0 violations |

**Total: 20 harness runs:**
- 12 with 10,000 jobs;
- 2 debug runs with 1,000 jobs;
- 6 with 2,000 jobs against mutant code (rows 11–13, two mutants each).

Plus 3 `go test` runs and 3 recorded mutation rounds. Their raw output is in `results/`.

**Not kept, stated so nothing is hidden:**
- Three early *test-only* mutation rounds were overwritten while I was still fixing the
  mutation script. In one of them, the no-fencing mutant was "killed" for the wrong reason:
  removing `attempt = $2` left `$2` untyped, so Postgres raised a type error instead of the
  test catching a stale write. Three mutants also "survived" there; that is how the
  equivalent mutants and the missing reaper test were found (SILENT_FAILURES.md §1, §8).
- One mutation round was interrupted halfway and rerun.

**What this means for runs 3–10:** they show no lost jobs, no duplicate ids, nothing stuck
and consistent cancels under kill -9. They do **not** show that attempt-fencing works against
a still-running new owner. Only `TestZombieCannotWriteStaleResult` showed that until harness v3.

## What the harness does NOT catch

- **Rare interleavings.** About 90 kills and 35 freezes per run. A bug that needs a
  specific ~1 ms window (e.g. a freeze between `UPDATE` and `COMMIT`, finding 7 in
  SILENT_FAILURES.md) will practically never be hit. Those are covered by deterministic
  tests, not by chaos.
- **Efficiency bugs hidden by a safe design.** A reaper that steals live jobs passes every
  invariant; only the new `EFFICIENCY WARNING` (re-executions vs. their causes) flags it.
- **Infrastructure failures.** Postgres is never killed, failed over, slowed down or
  partitioned from the workers; the API is never killed; no disk-full, no connection-pool
  exhaustion. Only process death and process freeze are simulated.
- **Clock jumps backwards** (leases look longer → slow dead-worker detection). Only forward
  jumps are simulated.
- **Real external side effects.** The effect lives in the same DB, so exactly-once is by
  construction. A real checkpoint in S3 would be at-least-once and is not modelled.
- **Long-running behaviour.** 3 minutes per run: no table bloat, autovacuum, unbounded
  growth of `job_runs`/`job_effects`, idempotency keys that never expire.
- **Poison jobs** that crash every worker that runs them (they would kill up to
  `max_attempts` workers in a row).
- **Heartbeat starvation:** a slow DB makes heartbeats late, live workers lose leases, work
  is duplicated. There is no DB-latency injection.

**"How do you know 10,000 jobs was enough?"** I don't, and the job count is not the right
measure. What matters is how often each failure path was *exercised*: the coverage line
(stale commits rejected, lease losses, re-executions per run), and whether the harness
fails on known-broken code (mutation check). Runs 11 and 12 show that "10,000 jobs, PASS"
can coexist with zero coverage of the property that matters most.

## Capacity (laptop, Docker, Postgres 16, 8 worker processes)

| | measured |
|---|---|
| API at 100 POST/s | p50 ≈ 3.5 ms, p99 ≈ 11 ms, 0 errors |
| API, unlimited, 64 clients | ≈ 1,700 POST/s, p99 49 ms, 0 errors |
| claim throughput | 1,000 claims / 0.34 s with 32 concurrent claimers (≈ 3,000/s) |
| workers, 8 × 1 slot, jobs avg ≈ 110 ms | ≈ 55 jobs/s, so the queue **grows** at 100 submissions/s |
| workers, 8 × 4 slots | ≈ 200 jobs/s |
| dead-worker detection | lease 5 s + reaper 1 s ≈ 6 s worst case |

## Verdict at 100 requests per second: **DON'T SHIP (yet)**

The core is ready, the service is not. Correctness under kill -9, freezes, duplicate and
concurrent requests, cancel races and every isolation level holds in every run and test.
The harness fails on each safety mutant I could think of. Capacity is not the problem: the
API has ≈ 17× headroom at 100 rps, the claim path ≈ 30×, and worker throughput is a
deployment knob.

The reason not to ship is that **this design is safe by turning failures into silence.**
A zombie's write is rejected, a stolen lease costs a re-execution, a crash costs a retry, and
none of these raise an error anywhere. In the harness I could see them only because it
counts log lines. In production nobody reads those lines, and the reaper mutant shows the
consequence: a system that runs every job twice passes every correctness check.

**Fix first, in order:**
1. **Metrics + alerts on the silent signals:**
   - `stale_commit_rejected` rate, leases reaped per minute, re-executions per job;
   - `lease-exhausted` failures, queue depth, age of the oldest queued job;
   - claim errors (`40001`).
   These are the only way to notice the failure modes this design deliberately hides.
2. **Backpressure.** At 100 submissions/s with 8 single-slot workers the queue grew without
   bound (≈ 55 jobs/s processed). The API should return 429 above a queue-depth limit
   instead of accepting work it cannot schedule.
3. **Separate crash retries from job failures.** `max_attempts = 5` counts *infrastructure*
   deaths: in runs 14–16, 1–3 healthy 4 s jobs per run were failed permanently because
   their workers were killed five times. A real training job lasting hours under normal
   worker churn would hit this. Count crashes separately (or with backoff) and treat a job
   that kills N different workers as poison.
4. **Retention.** 100 rps means 8.6 M jobs/day plus their `job_runs` / `job_effects`
   rows, and idempotency keys that never expire. Partition by time and set a key TTL.

Not blocking at 100 rps but next: replace polling with `LISTEN/NOTIFY` (idle polls grow with
worker count), test a Postgres failover (a clock jump on a new primary means a burst of
duplicate executions, not effects — run 17), add `UNIQUE(job_effects.job_id)` as a second
line of defence once the harness no longer needs to see raw duplicates.

## The discussion questions, answered in writing

The brief lists questions for the live discussion. Since there is no call, here are my answers.

**A worker froze for 5 minutes and came back: what happens?**
Its last heartbeat was at most 1 s before the freeze, so its lease expires ≤ 5 s into the
freeze. Within 1 s more, any worker's reaper sends the job back to `queued` (or to `failed`
if it has used `max_attempts`). Another worker claims it as `attempt + 1` and most likely
finishes it long before the 5 minutes are up. When the frozen worker wakes, its heartbeat or
its commit still carries the old attempt, so both match 0 rows. It logs
`lost_ownership` / `stale_commit_rejected`, drops the result and claims the next job. Cost:
one wasted execution, no second effect.
Edge cases:
- If nobody reclaimed the job (every other worker dead or busy), the late commit is
  accepted. Its result is still the only one (`TestExpiredButNotReclaimedOwnerMayCommit`).
- If the job was cancelled meanwhile, the commit is rejected.
- If it froze *inside* an open transaction, the row lock would block the reaper and cancel
  for those 5 minutes. No code path does this any more (`Complete` is one statement), and
  `idle_in_transaction_session_timeout = 5 s` would end such a session anyway
  (`TestFrozenTransactionHoldsRowLock`).
Evidence: `TestZombieCannotWriteStaleResult`, plus 62–65 freezes of 8 s per chaos run.
8 s and 5 min take the same code path, because anything longer than the lease is equivalent.
What I have not tested is a 5-minute freeze against server-side connection timeouts: the
pool would then have to reconnect, and the worker would see an error instead of 0 rows.
Fencing makes the outcome the same, but this path is not exercised.

**Where is the design weakest?**
1. **Everything rests on one Postgres, including its clock.** A failover to an async replica
   can lose recently committed transitions. A job the API reported as `succeeded` can come
   back as `running` and be executed again, and the new primary's clock can expire every
   lease at once (run 17 simulates the clock part: a burst of re-executions, 0 duplicate
   effects). Lost commits are not simulated.
2. **"Exactly-once effect" holds only because the effect is a row in the same database.**
   A real training job writes checkpoints to object storage and spends GPU time; those
   effects are at-least-once. The fencing token would have to travel to that storage and
   be checked there.
3. **Failures are silent by design.** Without metrics on stale commits, reaps and
   re-executions, a regression like the reaper mutant (every job run twice) passes every
   check and shows up only on the bill.
4. **`max_attempts` conflates crashes with job failures.** Long jobs are failed permanently
   by ordinary worker churn (1–6 per 10k run).

**What breaks first at 10× load?**
Not the API (≈ 1,700 POST/s measured) and not claiming (≈ 3,000/s). The single Postgres
write path breaks first. Each job costs about 6 writes (insert, claim, run row, completion
+ effect, plus heartbeats), and heartbeats scale with the number of *concurrently running*
jobs, not with the request rate. By Little's law, 1,000 job starts/s × 10 min per job =
600,000 running jobs = 600,000 heartbeat `UPDATE`s/s at a 1 s interval. That is impossible
on one Postgres long before anything else. Even at today's 100/s with 10-minute jobs it is
60,000/s. So the first fix at scale is the lease policy: for long jobs use a 60 s lease with
a 15 s heartbeat (15× fewer writes, slower dead-worker detection), batch heartbeats per
worker process, and partition `jobs` so autovacuum keeps up with the update churn. In my
runs the jobs are 20–200 ms, so this cost never showed up. The harness measured the wrong
regime for this question.

**How do you know 10,000 jobs was enough?**
I don't, and the number of jobs is the wrong axis. What matters is how often each failure
path was *exercised*: per final run, 154–165 kills, 62–65 freezes and 62–64 stale commits
rejected, printed in the coverage line. Equally important is whether the harness *fails on
broken code*. Runs 11 and 12 show a 10,000-job PASS with zero coverage of attempt-fencing.
Mutation testing (round 3) shows the harness now does fail on the safety bugs I could
think of. A bug that needs a ~1 ms window, like the frozen transaction, will practically
never be hit by chaos at any job count. That is why it has a deterministic test instead.

## What surprised me, what still concerns me

What surprised me most is that my own verification was the weakest component. The service
code needed two real fixes: a client-held transaction that let a frozen worker pin a row
lock, and an unjittered retry that broke under SERIALIZABLE. The harness, though, reported
PASS on broken code three separate ways: a chaos path that never ran, a workload that could
not trigger the bug, and an invariant that was a tautology because the value it checked was
derived from the row it was guarding. Each looked like solid evidence, a 10,000-job PASS
with thousands of duplicates and dozens of kills. Only running it against deliberately
broken code exposed them. What still concerns me is that this method finds only the
failures I thought to inject: Postgres failover, heartbeat starvation under a slow database,
and real external side effects that cannot be fenced are untested. And mutation testing
tells me which bugs the harness *can* see, not which ones it hasn't met yet.
