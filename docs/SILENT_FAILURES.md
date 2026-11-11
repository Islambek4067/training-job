# Silent failures — what can break without an error, and how each was checked

Three kinds of evidence, used together:

- **Targeted tests** (`internal/store/silent_test.go`): deterministic or high-contention
  reproductions, each concurrency test run under READ COMMITTED, REPEATABLE READ and
  SERIALIZABLE.
- **Chaos harness** (`soupq chaos`): the whole system under kill -9 / SIGSTOP / duplicate
  POSTs / cancels; invariants checked in the DB afterwards.
- **Mutation check** (`scripts/mutation.sh`): each safety mechanism is removed in a scratch
  copy, and the relevant test must then FAIL. A test that also passes on broken code proves
  nothing. Raw output: `results/mutations/`.

```bash
docker compose up -d db && go test -count=1 -v ./internal/store   # results/tests/
scripts/mutation.sh            # tests only
scripts/mutation.sh --chaos    # + chaos runs with mutant workers (needs api running)
```

| # | Failure | Mechanism | Test | Mutation removed → result |
|---|---|---|---|---|
| 1 | double claim | `FOR UPDATE SKIP LOCKED` + `status='queued'` re-check in the `UPDATE` | `TestNoDoubleClaim` (32 claimers × 1000 jobs) | lock only → survives · re-check only → survives · **both → killed** (≈298/300 jobs double-claimed) |
| 2 | zombie writes a stale result | fencing: `attempt` must match on heartbeat / complete / fail | `TestZombieCannotWriteStaleResult` | `attempt = $2` → **killed** by the test; chaos harness v1/v2 **passed** it, v3 fails it (4 FENCING violations) |
| 3 | cancel / complete race | both are conditional `UPDATE`s on one row; the loser re-checks or gets `40001` | `TestCancelCompleteRace` (300 races, a third also racing the reaper), `TestCancelCompleteInterleavings` (deterministic, both orders) | cancel without `status IN (…)` → **killed** |
| 4 | duplicate on request retry | `UNIQUE(idempotency_key)` + `ON CONFLICT DO NOTHING`, payload compared as `jsonb` | `TestIdempotentCreateUnderConcurrency` (8 concurrent creates × 300 keys) | — |
| 5 | isolation level | correctness from row locks, not snapshots; `40001`/`40P01` retried | every test above × 3 levels; chaos under RR and SERIALIZABLE | — |
| 6 | clock skew | lease set and compared only with the DB's `now()` | `TestLeaseUsesOnlyDatabaseClock`; chaos `-clock-jump-every` | — |
| 7 | worker frozen inside an open transaction | `Complete` is one statement; `idle_in_transaction_session_timeout` | `TestFrozenTransactionHoldsRowLock` | — |
| 8 | reaper takes a job from a live owner | `lease_until < now()` in the CTE **and** the outer `UPDATE` | `TestReaperLeavesLiveLeases` | CTE check only → survives · **both → killed** |

## 1. Double claim
Two workers both believe they own a job. Nothing errors; the job just runs twice. The test
collects every claim from 32 concurrent claimers and requires each of 1000 jobs exactly once
with `attempt = 1`. Mutations show that **either** the row lock **or** the re-check alone
prevents it (defence in depth, so removing one is an *equivalent mutant*). Removing both
gives about 298 double claims out of 300.

What SKIP LOCKED actually buys is throughput. Without it, under READ COMMITTED, a claimer
that lost the race waits on the row lock, re-checks, matches 0 rows and reports "queue empty"
while work is waiting: **4,246 false "empty" answers and 15× slower** (5.2 s vs 0.34 s for
1000 jobs). That is a silent failure too: workers idle with a full queue. The correct code
still gives about 50 false "empty" answers per storm (rows skipped while another claimer
holds them); with real polling intervals this costs at most one poll per worker.

## 2. Zombie
Worker A freezes (GC pause, SIGSTOP, VM migration) past its lease; the reaper requeues the
job; B claims it as attempt 2; A wakes and reports. The test plays exactly that sequence:
A's heartbeat, completion and failure are all rejected, A wrote no effect, B's completion is
accepted, and the single effect carries attempt 2.

In chaos, the worker log counts `stale_commit_rejected` (29–40 per 10k run). **These numbers
misled me at first.** With only short jobs, every zombie woke up after the new owner had
already *finished*, so the `status = 'running'` check rejected it and the `attempt` check
was never needed. A mutant without attempt-fencing passed the harness twice:
- `results/mutations-v1-harness-blind`: only short jobs.
- `results/mutations-v2-tautological-invariant`: 2 % long jobs added, so zombies did
  commit stale results while the new owner was running. But the effect row stored
  `RETURNING attempt` (the *current* owner's attempt), so my check "effect attempt == final
  attempt" was true by construction.

Harness v3 adds 4 s jobs, makes the effect record the writer's own attempt, and checks
effect writer == job owner independently. The same mutant now fails with 4 `FENCING`
violations in 2,000 jobs (`results/mutations/`).

Pinned design decision (`TestExpiredButNotReclaimedOwnerMayCommit`): an expired lease *alone*
does not reject the owner. If nobody reclaimed the job, the late result is still the only
result. Correctness rests on the fencing token, not on wall-clock time.

## 3. Cancel / complete race
The silent failure here would be "API says cancelled, but the effect was committed", or a
succeeded job flipped to cancelled. Both transitions are a conditional `UPDATE` of one row.
The deterministic test holds one of them uncommitted, starts the other, verifies that it
*blocks* (200 ms), then commits. The second one must observe the committed state and lose.
Under READ COMMITTED Postgres re-evaluates the `WHERE` on the new row version; under RR /
SERIALIZABLE it raises `40001` and our retry runs it again. The randomized test (300 races,
every third one also racing the reaper with an expired lease) checks
`effect ⇔ succeeded` and that the API's answer matches the final state. Both outcomes
occurred (e.g. 242 cancelled / 58 succeeded), so the race really happened in both directions.

The chaos harness checks the same thing end to end: every `200` from cancel must end
`cancelled` with 0 effects, and every `409` must end in the state the 409 reported.

## 4. Duplicate on request retry
A client times out and retries; two proxies replay one request. 8 identical creates fired at
once for each of 300 keys must yield exactly one row, exactly one `created=true` and one id.
The same payload with keys in a different order is a replay (`jsonb` equality), a different
payload is a `409`. In chaos, 30 % of keys are POSTed 2–4× at the same instant plus once
more up to 20 s later; the harness requires one id per key and at most one `201`.

Side effect found while testing: every duplicate `INSERT … ON CONFLICT` consumes a `BIGSERIAL`
value, so ids have gaps. Harmless, but `max(id) ≠ count(*)`.

Not covered: idempotency keys never expire, and a key is global, not per tenant.

## 5. Transaction isolation
All statements are single-statement transactions that rely on row locks and conditional
`UPDATE`s, so READ COMMITTED is sufficient. Every concurrency test runs under all three
levels. **First run failed** (`results/tests/01-FAIL-*`): under SERIALIZABLE, 18 claims hit
`40001` more than 20 times in a row. The linear, unjittered backoff kept 32 claimers in
lockstep. With exponential backoff + full jitter it fell to 6–9. Never a safety violation, but
SERIALIZABLE buys nothing here and costs throughput. Run at READ COMMITTED.

## 6. Clock skew
Workers never compare their clock with anything: the lease is written as
`now() + interval` and compared as `lease_until < now()`, both on the DB server. The test
asserts that `store.go` contains no `time.Now/Since/Until` and that the lease measured on the
DB clock is about 5 s. Worker-to-worker and worker-to-DB skew therefore cannot expire a lease.

What remains is the **DB clock itself jumping** (NTP step, failover to a replica with a
different clock). The chaos flag `-clock-jump-every` simulates it by expiring every live
lease at once: the owners are alive, the reaper requeues their jobs, and fencing must keep
effects ≤ 1. Consequence: a burst of duplicate *executions*, not duplicate effects.
Also note that `now()` is the transaction start time. That is fine for our single-statement
transactions, but would be wrong inside a long transaction.

## 7. Found while designing the tests: a worker frozen inside an open transaction
The first version of `Complete` ran `BEGIN; UPDATE; INSERT; COMMIT` from the client. If the
worker freezes or is partitioned between `UPDATE` and `COMMIT`, Postgres keeps the row lock
until it notices the session is gone. With a network partition and default TCP keepalives
that can take hours. Meanwhile the reaper silently skips the row (`SKIP LOCKED`) and
cancel blocks: the job is stuck in `running` long after its lease, with no error anywhere.
`TestFrozenTransactionHoldsRowLock/without_timeout` reproduces it.
Fix: `Complete` is now a single statement (CTE `UPDATE … RETURNING` + `INSERT … SELECT`), so
no client holds a lock across a round-trip. Backstop:
`idle_in_transaction_session_timeout=5s` on every connection, which the `with_timeout`
subtest verifies (the server kills the frozen transaction and cancel goes through).
The chaos harness would essentially never catch this: the window is about one round-trip.

## 8. Found by the mutation check: the reaper taking live jobs
No test originally checked that the reaper leaves live leases alone. A mutant that reaps
*every* running job passed all tests. The chaos harness also passes it, and correctly so:
fencing still keeps effects ≤ 1. The only symptom is cost: 3,891 executions for 2,000 jobs,
1,954 rejected stale commits (`results/mutations/chaos-reaper-ignores-lease`). Added
`TestReaperLeavesLiveLeases`, and the harness now prints an `EFFICIENCY WARNING` when
re-executions exceed what kills, freezes and forced expiries can explain (2,059 vs 63 here).
Lesson: the invariants check *safety*; *efficiency* regressions hide behind a safe design.

## 9. Found in the harness itself: three ways it said PASS without checking anything
1. **A chaos path that never ran.** Frozen workers were picked by the kill loop and died
   before waking (`results/debug/run-20261111T101019Z`). Fix: killer skips frozen workers,
   pauses target workers that hold a job, and coverage counters with warnings.
2. **A workload that could not trigger the bug.** All jobs were shorter than
   `pause − lease`, so attempt-fencing was never needed (section 2). Fix: 2 % 4 s jobs.
3. **A tautological invariant.** The value it checked was written by the code under test
   from the row it was guarding (section 2). Fix: record the writer's own attempt, and check
   the writer against the owner with a second, independent field.

All three were found only by running the harness against deliberately broken code.
