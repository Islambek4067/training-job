# How I used AI

I built this with Claude Code running as an agent in my terminal. The agent
wrote the code, ran Docker, the tests and the chaos harness, and read their output. I owned
the decisions: what to build, in which stack, in what order, and what counts as done.

The working rule for the whole project: **nothing the AI produced was accepted because it
looked right.** Each design claim had to be backed by a run, with the raw output kept in
`results/`, including the runs that failed or misled. That rule is the reason the list
below exists.

## My part
- **Stack and storage:** Go, and Postgres over SQLite (row locks, `FOR UPDATE SKIP LOCKED`,
  one server-side clock; reasoning in `docs/DESIGN.md`).
- **Scope and order:** design note first, then service and harness, then targeted
  silent-failure tests and a mutation check once the first chaos results were in.
- **Review and submission:** I decided what goes into the repository and what doesn't
  (e.g. the brief itself), and I signed off on the final verdict.

## The agent's part
Essentially all of the code (store, API, worker, chaos harness, tests, scripts) and the
first drafts of the documents.

## Where the AI was wrong, and how it was caught
The most useful outcome of the "prove it by running" rule. The AI's own verification
reported success on broken code three times:

1. **A chaos path that never ran.** The first harness froze workers, but its `kill -9` loop
   also killed the frozen ones before they could wake. The report said PASS, with
   `stale_commit_rejected=0` after 4 freezes. Tracing `job_runs` against the signal log
   confirmed it (job 839: frozen at 10:10:26.57, killed at 10:10:32.34).
   Raw output: `results/debug/`.
2. **A workload that could not trigger the bug.** All jobs were ≤ 200 ms, so a zombie always
   woke up after the new owner had finished. A mutant with fencing removed passed 2,000 jobs
   (`results/mutations-v1-harness-blind`).
3. **A tautological invariant.** With long jobs added, the mutant really did commit stale
   results (job 259), and it still passed: the effect row stored `RETURNING attempt` from
   the very row it was guarding (`results/mutations-v2-tautological-invariant`). Harness
   v3 fails that mutant with 4 FENCING violations.

Takeaway: AI-written tests checked against AI-written code share the same blind spots.
Deliberately breaking the code (`scripts/mutation.sh`) is what exposed them, and I would
use that check on any AI-written verification from now on.

Design flaws in the first code, found by the tests and fixed:
- `Complete` was a client-side `BEGIN…COMMIT`, so a frozen worker could pin a row lock and
  leave its job stuck in `running`. It is now one statement.
- An unjittered retry backoff failed under SERIALIZABLE (`results/tests/01-FAIL-*`).
- There was no test that the reaper leaves live jobs alone until a mutant survived.

## Verified by running, not by reading
- Every endpoint by curl: replay `200`, different payload `409`, cancel of a running job
  (the worker drops it, 0 effects), keyset pagination.
- `go test` silent-failure suite under three isolation levels: `results/tests/`.
- 12 chaos runs with 10,000 jobs each, plus runs against mutant code, summarised in
  `docs/REPORT.md`.
- A fresh clone of this repository, with no prebuilt image, started with plain
  `docker compose up` exactly as the brief says, and the tests
  and a chaos run passed from that clone before submission.
