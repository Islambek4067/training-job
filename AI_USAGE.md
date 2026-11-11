# AI usage

**Tool:** Claude Code (Claude Opus), used as an agent in my terminal: it read the brief, wrote
code and docs, ran Docker, the tests and the harness, and read their output.

## Who did what
- **My decisions:** Go as the language, Postgres as the storage, the scope and the order of
  the work, the move from design to code, adding targeted tests and a mutation check after
  the first chaos results, and answering the discussion questions in writing once the call
  was cancelled.
- **AI-written:** essentially all of the code (store, API, worker, chaos harness, tests,
  scripts) and the first drafts of every document, including this one.
- **How suggestions were accepted:** nothing was accepted on reading alone. Every claim in
  the docs is backed by a run whose raw output is in `results/`. Where a run contradicted
  the AI's own design or tests, the code or the harness was changed and the failing output
  was kept. Those cases are listed below; they are the most useful part of this file.

## Where the AI was unreliable, and how that was caught
1. **Its first harness reported PASS without testing zombies.** The `kill -9` loop also
   picked frozen workers, so they died before waking. Caught because the report showed
   `stale_commit_rejected=0` after 4 freezes; tracing `job_runs` against the signal log
   confirmed it (job 839: worker w1.g0 frozen mid-job at 10:10:26.57, killed at 10:10:32.34).
   Raw output: `results/debug/`.
2. **Its workload could not trigger the bug it was meant to find.** All jobs were ≤ 200 ms,
   so a zombie always woke up after the new owner had finished. A mutant with fencing
   removed passed 2,000 jobs (`results/mutations-v1-harness-blind`).
3. **Its invariant was a tautology.** After long jobs were added, the mutant really did
   commit stale results (job 259), and it still passed, because the effect row stored
   `RETURNING attempt` from the very row it was guarding
   (`results/mutations-v2-tautological-invariant`). Fixed in harness v3, which fails the
   mutant with 4 FENCING violations.

   Lesson: AI-written tests checked against AI-written code share the same assumptions.
   Deliberately breaking the code (`scripts/mutation.sh`) is what exposed it.
4. **Design flaws in its first code, found by its tests:**
   - `Complete` was a client-side `BEGIN…COMMIT`, so a frozen worker could pin a row lock.
     It is now one statement.
   - An unjittered retry backoff failed under SERIALIZABLE (`results/tests/01-FAIL-*`).
   - There was no test that the reaper leaves live jobs alone until a mutant survived.
5. **Noticed while testing:** duplicate POSTs burn `BIGSERIAL` values, so ids have gaps.
   Harmless, but `max(id) ≠ count(*)`.

## Verified by running, not by reading
- Every endpoint by curl: replay `200`, different payload `409`, cancel of a running job
  (the worker logs `lost_ownership`, 0 effects), keyset pagination.
- `go test` silent-failure suite under three isolation levels: `results/tests/`.
- 12 chaos runs with 10,000 jobs each, plus mutant runs: `results/`, summarised in
  `docs/REPORT.md`.
- The repository was cloned fresh and started with `docker compose up --build`, then the
  tests and a chaos run were executed from that clone before submission.
