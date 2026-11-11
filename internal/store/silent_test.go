package store

// Targeted tests for the failures that do NOT raise errors. They need a Postgres:
//   docker compose up -d db && go test -count=1 -v ./internal/store
// Each concurrency test runs under READ COMMITTED, REPEATABLE READ and SERIALIZABLE.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const lease = 5 * time.Second

var isolations = []string{"read committed", "repeatable read", "serializable"}

func adminURL() string {
	return envOr("TEST_ADMIN_URL", "postgres://soup:soup@localhost:5432/soup?sslmode=disable")
}
func testURL() string {
	return envOr("TEST_DATABASE_URL", "postgres://soup:soup@localhost:5432/soup_test?sslmode=disable")
}
func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

var createDB sync.Once

func setup(t *testing.T, isolation string, urlSuffix ...string) *Store {
	t.Helper()
	ctx := context.Background()
	createDB.Do(func() {
		conn, err := pgx.Connect(ctx, adminURL())
		if err != nil {
			t.Skipf("no postgres: %v", err)
		}
		defer conn.Close(ctx)
		_, err = conn.Exec(ctx, `CREATE DATABASE soup_test`)
		var pgErr *pgconn.PgError
		if err != nil && !(errors.As(err, &pgErr) && pgErr.Code == "42P04") {
			t.Fatal(err)
		}
	})
	st, err := Open(ctx, testURL()+strings.Join(urlSuffix, ""), isolation)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Pool.Exec(ctx, `TRUNCATE job_effects, job_runs, jobs RESTART IDENTITY`); err != nil {
		t.Fatal(err)
	}
	return st
}

func forEachIsolation(t *testing.T, fn func(t *testing.T, st *Store)) {
	for _, iso := range isolations {
		t.Run(strings.ReplaceAll(iso, " ", "_"), func(t *testing.T) { fn(t, setup(t, iso)) })
	}
}

func mustCreate(t *testing.T, st *Store, key string) Job {
	t.Helper()
	j, _, err := st.CreateJob(context.Background(), key, []byte(`{"duration_ms":1}`), 5)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func mustClaim(t *testing.T, st *Store, worker string) *Claim {
	t.Helper()
	c, err := st.Claim(context.Background(), worker, lease)
	if err != nil || c == nil {
		t.Fatalf("claim: %v %v", c, err)
	}
	return c
}

func effects(t *testing.T, st *Store, id int64) (n int, attempts []int) {
	t.Helper()
	rows, err := st.Pool.Query(context.Background(), `SELECT attempt FROM job_effects WHERE job_id = $1`, id)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var a int
		rows.Scan(&a)
		attempts = append(attempts, a)
	}
	return len(attempts), attempts
}

func expireLease(t *testing.T, st *Store, id int64) {
	t.Helper()
	if _, err := st.Pool.Exec(context.Background(),
		`UPDATE jobs SET lease_until = now() - interval '1 second' WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// 1. Double claim: 32 workers drain 1000 jobs concurrently; every job must be claimed once.

func claimStorm(t *testing.T, st *Store, n int, claim func(ctx context.Context, w string) (*Claim, error)) map[int64]int {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		mustCreate(t, st, fmt.Sprintf("k%d", i))
	}
	var mu sync.Mutex
	seen := map[int64]int{}
	var exhausted, falseEmpty atomic.Int64
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < 32; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				c, err := claim(ctx, fmt.Sprintf("w%d", w))
				var pgErr *pgconn.PgError
				if errors.As(err, &pgErr) && pgErr.Code == "40001" {
					// what the worker does: log, sleep, claim again. Safe, but not free.
					exhausted.Add(1)
					time.Sleep(10 * time.Millisecond)
					continue
				}
				if err != nil {
					t.Error(err)
					return
				}
				if c == nil {
					// "queue empty" while jobs are still queued = a worker idles while
					// work waits. Not a safety bug, but a silent throughput one.
					var left int
					st.Pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE status = 'queued'`).Scan(&left)
					if left > 0 {
						falseEmpty.Add(1)
						time.Sleep(time.Millisecond)
						continue
					}
					return
				}
				mu.Lock()
				seen[c.JobID]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	t.Logf("claim storm: %d jobs, 32 claimers, %v, false 'queue empty' answers=%d",
		n, time.Since(start).Round(time.Millisecond), falseEmpty.Load())
	if n := exhausted.Load(); n > 0 {
		t.Logf("%d claims gave up after 20 serialization retries (worker would retry)", n)
	}
	return seen
}

func TestNoDoubleClaim(t *testing.T) {
	forEachIsolation(t, func(t *testing.T, st *Store) {
		seen := claimStorm(t, st, 1000, func(ctx context.Context, w string) (*Claim, error) {
			return st.Claim(ctx, w, lease)
		})
		if len(seen) != 1000 {
			t.Fatalf("claimed %d distinct jobs, want 1000", len(seen))
		}
		for id, n := range seen {
			if n != 1 {
				t.Errorf("job %d claimed %d times", id, n)
			}
		}
		var maxAttempt int
		st.Pool.QueryRow(context.Background(), `SELECT max(attempt) FROM jobs`).Scan(&maxAttempt)
		if maxAttempt != 1 {
			t.Errorf("max attempt %d, want 1", maxAttempt)
		}
	})
}

// The same storm with the row lock removed must FAIL — proves the test can see the bug.
func TestNoDoubleClaim_DetectorWorks(t *testing.T) {
	st := setup(t, "read committed")
	unsafe := strings.Replace(ClaimSQL, "FOR UPDATE SKIP LOCKED", "", 1)
	unsafe = strings.Replace(unsafe, "AND j.status = 'queued'", "", 1)
	doubles := 0
	for attempt := 0; attempt < 5 && doubles == 0; attempt++ {
		st.Pool.Exec(context.Background(), `TRUNCATE job_effects, job_runs, jobs RESTART IDENTITY`)
		seen := claimStorm(t, st, 300, func(ctx context.Context, w string) (*Claim, error) {
			var c Claim
			err := st.Pool.QueryRow(ctx, unsafe, w, lease.Seconds()).Scan(&c.JobID, &c.Attempt, &c.Payload)
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, nil
			}
			return &c, err
		})
		for _, n := range seen {
			if n > 1 {
				doubles++
			}
		}
	}
	if doubles == 0 {
		t.Fatal("detector found no double claims with the row lock removed; the test is too weak")
	}
	t.Logf("without FOR UPDATE SKIP LOCKED: %d jobs were claimed more than once", doubles)
}

// ---------------------------------------------------------------------------
// 2. Zombie: worker A freezes past its lease, the job is reaped and reclaimed by B,
//    then A wakes up and tries to heartbeat / complete / fail with its old attempt.

func TestZombieCannotWriteStaleResult(t *testing.T) {
	forEachIsolation(t, func(t *testing.T, st *Store) {
		ctx := context.Background()
		j := mustCreate(t, st, "zombie")
		a := mustClaim(t, st, "A")
		expireLease(t, st, j.ID) // A is frozen; its lease runs out
		if rq, _, err := st.ReapExpired(ctx, 10); err != nil || rq != 1 {
			t.Fatalf("reap: requeued=%d err=%v", rq, err)
		}
		b := mustClaim(t, st, "B")
		if b.Attempt != a.Attempt+1 {
			t.Fatalf("B attempt %d, want %d", b.Attempt, a.Attempt+1)
		}
		// A wakes up.
		if ok, _ := st.Heartbeat(ctx, a, lease); ok {
			t.Error("zombie heartbeat accepted")
		}
		if ok, _ := st.Complete(ctx, a, "A"); ok {
			t.Error("zombie completion accepted")
		}
		if ok, _ := st.Fail(ctx, a, "zombie says failed"); ok {
			t.Error("zombie failure accepted")
		}
		if n, _ := effects(t, st, j.ID); n != 0 {
			t.Fatalf("zombie wrote %d effects", n)
		}
		if ok, err := st.Complete(ctx, b, "B"); !ok || err != nil {
			t.Fatalf("owner completion rejected: %v", err)
		}
		n, att := effects(t, st, j.ID)
		got, _ := st.GetJob(ctx, j.ID)
		if n != 1 || att[0] != b.Attempt || got.Status != StatusSucceeded {
			t.Fatalf("effects=%d attempts=%v status=%s", n, att, got.Status)
		}
	})
}

// The reaper must never take a job away from a live owner. If it did, fencing would still
// keep effects at <= 1, so the chaos harness would PASS — only re-executions would grow.
func TestReaperLeavesLiveLeases(t *testing.T) {
	forEachIsolation(t, func(t *testing.T, st *Store) {
		ctx := context.Background()
		for i := 0; i < 20; i++ {
			mustCreate(t, st, fmt.Sprintf("live%d", i))
			mustClaim(t, st, "W")
		}
		if rq, f, err := st.ReapExpired(ctx, 100); err != nil || rq+f != 0 {
			t.Fatalf("reaper took %d live jobs (err=%v)", rq+f, err)
		}
	})
}

// Design decision, pinned by a test: an expired lease alone does not invalidate the owner.
// If nobody reclaimed the job, the late result is still correct and is accepted.
func TestExpiredButNotReclaimedOwnerMayCommit(t *testing.T) {
	st := setup(t, "read committed")
	ctx := context.Background()
	j := mustCreate(t, st, "late")
	a := mustClaim(t, st, "A")
	expireLease(t, st, j.ID)
	if ok, err := st.Complete(ctx, a, "A"); !ok || err != nil {
		t.Fatalf("late owner rejected: %v", err)
	}
	if rq, f, _ := st.ReapExpired(ctx, 10); rq+f != 0 {
		t.Fatal("reaper touched a finished job")
	}
}

// ---------------------------------------------------------------------------
// 3. cancel/complete race (+ the reaper): exactly one transition wins, and the effect
//    exists iff the final state is succeeded.

func TestCancelCompleteRace(t *testing.T) {
	forEachIsolation(t, func(t *testing.T, st *Store) {
		ctx := context.Background()
		wins := map[string]int{}
		for i := 0; i < 300; i++ {
			j := mustCreate(t, st, fmt.Sprintf("race%d", i))
			c := mustClaim(t, st, "W")
			withReaper := i%3 == 0
			if withReaper {
				expireLease(t, st, j.ID)
			}
			start := make(chan struct{})
			var wg sync.WaitGroup
			var completed bool
			var cancelErr error
			wg.Add(2)
			go func() { defer wg.Done(); <-start; completed, _ = st.Complete(ctx, c, "W") }()
			go func() { defer wg.Done(); <-start; _, cancelErr = st.CancelJob(ctx, j.ID) }()
			if withReaper {
				wg.Add(1)
				go func() { defer wg.Done(); <-start; st.ReapExpired(ctx, 10) }()
			}
			close(start)
			wg.Wait()

			got, _ := st.GetJob(ctx, j.ID)
			n, _ := effects(t, st, j.ID)
			wins[got.Status]++
			switch got.Status {
			case StatusSucceeded:
				if !completed || n != 1 || !errors.Is(cancelErr, ErrAlreadyTerminal) {
					t.Fatalf("job %d succeeded: completed=%v effects=%d cancelErr=%v", j.ID, completed, n, cancelErr)
				}
			case StatusCancelled:
				if completed || n != 0 || cancelErr != nil {
					t.Fatalf("job %d cancelled: completed=%v effects=%d cancelErr=%v", j.ID, completed, n, cancelErr)
				}
			default:
				t.Fatalf("job %d ended %s", j.ID, got.Status)
			}
		}
		t.Logf("outcomes over 300 races: %v", wins)
		if wins[StatusSucceeded] == 0 || wins[StatusCancelled] == 0 {
			t.Logf("WARNING: one side never won; the race may not have been exercised")
		}
	})
}

// Deterministic interleaving of the same race: hold one transition's row lock, start the
// other, then commit. The second must re-check its WHERE against the committed row
// (READ COMMITTED) or get 40001 and retry (stricter levels) — never overwrite.
func TestCancelCompleteInterleavings(t *testing.T) {
	forEachIsolation(t, func(t *testing.T, st *Store) {
		ctx := context.Background()

		// complete holds the lock, cancel arrives second
		j := mustCreate(t, st, "il-1")
		c := mustClaim(t, st, "W")
		tx, _ := st.Pool.Begin(ctx)
		if tag, err := tx.Exec(ctx, CompleteSQL, c.JobID, c.Attempt, "W"); err != nil || tag.RowsAffected() != 1 {
			t.Fatal(err)
		}
		cancelDone := make(chan error, 1)
		go func() { _, err := st.CancelJob(ctx, j.ID); cancelDone <- err }()
		select {
		case err := <-cancelDone:
			t.Fatalf("cancel did not wait for the row lock: %v", err)
		case <-time.After(200 * time.Millisecond):
		}
		tx.Commit(ctx)
		if err := <-cancelDone; !errors.Is(err, ErrAlreadyTerminal) {
			t.Fatalf("cancel after complete: %v, want ErrAlreadyTerminal", err)
		}

		// cancel holds the lock, complete arrives second
		j = mustCreate(t, st, "il-2")
		c = mustClaim(t, st, "W")
		tx, _ = st.Pool.Begin(ctx)
		tx.Exec(ctx, `UPDATE jobs SET status='cancelled', lease_until=NULL, finished_at=now() WHERE id=$1`, j.ID)
		completeDone := make(chan bool, 1)
		go func() { ok, _ := st.Complete(ctx, c, "W"); completeDone <- ok }()
		time.Sleep(200 * time.Millisecond)
		tx.Commit(ctx)
		if <-completeDone {
			t.Fatal("complete overwrote a committed cancel")
		}
		if n, _ := effects(t, st, j.ID); n != 0 {
			t.Fatalf("cancelled job has %d effects", n)
		}
	})
}

// ---------------------------------------------------------------------------
// 4. Duplicate on request retry: 8 concurrent creates per key, 300 keys.

func TestIdempotentCreateUnderConcurrency(t *testing.T) {
	forEachIsolation(t, func(t *testing.T, st *Store) {
		ctx := context.Background()
		type res struct {
			id      int64
			created bool
			err     error
		}
		for k := 0; k < 300; k++ {
			key := fmt.Sprintf("dup%d", k)
			out := make(chan res, 8)
			start := make(chan struct{})
			for i := 0; i < 8; i++ {
				go func() {
					<-start
					j, created, err := st.CreateJob(ctx, key, []byte(`{"a":1,"b":2}`), 5)
					out <- res{j.ID, created, err}
				}()
			}
			close(start)
			created, ids := 0, map[int64]bool{}
			for i := 0; i < 8; i++ {
				r := <-out
				if r.err != nil {
					t.Fatalf("key %s: %v", key, r.err)
				}
				ids[r.id] = true
				if r.created {
					created++
				}
			}
			if created != 1 || len(ids) != 1 {
				t.Fatalf("key %s: created=%d ids=%v", key, created, ids)
			}
		}
		var n int
		st.Pool.QueryRow(ctx, `SELECT count(*) FROM jobs`).Scan(&n)
		if n != 300 {
			t.Fatalf("%d rows, want 300", n)
		}
		// same payload, different key order: a replay, not a conflict (jsonb equality)
		if _, created, err := st.CreateJob(ctx, "dup0", []byte(`{"b":2,"a":1}`), 5); err != nil || created {
			t.Fatalf("reordered payload: created=%v err=%v", created, err)
		}
		if _, _, err := st.CreateJob(ctx, "dup0", []byte(`{"a":1,"b":3}`), 5); !errors.Is(err, ErrIdempotencyReuse) {
			t.Fatalf("different payload: %v, want ErrIdempotencyReuse", err)
		}
	})
}

// ---------------------------------------------------------------------------
// 5. A worker frozen INSIDE an open transaction. Reproduces why Complete is a single
//    statement, and shows the idle_in_transaction_session_timeout backstop works.

func TestFrozenTransactionHoldsRowLock(t *testing.T) {
	ctx := context.Background()
	t.Run("without_timeout", func(t *testing.T) {
		st := setup(t, "", "&idle_in_transaction_session_timeout=0")
		j := mustCreate(t, st, "frozen")
		c := mustClaim(t, st, "W")
		tx, _ := st.Pool.Begin(ctx)
		defer tx.Rollback(ctx)
		// the old two-statement Complete, frozen between UPDATE and COMMIT
		tx.Exec(ctx, `UPDATE jobs SET status='succeeded', lease_until=NULL, finished_at=now()
			WHERE id=$1 AND attempt=$2 AND status='running'`, c.JobID, c.Attempt)
		expireLease2 := func() error {
			cctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
			defer cancel()
			_, err := st.Pool.Exec(cctx, `UPDATE jobs SET lease_until = now() - interval '1s' WHERE id=$1`, j.ID)
			return err
		}
		if err := expireLease2(); err == nil {
			t.Fatal("expected the row to be locked")
		}
		if rq, f, _ := st.ReapExpired(ctx, 10); rq+f != 0 {
			t.Fatal("reaper should skip a locked row")
		}
		cctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		if _, err := st.CancelJob(cctx, j.ID); err == nil {
			t.Fatal("cancel should be blocked by the frozen transaction")
		}
		t.Log("confirmed: a frozen open transaction blocks cancel and is invisible to the reaper")
	})
	t.Run("with_timeout", func(t *testing.T) {
		st := setup(t, "", "&idle_in_transaction_session_timeout=500")
		j := mustCreate(t, st, "frozen2")
		c := mustClaim(t, st, "W")
		tx, _ := st.Pool.Begin(ctx)
		tx.Exec(ctx, `UPDATE jobs SET status='succeeded', lease_until=NULL, finished_at=now()
			WHERE id=$1 AND attempt=$2 AND status='running'`, c.JobID, c.Attempt)
		time.Sleep(1500 * time.Millisecond) // frozen past the timeout
		if err := tx.Commit(ctx); err == nil {
			t.Fatal("frozen transaction should have been terminated by the server")
		}
		got, err := st.CancelJob(ctx, j.ID)
		if err != nil || got.Status != StatusCancelled {
			t.Fatalf("cancel after timeout: %v %v", got.Status, err)
		}
	})
}

// ---------------------------------------------------------------------------
// 6. Clock skew: every lease timestamp is produced and compared by the database clock.

func TestLeaseUsesOnlyDatabaseClock(t *testing.T) {
	src, err := os.ReadFile("store.go")
	if err != nil {
		t.Fatal(err)
	}
	// time.Now / time.Since in the store would mean a worker clock leaks into SQL.
	for _, bad := range []string{"time.Now(", "time.Since(", "time.Until("} {
		if strings.Contains(string(src), bad) {
			t.Errorf("store.go uses %s: lease logic must use the database clock only", bad)
		}
	}
	st := setup(t, "read committed")
	ctx := context.Background()
	mustCreate(t, st, "clock")
	mustClaim(t, st, "W")
	var remaining float64
	st.Pool.QueryRow(ctx, `SELECT extract(epoch FROM lease_until - now()) FROM jobs`).Scan(&remaining)
	if remaining < lease.Seconds()-1 || remaining > lease.Seconds() {
		t.Fatalf("lease remaining by DB clock = %.2fs, want ~%v", remaining, lease)
	}
}
