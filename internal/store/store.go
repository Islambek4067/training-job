// Package store holds every SQL statement of the queue. All state transitions are
// single conditional UPDATEs (compare-and-set) evaluated against the database clock.
package store

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaSQL string

const (
	StatusQueued    = "queued"
	StatusRunning   = "running"
	StatusSucceeded = "succeeded"
	StatusFailed    = "failed"
	StatusCancelled = "cancelled"
)

var (
	ErrNotFound         = errors.New("job not found")
	ErrIdempotencyReuse = errors.New("idempotency key reused with a different payload")
	ErrAlreadyTerminal  = errors.New("job already finished")
)

type Job struct {
	ID             int64           `json:"id"`
	IdempotencyKey string          `json:"idempotency_key"`
	Payload        json.RawMessage `json:"payload"`
	Status         string          `json:"status"`
	Attempt        int             `json:"attempt"`
	MaxAttempts    int             `json:"max_attempts"`
	WorkerID       *string         `json:"worker_id"`
	LeaseUntil     *time.Time      `json:"lease_until"`
	LastError      *string         `json:"last_error"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	StartedAt      *time.Time      `json:"started_at"`
	FinishedAt     *time.Time      `json:"finished_at"`
}

const jobCols = `id, idempotency_key, payload, status, attempt, max_attempts, worker_id,
	lease_until, last_error, created_at, updated_at, started_at, finished_at`

func scanJob(row pgx.Row) (Job, error) {
	var j Job
	err := row.Scan(&j.ID, &j.IdempotencyKey, &j.Payload, &j.Status, &j.Attempt, &j.MaxAttempts,
		&j.WorkerID, &j.LeaseUntil, &j.LastError, &j.CreatedAt, &j.UpdatedAt, &j.StartedAt, &j.FinishedAt)
	return j, err
}

type Store struct {
	Pool *pgxpool.Pool
}

// Open connects to Postgres. isolation, if non-empty, sets default_transaction_isolation
// for every connection (used to run the race tests under stricter levels).
func Open(ctx context.Context, url, isolation string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	if isolation != "" {
		cfg.ConnConfig.RuntimeParams["default_transaction_isolation"] = isolation
	}
	// Defence in depth: no code path holds a transaction open across a client round-trip,
	// but if one ever does and the client freezes or is partitioned, Postgres would keep the
	// row lock (reaper skips it, cancel blocks) until TCP gives up. Bound that.
	if _, ok := cfg.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"]; !ok {
		cfg.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "5000"
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Store{Pool: pool}, nil
}

func (s *Store) Close() { s.Pool.Close() }

// Migrate applies the schema under an advisory lock so concurrent starters don't race.
func (s *Store) Migrate(ctx context.Context) error {
	conn, err := s.Pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(7710001)`); err != nil {
		return err
	}
	defer conn.Exec(context.Background(), `SELECT pg_advisory_unlock(7710001)`)
	_, err = conn.Exec(ctx, schemaSQL)
	return err
}

// retry re-runs fn on serialization failures and deadlocks. Under READ COMMITTED these
// should not occur for our statements; under REPEATABLE READ / SERIALIZABLE they are the
// normal way a lost race is reported.
func retry[T any](ctx context.Context, fn func() (T, error)) (T, error) {
	var zero T
	for i := 0; ; i++ {
		v, err := fn()
		var pgErr *pgconn.PgError
		if err != nil && errors.As(err, &pgErr) &&
			(pgErr.Code == "40001" || pgErr.Code == "40P01") && i < 20 {
			select {
			case <-ctx.Done():
				return zero, ctx.Err()
			// exponential backoff with full jitter, capped: linear backoff without jitter
			// kept 32 claimers in lockstep under SERIALIZABLE (results/tests/01-FAIL-*).
			case <-time.After(time.Duration(rand.Int64N(int64(min(2<<i, 200)) * int64(time.Millisecond)))):
			}
			continue
		}
		return v, err
	}
}

// ---------- API side ----------

// CreateJob is idempotent on key. created=false means an existing job was returned.
func (s *Store) CreateJob(ctx context.Context, key string, payload []byte, maxAttempts int) (Job, bool, error) {
	type res struct {
		j       Job
		created bool
	}
	r, err := retry(ctx, func() (res, error) {
		j, err := scanJob(s.Pool.QueryRow(ctx, `
			INSERT INTO jobs (idempotency_key, payload, max_attempts) VALUES ($1, $2, $3)
			ON CONFLICT (idempotency_key) DO NOTHING
			RETURNING `+jobCols, key, payload, maxAttempts))
		if err == nil {
			return res{j, true}, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return res{}, err
		}
		// Conflict: the row exists (committed, since ON CONFLICT waited for it).
		var same bool
		row := s.Pool.QueryRow(ctx, `SELECT `+jobCols+`, payload = $2::jsonb
			FROM jobs WHERE idempotency_key = $1`, key, payload)
		err = row.Scan(&j.ID, &j.IdempotencyKey, &j.Payload, &j.Status, &j.Attempt, &j.MaxAttempts,
			&j.WorkerID, &j.LeaseUntil, &j.LastError, &j.CreatedAt, &j.UpdatedAt, &j.StartedAt,
			&j.FinishedAt, &same)
		if err != nil {
			return res{}, err
		}
		if !same {
			return res{j, false}, ErrIdempotencyReuse
		}
		return res{j, false}, nil
	})
	return r.j, r.created, err
}

func (s *Store) GetJob(ctx context.Context, id int64) (Job, error) {
	j, err := scanJob(s.Pool.QueryRow(ctx, `SELECT `+jobCols+` FROM jobs WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return j, ErrNotFound
	}
	return j, err
}

// ListJobs uses keyset pagination (newest first): stable under concurrent inserts,
// unlike OFFSET. before=0 means "from the newest".
func (s *Store) ListJobs(ctx context.Context, status string, before int64, limit int) ([]Job, int64, error) {
	rows, err := s.Pool.Query(ctx, `SELECT `+jobCols+` FROM jobs
		WHERE ($1 = '' OR status = $1) AND ($2 = 0 OR id < $2)
		ORDER BY id DESC LIMIT $3`, status, before, limit+1)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	jobs := make([]Job, 0, limit)
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, 0, err
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	var next int64
	if len(jobs) > limit {
		jobs = jobs[:limit]
		next = jobs[limit-1].ID
	}
	return jobs, next, nil
}

// CancelJob moves queued/running -> cancelled. It races with Complete on the row lock;
// exactly one of them commits. Cancelling an already-cancelled job is a no-op success.
func (s *Store) CancelJob(ctx context.Context, id int64) (Job, error) {
	return retry(ctx, func() (Job, error) {
		j, err := scanJob(s.Pool.QueryRow(ctx, `
			UPDATE jobs SET status = 'cancelled', lease_until = NULL,
			       finished_at = now(), updated_at = now()
			WHERE id = $1 AND status IN ('queued', 'running')
			RETURNING `+jobCols, id))
		if err == nil {
			return j, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return j, err
		}
		j, err = s.GetJob(ctx, id)
		if err != nil {
			return j, err
		}
		if j.Status == StatusCancelled {
			return j, nil
		}
		return j, ErrAlreadyTerminal
	})
}

// ---------- worker side ----------

type Claim struct {
	JobID   int64
	Attempt int
	Payload json.RawMessage
}

func secs(d time.Duration) float64 { return d.Seconds() }

// ClaimSQL takes the oldest queued job. Exported so the mutation script can find it.
const ClaimSQL = `
	WITH c AS (
		SELECT id FROM jobs WHERE status = 'queued'
		ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED
	)
	UPDATE jobs j SET status = 'running', attempt = j.attempt + 1, worker_id = $1,
	       lease_until = now() + make_interval(secs => $2),
	       started_at = now(), updated_at = now()
	FROM c WHERE j.id = c.id AND j.status = 'queued'
	RETURNING j.id, j.attempt, j.payload`

// Claim returns nil, nil when the queue is empty.
func (s *Store) Claim(ctx context.Context, workerID string, lease time.Duration) (*Claim, error) {
	return retry(ctx, func() (*Claim, error) {
		var c Claim
		err := s.Pool.QueryRow(ctx, ClaimSQL, workerID, secs(lease)).
			Scan(&c.JobID, &c.Attempt, &c.Payload)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		return &c, nil
	})
}

// RecordRun logs that execution really started (the at-least-once part).
func (s *Store) RecordRun(ctx context.Context, c *Claim, workerID string) error {
	_, err := s.Pool.Exec(ctx, `INSERT INTO job_runs (job_id, attempt, worker_id) VALUES ($1, $2, $3)`,
		c.JobID, c.Attempt, workerID)
	return err
}

// Heartbeat extends the lease. false means we no longer own the job (reaped or cancelled).
func (s *Store) Heartbeat(ctx context.Context, c *Claim, lease time.Duration) (bool, error) {
	return retry(ctx, func() (bool, error) {
		tag, err := s.Pool.Exec(ctx, `
			UPDATE jobs SET lease_until = now() + make_interval(secs => $3), updated_at = now()
			WHERE id = $1 AND attempt = $2 AND status = 'running'`, c.JobID, c.Attempt, secs(lease))
		return tag.RowsAffected() == 1, err
	})
}

// CompleteSQL commits running -> succeeded together with the side effect, fenced by attempt.
// It is ONE statement on purpose: a multi-statement transaction would let a worker freeze
// (GC, SIGSTOP, network partition) between UPDATE and COMMIT while holding the row lock.
// Exported so the mutation script can find it.
const CompleteSQL = `
	WITH done AS (
		UPDATE jobs SET status = 'succeeded', lease_until = NULL,
		       finished_at = now(), updated_at = now(), last_error = NULL
		WHERE id = $1 AND attempt = $2 AND status = 'running'
		RETURNING id, attempt
	)
	INSERT INTO job_effects (job_id, attempt, worker_id)
	SELECT id, $2::int, $3 FROM done`

// Note: the effect records the attempt the WRITER claimed ($2), not done.attempt. With
// done.attempt the harness's "effect attempt == final attempt" check was a tautology and a
// mutant without fencing passed it (results/mutations-v2-*).

// Complete returns false when the result was stale (job reaped, reclaimed or cancelled);
// in that case nothing was written.
func (s *Store) Complete(ctx context.Context, c *Claim, workerID string) (bool, error) {
	return retry(ctx, func() (bool, error) {
		tag, err := s.Pool.Exec(ctx, CompleteSQL, c.JobID, c.Attempt, workerID)
		return tag.RowsAffected() == 1, err
	})
}

// Fail records an application-level failure (terminal, no retry), fenced by attempt.
func (s *Store) Fail(ctx context.Context, c *Claim, msg string) (bool, error) {
	return retry(ctx, func() (bool, error) {
		tag, err := s.Pool.Exec(ctx, `
			UPDATE jobs SET status = 'failed', lease_until = NULL, last_error = $3,
			       finished_at = now(), updated_at = now()
			WHERE id = $1 AND attempt = $2 AND status = 'running'`, c.JobID, c.Attempt, msg)
		return tag.RowsAffected() == 1, err
	})
}

// ReapExpired returns jobs with an expired lease to the queue, or fails them once they
// have used all attempts. Safe to run from every worker concurrently.
func (s *Store) ReapExpired(ctx context.Context, limit int) (requeued, failed int, err error) {
	type res struct{ r, f int }
	out, err := retry(ctx, func() (res, error) {
		rows, err := s.Pool.Query(ctx, `
			WITH expired AS (
				SELECT id FROM jobs
				WHERE status = 'running' AND lease_until < now()
				ORDER BY lease_until LIMIT $1 FOR UPDATE SKIP LOCKED
			)
			UPDATE jobs j SET
			    status      = CASE WHEN j.attempt >= j.max_attempts THEN 'failed' ELSE 'queued' END,
			    finished_at = CASE WHEN j.attempt >= j.max_attempts THEN now() END,
			    last_error  = format('lease expired: worker=%s attempt=%s', j.worker_id, j.attempt),
			    lease_until = NULL, updated_at = now()
			FROM expired
			WHERE j.id = expired.id AND j.status = 'running' AND j.lease_until < now()
			RETURNING j.status`, limit)
		if err != nil {
			return res{}, err
		}
		defer rows.Close()
		var r res
		for rows.Next() {
			var st string
			if err := rows.Scan(&st); err != nil {
				return res{}, err
			}
			if st == StatusFailed {
				r.f++
			} else {
				r.r++
			}
		}
		return r, rows.Err()
	})
	return out.r, out.f, err
}

// WaitReady blocks until the database answers and the schema exists.
func (s *Store) WaitReady(ctx context.Context) error {
	for {
		var ok bool
		err := s.Pool.QueryRow(ctx, `SELECT to_regclass('public.job_effects') IS NOT NULL`).Scan(&ok)
		if err == nil && ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("database not ready: %v (last error: %w)", ctx.Err(), err)
		case <-time.After(500 * time.Millisecond):
		}
	}
}
