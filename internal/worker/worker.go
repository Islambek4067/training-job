// Package worker claims jobs, heartbeats while executing them, and commits the result
// fenced by the attempt number it claimed. It also runs the lease reaper.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"soupq/internal/store"
)

type Config struct {
	ID           string
	Concurrency  int
	Lease        time.Duration
	Heartbeat    time.Duration
	Poll         time.Duration
	ReapInterval time.Duration
}

type Worker struct {
	Store *store.Store
	Cfg   Config
	Log   *slog.Logger
}

// Payload is the emulated training job.
type Payload struct {
	DurationMS int  `json:"duration_ms"`
	Fail       bool `json:"fail"`
}

// Run blocks until ctx is cancelled. In-flight jobs are abandoned on shutdown; their
// leases expire and they are retried elsewhere (same path as a crash).
func (w *Worker) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); w.reapLoop(ctx) }()
	for i := 0; i < w.Cfg.Concurrency; i++ {
		slot := fmt.Sprintf("%s/%d", w.Cfg.ID, i)
		wg.Add(1)
		go func() { defer wg.Done(); w.slotLoop(ctx, slot) }()
	}
	wg.Wait()
}

func (w *Worker) reapLoop(ctx context.Context) {
	t := time.NewTicker(w.Cfg.ReapInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		requeued, failed, err := w.Store.ReapExpired(ctx, 500)
		if err != nil {
			if ctx.Err() == nil {
				w.Log.Error("reap failed", "err", err)
			}
			continue
		}
		if requeued+failed > 0 {
			w.Log.Info("reaped", "event", "reaped", "requeued", requeued, "failed", failed)
		}
	}
}

func (w *Worker) slotLoop(ctx context.Context, slot string) {
	for ctx.Err() == nil {
		c, err := w.Store.Claim(ctx, slot, w.Cfg.Lease)
		if err != nil {
			if ctx.Err() == nil {
				w.Log.Error("claim failed", "err", err)
			}
			sleep(ctx, w.Cfg.Poll)
			continue
		}
		if c == nil {
			// jitter so idle workers don't poll in lockstep
			sleep(ctx, w.Cfg.Poll/2+rand.N(w.Cfg.Poll))
			continue
		}
		w.handle(ctx, slot, c)
	}
}

func (w *Worker) handle(ctx context.Context, slot string, c *store.Claim) {
	log := w.Log.With("job", c.JobID, "attempt", c.Attempt, "slot", slot)
	if err := w.Store.RecordRun(ctx, c, slot); err != nil {
		log.Error("record run failed", "err", err)
		return // lease will expire and the job will be retried
	}

	runCtx, stopRun := context.WithCancel(ctx)
	defer stopRun()
	var lost bool
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		t := time.NewTicker(w.Cfg.Heartbeat)
		defer t.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-t.C:
			}
			ok, err := w.Store.Heartbeat(runCtx, c, w.Cfg.Lease)
			if err != nil {
				// Keep working: the lease is still valid for a while, and the fenced
				// commit will reject us if we really lost the job.
				if runCtx.Err() == nil {
					log.Warn("heartbeat error", "err", err)
				}
				continue
			}
			if !ok {
				lost = true
				stopRun()
				return
			}
		}
	}()

	execErr := execute(runCtx, c.Payload)
	stopRun()
	<-hbDone // after this, `lost` is safe to read

	if lost {
		log.Info("lost ownership (cancelled or reaped); dropping result", "event", "lost_ownership")
		return
	}
	if ctx.Err() != nil {
		return // shutting down; lease will expire
	}
	// Use a fresh context: the commit must not be half-cancelled by a shutdown signal.
	commitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var ok bool
	var err error
	if execErr != nil {
		ok, err = w.Store.Fail(commitCtx, c, execErr.Error())
	} else {
		ok, err = w.Store.Complete(commitCtx, c, slot)
	}
	switch {
	case err != nil:
		log.Error("commit failed", "err", err)
	case !ok:
		// The zombie case: we finished, but someone else owns (or cancelled) the job now.
		log.Warn("stale result rejected by fencing", "event", "stale_commit_rejected")
	case execErr != nil:
		log.Info("job failed", "event", "failed", "err", execErr)
	default:
		log.Debug("job succeeded", "event", "succeeded")
	}
}

var errSimulated = errors.New("simulated training failure")

func execute(ctx context.Context, raw json.RawMessage) error {
	var p Payload
	if err := json.Unmarshal(raw, &p); err != nil {
		return fmt.Errorf("bad payload: %w", err)
	}
	if !sleep(ctx, time.Duration(p.DurationMS)*time.Millisecond) {
		return ctx.Err()
	}
	if p.Fail {
		return errSimulated
	}
	return nil
}

func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
