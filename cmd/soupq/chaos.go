package main

// The chaos harness. It owns the worker processes (so kill -9 / SIGSTOP are real signals
// on real processes), drives the API over HTTP like a misbehaving client, and at the end
// checks the invariants directly against the database.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"soupq/internal/store"
)

type chaosCfg struct {
	api, outDir, workerBin       string
	jobs, workers, rate, clients int
	killEvery, pauseEvery        time.Duration
	pauseFor, clockJumpEvery     time.Duration
	dupRate, cancelRate          float64
	failRate, mismatchRate       float64
	minMS, maxMS, longMS         int
	longRate                     float64
	drainTimeout                 time.Duration
	reset                        bool
	seed                         uint64
}

// What the client observed for one idempotency key.
type keyRec struct {
	key      string
	idx      int
	fail     bool
	payload  []byte
	mu       sync.Mutex
	ids      map[int64]int // job id -> number of 2xx responses carrying it
	created  int           // number of 201s
	posts    int
	gaveUp   bool
	cancels  []cancelObs
	mismatch []int // status codes of "same key, different payload" probes
}

type cancelObs struct {
	code   int
	status string // job status in the response body
}

type chaosRun struct {
	cfg    chaosCfg
	runID  string
	dir    string
	log    *slog.Logger
	st     *store.Store
	client *http.Client

	keys []*keyRec

	procMu  sync.Mutex
	procs   []*proc
	paused  map[int]bool
	logMu   sync.Mutex
	logFile *os.File

	kills, pauses, clockJumps, respawns atomic.Int64
	jumpExpired                         atomic.Int64
	posts, postRetries, postErrors      atomic.Int64
	events                              sync.Map // event name -> *atomic.Int64

	latMu sync.Mutex
	lat   []time.Duration
}

type proc struct {
	slot, gen int
	cmd       *exec.Cmd
	done      chan struct{}
}

func runChaos(ctx context.Context, log *slog.Logger, args []string) error {
	var c chaosCfg
	fs := flag.NewFlagSet("chaos", flag.ExitOnError)
	fs.StringVar(&c.api, "api", env("API_URL", "http://localhost:8080"), "API base URL")
	fs.StringVar(&c.outDir, "out", env("RESULTS_DIR", "results"), "directory for raw output")
	fs.StringVar(&c.workerBin, "worker-bin", "", "worker binary (default: this executable)")
	fs.IntVar(&c.jobs, "jobs", 10000, "number of distinct jobs (idempotency keys)")
	fs.IntVar(&c.workers, "workers", 8, "worker processes")
	fs.IntVar(&c.rate, "rate", 100, "new-job submissions per second (0 = unlimited)")
	fs.IntVar(&c.clients, "clients", 32, "concurrent HTTP submitters")
	fs.DurationVar(&c.killEvery, "kill-every", 2*time.Second, "mean interval between kill -9 (0 = off)")
	fs.DurationVar(&c.pauseEvery, "pause-every", 5*time.Second, "mean interval between SIGSTOP of a worker (0 = off)")
	fs.DurationVar(&c.pauseFor, "pause-for", 8*time.Second, "how long a paused worker stays frozen (> lease => zombie)")
	fs.DurationVar(&c.clockJumpEvery, "clock-jump-every", 0, "mean interval between forcing every running lease to expire (0 = off)")
	fs.Float64Var(&c.dupRate, "dup-rate", 0.3, "fraction of keys POSTed several times concurrently + replayed later")
	fs.Float64Var(&c.cancelRate, "cancel-rate", 0.1, "fraction of jobs cancelled at a random moment")
	fs.Float64Var(&c.failRate, "fail-rate", 0.02, "fraction of jobs whose execution fails")
	fs.Float64Var(&c.mismatchRate, "mismatch-rate", 0.01, "fraction of keys re-POSTed with a different payload (expect 409)")
	fs.IntVar(&c.minMS, "min-ms", 20, "min job duration")
	fs.IntVar(&c.maxMS, "max-ms", 200, "max job duration")
	// Long jobs matter: with only short jobs, a zombie always wakes after the new owner has
	// FINISHED, so the status check rejects it and attempt-fencing is never exercised
	// (a mutant without fencing passed 2000 jobs — results/mutations/).
	fs.Float64Var(&c.longRate, "long-rate", 0.02, "fraction of long jobs")
	fs.IntVar(&c.longMS, "long-ms", 4000, "duration of a long job (pause-for - lease < long-ms => new owner still running when the zombie wakes)")
	fs.DurationVar(&c.drainTimeout, "drain-timeout", 10*time.Minute, "max wait for the queue to drain")
	fs.BoolVar(&c.reset, "reset", true, "truncate tables before the run")
	fs.Uint64Var(&c.seed, "seed", 0, "random seed (0 = time based)")
	fs.Parse(args)
	if c.seed == 0 {
		c.seed = uint64(time.Now().UnixNano())
	}
	if c.workerBin == "" {
		exe, err := os.Executable()
		if err != nil {
			return err
		}
		c.workerBin = exe
	}

	r := &chaosRun{
		cfg:    c,
		runID:  time.Now().UTC().Format("20060102T150405Z"),
		log:    log,
		paused: map[int]bool{},
		client: &http.Client{
			Timeout:   5 * time.Second,
			Transport: &http.Transport{MaxIdleConnsPerHost: 256, MaxConnsPerHost: 256},
		},
	}
	r.dir = filepath.Join(c.outDir, "run-"+r.runID)
	if err := os.MkdirAll(r.dir, 0o755); err != nil {
		return err
	}
	f, err := os.Create(filepath.Join(r.dir, "workers.log"))
	if err != nil {
		return err
	}
	defer f.Close()
	r.logFile = f

	st, err := openStore(ctx)
	if err != nil {
		return err
	}
	defer st.Close()
	r.st = st
	if err := st.WaitReady(ctx); err != nil {
		return err
	}
	return r.run(ctx)
}

func (r *chaosRun) run(ctx context.Context) error {
	c := r.cfg
	rng := rand.New(rand.NewPCG(c.seed, 1))
	if c.reset {
		if _, err := r.st.Pool.Exec(ctx, `TRUNCATE job_effects, job_runs, jobs RESTART IDENTITY`); err != nil {
			return err
		}
	}
	for i := 0; i < c.jobs; i++ {
		k := &keyRec{key: fmt.Sprintf("%s-%06d", r.runID, i), idx: i, ids: map[int64]int{}}
		k.fail = rng.Float64() < c.failRate
		d := c.minMS + rng.IntN(c.maxMS-c.minMS+1)
		if rng.Float64() < c.longRate {
			d = c.longMS
		}
		k.payload, _ = json.Marshal(map[string]any{"payload": map[string]any{"i": i, "duration_ms": d, "fail": k.fail}})
		r.keys = append(r.keys, k)
	}

	start := time.Now()
	r.say("run %s seed=%d jobs=%d workers=%d rate=%d/s kill-every=%v pause-every=%v pause-for=%v clock-jump-every=%v dup=%.2f cancel=%.2f fail=%.2f long=%.2fx%dms isolation=%q",
		r.runID, c.seed, c.jobs, c.workers, c.rate, c.killEvery, c.pauseEvery, c.pauseFor, c.clockJumpEvery,
		c.dupRate, c.cancelRate, c.failRate, c.longRate, c.longMS, os.Getenv("PG_ISOLATION"))

	for i := 0; i < c.workers; i++ {
		if err := r.spawn(i, 0); err != nil {
			return err
		}
	}
	defer r.stopAll()

	chaosCtx, stopChaos := context.WithCancel(ctx)
	var chaosWG sync.WaitGroup
	if c.killEvery > 0 {
		chaosWG.Add(1)
		go func() { defer chaosWG.Done(); r.killer(chaosCtx, rand.New(rand.NewPCG(c.seed, 2))) }()
	}
	if c.pauseEvery > 0 {
		chaosWG.Add(1)
		go func() { defer chaosWG.Done(); r.pauser(chaosCtx, rand.New(rand.NewPCG(c.seed, 3))) }()
	}
	if c.clockJumpEvery > 0 {
		chaosWG.Add(1)
		go func() { defer chaosWG.Done(); r.clockJumper(chaosCtx, rand.New(rand.NewPCG(c.seed, 4))) }()
	}
	go r.progress(chaosCtx, start)

	// ---- client load: submissions, duplicates, cancels, mismatched replays ----
	var clientWG sync.WaitGroup
	tokens := make(chan struct{})
	go func() {
		defer close(tokens)
		var tick <-chan time.Time
		if c.rate > 0 {
			t := time.NewTicker(time.Second / time.Duration(c.rate))
			defer t.Stop()
			tick = t.C
		}
		for range r.keys {
			if tick != nil {
				select {
				case <-tick:
				case <-ctx.Done():
					return
				}
			}
			select {
			case tokens <- struct{}{}:
			case <-ctx.Done():
				return
			}
		}
	}()
	next := atomic.Int64{}
	var submitWG sync.WaitGroup
	for w := 0; w < c.clients; w++ {
		submitWG.Add(1)
		wrng := rand.New(rand.NewPCG(c.seed, uint64(100+w)))
		go func() {
			defer submitWG.Done()
			for range tokens {
				k := r.keys[next.Add(1)-1]
				r.submit(ctx, wrng, k, &clientWG)
			}
		}()
	}
	submitWG.Wait()
	clientWG.Wait()
	r.say("client load finished after %v; waiting for drain", time.Since(start).Round(time.Millisecond))

	// ---- drain: chaos keeps going until nothing is queued or running ----
	drainErr := r.waitDrain(ctx, c.drainTimeout)
	stopChaos()
	chaosWG.Wait()
	r.contAll() // never leave a worker frozen
	if drainErr == nil {
		// Grace period: anything a zombie or late reaper could still flip shows up here.
		lease := time.Duration(envInt("LEASE_MS", 5000)) * time.Millisecond
		time.Sleep(2*lease + time.Second)
		drainErr = r.waitDrain(ctx, 30*time.Second)
	}
	r.stopAll()
	elapsed := time.Since(start)

	return r.verify(ctx, elapsed, drainErr)
}

// ---------- client side ----------

func (r *chaosRun) submit(ctx context.Context, rng *rand.Rand, k *keyRec, wg *sync.WaitGroup) {
	c := r.cfg
	dup := rng.Float64() < c.dupRate
	// The first POST(s). For "dup" keys we fire 2-4 identical POSTs at the same instant to
	// race the INSERT ... ON CONFLICT path.
	n := 1
	if dup {
		n = 2 + rng.IntN(3)
	}
	var barrier sync.WaitGroup
	var inner sync.WaitGroup
	barrier.Add(1)
	for i := 0; i < n; i++ {
		inner.Add(1)
		go func() { defer inner.Done(); barrier.Wait(); r.post(ctx, k, k.payload, true) }()
	}
	barrier.Done()
	inner.Wait()

	id := k.anyID()
	if id == 0 {
		return
	}
	if dup { // a late replay, possibly after the job finished
		wg.Add(1)
		delay := time.Duration(rng.IntN(20000)) * time.Millisecond
		go func() { defer wg.Done(); sleepCtx(ctx, delay); r.post(ctx, k, k.payload, true) }()
	}
	if rng.Float64() < c.mismatchRate {
		wg.Add(1)
		go func() {
			defer wg.Done()
			other, _ := json.Marshal(map[string]any{"payload": map[string]any{"i": k.idx, "duration_ms": 1, "different": true}})
			code := r.post(ctx, k, other, false)
			k.mu.Lock()
			k.mismatch = append(k.mismatch, code)
			k.mu.Unlock()
		}()
	}
	if rng.Float64() < c.cancelRate {
		twice := rng.Float64() < 0.2
		delay := time.Duration(rng.IntN(2500)) * time.Millisecond
		wg.Add(1)
		go func() {
			defer wg.Done()
			sleepCtx(ctx, delay)
			if twice {
				var w sync.WaitGroup
				for i := 0; i < 2; i++ {
					w.Add(1)
					go func() { defer w.Done(); r.cancel(ctx, k, id) }()
				}
				w.Wait()
			} else {
				r.cancel(ctx, k, id)
			}
		}()
	}
}

func (k *keyRec) anyID() int64 {
	k.mu.Lock()
	defer k.mu.Unlock()
	for id := range k.ids {
		return id
	}
	return 0
}

// post sends one create request, retrying (with the same key) on transport errors and 5xx,
// exactly like a real client would. record=false is used for the mismatch probe.
func (r *chaosRun) post(ctx context.Context, k *keyRec, body []byte, record bool) int {
	for attempt := 0; attempt < 50; attempt++ {
		if attempt > 0 {
			r.postRetries.Add(1)
			sleepCtx(ctx, time.Duration(50*attempt)*time.Millisecond)
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, r.cfg.api+"/jobs", bytes.NewReader(body))
		req.Header.Set("Idempotency-Key", k.key)
		req.Header.Set("Content-Type", "application/json")
		t0 := time.Now()
		resp, err := r.client.Do(req)
		r.posts.Add(1)
		if err != nil {
			r.postErrors.Add(1)
			if ctx.Err() != nil {
				return 0
			}
			continue
		}
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		r.recordLatency(time.Since(t0))
		if resp.StatusCode >= 500 {
			r.postErrors.Add(1)
			continue
		}
		if record {
			k.mu.Lock()
			k.posts++
			if resp.StatusCode == 201 || resp.StatusCode == 200 {
				var j store.Job
				if json.Unmarshal(data, &j) == nil {
					k.ids[j.ID]++
				}
				if resp.StatusCode == 201 {
					k.created++
				}
			}
			k.mu.Unlock()
		}
		return resp.StatusCode
	}
	k.mu.Lock()
	k.gaveUp = true
	k.mu.Unlock()
	return 0
}

func (r *chaosRun) cancel(ctx context.Context, k *keyRec, id int64) {
	for attempt := 0; attempt < 20; attempt++ {
		if attempt > 0 {
			sleepCtx(ctx, time.Duration(50*attempt)*time.Millisecond)
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("%s/jobs/%d/cancel", r.cfg.api, id), nil)
		resp, err := r.client.Do(req)
		if err != nil {
			continue
		}
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode >= 500 {
			continue
		}
		var obs cancelObs
		obs.code = resp.StatusCode
		if resp.StatusCode == 200 {
			var j store.Job
			json.Unmarshal(data, &j)
			obs.status = j.Status
		} else if resp.StatusCode == 409 {
			var body struct {
				Job store.Job `json:"job"`
			}
			json.Unmarshal(data, &body)
			obs.status = body.Job.Status
		}
		k.mu.Lock()
		k.cancels = append(k.cancels, obs)
		k.mu.Unlock()
		return
	}
}

func (r *chaosRun) recordLatency(d time.Duration) {
	r.latMu.Lock()
	r.lat = append(r.lat, d)
	r.latMu.Unlock()
}

// ---------- process chaos ----------

func (r *chaosRun) spawn(slot, gen int) error {
	cmd := exec.Command(r.cfg.workerBin, "worker")
	id := fmt.Sprintf("w%d.g%d", slot, gen)
	cmd.Env = append(os.Environ(), "WORKER_ID="+id)
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL} // die with the harness
	out, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		return err
	}
	p := &proc{slot: slot, gen: gen, cmd: cmd, done: make(chan struct{})}
	go func() {
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			line := sc.Text()
			if i := strings.Index(line, "event="); i >= 0 {
				ev := strings.Fields(line[i+len("event="):])[0]
				v, _ := r.events.LoadOrStore(ev, new(atomic.Int64))
				v.(*atomic.Int64).Add(1)
			}
			r.logMu.Lock()
			fmt.Fprintf(r.logFile, "%s %s\n", id, line)
			r.logMu.Unlock()
		}
		cmd.Wait()
		close(p.done)
	}()
	r.procMu.Lock()
	if slot < len(r.procs) {
		r.procs[slot] = p
	} else {
		r.procs = append(r.procs, p)
	}
	r.procMu.Unlock()
	return nil
}

func jitter(rng *rand.Rand, mean time.Duration) time.Duration {
	return mean/2 + time.Duration(rng.Int64N(int64(mean)))
}

func (r *chaosRun) killer(ctx context.Context, rng *rand.Rand) {
	for sleepCtx(ctx, jitter(rng, r.cfg.killEvery)) {
		// Never kill a frozen worker: it must get the chance to come back as a zombie.
		// (The first version of this harness did, and the zombie scenario silently
		// never happened — see results/debug.)
		r.procMu.Lock()
		var live []*proc
		for i, p := range r.procs {
			if !r.paused[i] {
				live = append(live, p)
			}
		}
		if len(live) == 0 {
			r.procMu.Unlock()
			continue
		}
		p := live[rng.IntN(len(live))]
		r.procMu.Unlock()
		p.cmd.Process.Signal(syscall.SIGKILL) // kill -9, also works on a SIGSTOPped process
		<-p.done
		r.kills.Add(1)
		r.event("harness: kill -9 w%d.g%d (pid %d)", p.slot, p.gen, p.cmd.Process.Pid)
		if err := r.spawn(p.slot, p.gen+1); err != nil {
			r.log.Error("respawn failed", "err", err)
		}
		r.respawns.Add(1)
	}
}

func (r *chaosRun) pauser(ctx context.Context, rng *rand.Rand) {
	for sleepCtx(ctx, jitter(rng, r.cfg.pauseEvery)) {
		// Freeze a worker that holds a job right now; freezing an idle one tests nothing.
		owners := map[string]int64{}
		rows, err := r.st.Pool.Query(ctx, `SELECT worker_id, id FROM jobs WHERE status = 'running'`)
		if err != nil {
			continue
		}
		for rows.Next() {
			var w string
			var id int64
			rows.Scan(&w, &id)
			owners[strings.SplitN(w, "/", 2)[0]] = id
		}
		rows.Close()
		r.procMu.Lock()
		var cands []*proc
		for i, p := range r.procs {
			if _, busy := owners[fmt.Sprintf("w%d.g%d", p.slot, p.gen)]; busy && !r.paused[i] {
				cands = append(cands, p)
			}
		}
		if len(cands) == 0 {
			r.procMu.Unlock()
			continue
		}
		p := cands[rng.IntN(len(cands))]
		slot := p.slot
		r.paused[slot] = true
		r.procMu.Unlock()
		p.cmd.Process.Signal(syscall.SIGSTOP)
		r.pauses.Add(1)
		r.event("harness: SIGSTOP w%d.g%d (holds job %d) for %v", p.slot, p.gen,
			owners[fmt.Sprintf("w%d.g%d", p.slot, p.gen)], r.cfg.pauseFor)
		go func() {
			time.Sleep(r.cfg.pauseFor)
			p.cmd.Process.Signal(syscall.SIGCONT) // no-op if it was killed meanwhile
			r.event("harness: SIGCONT w%d.g%d", p.slot, p.gen)
			r.procMu.Lock()
			delete(r.paused, slot)
			r.procMu.Unlock()
		}()
	}
}

// clockJumper emulates the DB clock jumping forward past every lease (or every worker
// stalling at once): all running leases expire while their owners are alive and well.
func (r *chaosRun) clockJumper(ctx context.Context, rng *rand.Rand) {
	for sleepCtx(ctx, jitter(rng, r.cfg.clockJumpEvery)) {
		tag, err := r.st.Pool.Exec(ctx, `UPDATE jobs SET lease_until = now() - interval '1 second'
			WHERE status = 'running'`)
		if err != nil {
			continue
		}
		r.clockJumps.Add(1)
		r.jumpExpired.Add(tag.RowsAffected())
		r.event("harness: clock jump, expired %d live leases", tag.RowsAffected())
	}
}

func (r *chaosRun) contAll() {
	r.procMu.Lock()
	defer r.procMu.Unlock()
	for _, p := range r.procs {
		p.cmd.Process.Signal(syscall.SIGCONT)
	}
}

func (r *chaosRun) stopAll() {
	r.procMu.Lock()
	procs := append([]*proc(nil), r.procs...)
	r.procMu.Unlock()
	for _, p := range procs {
		p.cmd.Process.Signal(syscall.SIGCONT)
		p.cmd.Process.Signal(syscall.SIGTERM)
	}
	for _, p := range procs {
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
			p.cmd.Process.Kill()
			<-p.done
		}
	}
}

func (r *chaosRun) waitDrain(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		var q, run int
		err := r.st.Pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE status = 'queued'),
			count(*) FILTER (WHERE status = 'running') FROM jobs`).Scan(&q, &run)
		if err == nil && q == 0 && run == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("queue did not drain within %v: queued=%d running=%d err=%v", timeout, q, run, err)
		}
		if !sleepCtx(ctx, 500*time.Millisecond) {
			return ctx.Err()
		}
	}
}

func (r *chaosRun) progress(ctx context.Context, start time.Time) {
	for sleepCtx(ctx, 10*time.Second) {
		counts := map[string]int{}
		rows, err := r.st.Pool.Query(ctx, `SELECT status, count(*) FROM jobs GROUP BY status`)
		if err != nil {
			continue
		}
		for rows.Next() {
			var s string
			var n int
			rows.Scan(&s, &n)
			counts[s] = n
		}
		rows.Close()
		r.say("t=%3.0fs posts=%d kills=%d pauses=%d jumps=%d  queued=%d running=%d succeeded=%d failed=%d cancelled=%d",
			time.Since(start).Seconds(), r.posts.Load(), r.kills.Load(), r.pauses.Load(), r.clockJumps.Load(),
			counts["queued"], counts["running"], counts["succeeded"], counts["failed"], counts["cancelled"])
	}
}

// ---------- invariants ----------

type dbJob struct {
	id               int64
	key, status      string
	attempt          int
	lastError        *string
	finished         bool
	payloadFail      bool
	effects, runs    int
	effectAttempts   []int
	effectWorkers    []string
	owner            *string
	runMaxAttempt    int
	foreignRunWorker string
}

func (r *chaosRun) verify(ctx context.Context, elapsed time.Duration, drainErr error) error {
	var violations []string
	bad := func(kind, format string, a ...any) {
		violations = append(violations, kind+": "+fmt.Sprintf(format, a...))
	}
	if drainErr != nil {
		bad("STUCK", "%v", drainErr)
	}

	jobs := map[int64]*dbJob{}
	byKey := map[string]*dbJob{}
	rows, err := r.st.Pool.Query(ctx, `SELECT id, idempotency_key, status, attempt, worker_id, last_error,
		finished_at IS NOT NULL, coalesce((payload->>'fail')::boolean, false) FROM jobs`)
	if err != nil {
		return err
	}
	for rows.Next() {
		j := &dbJob{}
		if err := rows.Scan(&j.id, &j.key, &j.status, &j.attempt, &j.owner, &j.lastError, &j.finished, &j.payloadFail); err != nil {
			return err
		}
		jobs[j.id] = j
		byKey[j.key] = j
	}
	rows.Close()
	rows, err = r.st.Pool.Query(ctx, `SELECT job_id, attempt, worker_id FROM job_effects`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id int64
		var a int
		var w string
		rows.Scan(&id, &a, &w)
		if j := jobs[id]; j != nil {
			j.effects++
			j.effectAttempts = append(j.effectAttempts, a)
			j.effectWorkers = append(j.effectWorkers, w)
		}
	}
	rows.Close()
	rows, err = r.st.Pool.Query(ctx, `SELECT job_id, attempt, worker_id FROM job_runs`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id int64
		var a int
		var w string
		rows.Scan(&id, &a, &w)
		if j := jobs[id]; j != nil {
			j.runs++
			j.runMaxAttempt = max(j.runMaxAttempt, a)
			if !strings.HasPrefix(w, "w") {
				j.foreignRunWorker = w
			}
		}
	}
	rows.Close()

	// I1: no job lost, no phantom job, one id per key.
	submitted := map[string]bool{}
	cancelWon, cancelLost, cancelOther := 0, 0, 0
	mismatchOK, dupPosts := 0, 0
	for _, k := range r.keys {
		submitted[k.key] = true
		k.mu.Lock()
		dupPosts += max(0, k.posts-1)
		j := byKey[k.key]
		switch {
		case len(k.ids) == 0 && j == nil:
			if k.gaveUp {
				bad("SUBMIT", "key %s never got a 2xx and is not in the DB (client gave up)", k.key)
			}
		case len(k.ids) == 0 && j != nil:
			// server created it, client never heard back: fine, that's what idempotency is for
		case j == nil:
			bad("LOST", "key %s acknowledged as id %v but missing from DB", k.key, keysOf(k.ids))
		case len(k.ids) > 1:
			bad("DUP_ID", "key %s returned several ids %v", k.key, keysOf(k.ids))
		case keysOf(k.ids)[0] != j.id:
			bad("DUP_ID", "key %s acknowledged id %d but DB has id %d", k.key, keysOf(k.ids)[0], j.id)
		}
		if k.created > 1 {
			bad("DUP_CREATE", "key %s got %d x 201 Created", k.key, k.created)
		}
		for _, code := range k.mismatch {
			if code == 409 {
				mismatchOK++
			} else {
				bad("IDEMPOTENCY", "key %s reused with a different payload got %d, want 409", k.key, code)
			}
		}
		// I5a: cancel responses must agree with the final state.
		for _, c := range k.cancels {
			switch {
			case c.code == 200:
				cancelWon++
				if j != nil && j.status != store.StatusCancelled {
					bad("CANCEL", "job %d: cancel returned 200 but final status is %s", j.id, j.status)
				}
			case c.code == 409:
				cancelLost++
				if j != nil && j.status != c.status {
					bad("CANCEL", "job %d: cancel returned 409 (status %s) but final status is %s", j.id, c.status, j.status)
				}
			default:
				cancelOther++
				bad("CANCEL", "job %s: cancel returned %d", k.key, c.code)
			}
		}
		k.mu.Unlock()
	}
	for key, j := range byKey {
		if !submitted[key] {
			bad("PHANTOM", "job %d with key %s was never submitted by this run", j.id, key)
		}
	}

	// I2-I5: per-job checks against the DB.
	status := map[string]int{}
	runsHist := map[int]int{}
	totalRuns, totalEffects, leaseExhausted := 0, 0, 0
	for _, j := range jobs {
		status[j.status]++
		runsHist[min(j.runs, 5)]++
		totalRuns += j.runs
		totalEffects += j.effects
		if j.effects > 1 {
			bad("DOUBLE_EFFECT", "job %d has %d side effects (attempts %v)", j.id, j.effects, j.effectAttempts)
		}
		switch j.status {
		case store.StatusSucceeded:
			if j.effects != 1 {
				bad("EFFECT", "job %d succeeded with %d effects", j.id, j.effects)
			} else if j.effectAttempts[0] != j.attempt {
				bad("FENCING", "job %d effect written by attempt %d, final attempt %d", j.id, j.effectAttempts[0], j.attempt)
			} else if j.owner == nil || j.effectWorkers[0] != *j.owner {
				// independent of the attempt column: who wrote the effect vs who owns the job
				bad("FENCING", "job %d effect written by %s, but the job is owned by %s", j.id, j.effectWorkers[0], deref(j.owner))
			}
			if j.payloadFail {
				bad("STATE", "job %d succeeded but its payload always fails", j.id)
			}
		case store.StatusQueued, store.StatusRunning:
			bad("STUCK", "job %d still %s", j.id, j.status)
		default:
			if j.effects != 0 {
				bad("EFFECT", "job %d is %s but has %d effects", j.id, j.status, j.effects)
			}
		}
		if j.status == store.StatusFailed && !j.payloadFail {
			if j.lastError != nil && strings.HasPrefix(*j.lastError, "lease expired") {
				leaseExhausted++ // by design: killed max_attempts times
			} else {
				bad("STATE", "job %d failed but its payload never fails (last_error=%v)", j.id, deref(j.lastError))
			}
		}
		isTerminal := j.status == store.StatusSucceeded || j.status == store.StatusFailed || j.status == store.StatusCancelled
		if isTerminal != j.finished {
			bad("STATE", "job %d status %s but finished_at set=%v", j.id, j.status, j.finished)
		}
		if j.runMaxAttempt > j.attempt {
			bad("STATE", "job %d has a run with attempt %d > job attempt %d", j.id, j.runMaxAttempt, j.attempt)
		}
		if j.status == store.StatusSucceeded && j.runs == 0 {
			bad("STATE", "job %d succeeded without any recorded run", j.id)
		}
		if j.foreignRunWorker != "" {
			bad("FOREIGN", "job %d was run by %s, not a harness worker (are compose workers still running?)", j.id, j.foreignRunWorker)
		}
	}

	// ---------- report ----------
	var b strings.Builder
	p := func(format string, a ...any) { fmt.Fprintf(&b, format+"\n", a...) }
	p("==================== CHAOS RUN %s ====================", r.runID)
	p("seed=%d elapsed=%v jobs=%d workers=%d isolation=%q", r.cfg.seed, elapsed.Round(time.Millisecond), r.cfg.jobs, r.cfg.workers, os.Getenv("PG_ISOLATION"))
	p("chaos: kill -9=%d  SIGSTOP(%v)=%d  clock-jumps=%d  respawns=%d", r.kills.Load(), r.cfg.pauseFor, r.pauses.Load(), r.clockJumps.Load(), r.respawns.Load())
	p("client: POSTs=%d (duplicate/replayed=%d, retries=%d, transport/5xx errors=%d)  mismatch-409=%d", r.posts.Load(), dupPosts, r.postRetries.Load(), r.postErrors.Load(), mismatchOK)
	p("client: cancels 200=%d 409=%d other=%d", cancelWon, cancelLost, cancelOther)
	p50, p99, pmax := r.percentiles()
	p("client: POST latency p50=%v p99=%v max=%v", p50, p99, pmax)
	p("db: jobs=%d  succeeded=%d failed=%d (lease-exhausted=%d) cancelled=%d queued=%d running=%d",
		len(jobs), status["succeeded"], status["failed"], leaseExhausted, status["cancelled"], status["queued"], status["running"])
	p("db: executions (job_runs)=%d  committed effects=%d", totalRuns, totalEffects)
	p("db: runs per job histogram: 0=%d 1=%d 2=%d 3=%d 4=%d 5+=%d", runsHist[0], runsHist[1], runsHist[2], runsHist[3], runsHist[4], runsHist[5])
	var evs []string
	r.events.Range(func(k, v any) bool { evs = append(evs, fmt.Sprintf("%s=%d", k, v.(*atomic.Int64).Load())); return true })
	sort.Strings(evs)
	p("worker events: %s", strings.Join(evs, " "))
	// Coverage: a PASS only means something if the failure paths were actually taken.
	ev := func(name string) int64 {
		if v, ok := r.events.Load(name); ok {
			return v.(*atomic.Int64).Load()
		}
		return 0
	}
	zombieWrites := ev("stale_commit_rejected") + ev("lost_ownership")
	p("coverage: zombie/cancelled owners stopped by fencing=%d (stale commits rejected=%d), re-executed jobs=%d",
		zombieWrites, ev("stale_commit_rejected"), totalRuns-len(jobs)+runsHist[0])
	if r.pauses.Load() > 0 && ev("stale_commit_rejected") == 0 {
		p("COVERAGE WARNING: workers were frozen but no zombie ever attempted a stale commit")
	}
	if r.kills.Load() > 0 && runsHist[2]+runsHist[3]+runsHist[4]+runsHist[5] == 0 {
		p("COVERAGE WARNING: workers were killed but no job was ever re-executed")
	}
	// Every legitimate re-execution has a cause: a kill, a freeze or a forced lease expiry
	// (a cancel never re-executes). Many more than that means leases are being taken from
	// live owners — safe thanks to fencing, so no invariant fails, but wasteful.
	causes := r.kills.Load() + r.pauses.Load() + r.jumpExpired.Load()
	reexec := int64(totalRuns - len(jobs) + runsHist[0])
	if reexec > 2*causes+10 {
		p("EFFICIENCY WARNING: %d re-executions but only %d kills/freezes/forced expiries; leases are lost by live workers", reexec, causes)
	}
	if r.cfg.cancelRate > 0 && cancelWon == 0 {
		p("COVERAGE WARNING: no cancel ever won")
	}
	if len(violations) == 0 {
		p("invariants: no lost jobs, no duplicate ids, <=1 effect per job, none stuck, states consistent")
		p("RESULT: PASS")
	} else {
		byKind := map[string]int{}
		for _, v := range violations {
			byKind[strings.SplitN(v, ":", 2)[0]]++
		}
		p("violations by kind: %v", byKind)
		for i, v := range violations {
			if i == 50 {
				p("  ... %d more in violations.txt", len(violations)-50)
				break
			}
			p("  %s", v)
		}
		os.WriteFile(filepath.Join(r.dir, "violations.txt"), []byte(strings.Join(violations, "\n")+"\n"), 0o644)
		p("RESULT: FAIL (%d violations)", len(violations))
	}
	fmt.Print(b.String())
	os.WriteFile(filepath.Join(r.dir, "report.txt"), []byte(b.String()), 0o644)
	if len(violations) > 0 {
		return fmt.Errorf("%d invariant violations", len(violations))
	}
	return nil
}

func (r *chaosRun) percentiles() (p50, p99, pmax time.Duration) {
	r.latMu.Lock()
	defer r.latMu.Unlock()
	if len(r.lat) == 0 {
		return
	}
	sort.Slice(r.lat, func(i, j int) bool { return r.lat[i] < r.lat[j] })
	q := func(f float64) time.Duration { return r.lat[int(f*float64(len(r.lat)-1))].Round(time.Microsecond) }
	return q(0.50), q(0.99), r.lat[len(r.lat)-1].Round(time.Microsecond)
}

// say prints to stdout and to the run's event log.
func (r *chaosRun) say(format string, a ...any) {
	line := fmt.Sprintf(format, a...)
	fmt.Println(line)
	r.logMu.Lock()
	fmt.Fprintf(r.logFile, "harness %s %s\n", time.Now().UTC().Format(time.RFC3339Nano), line)
	r.logMu.Unlock()
}

// event goes to the log file only (too noisy for stdout).
func (r *chaosRun) event(format string, a ...any) {
	r.logMu.Lock()
	fmt.Fprintf(r.logFile, "harness %s %s\n", time.Now().UTC().Format(time.RFC3339Nano), fmt.Sprintf(format, a...))
	r.logMu.Unlock()
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func keysOf(m map[int64]int) []int64 {
	out := make([]int64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func deref(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}
