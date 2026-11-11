// soupq is one binary with subcommands: migrate, api, worker, chaos.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"soupq/internal/api"
	"soupq/internal/store"
	"soupq/internal/worker"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: soupq migrate|api|worker|chaos [flags]")
		os.Exit(2)
	}
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel()}))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var err error
	switch os.Args[1] {
	case "migrate":
		err = runMigrate(ctx, log)
	case "api":
		err = runAPI(ctx, log)
	case "worker":
		err = runWorker(ctx, log)
	case "chaos":
		err = runChaos(ctx, log, os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func openStore(ctx context.Context) (*store.Store, error) {
	return store.Open(ctx, env("DATABASE_URL", "postgres://soup:soup@localhost:5432/soup?sslmode=disable"),
		os.Getenv("PG_ISOLATION"))
}

func runMigrate(ctx context.Context, log *slog.Logger) error {
	st, err := openStore(ctx)
	if err != nil {
		return err
	}
	defer st.Close()
	deadline, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	for {
		if err = st.Migrate(deadline); err == nil {
			log.Info("schema applied")
			return nil
		}
		log.Warn("migrate: waiting for database", "err", err)
		select {
		case <-deadline.Done():
			return err
		case <-time.After(time.Second):
		}
	}
}

func runAPI(ctx context.Context, log *slog.Logger) error {
	st, err := openStore(ctx)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.WaitReady(ctx); err != nil {
		return err
	}
	srv := &http.Server{
		Addr:              env("LISTEN_ADDR", ":8080"),
		Handler:           (&api.Server{Store: st, MaxAttempts: envInt("MAX_ATTEMPTS", 5), Log: log}).Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdown)
	}()
	log.Info("api listening", "addr", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func runWorker(ctx context.Context, log *slog.Logger) error {
	st, err := openStore(ctx)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.WaitReady(ctx); err != nil {
		return err
	}
	host, _ := os.Hostname()
	cfg := worker.Config{
		ID:           env("WORKER_ID", fmt.Sprintf("%s-%d", host, os.Getpid())),
		Concurrency:  envInt("WORKER_CONCURRENCY", 1),
		Lease:        time.Duration(envInt("LEASE_MS", 5000)) * time.Millisecond,
		Heartbeat:    time.Duration(envInt("HEARTBEAT_MS", 1000)) * time.Millisecond,
		Poll:         time.Duration(envInt("POLL_MS", 100)) * time.Millisecond,
		ReapInterval: time.Duration(envInt("REAP_MS", 1000)) * time.Millisecond,
	}
	if cfg.Heartbeat*2 >= cfg.Lease {
		return fmt.Errorf("HEARTBEAT_MS must be well below LEASE_MS (got %v / %v)", cfg.Heartbeat, cfg.Lease)
	}
	log = log.With("worker", cfg.ID)
	log.Info("worker started", "concurrency", cfg.Concurrency, "lease", cfg.Lease, "heartbeat", cfg.Heartbeat)
	(&worker.Worker{Store: st, Cfg: cfg, Log: log}).Run(ctx)
	log.Info("worker stopped")
	return nil
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func logLevel() slog.Level {
	if os.Getenv("LOG_LEVEL") == "debug" {
		return slog.LevelDebug
	}
	return slog.LevelInfo
}
