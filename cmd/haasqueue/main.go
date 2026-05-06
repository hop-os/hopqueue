// haasqueue is the queue server binary.
//
// Two run modes:
//
//   - Single-node (no QUEUE_LEASE_URL): the binary always considers
//     itself the leader. Session is bound at startup; sweeper +
//     retention run for the lifetime of the process.
//
//   - Cluster (QUEUE_LEASE_URL set): a haaslock.Elector races against
//     peers for an S3-backed lease. Bind/Unbind on api.Server, plus
//     the sweeper + retention goroutines, are driven by the
//     leadership stint via lease.Elector.Lead. A non-leader node
//     responds 503 + X-Active-Leader to all queue requests.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"haaslock"

	"haasqueue/internal/api"
	"haasqueue/internal/eventbus"
	"haasqueue/internal/follower"
	"haasqueue/internal/idgen"
	"haasqueue/internal/lease"
	"haasqueue/internal/retention"
	"haasqueue/internal/scheduler"
	"haasqueue/internal/stats"
	"haasqueue/internal/storage"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	flag.Parse()

	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}
	dbPath := filepath.Join(cfg.DataDir, "queue.db")

	store, err := storage.Open(ctx, dbPath)
	if err != nil {
		return fmt.Errorf("open storage: %w", err)
	}
	defer store.Close()
	store.SetNodeID(cfg.NodeID)
	logger.Info("storage opened", "path", dbPath, "node_id", cfg.NodeID)

	bus := eventbus.New()
	// Wire storage commits onto the bus so /replication/stream
	// subscribers see live events as soon as they're durable.
	store.SetReplogPublisher(func(rows ...storage.Replog) {
		for _, r := range rows {
			bus.PublishReplog(eventbus.ReplogRow{
				Seq:        r.Seq,
				EventType:  r.EventType,
				JobID:      r.JobID,
				Payload:    r.Payload,
				CreatedAt:  r.CreatedAt,
				OriginNode: r.OriginNode,
			})
		}
	})

	apiCfg := api.DefaultConfig()
	apiCfg.LockDuration = cfg.LockDuration
	srv := api.NewServer(bus, apiCfg, logger)
	idGen := idgen.New()
	session := &api.Session{Store: store, IDGen: idGen}

	leaderCfg := leaderTasksConfig{
		SweepInterval: cfg.SweepInterval,
		Retention: retention.Config{
			DoneAge:   cfg.RetentionDone,
			FailedAge: cfg.RetentionFailed,
			LogAge:    cfg.RetentionLog,
			Interval:  cfg.RetentionInterval,
		},
		Schedules: schedules(),
		IDGen:     idGen,
		Stats:     srv.Stats(),
	}

	// Background errors flow into runDone so we can fail the whole
	// process on a fatal subsystem exit.
	runDone := make(chan error, 1)

	if cfg.lease == nil {
		srv.Bind(session)
		srv.SetLeaderInfo(time.Now(), 1)
		logger.Info("running without leader election (single-node mode)")
		go func() {
			runLeaderTasks(ctx, store, bus, leaderCfg, logger)
			runDone <- nil
		}()
	} else {
		elector, err := lease.New(*cfg.lease)
		if err != nil {
			return fmt.Errorf("lease: %w", err)
		}
		logger.Info("running with leader election", "lease_url", cfg.lease.LeaseURL, "ttl", cfg.lease.TTL, "owner", elector.SelfOwner())

		// Follower applier runs always: it's idle while we hold the
		// lease ourselves and active when a peer is leader. No
		// orchestration needed beyond Run.
		fw := follower.New(store, elector, follower.Config{
			SelfOwner: elector.SelfOwner(),
		}, logger)
		go fw.Run(ctx)

		go func() {
			runDone <- elector.Lead(ctx, func(leaderCtx context.Context, l haaslock.Lease) error {
				logger.Info("acquired leadership", "generation", l.Generation, "owner", l.Owner)
				srv.Bind(session)
				srv.SetLeaderInfo(time.Now(), l.Generation)
				defer func() {
					srv.Unbind()
					srv.SetLeaderInfo(time.Time{}, 0)
					logger.Info("released leadership", "generation", l.Generation)
				}()
				runLeaderTasks(leaderCtx, store, bus, leaderCfg, logger)
				return nil
			})
		}()
	}

	hsrv := &http.Server{
		Addr:    cfg.Listen,
		Handler: srv.Routes(),
		// SSE streams stay open for the duration of a job/subscription.
		// Heartbeats + ctx cancellation own the timeout story; plain
		// Read/Write timeouts would tear down healthy long-poll clients.
		ReadTimeout:  0,
		WriteTimeout: 0,
		BaseContext:  func(_ net.Listener) context.Context { return ctx },
	}

	logger.Info("haasqueue listening", "addr", cfg.Listen, "node_id", cfg.NodeID)
	httpErr := make(chan error, 1)
	go func() {
		if err := hsrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			httpErr <- err
		}
		close(httpErr)
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-httpErr:
		return err
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := hsrv.Shutdown(shutCtx); err != nil {
		logger.Warn("http shutdown failed", "err", err)
	}

	select {
	case err := <-runDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Warn("background task exited with error", "err", err)
		}
	case <-time.After(10 * time.Second):
		logger.Warn("background tasks did not exit within 10s of shutdown")
	}
	return nil
}

// leaderTasksConfig is the slice of cfg that the leader-tasks
// coordinator needs.
type leaderTasksConfig struct {
	SweepInterval time.Duration
	Retention     retention.Config
	Schedules     []scheduler.Task
	IDGen         *idgen.Generator
	Stats         *stats.Registry
}

// runLeaderTasks runs the goroutines that mutate state — they only
// run while this node holds leadership. ctx is the leader-stint ctx
// (cancelled on demotion) in cluster mode, or the process ctx in
// single-node mode. Returns when ctx fires and all subordinates exit.
func runLeaderTasks(ctx context.Context, store *storage.DB, bus *eventbus.Bus, cfg leaderTasksConfig, logger *slog.Logger) {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		api.NewSweeper(store, bus, cfg.SweepInterval, logger).Run(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		w := retention.New(store, bus, cfg.Retention, logger)
		if cfg.Stats != nil {
			w.SetStats(cfg.Stats)
		}
		w.Run(ctx)
	}()

	if len(cfg.Schedules) > 0 && cfg.IDGen != nil {
		sched, err := scheduler.New(store, bus, cfg.IDGen, cfg.Schedules, logger)
		if err != nil {
			logger.Error("scheduler: invalid task definition", "err", err)
		} else {
			wg.Add(1)
			go func() {
				defer wg.Done()
				sched.Run(ctx)
			}()
		}
	}

	wg.Wait()
}

type config struct {
	NodeID            string
	DataDir           string
	Listen            string
	LockDuration      time.Duration
	SweepInterval     time.Duration
	RetentionDone     time.Duration
	RetentionFailed   time.Duration
	RetentionLog      time.Duration
	RetentionInterval time.Duration

	lease *lease.Config
}

func loadConfig() (*config, error) {
	cfg := &config{
		NodeID:            envOr("QUEUE_NODE_ID", "node-1"),
		DataDir:           envOr("QUEUE_DATA_DIR", "./data"),
		Listen:            envOr("QUEUE_LISTEN", ":8080"),
		LockDuration:      durationEnvOr("QUEUE_LOCK_DURATION", 5*time.Minute),
		SweepInterval:     durationEnvOr("QUEUE_SWEEP_INTERVAL", 5*time.Second),
		RetentionDone:     durationEnvOr("QUEUE_RETENTION_DONE", 24*time.Hour),
		RetentionFailed:   durationEnvOr("QUEUE_RETENTION_FAILED", 7*24*time.Hour),
		RetentionLog:      durationEnvOr("QUEUE_RETENTION_LOG", 7*24*time.Hour),
		RetentionInterval: durationEnvOr("QUEUE_RETENTION_INTERVAL", 5*time.Minute),
	}

	if leaseURL := os.Getenv("QUEUE_LEASE_URL"); leaseURL != "" {
		// Owner needs to be a URL peers can reach. QUEUE_PUBLIC_URL is
		// the canonical source; if missing, fall back to the listen
		// address as a best-effort default for local dev.
		owner := os.Getenv("QUEUE_PUBLIC_URL")
		if owner == "" {
			owner = "http://" + strings.TrimPrefix(cfg.Listen, ":")
			if strings.HasPrefix(cfg.Listen, ":") {
				owner = "http://localhost" + cfg.Listen
			}
		}

		cfg.lease = &lease.Config{
			NodeID:          cfg.NodeID,
			Owner:           owner,
			LeaseURL:        leaseURL,
			Region:          os.Getenv("AWS_REGION"),
			AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
			SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
			SessionToken:    os.Getenv("AWS_SESSION_TOKEN"),
			Endpoint:        os.Getenv("QUEUE_S3_ENDPOINT"),
			TTL:             durationEnvOr("QUEUE_LEASE_TTL", 30*time.Second),
		}
		if v := os.Getenv("QUEUE_S3_PATH_STYLE"); v != "" {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return nil, fmt.Errorf("parse QUEUE_S3_PATH_STYLE=%q: %w", v, err)
			}
			cfg.lease.UsePathStyle = &b
		}
	}

	return cfg, nil
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func durationEnvOr(name string, fallback time.Duration) time.Duration {
	v := os.Getenv(name)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}
