// Package retention removes terminal jobs and trims the replication
// log on a schedule. The worker is leader-only — only the active
// leader writes to the queue, so only the active leader prunes. In
// cluster mode the orchestrator binds the worker's lifecycle to the
// leadership stint; in single-node mode it runs forever.
package retention

import (
	"context"
	"log/slog"
	"time"

	"github.com/xinix00/hopqueue/internal/eventbus"
	"github.com/xinix00/hopqueue/internal/stats"
	"github.com/xinix00/hopqueue/internal/storage"
	"github.com/xinix00/hopqueue/pkg/protocol"
)

// Config controls the retention worker. Zeros pick reasonable
// defaults: jobs in 'done' kept for 24h, 'failed' for 7d, the
// replication log trimmed at 7d, and the worker runs every 5m.
//
// The replication-log horizon is the same as the failed-jobs horizon
// because that's how far back a follower can sensibly catch up — past
// 7 days, an out-of-date follower must be re-initialised manually.
type Config struct {
	DoneAge   time.Duration
	FailedAge time.Duration
	LogAge    time.Duration
	Interval  time.Duration
}

func (c Config) withDefaults() Config {
	if c.DoneAge <= 0 {
		c.DoneAge = 24 * time.Hour
	}
	if c.FailedAge <= 0 {
		c.FailedAge = 7 * 24 * time.Hour
	}
	if c.LogAge <= 0 {
		c.LogAge = 7 * 24 * time.Hour
	}
	if c.Interval <= 0 {
		c.Interval = 5 * time.Minute
	}
	return c
}

// Worker is the retention background goroutine.
type Worker struct {
	store  *storage.DB
	bus    *eventbus.Bus
	cfg    Config
	logger *slog.Logger
	stats  *stats.Registry // optional; may be nil in tests
}

// New constructs a Worker. bus may be nil if you don't need to fan out
// 'deleted' events (e.g. tests that only want to assert the storage
// effect).
func New(store *storage.DB, bus *eventbus.Bus, cfg Config, logger *slog.Logger) *Worker {
	if logger == nil {
		logger = slog.Default()
	}
	return &Worker{store: store, bus: bus, cfg: cfg.withDefaults(), logger: logger}
}

// SetStats wires the metrics registry that records run timestamps and
// counts. Optional — workers without a registry just skip recording.
func (w *Worker) SetStats(r *stats.Registry) { w.stats = r }

// Run loops on a ticker until ctx is cancelled. Designed to be invoked
// as `go worker.Run(ctx)` from the leader-tasks coordinator.
func (w *Worker) Run(ctx context.Context) {
	t := time.NewTicker(w.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.tick(ctx)
		}
	}
}

// Tick runs a single retention pass synchronously. Exposed for tests
// that want to advance the worker deterministically.
func (w *Worker) Tick(ctx context.Context) {
	w.tick(ctx)
}

func (w *Worker) tick(ctx context.Context) {
	var doneCount, failedCount int

	deleted, err := w.store.DeleteJobsForRetention(ctx, w.cfg.DoneAge, w.cfg.FailedAge)
	if err != nil {
		w.logger.Error("retention: delete jobs failed", "err", err)
	} else if len(deleted) > 0 {
		for _, d := range deleted {
			if w.bus != nil {
				w.bus.PublishJob(eventbus.Event{
					ID:    d.Seq,
					JobID: d.JobID,
					Type:  protocol.EventDeleted,
					Data:  d.EventData,
				})
			}
		}
		// We don't currently have a per-state breakdown returned from
		// DeleteJobsForRetention — both done and failed are folded into
		// the `deleted` slice. For dashboard purposes we expose the
		// total under "done" and leave "failed" at 0; tightening this
		// would require teaching DeleteJobsForRetention to label its
		// returns.
		doneCount = len(deleted)
		w.logger.Info("retention: deleted terminal jobs", "count", len(deleted))
	}

	trimmed, err := w.store.TrimReplicationLog(ctx, w.cfg.LogAge)
	if err != nil {
		w.logger.Error("retention: trim replication log failed", "err", err)
	} else if trimmed > 0 {
		w.logger.Info("retention: trimmed replication log", "count", trimmed)
	}

	if w.stats != nil {
		w.stats.RecordRetentionRun(time.Now(), doneCount, failedCount, trimmed)
	}
}
