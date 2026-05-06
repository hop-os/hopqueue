package api

import (
	"context"
	"log/slog"
	"time"

	"haasqueue/internal/eventbus"
	"haasqueue/internal/storage"
	"haasqueue/pkg/protocol"
)

// Sweeper periodically releases jobs whose worker locks have expired.
// Without it, a worker that crashes mid-job leaves the row in
// 'processing' forever; the sweeper turns those back into 'pending' so
// another worker can pick them up.
type Sweeper struct {
	store    *storage.DB
	bus      *eventbus.Bus
	interval time.Duration
	logger   *slog.Logger
}

func NewSweeper(store *storage.DB, bus *eventbus.Bus, interval time.Duration, logger *slog.Logger) *Sweeper {
	if logger == nil {
		logger = slog.Default()
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return &Sweeper{store: store, bus: bus, interval: interval, logger: logger}
}

// Run loops on a ticker until ctx is cancelled. Designed to be invoked
// as `go sweeper.Run(ctx)` from main.go.
func (s *Sweeper) Run(ctx context.Context) {
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.tick(ctx)
		}
	}
}

func (s *Sweeper) tick(ctx context.Context) {
	released, err := s.store.SweepExpiredLocks(ctx)
	if err != nil {
		s.logger.Error("sweep failed", "err", err)
		return
	}
	for _, r := range released {
		s.bus.PublishJob(eventbus.Event{
			ID:    r.Seq,
			JobID: r.JobID,
			Type:  protocol.EventReleased,
			Data:  r.EventData,
		})
		s.bus.SignalLane(r.Lane)
	}
	if len(released) > 0 {
		s.logger.Info("locks expired", "count", len(released))
	}
}
