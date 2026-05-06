// Package scheduler fires cron-driven enqueues against the queue.
//
// The scheduler is leader-only. The runLeaderTasks coordinator starts
// it as part of a leadership stint; on demotion the ctx fires and all
// in-flight tickers stop. A demoted node never enqueues — the new
// leader picks up at its next scheduled tick.
//
// Tasks are configured in code (cmd/haasqueue/schedules.go) rather
// than via env: a cron expression + the EnqueueRequest shape that
// fires when the expression matches. Each tick goes through the same
// storage.Enqueue path a real client would, so replication, durability
// confirmation, and retention all behave identically for scheduled
// jobs.
package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/robfig/cron/v3"

	"haasqueue/internal/eventbus"
	"haasqueue/internal/idgen"
	"haasqueue/internal/storage"
	"haasqueue/pkg/protocol"
)

// Task is one cron-driven enqueue.
type Task struct {
	// Spec is a cron expression (5- or 6-field). robfig/cron/v3
	// accepts "@every 1h", "@daily", and standard 5-field syntax.
	Spec string

	// Lane the job lands on.
	Lane string

	// Entity is the prefix of the generated job id. Use a unique
	// value per scheduled task so logs let operators tell them
	// apart (e.g. "scheduled/daily-cleanup").
	Entity string

	// Document is the opaque payload handed to the worker. Whatever
	// the worker keys off (e.g. {"task":"cleanup"}) goes here.
	Document json.RawMessage

	// DurableFactor passes through to Enqueue. Default 0
	// (fire-and-forget) is fine for most scheduled jobs.
	DurableFactor int
}

// Validate returns an error if the task is missing required fields
// or has a malformed cron expression. Called from New so wiring
// errors surface at startup, not on the first tick.
func (t Task) Validate() error {
	if t.Spec == "" {
		return errors.New("scheduler: Task.Spec required")
	}
	if t.Lane == "" {
		return errors.New("scheduler: Task.Lane required")
	}
	if t.Entity == "" {
		return errors.New("scheduler: Task.Entity required")
	}
	if len(t.Document) == 0 {
		return errors.New("scheduler: Task.Document required")
	}
	if err := idgen.ValidateEntity(t.Entity); err != nil {
		return err
	}
	if _, err := cron.ParseStandard(t.Spec); err != nil {
		return err
	}
	return nil
}

// Scheduler runs a set of Tasks against the leader's storage.
type Scheduler struct {
	store  *storage.DB
	bus    *eventbus.Bus
	idgen  *idgen.Generator
	tasks  []Task
	logger *slog.Logger
}

// New constructs a Scheduler. All tasks are validated up-front.
func New(store *storage.DB, bus *eventbus.Bus, idGen *idgen.Generator, tasks []Task, logger *slog.Logger) (*Scheduler, error) {
	if logger == nil {
		logger = slog.Default()
	}
	for i, t := range tasks {
		if err := t.Validate(); err != nil {
			return nil, errors.Join(errors.New("scheduler: task validation failed"), err, errors.New(t.Spec))
		}
		_ = i
	}
	return &Scheduler{
		store:  store,
		bus:    bus,
		idgen:  idGen,
		tasks:  tasks,
		logger: logger,
	}, nil
}

// Run blocks until ctx is cancelled. While running, each task fires
// on its cron schedule and enqueues a job. Designed to be invoked
// as `go sched.Run(leaderCtx)` from runLeaderTasks.
func (s *Scheduler) Run(ctx context.Context) {
	if len(s.tasks) == 0 {
		<-ctx.Done()
		return
	}

	c := cron.New()
	for _, t := range s.tasks {
		task := t // capture
		_, err := c.AddFunc(task.Spec, func() {
			s.fire(ctx, task)
		})
		if err != nil {
			// Already validated at New; this should be unreachable.
			s.logger.Error("scheduler: re-parse failed", "spec", task.Spec, "err", err)
		}
	}
	c.Start()
	s.logger.Info("scheduler started", "tasks", len(s.tasks))

	<-ctx.Done()

	// Stop returns a context that fires once running jobs finish.
	// We bound the wait so a stuck enqueue doesn't hang shutdown.
	stopCtx := c.Stop()
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case <-stopCtx.Done():
	case <-timeoutCtx.Done():
	}
	s.logger.Info("scheduler stopped")
}

// fire enqueues the task. Errors are logged and swallowed: a single
// failed enqueue should not crash the scheduler — the next tick will
// retry.
func (s *Scheduler) fire(ctx context.Context, t Task) {
	if ctx.Err() != nil {
		return
	}
	res, err := s.store.Enqueue(ctx, protocol.EnqueueRequest{
		Lane:          t.Lane,
		Entity:        t.Entity,
		DurableFactor: t.DurableFactor,
		Document:      t.Document,
	}, s.idgen)
	if err != nil {
		s.logger.Error("scheduler: enqueue failed", "entity", t.Entity, "lane", t.Lane, "err", err)
		return
	}
	// Wake up workers on the lane so they pick this up promptly even
	// if the cron tick lands between dequeue idle-polls.
	s.bus.SignalLane(t.Lane)
	s.logger.Info("scheduler: tick", "entity", t.Entity, "job", res.JobID)
}
