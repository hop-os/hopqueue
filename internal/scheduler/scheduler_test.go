package scheduler

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/hop-os/hopqueue/internal/eventbus"
	"github.com/hop-os/hopqueue/internal/idgen"
	"github.com/hop-os/hopqueue/internal/storage"
	"github.com/hop-os/hopqueue/pkg/protocol"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func openDB(t *testing.T) *storage.DB {
	t.Helper()
	db, err := storage.Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	db.SetNodeID("test")
	return db
}

func TestTask_Validate(t *testing.T) {
	t.Parallel()
	good := Task{Spec: "@every 1s", Lane: "x", Entity: "e", Document: json.RawMessage(`{}`)}
	if err := good.Validate(); err != nil {
		t.Errorf("good task rejected: %v", err)
	}

	bad := []Task{
		{},
		{Spec: "@every 1s"},                          // missing lane
		{Spec: "@every 1s", Lane: "x"},               // missing entity
		{Spec: "@every 1s", Lane: "x", Entity: "e"},  // missing doc
		{Spec: "not a cron", Lane: "x", Entity: "e", Document: json.RawMessage(`{}`)},
		{Spec: "@every 1s", Lane: "x", Entity: "with/slash", Document: json.RawMessage(`{}`)},
	}
	for i, b := range bad {
		if err := b.Validate(); err == nil {
			t.Errorf("case %d: expected error for %+v", i, b)
		}
	}
}

func TestScheduler_FiresOnTick(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	bus := eventbus.New()
	gen := idgen.New()

	tasks := []Task{
		{
			Spec:     "@every 200ms",
			Lane:     "scheduled",
			Entity:   "test-task",
			Document: json.RawMessage(`{"task":"recurring"}`),
		},
	}

	sched, err := New(db, bus, gen, tasks, quietLogger())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		sched.Run(ctx)
		close(done)
	}()

	// Wait for at least two ticks to land. First tick fires at +200ms.
	deadline := time.After(2 * time.Second)
	var lastCount int
	for {
		counts, err := db.CountByState(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		lastCount = counts[protocol.StatePending]
		if lastCount >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("scheduler did not produce >= 2 jobs in 2s; got %d", lastCount)
		case <-time.After(50 * time.Millisecond):
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(7 * time.Second):
		t.Fatal("scheduler did not stop within 7s")
	}
}

func TestScheduler_NoTasks_RunsAndStops(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	bus := eventbus.New()
	gen := idgen.New()

	sched, err := New(db, bus, gen, nil, quietLogger())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		sched.Run(ctx)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("empty scheduler did not stop")
	}
}
