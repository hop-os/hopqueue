package retention

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/xinix00/hopqueue/internal/eventbus"
	"github.com/xinix00/hopqueue/internal/idgen"
	"github.com/xinix00/hopqueue/internal/storage"
	"github.com/xinix00/hopqueue/pkg/protocol"
)

func openTestDB(t *testing.T) *storage.DB {
	t.Helper()
	db, err := storage.Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	db.SetNodeID("test")
	return db
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// completeOne runs a job through enqueue → dequeue → done, then
// backdates completed_at to be eligible for retention.
func completeOne(t *testing.T, db *storage.DB, gen *idgen.Generator, fail bool) string {
	t.Helper()
	ctx := context.Background()
	enq, err := db.Enqueue(ctx, protocol.EnqueueRequest{
		Lane: "x", Entity: "e", Document: json.RawMessage(`{}`),
	}, gen)
	if err != nil {
		t.Fatal(err)
	}
	res, _, err := db.Dequeue(ctx, "x", "worker", time.Minute, nil)
	if err != nil {
		t.Fatal(err)
	}
	if fail {
		_, _, err = db.Fail(ctx, res.Job.ID, "worker", []byte(`{}`))
	} else {
		_, _, err = db.Done(ctx, res.Job.ID, "worker", []byte(`{}`))
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE jobs SET completed_at = 1 WHERE id = ?`, res.Job.ID); err != nil {
		t.Fatal(err)
	}
	return enq.JobID
}

func TestWorker_DeletesAndTrimsAndPublishes(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	gen := idgen.New()
	bus := eventbus.New()
	ctx := context.Background()

	// Two completed jobs, both eligible.
	doneID := completeOne(t, db, gen, false)
	failID := completeOne(t, db, gen, true)

	// Subscribe before tick so we observe the published 'deleted' events.
	doneCh, unsubDone := bus.SubscribeJob(doneID)
	defer unsubDone()
	failCh, unsubFail := bus.SubscribeJob(failID)
	defer unsubFail()

	w := New(db, bus, Config{
		DoneAge:   time.Hour,
		FailedAge: time.Hour,
		LogAge:    7 * 24 * time.Hour,
		Interval:  time.Hour, // never auto-fires; we drive Tick manually
	}, quietLogger())
	w.Tick(ctx)

	// Both jobs gone from storage.
	if _, err := db.GetJob(ctx, doneID); err == nil {
		t.Error("done job still present")
	}
	if _, err := db.GetJob(ctx, failID); err == nil {
		t.Error("failed job still present")
	}

	// Both 'deleted' events delivered.
	for label, ch := range map[string]<-chan eventbus.Event{
		"done": doneCh, "failed": failCh,
	} {
		select {
		case ev := <-ch:
			if ev.Type != protocol.EventDeleted {
				t.Errorf("%s: got event %q, want deleted", label, ev.Type)
			}
		case <-time.After(time.Second):
			t.Errorf("%s: no deleted event", label)
		}
	}
}

func TestWorker_PreservesRecent(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	gen := idgen.New()

	// Enqueue + done but with current completed_at.
	ctx := context.Background()
	enq, err := db.Enqueue(ctx, protocol.EnqueueRequest{
		Lane: "x", Entity: "e", Document: json.RawMessage(`{}`),
	}, gen)
	if err != nil {
		t.Fatal(err)
	}
	res, _, err := db.Dequeue(ctx, "x", "worker", time.Minute, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.Done(ctx, res.Job.ID, "worker", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}

	w := New(db, eventbus.New(), Config{}, quietLogger())
	w.Tick(ctx)

	if _, err := db.GetJob(ctx, enq.JobID); err != nil {
		t.Errorf("recent job pruned: %v", err)
	}
}

func TestWorker_DefaultsApplied(t *testing.T) {
	t.Parallel()
	w := New(openTestDB(t), nil, Config{}, quietLogger())
	if w.cfg.DoneAge == 0 || w.cfg.FailedAge == 0 || w.cfg.LogAge == 0 || w.cfg.Interval == 0 {
		t.Errorf("zero config not filled in: %+v", w.cfg)
	}
}
