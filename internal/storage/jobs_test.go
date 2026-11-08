package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hop-os/hopqueue/internal/idgen"
	"github.com/hop-os/hopqueue/pkg/protocol"
)

func openTestDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	db.SetNodeID("test-node")
	return db
}

func TestEnqueue_InsertsJobAndReplog(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	gen := idgen.New()
	ctx := context.Background()

	res, err := db.Enqueue(ctx, protocol.EnqueueRequest{
		Lane:     "background",
		Entity:   "customer-1",
		Document: json.RawMessage(`{"hello":"world"}`),
	}, gen)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if res.JobID == "" || res.EnqueuedSeq == 0 {
		t.Fatalf("zero result: %+v", res)
	}

	info, err := db.GetJob(ctx, res.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if info.State != protocol.StatePending {
		t.Errorf("state = %q, want pending", info.State)
	}
	if info.Lane != "background" {
		t.Errorf("lane = %q, want background", info.Lane)
	}

	events, err := db.EventsSince(ctx, res.JobID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].EventType != protocol.EventEnqueued {
		t.Errorf("events = %+v, want one 'enqueued'", events)
	}
	if events[0].OriginNode != "test-node" {
		t.Errorf("origin_node = %q, want test-node", events[0].OriginNode)
	}
}

func TestEnqueue_RejectsBadEntity(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	gen := idgen.New()
	ctx := context.Background()

	_, err := db.Enqueue(ctx, protocol.EnqueueRequest{
		Lane:     "background",
		Entity:   "with/slash",
		Document: json.RawMessage(`{}`),
	}, gen)
	if !errors.Is(err, idgen.ErrEntityHasSlash) {
		t.Errorf("got %v, want ErrEntityHasSlash", err)
	}
}

func TestDequeue_HappyPath(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	gen := idgen.New()
	ctx := context.Background()

	enq, err := db.Enqueue(ctx, protocol.EnqueueRequest{
		Lane:     "background",
		Entity:   "customer-1",
		Document: json.RawMessage(`{"task":"send-email"}`),
	}, gen)
	if err != nil {
		t.Fatal(err)
	}

	res, hit, err := db.Dequeue(ctx, "background", "worker-A", time.Minute, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !hit {
		t.Fatal("Dequeue did not return a job")
	}
	if res.Job.ID != enq.JobID {
		t.Errorf("dequeued id = %q, want %q", res.Job.ID, enq.JobID)
	}
	if res.Job.Attempts != 1 {
		t.Errorf("attempts = %d, want 1", res.Job.Attempts)
	}
	if res.ClaimedSeq <= enq.EnqueuedSeq {
		t.Errorf("ClaimedSeq %d should be > EnqueuedSeq %d", res.ClaimedSeq, enq.EnqueuedSeq)
	}
}

func TestDequeue_EmptyQueueReturnsFalse(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	_, hit, err := db.Dequeue(context.Background(), "background", "worker-A", time.Minute, nil)
	if err != nil {
		t.Fatal(err)
	}
	if hit {
		t.Error("expected hit=false on empty queue")
	}
}

func TestDequeue_FIFOByAvailableAt(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	gen := idgen.New()
	ctx := context.Background()

	first, err := db.Enqueue(ctx, protocol.EnqueueRequest{
		Lane: "background", Entity: "e", Document: json.RawMessage(`{}`),
	}, gen)
	if err != nil {
		t.Fatal(err)
	}
	second, err := db.Enqueue(ctx, protocol.EnqueueRequest{
		Lane: "background", Entity: "e", Document: json.RawMessage(`{}`),
	}, gen)
	if err != nil {
		t.Fatal(err)
	}

	a, _, err := db.Dequeue(ctx, "background", "worker-A", time.Minute, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := db.Dequeue(ctx, "background", "worker-B", time.Minute, nil)
	if err != nil {
		t.Fatal(err)
	}
	if a.Job.ID != first.JobID {
		t.Errorf("first dequeue = %q, want %q", a.Job.ID, first.JobID)
	}
	if b.Job.ID != second.JobID {
		t.Errorf("second dequeue = %q, want %q", b.Job.ID, second.JobID)
	}
}

// TestDequeue_NoDoubleDelivery hammers the atomic claim with N concurrent
// dequeue goroutines against M < N pending jobs and verifies that exactly
// M unique jobs come back, each claimed exactly once.
func TestDequeue_NoDoubleDelivery(t *testing.T) {
	t.Parallel()
	const (
		jobs    = 50
		workers = 30
	)
	db := openTestDB(t)
	gen := idgen.New()
	ctx := context.Background()

	for i := 0; i < jobs; i++ {
		if _, err := db.Enqueue(ctx, protocol.EnqueueRequest{
			Lane: "background", Entity: "e", Document: json.RawMessage(`{}`),
		}, gen); err != nil {
			t.Fatal(err)
		}
	}

	var (
		mu        sync.Mutex
		seen      = make(map[string]int)
		claimed   atomic.Int64
		emptyHits atomic.Int64
		wg        sync.WaitGroup
	)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID string) {
			defer wg.Done()
			for {
				res, hit, err := db.Dequeue(ctx, "background", workerID, time.Minute, nil)
				if err != nil {
					t.Errorf("worker %s: %v", workerID, err)
					return
				}
				if !hit {
					emptyHits.Add(1)
					return
				}
				mu.Lock()
				seen[res.Job.ID]++
				mu.Unlock()
				claimed.Add(1)
			}
		}("w-" + itoa(w))
	}
	wg.Wait()

	if int(claimed.Load()) != jobs {
		t.Errorf("claimed = %d, want %d", claimed.Load(), jobs)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != jobs {
		t.Errorf("unique jobs claimed = %d, want %d", len(seen), jobs)
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("job %q claimed %d times", id, n)
		}
	}
}

func TestDequeue_CapabilityFilter(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	gen := idgen.New()
	ctx := context.Background()

	// Three jobs with different capability hints in their documents.
	enqueueWithDoc := func(doc string) string {
		t.Helper()
		res, err := db.Enqueue(ctx, protocol.EnqueueRequest{
			Lane: "x", Entity: "e", Document: json.RawMessage(doc),
		}, gen)
		if err != nil {
			t.Fatal(err)
		}
		return res.JobID
	}
	emailID := enqueueWithDoc(`{"capability":"email"}`)
	pdfID := enqueueWithDoc(`{"capability":"pdf"}`)
	smsID := enqueueWithDoc(`{"capability":"sms"}`)

	// A worker that only handles email + pdf claims those two and
	// leaves sms.
	caps := []string{"email", "pdf"}
	got := map[string]bool{}
	for i := 0; i < 2; i++ {
		res, hit, err := db.Dequeue(ctx, "x", fmt.Sprintf("w-%d", i), time.Minute, caps)
		if err != nil || !hit {
			t.Fatalf("dequeue: hit=%v err=%v", hit, err)
		}
		got[res.Job.ID] = true
	}
	if !got[emailID] || !got[pdfID] {
		t.Errorf("filter missed expected jobs: got=%v want email=%s pdf=%s", got, emailID, pdfID)
	}
	if got[smsID] {
		t.Errorf("filter delivered sms job to email/pdf worker")
	}

	// A third claim with the same caps must come up empty (sms is the
	// only one left, and it doesn't match).
	if _, hit, err := db.Dequeue(ctx, "x", "w-3", time.Minute, caps); err != nil {
		t.Fatal(err)
	} else if hit {
		t.Errorf("expected no job for caps=%v after email+pdf claimed", caps)
	}

	// Without a filter the sms job is still claimable.
	res, hit, err := db.Dequeue(ctx, "x", "w-4", time.Minute, nil)
	if err != nil || !hit {
		t.Fatalf("unfiltered dequeue: hit=%v err=%v", hit, err)
	}
	if res.Job.ID != smsID {
		t.Errorf("expected sms job, got %s", res.Job.ID)
	}
}

func TestDone_TerminatesProcessingJob(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	gen := idgen.New()
	ctx := context.Background()

	enq, err := db.Enqueue(ctx, protocol.EnqueueRequest{
		Lane: "x", Entity: "e", Document: json.RawMessage(`{}`),
	}, gen)
	if err != nil {
		t.Fatal(err)
	}
	res, _, err := db.Dequeue(ctx, "x", "worker-A", time.Minute, nil)
	if err != nil {
		t.Fatal(err)
	}
	seq, _, err := db.Done(ctx, res.Job.ID, "worker-A", []byte(`{"ok":true}`))
	if err != nil {
		t.Fatal(err)
	}
	if seq <= res.ClaimedSeq {
		t.Errorf("done seq %d not > claim seq %d", seq, res.ClaimedSeq)
	}

	info, err := db.GetJob(ctx, enq.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if info.State != protocol.StateDone {
		t.Errorf("state = %q, want done", info.State)
	}
}

func TestDone_RejectsWrongWorker(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	gen := idgen.New()
	ctx := context.Background()
	if _, err := db.Enqueue(ctx, protocol.EnqueueRequest{
		Lane: "x", Entity: "e", Document: json.RawMessage(`{}`),
	}, gen); err != nil {
		t.Fatal(err)
	}
	res, _, err := db.Dequeue(ctx, "x", "worker-A", time.Minute, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = db.Done(ctx, res.Job.ID, "worker-B", nil)
	if !errors.Is(err, ErrLockMismatch) {
		t.Errorf("got %v, want ErrLockMismatch", err)
	}
}

func TestRelease_PutsBackToPending(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	gen := idgen.New()
	ctx := context.Background()
	if _, err := db.Enqueue(ctx, protocol.EnqueueRequest{
		Lane: "x", Entity: "e", Document: json.RawMessage(`{}`),
	}, gen); err != nil {
		t.Fatal(err)
	}
	first, _, err := db.Dequeue(ctx, "x", "worker-A", time.Minute, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.Release(ctx, first.Job.ID, "worker-A", 0); err != nil {
		t.Fatal(err)
	}
	second, hit, err := db.Dequeue(ctx, "x", "worker-B", time.Minute, nil)
	if err != nil || !hit {
		t.Fatalf("re-dequeue: hit=%v err=%v", hit, err)
	}
	if second.Job.ID != first.Job.ID {
		t.Errorf("re-dequeued different job")
	}
	if second.Job.Attempts != 2 {
		t.Errorf("attempts = %d, want 2", second.Job.Attempts)
	}
}

func TestSweepExpiredLocks_ReleasesAbandonedJobs(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	gen := idgen.New()
	ctx := context.Background()
	if _, err := db.Enqueue(ctx, protocol.EnqueueRequest{
		Lane: "x", Entity: "e", Document: json.RawMessage(`{}`),
	}, gen); err != nil {
		t.Fatal(err)
	}
	// Claim with a 1-nanosecond lock so it's expired before we look.
	res, _, err := db.Dequeue(ctx, "x", "worker-A", time.Nanosecond, nil)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	released, err := db.SweepExpiredLocks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(released) != 1 || released[0].JobID != res.Job.ID {
		t.Fatalf("sweep result = %+v", released)
	}
	info, err := db.GetJob(ctx, res.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if info.State != protocol.StatePending {
		t.Errorf("state after sweep = %q, want pending", info.State)
	}
}

func TestDeleteJobsForRetention(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	gen := idgen.New()
	ctx := context.Background()

	// Two done jobs and one failed job, all with completed_at far in the past.
	for i := 0; i < 3; i++ {
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
		if res.Job.ID != enq.JobID {
			t.Fatalf("dequeue mismatch")
		}
		if i < 2 {
			if _, _, err := db.Done(ctx, res.Job.ID, "worker", []byte(`{}`)); err != nil {
				t.Fatal(err)
			}
		} else {
			if _, _, err := db.Fail(ctx, res.Job.ID, "worker", []byte(`{}`)); err != nil {
				t.Fatal(err)
			}
		}
		// Backdate completed_at so retention picks this row up.
		if _, err := db.ExecContext(ctx,
			`UPDATE jobs SET completed_at = 1 WHERE id = ?`, res.Job.ID,
		); err != nil {
			t.Fatal(err)
		}
	}

	// Done retention: 1 day. Failed retention: 7 days. With completed_at=1
	// (epoch), both are far older than either threshold.
	deleted, err := db.DeleteJobsForRetention(ctx, 24*time.Hour, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("DeleteJobsForRetention: %v", err)
	}
	if len(deleted) != 3 {
		t.Errorf("deleted count = %d, want 3", len(deleted))
	}
	for _, d := range deleted {
		if d.Seq == 0 || len(d.EventData) == 0 {
			t.Errorf("deleted entry missing replog metadata: %+v", d)
		}
	}

	counts, err := db.CountByState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if total := counts[protocol.StateDone] + counts[protocol.StateFailed]; total != 0 {
		t.Errorf("after retention, terminal jobs remain: %+v", counts)
	}

	// And the deletion events are visible in the replication log.
	for _, d := range deleted {
		events, err := db.EventsSince(ctx, d.JobID, 0)
		if err != nil {
			t.Fatal(err)
		}
		var sawDeleted bool
		for _, ev := range events {
			if ev.EventType == protocol.EventDeleted {
				sawDeleted = true
			}
		}
		if !sawDeleted {
			t.Errorf("no deleted event in replog for %s", d.JobID)
		}
	}
}

func TestDeleteJobsForRetention_SkipsRecent(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	gen := idgen.New()
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

	// Job's completed_at is "now"; with a 1-day retention it should not be
	// deleted.
	deleted, err := db.DeleteJobsForRetention(ctx, 24*time.Hour, 7*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(deleted) != 0 {
		t.Errorf("recent job got deleted: %+v", deleted)
	}
	if _, err := db.GetJob(ctx, enq.JobID); err != nil {
		t.Errorf("recent job vanished: %v", err)
	}
}

func TestTrimReplicationLog(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	gen := idgen.New()
	ctx := context.Background()

	if _, err := db.Enqueue(ctx, protocol.EnqueueRequest{
		Lane: "x", Entity: "e", Document: json.RawMessage(`{}`),
	}, gen); err != nil {
		t.Fatal(err)
	}

	// Backdate everything by 10 days.
	if _, err := db.ExecContext(ctx,
		`UPDATE replication_log SET created_at = ?`, time.Now().Add(-10*24*time.Hour).Unix(),
	); err != nil {
		t.Fatal(err)
	}

	n, err := db.TrimReplicationLog(ctx, 7*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Error("expected to trim at least one row")
	}

	// And a recent row is preserved.
	if _, err := db.Enqueue(ctx, protocol.EnqueueRequest{
		Lane: "x", Entity: "e", Document: json.RawMessage(`{}`),
	}, gen); err != nil {
		t.Fatal(err)
	}
	n, err = db.TrimReplicationLog(ctx, 7*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("trimmed recent rows: %d", n)
	}
}

func TestProgress_RequiresLockHolder(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	gen := idgen.New()
	ctx := context.Background()
	if _, err := db.Enqueue(ctx, protocol.EnqueueRequest{
		Lane: "x", Entity: "e", Document: json.RawMessage(`{}`),
	}, gen); err != nil {
		t.Fatal(err)
	}
	res, _, err := db.Dequeue(ctx, "x", "worker-A", time.Minute, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.Progress(ctx, res.Job.ID, "worker-A", []byte(`{"step":1}`)); err != nil {
		t.Fatalf("progress as lock holder: %v", err)
	}
	_, _, err = db.Progress(ctx, res.Job.ID, "worker-B", []byte(`{"step":2}`))
	if !errors.Is(err, ErrLockMismatch) {
		t.Errorf("progress as wrong worker: got %v, want ErrLockMismatch", err)
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
