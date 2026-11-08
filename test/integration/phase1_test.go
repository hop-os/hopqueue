// Phase-1 acceptance: a single hopqueue node accepts 1000 enqueues,
// 10 concurrent workers drain the queue over the wire (HTTP/SSE), and
// every job ends up in state 'done' exactly once.
//
// Tests run against the real api.Server bound to a real *storage.DB
// (file-backed SQLite in t.TempDir), exposed through httptest.NewServer.
// Workers consume the production SSE wire format with a small in-test
// parser — that's the closest we can get to the binary's behaviour
// without spawning the binary itself.
package integration

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hop-os/hopqueue/internal/api"
	"github.com/hop-os/hopqueue/internal/eventbus"
	"github.com/hop-os/hopqueue/internal/idgen"
	"github.com/hop-os/hopqueue/internal/storage"
	"github.com/hop-os/hopqueue/pkg/protocol"
)

// testNode bundles everything a test needs to drive a running hopqueue
// node end-to-end: the HTTP server's URL plus the live *storage.DB so
// tests can read final state without re-opening the file.
type testNode struct {
	URL string
	DB  *storage.DB
}

// startServer spins up an in-process hopqueue using a fresh SQLite DB.
// Cleanup is registered with t. Accepts testing.TB so benchmarks can
// use the same helper.
func startServer(t testing.TB) *testNode {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "queue.db")
	db, err := storage.Open(context.Background(), dbPath)
	if err != nil {
		t.Fatal(err)
	}
	db.SetNodeID("test-node")

	bus := eventbus.New()
	cfg := api.DefaultConfig()
	// Short lock duration so a worker that crashes (or hits a transient
	// 500) doesn't stall the test for the production-default 5 minutes.
	cfg.LockDuration = 3 * time.Second
	cfg.HeartbeatInterval = time.Second
	cfg.DequeueIdlePoll = 200 * time.Millisecond
	srv := api.NewServer(bus, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv.Bind(&api.Session{Store: db, IDGen: idgen.New()})

	sweeperCtx, sweeperCancel := context.WithCancel(context.Background())
	sweeper := api.NewSweeper(db, bus, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	go sweeper.Run(sweeperCtx)

	ts := httptest.NewServer(srv.Routes())
	t.Cleanup(func() {
		sweeperCancel()
		ts.Close()
		db.Close()
	})
	return &testNode{URL: ts.URL, DB: db}
}

// sseEvent is one parsed SSE message.
type sseEvent struct {
	ID    int64
	Event string
	Data  []byte
}

// readSSE reads SSE messages from r until the reader closes or the
// context fires. Heartbeat lines (": heartbeat") are skipped silently;
// every full event is sent on out. Closes out when no more events can
// arrive.
func readSSE(ctx context.Context, r io.Reader, out chan<- sseEvent) {
	defer close(out)
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 4096), 1<<20)

	var cur sseEvent
	for scanner.Scan() {
		if ctx.Err() != nil {
			return
		}
		line := scanner.Text()
		if line == "" {
			// Event boundary.
			if cur.Event != "" || len(cur.Data) > 0 {
				select {
				case out <- cur:
				case <-ctx.Done():
					return
				}
			}
			cur = sseEvent{}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue // comment / heartbeat
		}
		switch {
		case strings.HasPrefix(line, "id: "):
			if v, err := strconv.ParseInt(line[4:], 10, 64); err == nil {
				cur.ID = v
			}
		case strings.HasPrefix(line, "event: "):
			cur.Event = line[7:]
		case strings.HasPrefix(line, "data: "):
			if len(cur.Data) > 0 {
				cur.Data = append(cur.Data, '\n')
			}
			cur.Data = append(cur.Data, []byte(line[6:])...)
		}
	}
}

// enqueue posts one job and returns its server-generated id.
func enqueue(t *testing.T, base, lane, entity string, doc any) string {
	t.Helper()
	body, err := json.Marshal(protocol.EnqueueRequest{
		Lane:     lane,
		Entity:   entity,
		Document: mustMarshal(t, doc),
	})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(base+"/enqueue", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		buf, _ := io.ReadAll(resp.Body)
		t.Fatalf("enqueue %s: status %d body %s", entity, resp.StatusCode, buf)
	}
	var er protocol.EnqueueResponse
	if err := json.NewDecoder(resp.Body).Decode(&er); err != nil {
		t.Fatal(err)
	}
	return er.JobID
}

func mustMarshal(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// runWorker opens GET /dequeue/{lane}, claims jobs sequentially, and
// POSTs /jobs/{id}/done for each. Stops when ctx fires or the test
// signals via the shared remaining counter (last job consumed).
func runWorker(t *testing.T, ctx context.Context, base, lane string, claimed *atomic.Int64, target int64) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/dequeue/"+lane, nil)
	if err != nil {
		t.Errorf("worker: build req: %v", err)
		return
	}
	req.Header.Set("Accept", "text/event-stream")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		t.Errorf("worker: do: %v", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("worker: dequeue status %d", resp.StatusCode)
		return
	}

	events := make(chan sseEvent, 4)
	go readSSE(ctx, resp.Body, events)

	var workerID string
	// No client.Timeout — under contention SQLite write-lock waits can
	// easily exceed an arbitrary per-request budget, and the server is
	// still making progress. The outer ctx (60s) still bounds the test.
	client := &http.Client{}

	for ev := range events {
		switch ev.Event {
		case "welcome":
			var welcome struct {
				WorkerID string `json:"worker_id"`
			}
			if err := json.Unmarshal(ev.Data, &welcome); err != nil {
				t.Errorf("worker: parse welcome: %v", err)
				return
			}
			workerID = welcome.WorkerID

		case "job":
			var job protocol.DequeuedJob
			if err := json.Unmarshal(ev.Data, &job); err != nil {
				t.Errorf("worker: parse job: %v", err)
				return
			}

			// Transient 500s under heavy contention (SQLite busy_timeout
			// exhaustion) are acceptable here — the lock expires after
			// LockDuration and another worker reclaims. The test verifies
			// the final ground truth (CountByState) rather than every
			// per-worker round-trip succeeding.
			doneReq, _ := http.NewRequestWithContext(ctx, http.MethodPost,
				base+"/jobs/"+job.ID+"/done", strings.NewReader(`{}`))
			doneReq.Header.Set("X-Worker-ID", workerID)
			doneReq.Header.Set("Content-Type", "application/json")
			doneResp, err := client.Do(doneReq)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				t.Logf("worker %s: done: %v (continuing — lock will expire)", workerID, err)
				return
			}
			doneResp.Body.Close()
			if doneResp.StatusCode != http.StatusNoContent {
				t.Logf("worker %s: done status %d for %s (continuing — lock will expire)", workerID, doneResp.StatusCode, job.ID)
				return
			}

			if claimed.Add(1) >= target {
				return // we've drained the queue; caller will cancel
			}
		}
	}
}

func TestPhase1_Acceptance_1000Jobs10Workers(t *testing.T) {
	const (
		jobs    = 1000
		workers = 10
		lane    = "background"
	)
	node := startServer(t)

	// Enqueue all jobs first so workers don't have to wait for the
	// producer. Doing this serially exercises the same code path
	// concurrent enqueue would, with deterministic ordering.
	t.Logf("enqueueing %d jobs", jobs)
	enqueueStart := time.Now()
	for i := 0; i < jobs; i++ {
		enqueue(t, node.URL, lane, fmt.Sprintf("entity-%d", i), map[string]int{"i": i})
	}
	t.Logf("enqueued %d jobs in %s", jobs, time.Since(enqueueStart))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var claimed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runWorker(t, ctx, node.URL, lane, &claimed, int64(jobs))
		}()
	}

	// Workers exit on their own once the counter reaches `jobs`; we
	// cancel the rest explicitly so any worker still mid-stream tears
	// down promptly.
	deadline := time.After(45 * time.Second)
	for claimed.Load() < int64(jobs) {
		select {
		case <-deadline:
			t.Fatalf("did not drain within 45s; claimed=%d", claimed.Load())
		case <-time.After(50 * time.Millisecond):
		}
	}
	cancel()
	wg.Wait()

	// Final ground truth: read storage directly.
	counts, err := node.DB.CountByState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if counts[protocol.StateDone] != jobs {
		t.Errorf("done count = %d, want %d (full counts: %+v)", counts[protocol.StateDone], jobs, counts)
	}
	other := counts[protocol.StatePending] + counts[protocol.StateProcessing] +
		counts[protocol.StateFailed] + counts[protocol.StateReleased]
	if other != 0 {
		t.Errorf("non-done states present: %+v", counts)
	}
	t.Logf("final state: %+v (took %s incl. enqueue)", counts, time.Since(enqueueStart))
}
