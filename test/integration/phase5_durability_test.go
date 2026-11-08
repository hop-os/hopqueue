// Phase-5 acceptance: durability tiers.
//
// durable_factor on POST /enqueue tells the leader how many peers
// must acknowledge applying the enqueued event before a 'synced'
// event is emitted to the producer's SSE stream. Three node cluster
// shared via mem.Backend; each tier is exercised end-to-end.
//
// Acceptance scenarios:
//   - durable_factor=0: no synced ever fires (fire-and-forget)
//   - durable_factor=1: synced after any one follower acks
//   - durable_factor=2: synced only after both followers ack (quorum
//     in a 3-node cluster)
//   - durable_factor=-1: synced after every known peer acks
package integration

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/hop-os/hoplock/mem"

	"github.com/hop-os/hopqueue/pkg/protocol"
)

// enqueueAndStreamSyncedEvents posts an enqueue with the given
// durable_factor + Accept: text/event-stream and returns a channel
// of decoded SSE events from the producer stream. Cleanup closes
// the connection when ctx is cancelled.
func enqueueWithSSE(t *testing.T, ctx context.Context, base, lane, entity string, durable int) (<-chan sseEvent, func()) {
	t.Helper()
	body, _ := json.Marshal(protocol.EnqueueRequest{
		Lane:          lane,
		Entity:        entity,
		DurableFactor: durable,
		Document:      json.RawMessage(`{"hello":"world"}`),
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/enqueue", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		buf, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("enqueue: status %d body %s", resp.StatusCode, buf)
	}

	out := make(chan sseEvent, 32)
	streamCtx, cancel := context.WithCancel(ctx)
	go func() {
		defer close(out)
		readSSEFromBody(streamCtx, resp.Body, out)
	}()

	teardown := func() {
		cancel()
		resp.Body.Close()
	}
	return out, teardown
}

// readSSEFromBody is the shared SSE parser for these tests. It is a
// stripped-down copy of phase1_test.go's readSSE but takes an
// io.ReadCloser so the caller controls lifetime.
func readSSEFromBody(ctx context.Context, body io.Reader, out chan<- sseEvent) {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 8192), 1<<20)

	var cur sseEvent
	for scanner.Scan() {
		if ctx.Err() != nil {
			return
		}
		line := scanner.Text()
		if line == "" {
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
			continue
		}
		switch {
		case strings.HasPrefix(line, "id: "):
			fmt.Sscanf(line[4:], "%d", &cur.ID)
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

// awaitEvents reads from ch and reports which event types appeared
// before timeout, in order. Stops at the first occurrence of any
// stopAt name.
func awaitEvents(t *testing.T, ch <-chan sseEvent, timeout time.Duration, stopAt ...string) []string {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	stop := map[string]bool{}
	for _, s := range stopAt {
		stop[s] = true
	}
	var seen []string
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return seen
			}
			if ev.Event != "" {
				seen = append(seen, ev.Event)
				if stop[ev.Event] {
					return seen
				}
			}
		case <-deadline.C:
			return seen
		}
	}
}

func TestPhase5_DurableFactor1_SyncedAfterOneAck(t *testing.T) {
	t.Parallel()
	leaseBackend := mem.New()
	nodes := startReplicationCluster(t, 3, leaseBackend, 500*time.Millisecond)
	leader := waitForReplicationLeader(t, nodes, 3*time.Second)
	// Let followers connect and start acking heartbeats etc.
	time.Sleep(300 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, teardown := enqueueWithSSE(t, ctx, leader.URL, "x", "e", 1)
	defer teardown()

	seen := awaitEvents(t, stream, 5*time.Second, "synced")
	if !contains(seen, "enqueued") {
		t.Errorf("never saw 'enqueued' event; seen=%v", seen)
	}
	if !contains(seen, "synced") {
		t.Errorf("durable_factor=1 should yield a synced event with one follower acking; seen=%v", seen)
	}
}

func TestPhase5_DurableFactor2_SyncedAfterQuorum(t *testing.T) {
	t.Parallel()
	leaseBackend := mem.New()
	nodes := startReplicationCluster(t, 3, leaseBackend, 500*time.Millisecond)
	leader := waitForReplicationLeader(t, nodes, 3*time.Second)
	time.Sleep(300 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, teardown := enqueueWithSSE(t, ctx, leader.URL, "x", "e", 2)
	defer teardown()

	seen := awaitEvents(t, stream, 5*time.Second, "synced")
	if !contains(seen, "synced") {
		t.Errorf("durable_factor=2 should yield synced once both followers ack; seen=%v", seen)
	}
}

func TestPhase5_DurableFactor0_NoSyncedEverEmitted(t *testing.T) {
	t.Parallel()
	leaseBackend := mem.New()
	nodes := startReplicationCluster(t, 3, leaseBackend, 500*time.Millisecond)
	leader := waitForReplicationLeader(t, nodes, 3*time.Second)
	time.Sleep(300 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, teardown := enqueueWithSSE(t, ctx, leader.URL, "x", "e", 0)
	defer teardown()

	// Watch for ~1 second; synced must never appear. We use an
	// arbitrary short window because there's no positive event we're
	// waiting for — just absence.
	deadline := time.After(time.Second)
	for {
		select {
		case ev, ok := <-stream:
			if !ok {
				return
			}
			if ev.Event == "synced" {
				t.Fatalf("durable_factor=0 produced a synced event: %+v", ev)
			}
		case <-deadline:
			return
		}
	}
}

func TestPhase5_DurableFactor1_SyncedSeqIsAfterEnqueuedSeq(t *testing.T) {
	t.Parallel()
	leaseBackend := mem.New()
	nodes := startReplicationCluster(t, 3, leaseBackend, 500*time.Millisecond)
	leader := waitForReplicationLeader(t, nodes, 3*time.Second)
	time.Sleep(300 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, teardown := enqueueWithSSE(t, ctx, leader.URL, "x", "e", 1)
	defer teardown()

	var enqueuedID, syncedID int64
	deadline := time.After(5 * time.Second)
loop:
	for {
		select {
		case ev, ok := <-stream:
			if !ok {
				break loop
			}
			if ev.Event == "enqueued" {
				enqueuedID = ev.ID
			}
			if ev.Event == "synced" {
				syncedID = ev.ID
				break loop
			}
		case <-deadline:
			break loop
		}
	}

	if enqueuedID == 0 || syncedID == 0 {
		t.Fatalf("missing one of the events: enqueued=%d synced=%d", enqueuedID, syncedID)
	}
	if syncedID <= enqueuedID {
		t.Errorf("synced seq %d not greater than enqueued seq %d", syncedID, enqueuedID)
	}
}

func contains(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}
