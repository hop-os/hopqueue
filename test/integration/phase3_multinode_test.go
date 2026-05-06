// Phase-3 acceptance: leader election with three in-process nodes
// sharing a haaslock/mem.Backend. The mem backend simulates the cluster
// contention an S3 lease would arbitrate, without touching any real
// network — the queue's leadership-binding logic is what's under test.
//
// Each node has its own SQLite DB and HTTP server (since real clusters
// have independent storage). The lease-election layer is the only
// shared resource. Without Phase-4 replication, only the leader's DB
// receives writes; followers respond 503 + X-Active-Leader.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"haaslock"
	"haaslock/mem"

	"haasqueue/internal/api"
	"haasqueue/internal/eventbus"
	"haasqueue/internal/idgen"
	"haasqueue/internal/lease"
	"haasqueue/internal/storage"
	"haasqueue/pkg/protocol"
)

// clusterNode is one in-process haasqueue node, with its own storage,
// HTTP server, and lease elector. The Cancel function tears the node
// down; all goroutines exit before it returns.
type clusterNode struct {
	NodeID  string
	URL     string
	Cancel  context.CancelFunc
	Done    chan struct{}
	server  *api.Server
	wg      *sync.WaitGroup
	elector *lease.Elector
}

// IsLeader is a snapshot — the node may transition between the check
// and any subsequent action.
func (n *clusterNode) IsLeader() bool { return n.elector.IsLeader() }

// startCluster spins up `n` in-process nodes, all sharing leaseBackend
// for haaslock-driven election. Each node runs its own storage in a
// per-node temp dir.
func startCluster(t *testing.T, n int, leaseBackend haaslock.Backend, ttl time.Duration) []*clusterNode {
	t.Helper()
	out := make([]*clusterNode, 0, n)
	for i := 0; i < n; i++ {
		nodeID := fmt.Sprintf("node-%d", i+1)
		out = append(out, startNode(t, nodeID, leaseBackend, ttl))
	}
	t.Cleanup(func() {
		for _, node := range out {
			node.Cancel()
			<-node.Done
		}
	})
	return out
}

func startNode(t *testing.T, nodeID string, leaseBackend haaslock.Backend, ttl time.Duration) *clusterNode {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "queue.db")
	db, err := storage.Open(context.Background(), dbPath)
	if err != nil {
		t.Fatal(err)
	}
	db.SetNodeID(nodeID)

	bus := eventbus.New()
	cfg := api.DefaultConfig()
	cfg.LockDuration = 3 * time.Second
	cfg.HeartbeatInterval = 500 * time.Millisecond
	cfg.DequeueIdlePoll = 200 * time.Millisecond
	srv := api.NewServer(bus, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	session := &api.Session{Store: db, IDGen: idgen.New()}

	elector, err := lease.New(lease.Config{
		NodeID:  nodeID,
		Backend: leaseBackend,
		TTL:     ttl,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = elector.Lead(ctx, func(leaderCtx context.Context, l haaslock.Lease) error {
			srv.Bind(session)
			defer srv.Unbind()
			// Run a sweeper to support tests that exercise
			// release-on-lock-expiry across leadership changes.
			sweepCtx, sweepCancel := context.WithCancel(leaderCtx)
			defer sweepCancel()
			go api.NewSweeper(db, bus, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil))).Run(sweepCtx)
			<-leaderCtx.Done()
			return nil
		})
	}()

	ts := httptest.NewServer(srv.Routes())

	go func() {
		wg.Wait()
		ts.Close()
		db.Close()
		close(done)
	}()

	return &clusterNode{
		NodeID:  nodeID,
		URL:     ts.URL,
		Cancel:  cancel,
		Done:    done,
		server:  srv,
		wg:      &wg,
		elector: elector,
	}
}

// waitForLeader returns the node currently bound (and therefore
// accepting writes), or fatals after deadline.
func waitForLeader(t *testing.T, nodes []*clusterNode, timeout time.Duration) *clusterNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.IsLeader() {
				return n
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no node became leader within %s", timeout)
	return nil
}

// waitForLeaderExcept waits until any node other than skip is leader.
// Used to assert failover after the previous leader exits.
func waitForLeaderExcept(t *testing.T, nodes []*clusterNode, skip *clusterNode, timeout time.Duration) *clusterNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n != skip && n.IsLeader() {
				return n
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no successor leader within %s", timeout)
	return nil
}

// postEnqueue is a tiny helper for the multi-node tests that doesn't
// share the elaborate SSE-aware machinery of the Phase-1 test.
func postEnqueue(t *testing.T, base, lane, entity string) (status int, jobID string) {
	t.Helper()
	body, _ := json.Marshal(protocol.EnqueueRequest{
		Lane: lane, Entity: entity, Document: json.RawMessage(`{}`),
	})
	resp, err := http.Post(base+"/enqueue", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		var er protocol.EnqueueResponse
		if err := json.NewDecoder(resp.Body).Decode(&er); err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, er.JobID
	}
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, ""
}

func TestPhase3_ExactlyOneLeaderInThreeNodes(t *testing.T) {
	t.Parallel()
	leaseBackend := mem.New()
	nodes := startCluster(t, 3, leaseBackend, 500*time.Millisecond)

	leader := waitForLeader(t, nodes, 3*time.Second)
	t.Logf("initial leader: %s", leader.NodeID)

	// At least 200ms after acquisition, exactly one node still reports
	// leadership — i.e. nobody else acquired the lease in parallel.
	time.Sleep(200 * time.Millisecond)
	leaders := 0
	for _, n := range nodes {
		if n.IsLeader() {
			leaders++
		}
	}
	if leaders != 1 {
		t.Errorf("got %d leaders, want exactly 1", leaders)
	}
}

func TestPhase3_LeaderAcceptsWritesFollowersReturn503(t *testing.T) {
	t.Parallel()
	leaseBackend := mem.New()
	nodes := startCluster(t, 3, leaseBackend, 500*time.Millisecond)
	leader := waitForLeader(t, nodes, 3*time.Second)

	// Leader accepts.
	if status, id := postEnqueue(t, leader.URL, "x", "e1"); status != http.StatusOK || id == "" {
		t.Errorf("leader %s: enqueue status=%d id=%q", leader.NodeID, status, id)
	}

	// Followers return 503.
	for _, n := range nodes {
		if n == leader {
			continue
		}
		status, _ := postEnqueue(t, n.URL, "x", "e1")
		if status != http.StatusServiceUnavailable {
			t.Errorf("follower %s: enqueue status=%d, want 503", n.NodeID, status)
		}
	}
}

func TestPhase3_FailoverAfterLeaderExits(t *testing.T) {
	t.Parallel()
	leaseBackend := mem.New()
	nodes := startCluster(t, 3, leaseBackend, 500*time.Millisecond)

	leader := waitForLeader(t, nodes, 3*time.Second)
	t.Logf("first leader: %s", leader.NodeID)

	// Cancel the leader's context. haaslock's Lead wrapper releases
	// the lease cleanly on shutdown so a successor can acquire fresh
	// rather than waiting for TTL expiry.
	leader.Cancel()
	<-leader.Done

	successor := waitForLeaderExcept(t, nodes, leader, 5*time.Second)
	t.Logf("successor leader: %s", successor.NodeID)

	// Successor accepts writes.
	if status, id := postEnqueue(t, successor.URL, "x", "e2"); status != http.StatusOK || id == "" {
		t.Errorf("successor %s: enqueue status=%d id=%q", successor.NodeID, status, id)
	}
}

// noDeleteBackend wraps a haaslock.Backend and turns Delete into a
// no-op. Tests use this to simulate a leader that crashes hard
// (without cooperative release) so the successor takes over via the
// expired-lease path and the generation bumps.
type noDeleteBackend struct{ haaslock.Backend }

func (b *noDeleteBackend) Delete(ctx context.Context, handle string) error {
	return nil
}

func TestPhase3_GenerationBumpsOnCrashFailover(t *testing.T) {
	t.Parallel()
	// Hard-kill simulation: Delete is suppressed so the lease record
	// stays in place. The successor only takes over after TTL expiry,
	// which is the takeover path that bumps generation.
	leaseBackend := &noDeleteBackend{Backend: mem.New()}
	nodes := startCluster(t, 3, leaseBackend, 300*time.Millisecond)

	first := waitForLeader(t, nodes, 3*time.Second)
	gen1 := first.elector.CurrentGeneration()
	if gen1 == 0 {
		t.Fatalf("first leader %s reports generation 0", first.NodeID)
	}

	first.Cancel()
	<-first.Done

	// Have to wait at least TTL (300ms) before successor can take over.
	successor := waitForLeaderExcept(t, nodes, first, 5*time.Second)
	gen2 := successor.elector.CurrentGeneration()
	if gen2 <= gen1 {
		t.Errorf("successor generation %d not greater than predecessor %d", gen2, gen1)
	}
}
