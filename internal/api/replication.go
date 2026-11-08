package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/hop-os/hopqueue/internal/eventbus"
	"github.com/hop-os/hopqueue/internal/storage"
	"github.com/hop-os/hopqueue/pkg/protocol"
)

// replicationStreamBatch caps each replay batch read from
// replication_log. The leader streams in pages; very large logs don't
// pin a SQLite read for arbitrary durations.
const replicationStreamBatch = 256

// replicationStreamHeartbeat is the cadence of `: heartbeat` comments
// the leader emits when no new events are arriving. Followers use
// these to detect a stuck connection.
const replicationStreamHeartbeat = 5 * time.Second

// AckTracker holds per-follower watermarks: the highest seq each
// follower has acknowledged applying. The map is populated by
// /replication/ack and read by Phase-5 durability tracking. Zero value
// is usable.
type AckTracker struct {
	mu      sync.Mutex
	perNode map[string]int64
}

// Record advances nodeID's watermark to seq, never moving backwards.
func (t *AckTracker) Record(nodeID string, seq int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.perNode == nil {
		t.perNode = make(map[string]int64)
	}
	if seq > t.perNode[nodeID] {
		t.perNode[nodeID] = seq
	}
}

// Snapshot returns a copy of the per-follower watermarks. Cheap; the
// map is small (one entry per peer).
func (t *AckTracker) Snapshot() map[string]int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[string]int64, len(t.perNode))
	for k, v := range t.perNode {
		out[k] = v
	}
	return out
}

// CountAtOrAbove returns how many followers have acked at least seq.
// Used by Phase-5 durability tiers to decide when a job is "synced".
func (t *AckTracker) CountAtOrAbove(seq int64) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for _, v := range t.perNode {
		if v >= seq {
			n++
		}
	}
	return n
}

// Acks returns the ack tracker for this server. Callers wire it into
// Phase-5 sync logic.
func (s *Server) Acks() *AckTracker { return &s.acks }

// handleReplicationStream is the leader-side of the peer replication
// channel. The follower opens a long-lived SSE connection asking for
// events seq > since; the leader replays from replication_log then
// tails new events delivered through the eventbus.
//
// Internal-only endpoint — exposes raw replication payloads. Production
// deployments should keep this off the public network or behind a
// peer-only firewall.
func (s *Server) handleReplicationStream(w http.ResponseWriter, r *http.Request) {
	sess, ok := s.requireSession(w)
	if !ok {
		return
	}

	since := int64(0)
	if v := r.URL.Query().Get("since"); v != "" {
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil || parsed < 0 {
			http.Error(w, "invalid since", http.StatusBadRequest)
			return
		}
		since = parsed
	}

	// 410 horizon check: if the follower's "since" is below the leader's
	// MIN replog seq, the leader has retention-trimmed past the
	// follower's resume point. The follower must be re-initialised
	// manually; auto-recovery is too dangerous (we'd silently drop
	// events).
	minSeq, err := sess.Store.MinReplogSeq(r.Context())
	if err != nil {
		http.Error(w, "min seq lookup failed", http.StatusInternalServerError)
		return
	}
	if minSeq > 0 && since > 0 && since < minSeq-1 {
		w.Header().Set("X-Min-Available-Seq", strconv.FormatInt(minSeq, 10))
		http.Error(w, "follower is behind retention horizon", http.StatusGone)
		return
	}

	// Subscribe to new replog events BEFORE the historical replay so
	// nothing is missed in the window between SELECT and tail.
	tail, unsub := s.bus.SubscribeReplog()
	defer unsub()

	sse, err := newSSE(w)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	lastSent := since

	// Replay in pages until we've caught up to the live edge.
	for {
		batch, err := sess.Store.ReplogRowsSince(r.Context(), lastSent, replicationStreamBatch)
		if err != nil {
			s.logger.Error("replog replay failed", "err", err)
			return
		}
		if len(batch) == 0 {
			break
		}
		for _, row := range batch {
			if err := sendReplogEvent(sse, replogFromStorage(row)); err != nil {
				return
			}
			lastSent = row.Seq
		}
	}

	// Tail. Live events come in via the eventbus replog topic; we
	// dedupe against lastSent to handle a row that was both replayed
	// from disk AND broadcast on the bus during the replay window.
	heartbeat := time.NewTicker(replicationStreamHeartbeat)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			if err := sse.heartbeat(); err != nil {
				return
			}
		case row, ok := <-tail:
			if !ok {
				return
			}
			if row.Seq <= lastSent {
				continue
			}
			if err := sendReplogEvent(sse, row); err != nil {
				return
			}
			lastSent = row.Seq
		}
	}
}

// sendReplogEvent serialises a replog row in the wire format
// followers expect. The SSE id is the seq (so Last-Event-ID resume
// would work, though our follower uses an explicit ?since=). Event
// name is the user-facing type. data is the full ReplicationEvent JSON.
func sendReplogEvent(sse *sseWriter, row eventbus.ReplogRow) error {
	ev := protocol.ReplicationEvent{
		Seq:        row.Seq,
		JobID:      row.JobID,
		Type:       row.EventType,
		Payload:    row.Payload,
		OriginNode: row.OriginNode,
		CreatedAt:  row.CreatedAt,
	}
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	return sse.send(row.Seq, row.EventType, data)
}

// replogFromStorage projects a storage.Replog (as read from disk) into
// the bus type used on the live-tail path. Both shapes carry the same
// fields; the conversion exists only because the eventbus package
// can't import storage without a cycle.
func replogFromStorage(r storage.Replog) eventbus.ReplogRow {
	return eventbus.ReplogRow{
		Seq:        r.Seq,
		EventType:  r.EventType,
		JobID:      r.JobID,
		Payload:    r.Payload,
		CreatedAt:  r.CreatedAt,
		OriginNode: r.OriginNode,
	}
}

// handleReplicationAck records a follower's apply watermark. Body:
// {"node_id":"node-2","seq":12345}. The leader uses this to count
// how many peers have acked a given seq for Phase-5 durability.
func (s *Server) handleReplicationAck(w http.ResponseWriter, r *http.Request) {
	sess, ok := s.requireSession(w)
	if !ok {
		return
	}
	_ = sess // session is required (gated on leadership), but ack data goes to the in-memory tracker not storage

	var body struct {
		NodeID string `json:"node_id"`
		Seq    int64  `json:"seq"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, fmt.Sprintf("invalid json: %v", err), http.StatusBadRequest)
		return
	}
	if body.NodeID == "" || body.Seq < 0 {
		http.Error(w, "node_id and non-negative seq required", http.StatusBadRequest)
		return
	}
	s.acks.Record(body.NodeID, body.Seq)
	s.stats.Acks.Inc(body.NodeID)
	// Re-check pending durable enqueues; some of them may now have
	// crossed their threshold and need a 'synced' emit.
	s.checkSyncedJobs(r.Context(), sess)
	w.WriteHeader(http.StatusNoContent)
}

