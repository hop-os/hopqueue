package api

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/hop-os/hopqueue/internal/eventbus"
	"github.com/hop-os/hopqueue/pkg/protocol"
)

// pendingDurable tracks jobs that asked for durability confirmation
// (durable_factor != 0) but haven't yet hit their ack threshold. It
// lives on the api.Server because it needs access to both the
// AckTracker and the bus + storage to publish the 'synced' event.
//
// Lifecycle:
//   - handleEnqueue inserts after a successful Enqueue when
//     DurableFactor != 0.
//   - handleReplicationAck triggers a check of all in-flight entries
//     after recording the new watermark.
//   - Server.Unbind() clears the map: a node that loses leadership
//     is no longer responsible for these confirmations.
type pendingDurable struct {
	mu   sync.Mutex
	jobs map[int64]pendingDurableEntry // keyed by EnqueuedSeq
}

type pendingDurableEntry struct {
	JobID         string
	DurableFactor int
}

// markPendingDurable enrolls a job for synced-watch. Threshold is
// resolved here: durable_factor=N is N peers, durable_factor=-1 is
// "all currently-known peers" (snapshot at enrolment). 0 is a
// programming error — handleEnqueue filters those out before calling.
func (s *Server) markPendingDurable(seq int64, jobID string, durable int) {
	if durable == 0 {
		return
	}
	s.pending.mu.Lock()
	defer s.pending.mu.Unlock()
	if s.pending.jobs == nil {
		s.pending.jobs = make(map[int64]pendingDurableEntry)
	}
	s.pending.jobs[seq] = pendingDurableEntry{JobID: jobID, DurableFactor: durable}
}

// clearPendingDurable drops all in-flight durability watches. Called
// from Unbind so a demoted leader stops emitting synced for jobs the
// new leader has taken over.
func (s *Server) clearPendingDurable() {
	s.pending.mu.Lock()
	defer s.pending.mu.Unlock()
	s.pending.jobs = nil
}

// checkSyncedJobs scans pending entries against the current ack
// state and emits a 'synced' event for any that have hit their
// threshold. Called from handleReplicationAck after every recorded
// ack — cheap because the map is bounded by in-flight enqueue load.
func (s *Server) checkSyncedJobs(ctx context.Context, sess *Session) {
	if sess == nil {
		return
	}

	s.pending.mu.Lock()
	if len(s.pending.jobs) == 0 {
		s.pending.mu.Unlock()
		return
	}
	// Snapshot under the lock so the resolution work below doesn't
	// block other ack handlers waiting to record their watermarks.
	type ready struct {
		seq           int64
		jobID         string
		durable       int
		ackedBy       int
		acksSnapshot  int
	}
	var resolved []ready
	knownPeers := len(s.acks.Snapshot())
	for seq, pj := range s.pending.jobs {
		threshold := pj.DurableFactor
		if threshold == -1 {
			threshold = knownPeers
		}
		if threshold <= 0 {
			// durable_factor=-1 with no peers known yet — keep
			// waiting until at least one acks.
			continue
		}
		count := s.acks.CountAtOrAbove(seq)
		if count >= threshold {
			resolved = append(resolved, ready{
				seq: seq, jobID: pj.JobID, durable: pj.DurableFactor,
				ackedBy: count, acksSnapshot: knownPeers,
			})
			delete(s.pending.jobs, seq)
		}
	}
	s.pending.mu.Unlock()

	for _, r := range resolved {
		s.emitSynced(ctx, sess, r.seq, r.jobID, r.durable, r.ackedBy)
	}
}

// emitSynced writes a 'synced' replog row and publishes it on the
// bus. SSE producers tailing /events or /enqueue see the event in
// the same stream as 'enqueued', 'claimed', 'done' etc.
func (s *Server) emitSynced(ctx context.Context, sess *Session, enqueuedSeq int64, jobID string, durable, ackedBy int) {
	payload, err := json.Marshal(map[string]any{
		"enqueued_seq":   enqueuedSeq,
		"durable_factor": durable,
		"acked_by":       ackedBy,
	})
	if err != nil {
		s.logger.Error("synced: marshal payload", "err", err, "job", jobID)
		return
	}
	seq, data, err := sess.Store.MarkSynced(ctx, jobID, payload)
	if err != nil {
		s.logger.Error("synced: storage write", "err", err, "job", jobID)
		return
	}
	s.bus.PublishJob(eventbus.Event{
		ID:    seq,
		JobID: jobID,
		Type:  protocol.EventSynced,
		Data:  data,
	})
}
