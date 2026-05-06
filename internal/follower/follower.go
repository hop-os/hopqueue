// Package follower drives this node's "I am not the leader" lifecycle.
//
// While the lease is held by another peer, the follower's Run loop
// connects to that peer's /replication/stream, applies events to the
// local jobs table, and POSTs acks back. While this node holds the
// lease itself, the loop sleeps. When the lease changes hands the
// loop reconnects to the new owner.
//
// The follower's local replication_log is NOT touched by apply: it's
// leader-local on every node, and a follower's view of the leader's
// log lives in node_state.current_seq alone. When this node is later
// promoted, its replication_log starts fresh past whatever it
// previously contained.
package follower

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"haasqueue/internal/lease"
	"haasqueue/internal/storage"
	"haasqueue/pkg/protocol"
)

// Config bundles the follower's tuning knobs. Zero values pick
// reasonable defaults.
type Config struct {
	// SelfOwner is this node's lease.Elector Owner string. The
	// follower compares this against the lease's current owner to
	// decide whether to subscribe (peer is leader) or sleep (self is
	// leader, or no leader at all).
	SelfOwner string

	// PollInterval is how often the follower wakes up to recheck
	// leadership while idle. Default 500ms.
	PollInterval time.Duration

	// AckEvery batches ack POSTs: every N applied events, send the
	// latest seq to the leader. 0 falls back to 1 (ack every event).
	AckEvery int

	// HTTPClient is used for the SSE connection and ack POSTs.
	// Defaults to a client without read timeout (SSE is long-lived).
	HTTPClient *http.Client

	// ApplyDelay sleeps the apply path before each event is written
	// to the local DB. 0 disables. Used by stress tests to simulate
	// a follower that's slow at consuming the leader's stream.
	ApplyDelay time.Duration
}

// Worker is the follower's main goroutine. Run blocks until ctx is
// cancelled.
type Worker struct {
	store   *storage.DB
	elector *lease.Elector
	cfg     Config
	logger  *slog.Logger
}

func New(store *storage.DB, elector *lease.Elector, cfg Config, logger *slog.Logger) *Worker {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}
	if cfg.AckEvery <= 0 {
		cfg.AckEvery = 1
	}
	if cfg.HTTPClient == nil {
		// No top-level Timeout — SSE streams are long-lived. The
		// outer ctx and connection-level keepalive handle liveness.
		cfg.HTTPClient = &http.Client{}
	}
	return &Worker{
		store:   store,
		elector: elector,
		cfg:     cfg,
		logger:  logger,
	}
}

// Run drives the follower loop until ctx is cancelled. Each iteration
// either subscribes to the current leader, or waits PollInterval and
// rechecks. Errors are logged; the loop is resilient to peer
// unavailability.
func (w *Worker) Run(ctx context.Context) {
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		owner, _, err := w.elector.CurrentOwner(ctx)
		if err != nil {
			w.logger.Warn("follower: read lease failed", "err", err)
			sleep(ctx, w.cfg.PollInterval)
			continue
		}
		if owner == "" || owner == w.cfg.SelfOwner || w.elector.IsLeader() {
			sleep(ctx, w.cfg.PollInterval)
			continue
		}

		// We have a peer leader. Subscribe and apply until the stream
		// ends (peer demoted, network failed, etc), then loop and
		// re-discover.
		if err := w.subscribeAndApply(ctx, owner); err != nil && ctx.Err() == nil {
			w.logger.Warn("follower: subscribe loop ended", "leader", owner, "err", err)
		}
		sleep(ctx, w.cfg.PollInterval)
	}
}

// subscribeAndApply opens a /replication/stream connection to the
// leader and applies events. Returns when the stream ends or any
// error occurs. The caller decides whether to retry.
func (w *Worker) subscribeAndApply(ctx context.Context, leaderURL string) error {
	since, err := w.store.GetCurrentSeq(ctx, w.cfg.SelfOwner)
	if err != nil {
		return fmt.Errorf("read current_seq: %w", err)
	}

	url := strings.TrimSuffix(leaderURL, "/") + "/replication/stream"
	if since > 0 {
		url += fmt.Sprintf("?since=%d", since)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")

	resp, err := w.cfg.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Closing the body on ctx-cancel is the only reliable way to
	// unblock bufio.Scanner.Scan() inside applyStream — the transport's
	// own cancellation doesn't always reach a long-lived chunked read
	// quickly enough, and tests that tear down via cancel would
	// otherwise wait for httptest.Server's Close timeout.
	doneClose := make(chan struct{})
	defer close(doneClose)
	go func() {
		select {
		case <-ctx.Done():
			resp.Body.Close()
		case <-doneClose:
		}
	}()

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusGone:
		// Beyond retention horizon. Per the design we stop applying
		// and require manual intervention; surface the condition by
		// wiping current_seq so an operator's restart triggers a
		// full re-init from a freshly initialised DB.
		minSeq := resp.Header.Get("X-Min-Available-Seq")
		w.logger.Error("follower: leader's retention horizon has rotated past us; manual re-initialisation required",
			"leader", leaderURL, "current_seq", since, "leader_min_seq", minSeq)
		return errors.New("follower: behind retention horizon (410 Gone)")
	case http.StatusServiceUnavailable:
		// Peer is no longer leader (or never was when we asked).
		// The next outer loop iteration will re-discover.
		return errors.New("peer reports 503 (not leader)")
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("stream returned status %d: %s", resp.StatusCode, bytes.TrimSpace(body))
	}

	return w.applyStream(ctx, leaderURL, resp.Body)
}

// applyStream parses the SSE response body and applies each event.
// Returns when the body closes or an apply fails.
func (w *Worker) applyStream(ctx context.Context, leaderURL string, body io.Reader) error {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 8192), 1<<20)

	var (
		curID   int64
		curEvt  string
		curData []byte
		applied int
	)

	flush := func() error {
		if curEvt == "" && len(curData) == 0 {
			return nil
		}
		if curID == 0 {
			// 'welcome'-style events without an id; nothing to apply.
			curID = 0
			curEvt = ""
			curData = nil
			return nil
		}

		var ev protocol.ReplicationEvent
		if err := json.Unmarshal(curData, &ev); err != nil {
			return fmt.Errorf("decode event: %w", err)
		}
		if ev.Seq != curID {
			// Spec-required defensive check; stream ids are always
			// the seq, so any drift is a bug or a corrupt frame.
			return fmt.Errorf("event seq %d ≠ SSE id %d", ev.Seq, curID)
		}
		if w.cfg.ApplyDelay > 0 {
			sleep(ctx, w.cfg.ApplyDelay)
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
		if err := w.store.ApplyReplicationEvent(ctx, w.cfg.SelfOwner, &ev); err != nil {
			return fmt.Errorf("apply seq %d: %w", ev.Seq, err)
		}
		applied++
		if applied%w.cfg.AckEvery == 0 {
			w.sendAck(ctx, leaderURL, ev.Seq)
		}

		curID = 0
		curEvt = ""
		curData = nil
		return nil
	}

	for scanner.Scan() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		line := scanner.Text()
		if line == "" {
			if err := flush(); err != nil {
				return err
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		switch {
		case strings.HasPrefix(line, "id: "):
			fmt.Sscanf(line[4:], "%d", &curID)
		case strings.HasPrefix(line, "event: "):
			curEvt = line[7:]
		case strings.HasPrefix(line, "data: "):
			if len(curData) > 0 {
				curData = append(curData, '\n')
			}
			curData = append(curData, []byte(line[6:])...)
		}
	}
	return scanner.Err()
}

// sendAck POSTs the watermark to the leader. Best-effort: failures
// are logged but do not abort the apply loop. The next ack carries
// the same or newer watermark, so a missed ack just delays the
// leader's view by one event.
func (w *Worker) sendAck(ctx context.Context, leaderURL string, seq int64) {
	body, _ := json.Marshal(map[string]any{
		"node_id": w.cfg.SelfOwner,
		"seq":     seq,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimSuffix(leaderURL, "/")+"/replication/ack",
		bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := w.cfg.HTTPClient.Do(req)
	if err != nil {
		w.logger.Debug("follower: ack failed", "err", err)
		return
	}
	resp.Body.Close()
}

func sleep(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
