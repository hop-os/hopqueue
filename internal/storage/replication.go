package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/hop-os/hopqueue/pkg/protocol"
)

var timeNow = time.Now

// MinReplogSeq returns the lowest seq currently in replication_log,
// or 0 if the log is empty. Used by /replication/stream to decide
// whether a follower's "since" is still serviceable: if since <
// MinReplogSeq, the leader has rotated past the follower's horizon
// and must respond 410 Gone.
func (db *DB) MinReplogSeq(ctx context.Context) (int64, error) {
	var min sql.NullInt64
	err := db.QueryRowContext(ctx, `SELECT MIN(seq) FROM replication_log`).Scan(&min)
	if err != nil {
		return 0, err
	}
	if !min.Valid {
		return 0, nil
	}
	return min.Int64, nil
}

// MaxReplogSeq returns the highest seq currently in replication_log,
// or 0 if the log is empty. Used by promotion-verification to confirm
// no peer is ahead before this node starts accepting writes as leader.
func (db *DB) MaxReplogSeq(ctx context.Context) (int64, error) {
	var max sql.NullInt64
	err := db.QueryRowContext(ctx, `SELECT MAX(seq) FROM replication_log`).Scan(&max)
	if err != nil {
		return 0, err
	}
	if !max.Valid {
		return 0, nil
	}
	return max.Int64, nil
}

// ReplogRowsSince streams replication_log rows with seq > since, in
// seq order. limit caps the batch (0 = no limit). Used by the
// /replication/stream replay path.
func (db *DB) ReplogRowsSince(ctx context.Context, since int64, limit int) ([]Replog, error) {
	q := `
		SELECT seq, event_type, job_id, payload, created_at, origin_node
		  FROM replication_log
		 WHERE seq > ?
		 ORDER BY seq
	`
	args := []any{since}
	if limit > 0 {
		q += " LIMIT ?"
		args = append(args, limit)
	}
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Replog
	for rows.Next() {
		var r Replog
		var payload string
		if err := rows.Scan(&r.Seq, &r.EventType, &r.JobID, &payload, &r.CreatedAt, &r.OriginNode); err != nil {
			return nil, err
		}
		r.Payload = []byte(payload)
		out = append(out, r)
	}
	return out, rows.Err()
}

// GetCurrentSeq returns the latest applied replication seq for nodeID,
// or 0 if no row exists yet. The follower applier reads this on
// startup to know where to resume.
func (db *DB) GetCurrentSeq(ctx context.Context, nodeID string) (int64, error) {
	var seq sql.NullInt64
	err := db.QueryRowContext(ctx,
		`SELECT current_seq FROM node_state WHERE node_id = ?`, nodeID,
	).Scan(&seq)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if !seq.Valid {
		return 0, nil
	}
	return seq.Int64, nil
}

// SetCurrentSeq upserts node_state.current_seq for nodeID. Called by
// the follower applier after successfully applying an event.
func (db *DB) SetCurrentSeq(ctx context.Context, nodeID string, seq int64) error {
	_, err := db.ExecContext(ctx, `
		INSERT INTO node_state (node_id, current_seq)
		VALUES (?, ?)
		ON CONFLICT(node_id) DO UPDATE SET current_seq = excluded.current_seq
	`, nodeID, seq)
	return err
}

// ResetCurrentSeq sets the current_seq back to 0. Called when the
// follower detects it's connected to a different leader (different
// generation, or unrecognised stream) so a fresh replay starts from
// the beginning of the new leader's log.
func (db *DB) ResetCurrentSeq(ctx context.Context, nodeID string) error {
	return db.SetCurrentSeq(ctx, nodeID, 0)
}

// MarkSynced records that a job has reached its durable_factor
// threshold (the configured number of peers have acknowledged the
// 'enqueued' replog row). Writes a 'synced' row in replication_log,
// which the bus publisher fans out to SSE producers and to followers.
//
// payload is opaque per-event JSON; today we just record the
// confirmed seq, but Phase-5 callers may attach additional context
// (e.g. ack watermark snapshots) without breaking the wire format.
func (db *DB) MarkSynced(ctx context.Context, jobID string, payload []byte) (int64, []byte, error) {
	if len(payload) == 0 {
		payload = []byte(`{}`)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, nil, err
	}
	defer tx.Rollback()

	row, err := db.appendReplog(ctx, tx, protocol.EventSynced, jobID, string(payload), nowUnix())
	if err != nil {
		return 0, nil, err
	}
	if err := db.commitWithReplog(tx, row); err != nil {
		return 0, nil, err
	}
	return row.Seq, payload, nil
}

// nowUnix is a tiny indirection so tests could inject a clock later
// without rewriting this file. For now it just calls time.Now.
func nowUnix() int64 { return timeNow().Unix() }

// ApplyReplicationEvent is the follower's apply hook. Given a
// replication event from the leader, it mutates the local jobs table
// to reflect the leader's state and bumps node_state.current_seq.
//
// This method does NOT insert into the local replication_log — that
// table is leader-local on each node, and a follower's view of the
// leader's events lives in node_state.current_seq alone. When this
// node is later promoted to leader, its replication_log starts fresh
// (the next AUTOINCREMENT seq picks up after the local max).
//
// The apply paths are written to be idempotent so a duplicate event
// (replayed because of an ack-loss reconnect) leaves the row in the
// same final state.
func (db *DB) ApplyReplicationEvent(ctx context.Context, nodeID string, ev *protocol.ReplicationEvent) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := applyEvent(ctx, tx, ev); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO node_state (node_id, current_seq)
		VALUES (?, ?)
		ON CONFLICT(node_id) DO UPDATE SET current_seq = excluded.current_seq
	`, nodeID, ev.Seq); err != nil {
		return fmt.Errorf("update current_seq: %w", err)
	}

	return tx.Commit()
}

func applyEvent(ctx context.Context, tx *sql.Tx, ev *protocol.ReplicationEvent) error {
	switch ev.Type {
	case protocol.EventEnqueued:
		var p protocol.EnqueuedPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			return fmt.Errorf("decode enqueued: %w", err)
		}
		// INSERT OR IGNORE so a duplicate event (re-applied after a
		// reconnect) is a no-op rather than a primary-key error.
		_, err := tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO jobs
			    (id, lane, durable_factor, document, state, attempts,
			     available_at, created_at, updated_at)
			VALUES (?, ?, ?, ?, 'pending', 0, ?, ?, ?)
		`, ev.JobID, p.Lane, p.DurableFactor, string(p.Document), p.AvailableAt, p.CreatedAt, p.CreatedAt)
		return err

	case protocol.EventClaimed:
		var p protocol.ClaimedPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			return fmt.Errorf("decode claimed: %w", err)
		}
		_, err := tx.ExecContext(ctx, `
			UPDATE jobs
			   SET state = 'processing',
			       worker_id = ?,
			       locked_until = ?,
			       attempts = ?,
			       updated_at = ?
			 WHERE id = ?
		`, p.WorkerID, p.LockedUntil, p.Attempts, p.UpdatedAt, ev.JobID)
		return err

	case protocol.EventDone:
		var p protocol.DonePayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			return fmt.Errorf("decode done: %w", err)
		}
		var result any
		if len(p.Result) > 0 {
			result = string(p.Result)
		}
		_, err := tx.ExecContext(ctx, `
			UPDATE jobs
			   SET state = 'done',
			       result = ?,
			       locked_until = NULL,
			       completed_at = ?,
			       updated_at = ?
			 WHERE id = ?
		`, result, p.CompletedAt, p.UpdatedAt, ev.JobID)
		return err

	case protocol.EventFailed:
		var p protocol.FailedPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			return fmt.Errorf("decode failed: %w", err)
		}
		var errCol any
		if len(p.Error) > 0 {
			errCol = string(p.Error)
		}
		_, err := tx.ExecContext(ctx, `
			UPDATE jobs
			   SET state = 'failed',
			       error = ?,
			       locked_until = NULL,
			       completed_at = ?,
			       updated_at = ?
			 WHERE id = ?
		`, errCol, p.CompletedAt, p.UpdatedAt, ev.JobID)
		return err

	case protocol.EventReleased:
		var p protocol.ReleasedPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			return fmt.Errorf("decode released: %w", err)
		}
		_, err := tx.ExecContext(ctx, `
			UPDATE jobs
			   SET state = 'pending',
			       worker_id = NULL,
			       locked_until = NULL,
			       available_at = ?,
			       updated_at = ?
			 WHERE id = ?
		`, p.AvailableAt, p.UpdatedAt, ev.JobID)
		return err

	case protocol.EventDeleted:
		_, err := tx.ExecContext(ctx, `DELETE FROM jobs WHERE id = ?`, ev.JobID)
		return err

	case protocol.EventProgress, protocol.EventSynced:
		// No state change — these are pure events. Tracked via
		// node_state.current_seq alone.
		return nil

	default:
		return fmt.Errorf("apply: unknown event type %q", ev.Type)
	}
}

