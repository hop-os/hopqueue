package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/xinix00/hopqueue/internal/idgen"
	"github.com/xinix00/hopqueue/pkg/protocol"
)

// Sentinel errors. Handlers translate these to HTTP statuses; tests
// assert on them with errors.Is.
var (
	ErrNotFound     = errors.New("storage: job not found")
	ErrInvalidState = errors.New("storage: job not in expected state")
	ErrLockMismatch = errors.New("storage: worker does not hold the lock")
)

// NodeID is the identifier recorded in replication_log.origin_node for
// rows this node emits. Set once after Open via SetNodeID; defaults to
// "local" so tests that don't care about replication don't have to plumb
// a node id everywhere.
func (db *DB) SetNodeID(id string) {
	if id == "" {
		id = "local"
	}
	db.nodeID = id
}

// EnqueueResult is what Enqueue returns on success.
type EnqueueResult struct {
	JobID         string
	EnqueuedSeq   int64 // replication_log.seq of the 'enqueued' event
	DurableFactor int
}

// enqueueOp is one fully-prepared enqueue waiting for the batched
// writer to commit it. All fields are pre-validated and pre-marshaled
// by Enqueue before being placed on the channel; the writer just runs
// the two INSERTs.
type enqueueOp struct {
	jobID       string
	lane        string
	durable     int
	document    string // JSON, taken straight from req.Document
	availableAt int64
	now         int64
	payload     string // marshaled protocol.EnqueuedPayload
	resp        chan enqueueOpResult
}

type enqueueOpResult struct {
	seq int64
	err error
}

// Enqueue submits a new job to the batched writer and waits for the
// commit. From the caller's perspective it behaves exactly like the
// previous one-tx-per-call implementation; under the hood, multiple
// concurrent Enqueues are merged into a single transaction and a
// single fsync.
func (db *DB) Enqueue(ctx context.Context, req protocol.EnqueueRequest, gen *idgen.Generator) (EnqueueResult, error) {
	if req.Lane == "" {
		return EnqueueResult{}, fmt.Errorf("storage.Enqueue: lane required")
	}
	if len(req.Document) == 0 {
		return EnqueueResult{}, fmt.Errorf("storage.Enqueue: document required")
	}
	if req.Entity == "" {
		return EnqueueResult{}, fmt.Errorf("storage.Enqueue: entity required")
	}
	if err := idgen.ValidateEntity(req.Entity); err != nil {
		return EnqueueResult{}, err
	}

	jobID, err := gen.NewID(req.Entity)
	if err != nil {
		return EnqueueResult{}, fmt.Errorf("storage.Enqueue: id: %w", err)
	}

	now := time.Now().Unix()
	availableAt := req.AvailableAt
	if availableAt == 0 {
		availableAt = now
	}

	payload, err := json.Marshal(protocol.EnqueuedPayload{
		Lane:          req.Lane,
		DurableFactor: req.DurableFactor,
		Document:      req.Document,
		AvailableAt:   availableAt,
		CreatedAt:     now,
	})
	if err != nil {
		return EnqueueResult{}, err
	}

	op := enqueueOp{
		jobID:       jobID,
		lane:        req.Lane,
		durable:     req.DurableFactor,
		document:    string(req.Document),
		availableAt: availableAt,
		now:         now,
		payload:     string(payload),
		resp:        make(chan enqueueOpResult, 1),
	}

	select {
	case db.enqueueIn <- op:
	case <-ctx.Done():
		return EnqueueResult{}, ctx.Err()
	}

	select {
	case r := <-op.resp:
		if r.err != nil {
			return EnqueueResult{}, r.err
		}
		return EnqueueResult{JobID: jobID, EnqueuedSeq: r.seq, DurableFactor: req.DurableFactor}, nil
	case <-ctx.Done():
		return EnqueueResult{}, ctx.Err()
	}
}

// writerLoop is the single goroutine that owns the enqueue write path.
// On each iteration it blocks on an incoming op, then drains whatever
// else is sitting in the channel (up to a cap) into the same batch
// and commits them all in one transaction. While that commit is doing
// its fsync, more producers pile up on the channel; the next loop
// iteration scoops them all up. This naturally tunes batch size to
// load: idle = batch of 1, hot = batch of N.
func (db *DB) writerLoop() {
	defer close(db.writerDone)

	const maxBatch = 256
	for {
		var first enqueueOp
		select {
		case <-db.writerStop:
			return
		case first = <-db.enqueueIn:
		}

		batch := make([]enqueueOp, 0, maxBatch)
		batch = append(batch, first)
	drain:
		for len(batch) < maxBatch {
			select {
			case op := <-db.enqueueIn:
				batch = append(batch, op)
			default:
				break drain
			}
		}
		db.flushEnqueueBatch(batch)
	}
}

// flushEnqueueBatch commits one batch of enqueue ops in a single
// transaction. On error every op in the batch sees the same error;
// producers retry. The replog publisher is invoked once with all
// rows so SSE/replication subscribers get a single fanout per batch
// instead of N.
func (db *DB) flushEnqueueBatch(batch []enqueueOp) {
	ctx := context.Background()

	tx, err := db.DB.BeginTx(ctx, nil)
	if err != nil {
		for _, op := range batch {
			op.resp <- enqueueOpResult{err: fmt.Errorf("begin tx: %w", err)}
		}
		return
	}

	insertJob := tx.StmtContext(ctx, db.stmtInsertJob)
	insertReplog := tx.StmtContext(ctx, db.stmtInsertReplog)

	origin := db.NodeID()
	seqs := make([]int64, len(batch))
	rows := make([]Replog, 0, len(batch))

	var batchErr error
	for i, op := range batch {
		if _, err := insertJob.ExecContext(ctx,
			op.jobID, op.lane, op.durable, op.document,
			op.availableAt, op.now, op.now); err != nil {
			batchErr = fmt.Errorf("insert job: %w", err)
			break
		}
		var seq int64
		if err := insertReplog.QueryRowContext(ctx,
			protocol.EventEnqueued, op.jobID, op.payload, op.now, origin,
		).Scan(&seq); err != nil {
			batchErr = fmt.Errorf("insert replog: %w", err)
			break
		}
		seqs[i] = seq
		rows = append(rows, Replog{
			Seq:        seq,
			EventType:  protocol.EventEnqueued,
			JobID:      op.jobID,
			Payload:    []byte(op.payload),
			CreatedAt:  op.now,
			OriginNode: origin,
		})
	}

	if batchErr != nil {
		_ = tx.Rollback()
		for _, op := range batch {
			op.resp <- enqueueOpResult{err: batchErr}
		}
		return
	}

	if err := tx.Commit(); err != nil {
		for _, op := range batch {
			op.resp <- enqueueOpResult{err: fmt.Errorf("commit: %w", err)}
		}
		return
	}

	if cb := db.onReplog; cb != nil && len(rows) > 0 {
		cb(rows...)
	}
	for i, op := range batch {
		op.resp <- enqueueOpResult{seq: seqs[i]}
	}
}

// DequeueResult is what Dequeue returns when a job is claimed.
type DequeueResult struct {
	Job         protocol.DequeuedJob
	ClaimedSeq  int64
	ClaimedData []byte // JSON payload of the 'claimed' event, for bus fanout
}

// Dequeue atomically claims the next pending job on a lane for the given
// worker. Returns (result, true, nil) on success, (_, false, nil) when no
// job is available, or (_, false, err) on a real error.
//
// capabilities, when non-empty, restricts the candidate set to jobs whose
// document contains at least one of the listed substrings. Use this from
// a worker that can only handle a subset of the lane's jobs (e.g. a PDF
// renderer might pass ["pdf"] and skip "email" jobs on the same lane).
//
// _txlock=immediate makes the BeginTx serialise concurrent dequeues. The
// inner SELECT picks one row; the UPDATE with the same id either matches
// (we won) or finds zero rows because another transaction already moved
// the row to processing.
func (db *DB) Dequeue(ctx context.Context, lane, workerID string, lockDuration time.Duration, capabilities []string) (DequeueResult, bool, error) {
	now := time.Now()
	nowUnix := now.Unix()
	lockedUntil := now.Add(lockDuration).Unix()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return DequeueResult{}, false, err
	}
	defer tx.Rollback()

	// Build the SELECT clause. With no capability filter the WHERE is
	// straightforward; with capabilities we OR a list of INSTR()
	// checks against the document JSON. INSTR is a substring match,
	// not JSON-aware — capability strings should be unique tokens
	// unlikely to appear in unrelated parts of the document
	// (e.g. {"capability":"pdf"} not just {"task":"send pdf"}).
	whereClause := `lane = ? AND state = 'pending' AND available_at <= ?`
	args := []any{workerID, lockedUntil, nowUnix, lane, nowUnix}
	if len(capabilities) > 0 {
		ors := make([]string, 0, len(capabilities))
		for _, c := range capabilities {
			ors = append(ors, "INSTR(document, ?) > 0")
			args = append(args, c)
		}
		whereClause += " AND (" + strings.Join(ors, " OR ") + ")"
	}
	query := fmt.Sprintf(`
		UPDATE jobs
		   SET state = 'processing',
		       worker_id = ?,
		       locked_until = ?,
		       attempts = attempts + 1,
		       updated_at = ?
		 WHERE id = (
		   SELECT id FROM jobs
		    WHERE %s
		    ORDER BY available_at, id
		    LIMIT 1
		 )
		 RETURNING id, document, attempts
	`, whereClause)

	var (
		jobID    string
		document string
		attempts int64
	)
	err = tx.QueryRowContext(ctx, query, args...).Scan(&jobID, &document, &attempts)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return DequeueResult{}, false, nil
		}
		return DequeueResult{}, false, fmt.Errorf("claim job: %w", err)
	}

	payload, err := json.Marshal(protocol.ClaimedPayload{
		WorkerID:    workerID,
		LockedUntil: lockedUntil,
		Attempts:    attempts,
		UpdatedAt:   nowUnix,
	})
	if err != nil {
		return DequeueResult{}, false, err
	}
	row, err := db.appendReplog(ctx, tx, protocol.EventClaimed, jobID, string(payload), nowUnix)
	if err != nil {
		return DequeueResult{}, false, err
	}

	if err := db.commitWithReplog(tx, row); err != nil {
		return DequeueResult{}, false, err
	}

	return DequeueResult{
		Job: protocol.DequeuedJob{
			ID:               jobID,
			Lane:             lane,
			Document:         json.RawMessage(document),
			Attempts:         attempts,
			FirstAttemptedAt: now.UTC().Format(time.RFC3339),
		},
		ClaimedSeq:  row.Seq,
		ClaimedData: payload,
	}, true, nil
}

// Done transitions a processing job to done. Returns the seq and JSON
// payload of the 'done' replication_log row, both used for SSE fanout.
func (db *DB) Done(ctx context.Context, jobID, workerID string, result []byte) (int64, []byte, error) {
	now := time.Now().Unix()
	payload, err := json.Marshal(protocol.DonePayload{
		Result:      json.RawMessage(result),
		CompletedAt: now,
		UpdatedAt:   now,
	})
	if err != nil {
		return 0, nil, err
	}
	return db.terminate(ctx, jobID, workerID, protocol.StateDone, protocol.EventDone, "result", result, payload, now)
}

// Fail transitions a processing job to failed.
func (db *DB) Fail(ctx context.Context, jobID, workerID string, errBody []byte) (int64, []byte, error) {
	now := time.Now().Unix()
	payload, err := json.Marshal(protocol.FailedPayload{
		Error:       json.RawMessage(errBody),
		CompletedAt: now,
		UpdatedAt:   now,
	})
	if err != nil {
		return 0, nil, err
	}
	return db.terminate(ctx, jobID, workerID, protocol.StateFailed, protocol.EventFailed, "error", errBody, payload, now)
}

// terminate is the shared body of Done and Fail. column is the jobs
// column to set ("result" or "error"); columnValue is its raw bytes.
// eventPayload is the structured replication payload, already marshaled.
func (db *DB) terminate(ctx context.Context, jobID, workerID, newState, eventType, column string, columnValue, eventPayload []byte, now int64) (int64, []byte, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, nil, err
	}
	defer tx.Rollback()

	var columnArg any
	if len(columnValue) > 0 {
		columnArg = string(columnValue)
	}

	res, err := tx.ExecContext(ctx, fmt.Sprintf(`
		UPDATE jobs
		   SET state = ?,
		       %s = ?,
		       locked_until = NULL,
		       completed_at = ?,
		       updated_at = ?
		 WHERE id = ?
		   AND state = 'processing'
		   AND worker_id = ?
	`, column), newState, columnArg, now, now, jobID, workerID)
	if err != nil {
		return 0, nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return 0, nil, diagnoseTerminalFailureTx(ctx, tx, jobID, workerID)
	}

	row, err := db.appendReplog(ctx, tx, eventType, jobID, string(eventPayload), now)
	if err != nil {
		return 0, nil, err
	}
	if err := db.commitWithReplog(tx, row); err != nil {
		return 0, nil, err
	}
	return row.Seq, eventPayload, nil
}

// Release puts a processing job back to pending after the given delay.
// attempts is not reset — the count is the worker's record of having
// tried, regardless of outcome.
func (db *DB) Release(ctx context.Context, jobID, workerID string, delay time.Duration) (int64, []byte, error) {
	now := time.Now()
	nowUnix := now.Unix()
	availableAt := now.Add(delay).Unix()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, nil, err
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, `
		UPDATE jobs
		   SET state = 'pending',
		       worker_id = NULL,
		       locked_until = NULL,
		       available_at = ?,
		       updated_at = ?
		 WHERE id = ?
		   AND state = 'processing'
		   AND worker_id = ?
	`, availableAt, nowUnix, jobID, workerID)
	if err != nil {
		return 0, nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return 0, nil, diagnoseTerminalFailureTx(ctx, tx, jobID, workerID)
	}

	payload, err := json.Marshal(protocol.ReleasedPayload{
		AvailableAt: availableAt,
		UpdatedAt:   nowUnix,
	})
	if err != nil {
		return 0, nil, err
	}
	row, err := db.appendReplog(ctx, tx, protocol.EventReleased, jobID, string(payload), nowUnix)
	if err != nil {
		return 0, nil, err
	}
	if err := db.commitWithReplog(tx, row); err != nil {
		return 0, nil, err
	}
	return row.Seq, payload, nil
}

// Progress emits a progress event without changing job state. Worker
// must hold the current lock — otherwise ErrLockMismatch.
func (db *DB) Progress(ctx context.Context, jobID, workerID string, data []byte) (int64, []byte, error) {
	var state, currentWorker sql.NullString
	if err := db.QueryRowContext(ctx,
		`SELECT state, worker_id FROM jobs WHERE id = ?`, jobID,
	).Scan(&state, &currentWorker); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil, ErrNotFound
		}
		return 0, nil, err
	}
	if state.String != protocol.StateProcessing {
		return 0, nil, ErrInvalidState
	}
	if currentWorker.String != workerID {
		return 0, nil, ErrLockMismatch
	}
	if len(data) == 0 {
		data = []byte(`{}`)
	}
	now := time.Now().Unix()
	payload, err := json.Marshal(protocol.ProgressPayload{Data: json.RawMessage(data)})
	if err != nil {
		return 0, nil, err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, nil, err
	}
	defer tx.Rollback()

	row, err := db.appendReplog(ctx, tx, protocol.EventProgress, jobID, string(payload), now)
	if err != nil {
		return 0, nil, err
	}
	if err := db.commitWithReplog(tx, row); err != nil {
		return 0, nil, err
	}
	return row.Seq, payload, nil
}

// SweepExpired is a single job whose lock expired and was released.
type SweepExpired struct {
	JobID     string
	Lane      string
	Seq       int64
	EventData []byte
}

// SweepExpiredLocks releases processing jobs whose locks have expired
// back to pending. Returns one SweepExpired per affected job, with the
// replication_log row seq + payload that the caller should fan out on
// the eventbus. Empty result = nothing to do (the common case).
func (db *DB) SweepExpiredLocks(ctx context.Context) ([]SweepExpired, error) {
	now := time.Now().Unix()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		UPDATE jobs
		   SET state = 'pending',
		       worker_id = NULL,
		       locked_until = NULL,
		       available_at = ?,
		       updated_at = ?
		 WHERE state = 'processing'
		   AND locked_until IS NOT NULL
		   AND locked_until < ?
		 RETURNING id, lane
	`, now, now, now)
	if err != nil {
		return nil, err
	}
	var out []SweepExpired
	for rows.Next() {
		var s SweepExpired
		if err := rows.Scan(&s.JobID, &s.Lane); err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	rowsAdded := make([]Replog, 0, len(out))
	for i := range out {
		payload, err := json.Marshal(protocol.ReleasedPayload{
			AvailableAt: now,
			Reason:      "lock_expired",
			UpdatedAt:   now,
		})
		if err != nil {
			return nil, err
		}
		row, err := db.appendReplog(ctx, tx, protocol.EventReleased, out[i].JobID, string(payload), now)
		if err != nil {
			return nil, err
		}
		out[i].Seq = row.Seq
		out[i].EventData = payload
		rowsAdded = append(rowsAdded, row)
	}

	if err := db.commitWithReplog(tx, rowsAdded...); err != nil {
		return nil, err
	}
	return out, nil
}

// Replog is a row from replication_log.
type Replog struct {
	Seq        int64
	EventType  string
	JobID      string
	Payload    []byte
	CreatedAt  int64
	OriginNode string
}

// EventsSince returns replication_log rows for the given job with
// seq > sinceSeq, in seq order. Used by the SSE replay path.
func (db *DB) EventsSince(ctx context.Context, jobID string, sinceSeq int64) ([]Replog, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT seq, event_type, job_id, payload, created_at, origin_node
		  FROM replication_log
		 WHERE job_id = ? AND seq > ?
		 ORDER BY seq
	`, jobID, sinceSeq)
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

// JobInfo is the subset of job state needed by handlers for replay logic.
type JobInfo struct {
	ID    string
	Lane  string
	State string
}

// GetJob returns a JobInfo or ErrNotFound.
func (db *DB) GetJob(ctx context.Context, jobID string) (JobInfo, error) {
	var info JobInfo
	err := db.QueryRowContext(ctx,
		`SELECT id, lane, state FROM jobs WHERE id = ?`, jobID,
	).Scan(&info.ID, &info.Lane, &info.State)
	if errors.Is(err, sql.ErrNoRows) {
		return info, ErrNotFound
	}
	return info, err
}

// JobLane is the cheap lane-only lookup used by handlers that need to
// signal a lane after a state change.
func (db *DB) JobLane(ctx context.Context, jobID string) (string, error) {
	var lane string
	err := db.QueryRowContext(ctx,
		`SELECT lane FROM jobs WHERE id = ?`, jobID,
	).Scan(&lane)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	return lane, err
}

// DeletedJob describes one job pruned by retention. The Seq + EventData
// fields are the synthetic 'deleted' replication_log row written before
// the DELETE — followers consume them to prune in lockstep.
type DeletedJob struct {
	JobID     string
	Lane      string
	Seq       int64
	EventData []byte
}

// DeleteJobsForRetention removes terminal jobs older than the given
// thresholds and writes a 'deleted' replication_log row for each. Pass
// 0 (or negative) for either age to skip that state. The whole pass
// runs in a single transaction; partial pruning cannot leave a job
// without its deletion event or vice versa.
func (db *DB) DeleteJobsForRetention(ctx context.Context, doneAge, failedAge time.Duration) ([]DeletedJob, error) {
	now := time.Now().Unix()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var out []DeletedJob
	var allRows []Replog
	for _, target := range []struct {
		state  string
		age    time.Duration
		reason string
	}{
		{protocol.StateDone, doneAge, "retention_done"},
		{protocol.StateFailed, failedAge, "retention_failed"},
	} {
		if target.age <= 0 {
			continue
		}
		cutoff := now - int64(target.age.Seconds())
		rows, err := tx.QueryContext(ctx, `
			DELETE FROM jobs
			 WHERE state = ? AND completed_at IS NOT NULL AND completed_at < ?
			 RETURNING id, lane
		`, target.state, cutoff)
		if err != nil {
			return nil, fmt.Errorf("delete %s: %w", target.state, err)
		}
		batch := make([]DeletedJob, 0)
		for rows.Next() {
			var d DeletedJob
			if err := rows.Scan(&d.JobID, &d.Lane); err != nil {
				rows.Close()
				return nil, err
			}
			batch = append(batch, d)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()

		payload, err := json.Marshal(protocol.DeletedPayload{Reason: target.reason})
		if err != nil {
			return nil, err
		}
		for i := range batch {
			row, err := db.appendReplog(ctx, tx, protocol.EventDeleted, batch[i].JobID, string(payload), now)
			if err != nil {
				return nil, err
			}
			batch[i].Seq = row.Seq
			batch[i].EventData = payload
			allRows = append(allRows, row)
		}
		out = append(out, batch...)
	}

	if err := db.commitWithReplog(tx, allRows...); err != nil {
		return nil, err
	}
	return out, nil
}

// TrimReplicationLog deletes replication_log rows whose created_at is
// older than the given duration. Returns the number of rows removed.
//
// The retention horizon for replication_log determines how stale a
// follower can be before requiring manual re-initialisation: if a
// follower is more than this duration behind, the leader can no longer
// serve its catch-up.
func (db *DB) TrimReplicationLog(ctx context.Context, olderThan time.Duration) (int64, error) {
	if olderThan <= 0 {
		return 0, nil
	}
	cutoff := time.Now().Unix() - int64(olderThan.Seconds())
	res, err := db.ExecContext(ctx,
		`DELETE FROM replication_log WHERE created_at < ?`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("trim replication_log: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// CountByLaneState returns job counts grouped by (lane, state). The
// dashboard uses this to populate per-lane "pending / processing"
// rows. Empty states are omitted.
func (db *DB) CountByLaneState(ctx context.Context) (map[string]map[string]int, error) {
	rows, err := db.QueryContext(ctx, `SELECT lane, state, COUNT(*) FROM jobs GROUP BY lane, state`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]map[string]int)
	for rows.Next() {
		var lane, state string
		var n int
		if err := rows.Scan(&lane, &state, &n); err != nil {
			return nil, err
		}
		if out[lane] == nil {
			out[lane] = make(map[string]int)
		}
		out[lane][state] = n
	}
	return out, rows.Err()
}

// ReplogStats captures the size + age of the replication log for the
// dashboard's Replication section.
type ReplogStats struct {
	Count    int64
	MinSeq   int64
	MaxSeq   int64
	OldestAt int64 // unix seconds; 0 if log is empty
	NewestAt int64
}

// ReplogStatsSnapshot returns one-shot stats. Cheap — three small
// aggregate queries.
func (db *DB) ReplogStatsSnapshot(ctx context.Context) (ReplogStats, error) {
	var st ReplogStats
	row := db.QueryRowContext(ctx, `
		SELECT COUNT(*), COALESCE(MIN(seq), 0), COALESCE(MAX(seq), 0),
		       COALESCE(MIN(created_at), 0), COALESCE(MAX(created_at), 0)
		  FROM replication_log
	`)
	if err := row.Scan(&st.Count, &st.MinSeq, &st.MaxSeq, &st.OldestAt, &st.NewestAt); err != nil {
		return ReplogStats{}, err
	}
	return st, nil
}

// CountByState returns counts grouped by state — useful for the status
// page and the integration test's final tally.
func (db *DB) CountByState(ctx context.Context) (map[string]int, error) {
	rows, err := db.QueryContext(ctx, `SELECT state, COUNT(*) FROM jobs GROUP BY state`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]int)
	for rows.Next() {
		var state string
		var n int
		if err := rows.Scan(&state, &n); err != nil {
			return nil, err
		}
		out[state] = n
	}
	return out, rows.Err()
}

// appendReplog inserts a replication_log row in the given transaction
// and returns the materialised Replog struct (so callers can collect
// it for the post-commit publish). Centralising this makes it
// impossible to produce a state change without a corresponding replog
// entry.
//
// Uses RETURNING seq so we get the new seq in the same round-trip as
// the INSERT, and binds the prepared insert stmt to tx via StmtContext
// so the SQL is parsed once at Open rather than on every call.
func (db *DB) appendReplog(ctx context.Context, tx *sql.Tx, eventType, jobID, payload string, now int64) (Replog, error) {
	origin := db.NodeID()
	var seq int64
	if err := tx.StmtContext(ctx, db.stmtInsertReplog).QueryRowContext(ctx,
		eventType, jobID, payload, now, origin,
	).Scan(&seq); err != nil {
		return Replog{}, fmt.Errorf("append replication_log %s/%s: %w", eventType, jobID, err)
	}
	return Replog{
		Seq:        seq,
		EventType:  eventType,
		JobID:      jobID,
		Payload:    []byte(payload),
		CreatedAt:  now,
		OriginNode: origin,
	}, nil
}

// commitWithReplog commits tx and, on success, fans the appended
// replog rows out to OnReplog (if configured). On commit failure the
// callback is not invoked, ensuring followers never see events that
// were rolled back at the source.
func (db *DB) commitWithReplog(tx *sql.Tx, rows ...Replog) error {
	if err := tx.Commit(); err != nil {
		return err
	}
	if cb := db.onReplog; cb != nil && len(rows) > 0 {
		cb(rows...)
	}
	return nil
}

// diagnoseTerminalFailureTx is called when an UPDATE on a 'processing'
// row affects zero rows. It uses the open transaction so :memory: tests
// (which serialise on a single connection) don't deadlock.
func diagnoseTerminalFailureTx(ctx context.Context, tx *sql.Tx, jobID, workerID string) error {
	var state, currentWorker sql.NullString
	if err := tx.QueryRowContext(ctx,
		`SELECT state, worker_id FROM jobs WHERE id = ?`, jobID,
	).Scan(&state, &currentWorker); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	if state.String != protocol.StateProcessing {
		return ErrInvalidState
	}
	if currentWorker.String != workerID {
		return ErrLockMismatch
	}
	return ErrInvalidState
}
