// Package protocol defines the wire types shared between server and clients,
// and the constant string values used as job states and SSE event types.
package protocol

import "encoding/json"

// Job lifecycle states. Stored in jobs.state and validated by a CHECK
// constraint in the schema.
const (
	StatePending    = "pending"
	StateProcessing = "processing"
	StateDone       = "done"
	StateFailed     = "failed"
	StateReleased   = "released"
)

// SSE event types and replication_log.event_type values. The same strings
// flow over both channels — a row in replication_log replays directly as
// an SSE event without translation.
const (
	EventEnqueued = "enqueued"
	EventClaimed  = "claimed"
	EventProgress = "progress"
	EventDone     = "done"
	EventFailed   = "failed"
	EventReleased = "released"
	EventSynced   = "synced"
	EventDeleted  = "deleted"
)

// IsTerminal reports whether eventType is the last event a producer or
// /events subscriber will see for a given job. Once a terminal event is
// observed, the SSE stream may close.
func IsTerminal(eventType string) bool {
	switch eventType {
	case EventDone, EventFailed, EventReleased:
		return true
	}
	return false
}

// EnqueueRequest is the body of POST /enqueue.
type EnqueueRequest struct {
	Lane          string          `json:"lane"`
	DurableFactor int             `json:"durable_factor"`
	Entity        string          `json:"entity"`
	Document      json.RawMessage `json:"document"`
	AvailableAt   int64           `json:"available_at,omitempty"`
}

// EnqueueResponse is the body of a POST /enqueue with Accept: application/json.
type EnqueueResponse struct {
	JobID  string `json:"jobId"`
	Status string `json:"status"`
}

type DoneRequest struct {
	Result json.RawMessage `json:"result,omitempty"`
}

type FailRequest struct {
	Reason  string          `json:"reason,omitempty"`
	Details json.RawMessage `json:"details,omitempty"`
}

type ReleaseRequest struct {
	DelaySeconds int64 `json:"delay_seconds"`
}

type ProgressRequest struct {
	Data json.RawMessage `json:"data"`
}

// DequeuedJob is the payload of the "job" event on the /dequeue stream.
// FirstAttemptedAt is populated only on the first delivery; for retries
// it is the same as the original first-attempt time.
type DequeuedJob struct {
	ID               string          `json:"id"`
	Lane             string          `json:"lane"`
	Document         json.RawMessage `json:"document"`
	Attempts         int64           `json:"attempts"`
	FirstAttemptedAt string          `json:"first_attempted_at"`
}

// ReplicationEvent is what the leader sends to followers and what
// /events/{jobId} broadcasts to clients. The Type is one of the Event*
// constants; Payload is event-specific JSON whose shape is one of the
// types below (EnqueuedPayload, ClaimedPayload, etc.). Followers parse
// Payload into the matching shape based on Type.
type ReplicationEvent struct {
	Seq        int64           `json:"seq"`
	JobID      string          `json:"job_id"`
	Type       string          `json:"type"`
	Payload    json.RawMessage `json:"payload"`
	OriginNode string          `json:"origin_node"`
	CreatedAt  int64           `json:"created_at"`
}

// Per-event payload types. These are what gets serialised into
// replication_log.payload and ReplicationEvent.Payload. They are
// designed so a follower with only the JobID + Type + Payload can
// reconstruct the leader's jobs row without consulting any other
// state.

// EnqueuedPayload carries everything needed to INSERT a fresh job row.
type EnqueuedPayload struct {
	Lane          string          `json:"lane"`
	DurableFactor int             `json:"durable_factor"`
	Document      json.RawMessage `json:"document"`
	AvailableAt   int64           `json:"available_at"`
	CreatedAt     int64           `json:"created_at"`
}

// ClaimedPayload describes a transition from pending to processing.
type ClaimedPayload struct {
	WorkerID    string `json:"worker_id"`
	LockedUntil int64  `json:"locked_until"`
	Attempts    int64  `json:"attempts"`
	UpdatedAt   int64  `json:"updated_at"`
}

// DonePayload describes a transition to done.
type DonePayload struct {
	Result      json.RawMessage `json:"result,omitempty"`
	CompletedAt int64           `json:"completed_at"`
	UpdatedAt   int64           `json:"updated_at"`
}

// FailedPayload describes a transition to failed.
type FailedPayload struct {
	Error       json.RawMessage `json:"error,omitempty"`
	CompletedAt int64           `json:"completed_at"`
	UpdatedAt   int64           `json:"updated_at"`
}

// ReleasedPayload describes a transition back to pending. A nil
// AvailableAt means "available immediately"; the more interesting case
// is when a job is released with a delay.
type ReleasedPayload struct {
	AvailableAt int64  `json:"available_at"`
	Reason      string `json:"reason,omitempty"` // "lock_expired" or empty for cooperative release
	UpdatedAt   int64  `json:"updated_at"`
}

// ProgressPayload mirrors the body the worker sent.
type ProgressPayload struct {
	Data json.RawMessage `json:"data"`
}

// DeletedPayload records why a job was pruned.
type DeletedPayload struct {
	Reason string `json:"reason"`
}
