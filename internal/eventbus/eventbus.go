// Package eventbus provides in-memory pub/sub for SSE event distribution.
//
// Two topics:
//   - per-job event streams (consumers: GET /events/{id} and POST /enqueue with SSE Accept)
//   - per-lane wakeup signals (consumer: GET /dequeue/{lane})
//
// The bus does not persist anything. SSE handlers replay historical events
// from storage before subscribing here, and dedupe by event id. Slow
// consumers see drops, not blocks — recovery is via a fresh replay.
package eventbus

import (
	"sync"
)

// Event is the unit of fanout for job-keyed subscribers. ID matches the
// replication_log.seq, so subscribers can dedupe against a Last-Event-ID
// replay without ambiguity.
type Event struct {
	ID    int64
	JobID string
	Type  string
	Data  []byte
}

// Bus broadcasts events without persisting them.
type Bus struct {
	mu         sync.Mutex
	jobSubs    map[string]map[chan Event]struct{}
	laneSubs   map[string]map[chan struct{}]struct{}
	replogSubs map[chan ReplogRow]struct{}
}

// ReplogRow is the unit of fanout for replication-stream subscribers.
// It mirrors a row of the storage.Replog type without forcing the
// eventbus package to import storage (avoiding a cycle).
type ReplogRow struct {
	Seq        int64
	EventType  string
	JobID      string
	Payload    []byte
	CreatedAt  int64
	OriginNode string
}

func New() *Bus {
	return &Bus{
		jobSubs:    make(map[string]map[chan Event]struct{}),
		laneSubs:   make(map[string]map[chan struct{}]struct{}),
		replogSubs: make(map[chan ReplogRow]struct{}),
	}
}

// SubscribeJob returns a buffered channel of events for the given job
// and an unsubscribe function. PublishJob drops events on the floor
// when the buffer is full; consumers are expected to recover via a
// Last-Event-ID replay against storage.
func (b *Bus) SubscribeJob(jobID string) (<-chan Event, func()) {
	ch := make(chan Event, 32)
	b.mu.Lock()
	subs, ok := b.jobSubs[jobID]
	if !ok {
		subs = make(map[chan Event]struct{})
		b.jobSubs[jobID] = subs
	}
	subs[ch] = struct{}{}
	b.mu.Unlock()

	unsub := func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if subs, ok := b.jobSubs[jobID]; ok {
			delete(subs, ch)
			if len(subs) == 0 {
				delete(b.jobSubs, jobID)
			}
		}
		// Intentionally NOT closing ch. PublishJob may have already
		// snapshotted this channel into its targets slice before the
		// lock above was acquired; closing here while a publisher is
		// mid-send is a data race and would panic with "send on closed
		// channel" under load. Removing from the map is sufficient: no
		// new publishers see this channel, and the consumer ends its
		// loop via ctx.Done() rather than channel close.
	}
	return ch, unsub
}

// PublishJob delivers an event to all current subscribers of a job.
// Snapshot under the lock, then send outside it — long-running consumers
// don't block other publishers or unsubscribes.
func (b *Bus) PublishJob(ev Event) {
	b.mu.Lock()
	subs := b.jobSubs[ev.JobID]
	targets := make([]chan Event, 0, len(subs))
	for ch := range subs {
		targets = append(targets, ch)
	}
	b.mu.Unlock()

	for _, ch := range targets {
		select {
		case ch <- ev:
		default:
			// Drop on backpressure; consumer reconciles via replay.
		}
	}
}

// SubscribeLane returns a wakeup channel and unsubscribe function. The
// channel fires (non-blockingly coalesced) whenever a job becomes
// available on the lane — multiple signals while one is already queued
// collapse to a single wakeup.
func (b *Bus) SubscribeLane(lane string) (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	b.mu.Lock()
	subs, ok := b.laneSubs[lane]
	if !ok {
		subs = make(map[chan struct{}]struct{})
		b.laneSubs[lane] = subs
	}
	subs[ch] = struct{}{}
	b.mu.Unlock()

	unsub := func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if subs, ok := b.laneSubs[lane]; ok {
			delete(subs, ch)
			if len(subs) == 0 {
				delete(b.laneSubs, lane)
			}
		}
	}
	return ch, unsub
}

// SubscribeReplog returns a channel that receives every replog row
// published via PublishReplog. Used by /replication/stream on the
// leader to tail new events without polling the database.
func (b *Bus) SubscribeReplog() (<-chan ReplogRow, func()) {
	ch := make(chan ReplogRow, 64)
	b.mu.Lock()
	b.replogSubs[ch] = struct{}{}
	b.mu.Unlock()

	unsub := func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		delete(b.replogSubs, ch)
	}
	return ch, unsub
}

// PublishReplog delivers a replog row to all current subscribers.
// Drops on backpressure — slow consumers (e.g. an SSE follower) lose
// events and must reconnect with a since cursor to catch up.
func (b *Bus) PublishReplog(row ReplogRow) {
	b.mu.Lock()
	targets := make([]chan ReplogRow, 0, len(b.replogSubs))
	for ch := range b.replogSubs {
		targets = append(targets, ch)
	}
	b.mu.Unlock()

	for _, ch := range targets {
		select {
		case ch <- row:
		default:
		}
	}
}

// LaneSubscriberCount returns the current number of /dequeue
// subscribers on each lane. Powers the dashboard's "workers connected"
// view. Snapshot semantics — subscribers can come and go between this
// returning and the caller using the result.
func (b *Bus) LaneSubscriberCount() map[string]int {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make(map[string]int, len(b.laneSubs))
	for lane, subs := range b.laneSubs {
		out[lane] = len(subs)
	}
	return out
}

// SignalLane wakes any worker waiting on the lane. Coalesces if a
// wakeup is already pending — a worker that misses one signal will see
// the next one (or find the job via the idle-poll fallback).
func (b *Bus) SignalLane(lane string) {
	b.mu.Lock()
	subs := b.laneSubs[lane]
	targets := make([]chan struct{}, 0, len(subs))
	for ch := range subs {
		targets = append(targets, ch)
	}
	b.mu.Unlock()

	for _, ch := range targets {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
