// Package stats is the in-memory metrics store that powers both the
// HTML status dashboard and the Prometheus /metrics endpoint.
//
// Two primitives:
//
//   - RateCounter: 60-bucket second-resolution rolling counter, with
//     optional sub-counters keyed by a label string. Per-second rate
//     over the last 60s = sum(buckets) / 60. Cheap (constant memory)
//     and ages out automatically.
//
//   - Histogram: ring buffer of the last N latency samples. Sort on
//     query for percentiles. N is bounded so a hot loop can't grow
//     memory without bound.
//
// The Registry bundles a fixed set of these with the named gauges
// (current state) the dashboard cares about. Producers call
// RecordX(...); the dashboard / metrics endpoint reads via Snapshot.
package stats

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// rateBuckets is how many 1-second buckets each RateCounter holds.
// Sized for the dashboard's "last 60s" window. Increasing this
// proportionally increases memory per labelled bucket.
const rateBuckets = 60

// histogramSize bounds the latency ring buffer. 1024 samples is enough
// for stable p99 and small enough to sort on every dashboard refresh
// (~10us on modern hardware).
const histogramSize = 1024

// RateCounter is a sliding 60s counter, optionally broken down by a
// label (e.g. lane name, durable_factor). Goroutine-safe.
type RateCounter struct {
	mu        sync.Mutex
	buckets   [rateBuckets]int64
	byLabel   map[string]*[rateBuckets]int64
	lastSec   int64
}

// Inc increments the bucket for the current second. label is optional;
// pass "" for an unlabelled counter (only the aggregate is recorded).
func (r *RateCounter) Inc(label string) {
	r.IncN(label, 1)
}

// IncN adds n to the bucket for the current second.
func (r *RateCounter) IncN(label string, n int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.advance(time.Now().Unix())
	r.buckets[0] += n
	if label != "" {
		if r.byLabel == nil {
			r.byLabel = make(map[string]*[rateBuckets]int64)
		}
		buf, ok := r.byLabel[label]
		if !ok {
			var b [rateBuckets]int64
			buf = &b
			r.byLabel[label] = buf
		}
		buf[0] += n
	}
}

// advance shifts buckets so buckets[0] is the current second. Called
// under the lock by Inc and Snapshot.
func (r *RateCounter) advance(now int64) {
	if r.lastSec == 0 {
		r.lastSec = now
		return
	}
	delta := now - r.lastSec
	if delta <= 0 {
		return
	}
	if delta >= rateBuckets {
		// Older than the window: zero everything.
		for i := range r.buckets {
			r.buckets[i] = 0
		}
		for _, buf := range r.byLabel {
			for i := range buf {
				buf[i] = 0
			}
		}
	} else {
		// Slide right by delta.
		copy(r.buckets[delta:], r.buckets[:rateBuckets-delta])
		for i := int64(0); i < delta; i++ {
			r.buckets[i] = 0
		}
		for _, buf := range r.byLabel {
			copy(buf[delta:], buf[:rateBuckets-delta])
			for i := int64(0); i < delta; i++ {
				buf[i] = 0
			}
		}
	}
	r.lastSec = now
}

// Total returns the sum of all 60 buckets — events in the last 60s.
func (r *RateCounter) Total() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.advance(time.Now().Unix())
	var sum int64
	for _, v := range r.buckets {
		sum += v
	}
	return sum
}

// PerLabel returns the 60s totals broken down by label.
func (r *RateCounter) PerLabel() map[string]int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.advance(time.Now().Unix())
	out := make(map[string]int64, len(r.byLabel))
	for k, buf := range r.byLabel {
		var sum int64
		for _, v := range buf {
			sum += v
		}
		out[k] = sum
	}
	return out
}

// Histogram stores the last N duration samples for percentile
// reporting. Goroutine-safe.
type Histogram struct {
	mu      sync.Mutex
	samples [histogramSize]time.Duration
	count   int  // total recorded (capped at histogramSize for indexing)
	wrapped bool // true once we've filled the ring once
	head    int
}

// Record stores one sample. Old samples roll off as the ring fills.
func (h *Histogram) Record(d time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.samples[h.head] = d
	h.head = (h.head + 1) % histogramSize
	if h.head == 0 {
		h.wrapped = true
	}
	h.count++
}

// Percentiles returns p50, p95, p99 of the current ring. Returns
// zeros when no samples have been recorded.
func (h *Histogram) Percentiles() (p50, p95, p99 time.Duration) {
	h.mu.Lock()
	n := histogramSize
	if !h.wrapped {
		n = h.head
	}
	if n == 0 {
		h.mu.Unlock()
		return 0, 0, 0
	}
	cp := make([]time.Duration, n)
	copy(cp, h.samples[:n])
	h.mu.Unlock()

	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	idx := func(p int) time.Duration {
		i := n * p / 100
		if i >= n {
			i = n - 1
		}
		return cp[i]
	}
	return idx(50), idx(95), idx(99)
}

// Registry is the queue's shared metrics state. Construct with New.
type Registry struct {
	// Throughput counters (last 60s).
	Enqueue *RateCounter // labelled by durable_factor
	Dequeue *RateCounter // labelled by lane
	Errors  *RateCounter // unlabelled
	Acks    *RateCounter // labelled by node_id

	// Latency.
	EnqueueLatency *Histogram

	// Leadership.
	leaderSince      atomic.Int64 // unix seconds; 0 = not leader
	leaderGeneration atomic.Int64

	// Retention runtime.
	mu             sync.Mutex
	lastRetention  time.Time
	lastDoneCount  int
	lastFailedCount int
	lastTrimmed    int64
}

// New returns a fresh Registry.
func New() *Registry {
	return &Registry{
		Enqueue:        &RateCounter{},
		Dequeue:        &RateCounter{},
		Errors:         &RateCounter{},
		Acks:           &RateCounter{},
		EnqueueLatency: &Histogram{},
	}
}

// SetLeader records that we became leader at now with the given
// haaslock generation. Pass zeros (time.Time{}, 0) on demotion.
func (r *Registry) SetLeader(since time.Time, generation int64) {
	if since.IsZero() {
		r.leaderSince.Store(0)
	} else {
		r.leaderSince.Store(since.Unix())
	}
	r.leaderGeneration.Store(generation)
}

// LeaderInfo returns (since, generation, isLeader). isLeader is
// false when SetLeader was last called with zeros.
func (r *Registry) LeaderInfo() (since time.Time, generation int64, isLeader bool) {
	sec := r.leaderSince.Load()
	if sec == 0 {
		return time.Time{}, 0, false
	}
	return time.Unix(sec, 0), r.leaderGeneration.Load(), true
}

// RecordRetentionRun captures the latest retention pass's results
// for the dashboard's Retention section.
func (r *Registry) RecordRetentionRun(at time.Time, deletedDone, deletedFailed int, trimmedReplog int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastRetention = at
	r.lastDoneCount = deletedDone
	r.lastFailedCount = deletedFailed
	r.lastTrimmed = trimmedReplog
}

// RetentionSnapshot returns the most-recent retention pass info.
func (r *Registry) RetentionSnapshot() (at time.Time, done, failed int, trimmed int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastRetention, r.lastDoneCount, r.lastFailedCount, r.lastTrimmed
}
