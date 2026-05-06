package api

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
)

// handleMetrics writes Prometheus exposition format. We hand-roll the
// output rather than pulling in prometheus/client_golang — the format
// is small and stable, and our metric set is fixed. Dependency-free
// matches the rest of the codebase.
//
// The leader exposes the rich set (rates, latencies, follower lag).
// A follower's /metrics still returns the basic process gauges so a
// scrape job pointed at any node returns 200 — just with less data.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	var b strings.Builder

	// is_leader gauge (always available, regardless of session).
	since, _, isLeader := s.stats.LeaderInfo()
	leaderVal := 0
	if isLeader {
		leaderVal = 1
	}
	writeMetric(&b, "haasqueue_is_leader",
		"1 when this node holds the lease, 0 otherwise", "gauge",
		map[string]string{}, float64(leaderVal))
	if isLeader {
		writeMetric(&b, "haasqueue_leader_since_seconds",
			"Unix epoch when this node became leader (0 if not leader)", "gauge",
			map[string]string{}, float64(since.Unix()))
	}

	// Throughput counters (last 60s window, exposed as totals).
	writeMetric(&b, "haasqueue_enqueue_60s",
		"Enqueues in the last 60 seconds", "gauge",
		map[string]string{}, float64(s.stats.Enqueue.Total()))
	for k, v := range s.stats.Enqueue.PerLabel() {
		writeMetric(&b, "haasqueue_enqueue_60s",
			"", "",
			map[string]string{"durable_factor": k}, float64(v))
	}
	writeMetric(&b, "haasqueue_dequeue_60s",
		"Dequeues in the last 60 seconds", "gauge",
		map[string]string{}, float64(s.stats.Dequeue.Total()))
	for k, v := range s.stats.Dequeue.PerLabel() {
		writeMetric(&b, "haasqueue_dequeue_60s",
			"", "",
			map[string]string{"lane": k}, float64(v))
	}
	writeMetric(&b, "haasqueue_acks_60s",
		"Replication acks received in the last 60 seconds", "gauge",
		map[string]string{}, float64(s.stats.Acks.Total()))
	writeMetric(&b, "haasqueue_errors_60s",
		"Server errors in the last 60 seconds", "gauge",
		map[string]string{}, float64(s.stats.Errors.Total()))

	// Latency percentiles (only meaningful when we've served traffic).
	p50, p95, p99 := s.stats.EnqueueLatency.Percentiles()
	writeMetric(&b, "haasqueue_enqueue_latency_seconds",
		"Enqueue latency percentiles over the last ~1024 samples", "gauge",
		map[string]string{"quantile": "0.5"}, p50.Seconds())
	writeMetric(&b, "haasqueue_enqueue_latency_seconds",
		"", "",
		map[string]string{"quantile": "0.95"}, p95.Seconds())
	writeMetric(&b, "haasqueue_enqueue_latency_seconds",
		"", "",
		map[string]string{"quantile": "0.99"}, p99.Seconds())

	// Storage- and ack-derived metrics — only available with a
	// session (i.e. on the leader).
	if sess := s.session.Load(); sess != nil {
		ctx := r.Context()
		if counts, err := sess.Store.CountByLaneState(ctx); err == nil {
			for lane, byState := range counts {
				for state, n := range byState {
					writeMetric(&b, "haasqueue_jobs",
						"Jobs by lane and state", "gauge",
						map[string]string{"lane": lane, "state": state}, float64(n))
				}
			}
		}
		if rep, err := sess.Store.ReplogStatsSnapshot(ctx); err == nil {
			writeMetric(&b, "haasqueue_replog_size",
				"Number of rows in replication_log", "gauge",
				map[string]string{}, float64(rep.Count))
			writeMetric(&b, "haasqueue_replog_max_seq",
				"Highest seq in replication_log", "gauge",
				map[string]string{}, float64(rep.MaxSeq))
			writeMetric(&b, "haasqueue_replog_min_seq",
				"Lowest seq in replication_log", "gauge",
				map[string]string{}, float64(rep.MinSeq))
		}
		// Per-follower watermarks + derived lag.
		acks := s.acks.Snapshot()
		maxSeq := int64(0)
		if rep, err := sess.Store.ReplogStatsSnapshot(ctx); err == nil {
			maxSeq = rep.MaxSeq
		}
		for node, seq := range acks {
			writeMetric(&b, "haasqueue_follower_acked_seq",
				"Highest seq each follower has acked applying", "gauge",
				map[string]string{"node": node}, float64(seq))
			writeMetric(&b, "haasqueue_follower_lag_events",
				"Events the follower is behind the leader's max seq", "gauge",
				map[string]string{"node": node}, float64(maxSeq-seq))
		}
	}

	// Worker subscriber gauges.
	for lane, n := range s.bus.LaneSubscriberCount() {
		writeMetric(&b, "haasqueue_workers_connected",
			"Workers currently subscribed to /dequeue on each lane", "gauge",
			map[string]string{"lane": lane}, float64(n))
	}

	w.Write([]byte(b.String()))
}

// writeMetric emits one Prometheus-format metric line, with the HELP
// and TYPE preamble on the first call for a given metric name. The
// preamble is suppressed when help/typ are empty (used for additional
// labelled samples of the same metric).
func writeMetric(b *strings.Builder, name, help, typ string, labels map[string]string, value float64) {
	if help != "" {
		fmt.Fprintf(b, "# HELP %s %s\n", name, help)
	}
	if typ != "" {
		fmt.Fprintf(b, "# TYPE %s %s\n", name, typ)
	}
	if len(labels) == 0 {
		fmt.Fprintf(b, "%s %g\n", name, value)
		return
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	pairs := make([]string, 0, len(keys))
	for _, k := range keys {
		pairs = append(pairs, fmt.Sprintf(`%s="%s"`, k, escapeLabel(labels[k])))
	}
	fmt.Fprintf(b, "%s{%s} %g\n", name, strings.Join(pairs, ","), value)
}

// escapeLabel handles the three characters Prometheus requires
// escaped in label values: backslash, double-quote, newline.
func escapeLabel(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	return r.Replace(s)
}
