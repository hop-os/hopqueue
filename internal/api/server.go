// Package api exposes the queue's HTTP/SSE surface.
//
// The Server is configured once at startup. A bound *Session (storage
// + idgen) is the resources only the leader has. Every handler runs
// requireSession first; on a non-leader node that returns 503 with an
// X-Active-Leader header (when known) so clients can retry against
// whoever actually holds the lease.
//
// Phase 1 single-node mode binds a Session at startup and never
// unbinds. Phase 3 will plug the lease elector into Bind/Unbind.
package api

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/xinix00/hopqueue/internal/eventbus"
	"github.com/xinix00/hopqueue/internal/idgen"
	"github.com/xinix00/hopqueue/internal/stats"
	"github.com/xinix00/hopqueue/internal/storage"
	"github.com/xinix00/hopqueue/pkg/protocol"
)

// Activeness exposes the current active leader (best-effort) so the
// API can include it as an X-Active-Leader hint when this node is on
// standby. Implemented by the lease wrapper in Phase 3.
type Activeness interface {
	ActiveOwner() string
}

// Session bundles the resources that exist only while this node is
// the active writer. Wired in via Bind; cleared via Unbind on demotion.
type Session struct {
	Store *storage.DB
	IDGen *idgen.Generator
}

// Config holds the API tuning knobs.
type Config struct {
	// LockDuration is how long a claimed job stays locked before the
	// sweeper considers it abandoned. Default 5 minutes.
	LockDuration time.Duration
	// HeartbeatInterval bounds the gap between SSE heartbeat comments
	// on long-lived streams. Default 15 seconds.
	HeartbeatInterval time.Duration
	// DequeueIdlePoll is the safety-net poll cadence on the dequeue
	// stream. The lane signal wakes us on enqueue; this catches anything
	// that signal somehow misses (or that becomes available because of a
	// scheduled available_at). Default 5 seconds.
	DequeueIdlePoll time.Duration
	// APIKey, when non-empty, is the bearer token required on every
	// route except /health. Followers must pass the same key in their
	// Authorization header (see follower.Config.APIKey). Empty disables
	// authentication entirely; suitable for local development.
	APIKey string
}

// DefaultConfig returns the production defaults for Config.
func DefaultConfig() Config {
	return Config{
		LockDuration:      5 * time.Minute,
		HeartbeatInterval: 15 * time.Second,
		DequeueIdlePoll:   5 * time.Second,
	}
}

// Server is the queue's HTTP/SSE surface. Construct with NewServer,
// configure with Bind/SetActiveness, install with Routes().
type Server struct {
	bus        *eventbus.Bus
	cfg        Config
	logger     *slog.Logger
	session    atomic.Pointer[Session]
	activeness Activeness
	acks       AckTracker
	pending    pendingDurable
	stats      *stats.Registry
}

func NewServer(bus *eventbus.Bus, cfg Config, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	d := DefaultConfig()
	if cfg.LockDuration == 0 {
		cfg.LockDuration = d.LockDuration
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = d.HeartbeatInterval
	}
	if cfg.DequeueIdlePoll == 0 {
		cfg.DequeueIdlePoll = d.DequeueIdlePoll
	}
	return &Server{bus: bus, cfg: cfg, logger: logger, stats: stats.New()}
}

// Stats returns the shared metrics registry. Callers wire it into
// the leader-task coordinator (so retention can record its run) and
// into main.go (to set leader info on Bind).
func (s *Server) Stats() *stats.Registry { return s.stats }

// SetLeaderInfo records that this node became leader at now with the
// given hoplock generation. Pair with SetLeaderInfo(time.Time{}, 0)
// on demotion. Drives the Cluster section of the dashboard.
func (s *Server) SetLeaderInfo(since time.Time, generation int64) {
	s.stats.SetLeader(since, generation)
}

// Bind attaches a Session. Subsequent requests can use it.
// Goroutine-safe.
func (s *Server) Bind(sess *Session) { s.session.Store(sess) }

// Unbind detaches the current Session. New requests will get 503;
// in-flight requests will see the unbind take effect on their next
// session lookup. Also clears any pending durability watches: a
// demoted leader is no longer responsible for emitting 'synced'.
func (s *Server) Unbind() {
	s.session.Store(nil)
	s.clearPendingDurable()
}

// SetActiveness wires in the active-owner hint source for the
// X-Active-Leader header. Optional — when unset, the header is omitted
// even on 503s.
func (s *Server) SetActiveness(a Activeness) { s.activeness = a }

// Routes returns the HTTP handler tree. When Config.APIKey is set,
// every route except /health is wrapped with a bearer-token check.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", s.handleStatus)
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("POST /enqueue", s.handleEnqueue)
	mux.HandleFunc("GET /events/{jobId...}", s.handleEvents)
	mux.HandleFunc("GET /dequeue/{lane}", s.handleDequeue)
	mux.HandleFunc("POST /jobs/{id...}", s.handleJobAction)
	mux.HandleFunc("GET /replication/stream", s.handleReplicationStream)
	mux.HandleFunc("POST /replication/ack", s.handleReplicationAck)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	if s.cfg.APIKey == "" {
		return mux
	}
	return s.requireBearer(mux)
}

// requireBearer checks the Authorization header against the configured
// API key. /health is exempt so load balancers and orchestrators can
// probe liveness without credentials. Comparison is constant-time to
// avoid timing-side-channel leaks of the configured key.
func (s *Server) requireBearer(next http.Handler) http.Handler {
	expected := []byte("Bearer " + s.cfg.APIKey)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			next.ServeHTTP(w, r)
			return
		}
		got := []byte(r.Header.Get("Authorization"))
		if len(got) != len(expected) || subtle.ConstantTimeCompare(got, expected) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// requireSession is the gate every handler runs first. If no Session
// is bound, returns false after writing 503 + X-Active-Leader (when
// known) so clients can retry against the active leader.
func (s *Server) requireSession(w http.ResponseWriter) (*Session, bool) {
	if sess := s.session.Load(); sess != nil {
		return sess, true
	}
	if s.activeness != nil {
		if owner := s.activeness.ActiveOwner(); owner != "" {
			w.Header().Set("X-Active-Leader", owner)
		}
	}
	http.Error(w, "this node is not the active leader", http.StatusServiceUnavailable)
	return nil, false
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ready := s.session.Load() != nil
	role := "standby"
	if ready {
		role = "active"
	}
	owner := ""
	if s.activeness != nil {
		owner = s.activeness.ActiveOwner()
	}
	w.Header().Set("Content-Type", "application/json")
	if ready {
		w.WriteHeader(http.StatusOK)
	} else {
		if owner != "" {
			w.Header().Set("X-Active-Leader", owner)
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_, _ = fmt.Fprintf(w, `{"ready":%t,"role":%q,"active_leader":%q}`+"\n", ready, role, owner)
}

// dashboardData is the snapshot consumed by both the HTML dashboard
// and the JSON view. Built fresh per request so the numbers in each
// section are mutually consistent.
type dashboardData struct {
	NodeID         string
	Self           string
	Leader         string
	LeaderSince    time.Time
	LeaderGen      int64
	IsLeader       bool
	StateCounts    map[string]int
	LaneCounts     map[string]map[string]int
	LaneWorkers    map[string]int
	Replog         storage.ReplogStats
	FollowerAcks   map[string]int64
	EnqueueRate    int64
	EnqueueByDur   map[string]int64
	DequeueRate    int64
	DequeueByLane  map[string]int64
	AckRate        int64
	ErrorRate      int64
	P50, P95, P99  time.Duration
	RetentionLastRun time.Time
	RetentionDone    int
	RetentionFailed  int
	RetentionTrimmed int64
	Now            time.Time
}

func (s *Server) collectDashboard(r *http.Request, sess *Session) (dashboardData, error) {
	d := dashboardData{
		NodeID:        sess.Store.NodeID(),
		Self:          sess.Store.NodeID(),
		StateCounts:   map[string]int{},
		LaneCounts:    map[string]map[string]int{},
		LaneWorkers:   map[string]int{},
		FollowerAcks:  map[string]int64{},
		EnqueueByDur:  map[string]int64{},
		DequeueByLane: map[string]int64{},
		Now:           time.Now(),
	}
	if s.activeness != nil {
		d.Leader = s.activeness.ActiveOwner()
	}
	since, gen, isLeader := s.stats.LeaderInfo()
	d.LeaderSince = since
	d.LeaderGen = gen
	d.IsLeader = isLeader

	counts, err := sess.Store.CountByState(r.Context())
	if err != nil {
		return d, err
	}
	d.StateCounts = counts

	laneCounts, err := sess.Store.CountByLaneState(r.Context())
	if err != nil {
		return d, err
	}
	d.LaneCounts = laneCounts

	d.LaneWorkers = s.bus.LaneSubscriberCount()

	rep, err := sess.Store.ReplogStatsSnapshot(r.Context())
	if err != nil {
		return d, err
	}
	d.Replog = rep
	d.FollowerAcks = s.acks.Snapshot()

	d.EnqueueRate = s.stats.Enqueue.Total()
	d.EnqueueByDur = s.stats.Enqueue.PerLabel()
	d.DequeueRate = s.stats.Dequeue.Total()
	d.DequeueByLane = s.stats.Dequeue.PerLabel()
	d.AckRate = s.stats.Acks.Total()
	d.ErrorRate = s.stats.Errors.Total()
	d.P50, d.P95, d.P99 = s.stats.EnqueueLatency.Percentiles()

	d.RetentionLastRun, d.RetentionDone, d.RetentionFailed, d.RetentionTrimmed = s.stats.RetentionSnapshot()
	return d, nil
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	sess, ok := s.requireSession(w)
	if !ok {
		return
	}
	d, err := s.collectDashboard(r, sess)
	if err != nil {
		s.logger.Error("status: collect", "err", err)
		http.Error(w, "status failed", http.StatusInternalServerError)
		return
	}

	if accepts(r, "text/html") {
		renderDashboard(w, d)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"node_id":         d.NodeID,
		"is_leader":       d.IsLeader,
		"leader":          d.Leader,
		"leader_since":    d.LeaderSince,
		"leader_gen":      d.LeaderGen,
		"counts":          d.StateCounts,
		"lane_counts":     d.LaneCounts,
		"lane_workers":    d.LaneWorkers,
		"replog":          d.Replog,
		"follower_acks":   d.FollowerAcks,
		"enqueue_rate60":  d.EnqueueRate,
		"enqueue_by_dur":  d.EnqueueByDur,
		"dequeue_rate60":  d.DequeueRate,
		"dequeue_by_lane": d.DequeueByLane,
		"ack_rate60":      d.AckRate,
		"error_rate60":    d.ErrorRate,
		"latency_p50":     d.P50.String(),
		"latency_p95":     d.P95.String(),
		"latency_p99":     d.P99.String(),
		"retention": map[string]any{
			"last_run": d.RetentionLastRun,
			"done":     d.RetentionDone,
			"failed":   d.RetentionFailed,
			"trimmed":  d.RetentionTrimmed,
		},
	})
}

// renderDashboard writes the auto-refreshing operations dashboard.
// No external assets — inline CSS, no JavaScript. Five sections
// modeled on the user's wire-frame: Cluster, Throughput, Lanes,
// Replication, Retention.
func renderDashboard(w http.ResponseWriter, d dashboardData) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	var b strings.Builder
	b.WriteString(dashboardHeader(d.NodeID))
	dashboardCluster(&b, d)
	dashboardThroughput(&b, d)
	dashboardLanes(&b, d)
	dashboardReplication(&b, d)
	dashboardRetention(&b, d)
	b.WriteString(dashboardFooter)
	_, _ = w.Write([]byte(b.String()))
}

func dashboardHeader(nodeID string) string {
	return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="5">
<title>hopqueue ` + htmlEscape(nodeID) + `</title>
<style>
body { font: 13px/1.45 ui-monospace, SFMono-Regular, Menlo, monospace; max-width: 760px; margin: 2em auto; color: #222; padding: 0 1em; }
h1 { font-size: 1.3em; margin: 0 0 .25em; font-family: system-ui, sans-serif; }
.subtitle { color: #777; margin: 0 0 1.5em; font-family: system-ui, sans-serif; }
section { margin: 1.6em 0; }
section h2 { font-size: 1em; margin: 0 0 .4em; color: #444; padding-bottom: .15em; border-bottom: 1px solid #eee; font-family: system-ui, sans-serif; }
.row { display: flex; padding: .15em 0; }
.row .lbl { width: 11em; color: #555; flex-shrink: 0; }
.row .val { flex: 1; }
.muted { color: #999; }
.ok { color: #1a7f37; }
.warn { color: #b08600; }
.err { color: #cf222e; }
.empty { color: #999; font-style: italic; padding: .25em 0; }
table { border-collapse: collapse; margin: .25em 0; }
th, td { padding: .2em .9em .2em 0; text-align: left; }
th { font-weight: 600; color: #555; border-bottom: 1px solid #eee; }
td.num { text-align: right; font-variant-numeric: tabular-nums; }
.num-grid { display: inline-grid; grid-template-columns: max-content max-content; column-gap: 1em; row-gap: .15em; }
.num-grid > .k { color: #555; }
.num-grid > .v { font-variant-numeric: tabular-nums; }
</style>
</head>
<body>
<h1>hopqueue / ` + htmlEscape(nodeID) + `</h1>
<p class="subtitle">refresh: 5s &middot; ` + time.Now().UTC().Format("2006-01-02 15:04:05 UTC") + `</p>
`
}

const dashboardFooter = `</body></html>`

func dashboardCluster(b *strings.Builder, d dashboardData) {
	b.WriteString(`<section><h2>Cluster</h2>`)

	leaderLabel := d.Leader
	if leaderLabel == "" {
		// Single-node mode (no lease backend) reports IsLeader=true
		// but Activeness has no owner to surface. Showing
		// "(unknown)" would be misleading; we ARE the leader by
		// definition in this mode.
		if d.IsLeader {
			leaderLabel = "self &middot; " + htmlEscape(d.NodeID) + " (no lease backend)"
		} else {
			leaderLabel = "(unknown)"
		}
	}
	leaderClass := "muted"
	if d.IsLeader {
		leaderClass = "ok"
	}
	// leaderLabel is built from trusted strings (or already escaped
	// constants in the no-leader fallback); no extra escape needed
	// for the constant text. We DO escape the bare nodeID inserted
	// in the single-node branch above so it's safe.
	b.WriteString(fmt.Sprintf(`<div class="row"><div class="lbl">Leader</div><div class="val %s">%s</div></div>`,
		leaderClass, leaderLabel))

	if d.IsLeader {
		uptime := time.Since(d.LeaderSince).Truncate(time.Second)
		b.WriteString(fmt.Sprintf(`<div class="row"><div class="lbl">Generation</div><div class="val">%d &middot; held for %s</div></div>`,
			d.LeaderGen, uptime))
	}

	// Followers (anyone who has acked us, ranked by lag).
	if len(d.FollowerAcks) == 0 {
		b.WriteString(`<div class="row"><div class="lbl">Followers</div><div class="val muted">none observed</div></div>`)
	} else {
		var rows strings.Builder
		for node, ackedSeq := range d.FollowerAcks {
			lag := d.Replog.MaxSeq - ackedSeq
			cls := "ok"
			if lag > 100 {
				cls = "warn"
			}
			if lag > 1000 {
				cls = "err"
			}
			fmt.Fprintf(&rows, `<div class="row"><div class="lbl">&nbsp;</div><div class="val"><span class="muted">%s</span> &middot; <span class="%s">lag %d events</span></div></div>`,
				htmlEscape(node), cls, lag)
		}
		b.WriteString(fmt.Sprintf(`<div class="row"><div class="lbl">Followers (%d)</div><div class="val">&nbsp;</div></div>%s`,
			len(d.FollowerAcks), rows.String()))
	}

	b.WriteString(`</section>`)
}

func dashboardThroughput(b *strings.Builder, d dashboardData) {
	b.WriteString(`<section><h2>Throughput (last 60s)</h2>`)

	// Enqueue line with durable_factor breakdown.
	enqLine := fmt.Sprintf("%d total in 60s (%.1f r/s avg)", d.EnqueueRate, float64(d.EnqueueRate)/60.0)
	if len(d.EnqueueByDur) > 0 {
		var parts []string
		// Stable order: 0, 1, 2, -1.
		for _, k := range []string{"0", "1", "2", "-1"} {
			if v, ok := d.EnqueueByDur[k]; ok {
				parts = append(parts, fmt.Sprintf("durable=%s: %d", k, v))
			}
		}
		// Catch-all for anything we missed.
		for k, v := range d.EnqueueByDur {
			if k == "0" || k == "1" || k == "2" || k == "-1" {
				continue
			}
			parts = append(parts, fmt.Sprintf("durable=%s: %d", k, v))
		}
		if len(parts) > 0 {
			enqLine += " &middot; " + strings.Join(parts, ", ")
		}
	}
	b.WriteString(fmt.Sprintf(`<div class="row"><div class="lbl">Enqueue</div><div class="val">%s</div></div>`, enqLine))

	deqLine := fmt.Sprintf("%d total in 60s (%.1f r/s avg)", d.DequeueRate, float64(d.DequeueRate)/60.0)
	if len(d.DequeueByLane) > 0 {
		var parts []string
		for k, v := range d.DequeueByLane {
			parts = append(parts, fmt.Sprintf("%s: %d", k, v))
		}
		deqLine += " &middot; " + strings.Join(parts, ", ")
	}
	b.WriteString(fmt.Sprintf(`<div class="row"><div class="lbl">Dequeue</div><div class="val">%s</div></div>`, deqLine))

	b.WriteString(fmt.Sprintf(`<div class="row"><div class="lbl">Latency (enqueue)</div><div class="val">p50=%s &middot; p95=%s &middot; p99=%s</div></div>`,
		fmtDur(d.P50), fmtDur(d.P95), fmtDur(d.P99)))

	errClass := "ok"
	if d.ErrorRate > 0 {
		errClass = "warn"
	}
	if d.ErrorRate > 10 {
		errClass = "err"
	}
	b.WriteString(fmt.Sprintf(`<div class="row"><div class="lbl">Errors</div><div class="val %s">%d in last 60s</div></div>`,
		errClass, d.ErrorRate))

	b.WriteString(`</section>`)
}

func dashboardLanes(b *strings.Builder, d dashboardData) {
	b.WriteString(`<section><h2>Lanes</h2>`)
	if len(d.LaneCounts) == 0 && len(d.LaneWorkers) == 0 {
		b.WriteString(`<p class="empty">no lanes have seen activity yet</p></section>`)
		return
	}
	// Union of lanes from job-counts and worker-subscriptions.
	lanes := map[string]struct{}{}
	for k := range d.LaneCounts {
		lanes[k] = struct{}{}
	}
	for k := range d.LaneWorkers {
		lanes[k] = struct{}{}
	}
	b.WriteString(`<table><tr><th>lane</th><th>pending</th><th>processing</th><th>done</th><th>failed</th><th>workers</th></tr>`)
	for lane := range lanes {
		c := d.LaneCounts[lane]
		fmt.Fprintf(b,
			`<tr><td>%s</td><td class="num">%d</td><td class="num">%d</td><td class="num">%d</td><td class="num">%d</td><td class="num">%d</td></tr>`,
			htmlEscape(lane), c["pending"], c["processing"], c["done"], c["failed"], d.LaneWorkers[lane])
	}
	b.WriteString(`</table></section>`)
}

func dashboardReplication(b *strings.Builder, d dashboardData) {
	b.WriteString(`<section><h2>Replication</h2>`)

	oldestAge := "n/a"
	if d.Replog.OldestAt > 0 {
		oldestAge = time.Since(time.Unix(d.Replog.OldestAt, 0)).Truncate(time.Second).String() + " ago"
	}
	b.WriteString(fmt.Sprintf(`<div class="row"><div class="lbl">Log size</div><div class="val">%d events &middot; seq [%d, %d] &middot; oldest %s</div></div>`,
		d.Replog.Count, d.Replog.MinSeq, d.Replog.MaxSeq, oldestAge))

	b.WriteString(fmt.Sprintf(`<div class="row"><div class="lbl">Acks/sec</div><div class="val">%.1f (last 60s)</div></div>`,
		float64(d.AckRate)/60.0))

	// Average follower lag in events.
	if len(d.FollowerAcks) > 0 {
		var totalLag int64
		for _, seq := range d.FollowerAcks {
			totalLag += d.Replog.MaxSeq - seq
		}
		avgLag := float64(totalLag) / float64(len(d.FollowerAcks))
		cls := "ok"
		if avgLag > 100 {
			cls = "warn"
		}
		if avgLag > 1000 {
			cls = "err"
		}
		b.WriteString(fmt.Sprintf(`<div class="row"><div class="lbl">Avg follower lag</div><div class="val %s">%.1f events</div></div>`,
			cls, avgLag))
	}

	b.WriteString(`</section>`)
}

func dashboardRetention(b *strings.Builder, d dashboardData) {
	b.WriteString(`<section><h2>Retention</h2>`)

	if d.RetentionLastRun.IsZero() {
		b.WriteString(`<p class="empty">retention has not run yet on this leader</p></section>`)
		return
	}
	age := time.Since(d.RetentionLastRun).Truncate(time.Second)
	b.WriteString(fmt.Sprintf(`<div class="row"><div class="lbl">Last run</div><div class="val">%s ago</div></div>`, age))
	b.WriteString(fmt.Sprintf(`<div class="row"><div class="lbl">Last pruned</div><div class="val">%d jobs &middot; %d replog rows</div></div>`,
		d.RetentionDone+d.RetentionFailed, d.RetentionTrimmed))
	b.WriteString(fmt.Sprintf(`<div class="row"><div class="lbl">Done jobs now</div><div class="val">%d (kept 1d)</div></div>`,
		d.StateCounts["done"]))
	b.WriteString(fmt.Sprintf(`<div class="row"><div class="lbl">Failed jobs now</div><div class="val">%d (kept 7d)</div></div>`,
		d.StateCounts["failed"]))
	b.WriteString(`</section>`)
}

// fmtDur prints durations a human can read at a glance — µs/ms/s with
// at most 2 decimal places.
func fmtDur(d time.Duration) string {
	if d == 0 {
		return "—"
	}
	switch {
	case d < time.Microsecond:
		return d.String()
	case d < time.Millisecond:
		return fmt.Sprintf("%.1fµs", float64(d.Nanoseconds())/1000.0)
	case d < time.Second:
		return fmt.Sprintf("%.1fms", float64(d.Nanoseconds())/1e6)
	default:
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
}

// htmlEscape is the minimal escape needed for the few user-controlled
// strings (node id, owner URL) that land in the dashboard. We don't
// pull in html/template because the page is otherwise just static.
func htmlEscape(s string) string {
	r := strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		"\"", "&quot;",
		"'", "&#39;",
	)
	return r.Replace(s)
}

func (s *Server) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	enqueueStart := time.Now()
	sess, ok := s.requireSession(w)
	if !ok {
		return
	}
	var req protocol.EnqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid json: %v", err), http.StatusBadRequest)
		return
	}
	if req.Lane == "" {
		http.Error(w, "lane required", http.StatusBadRequest)
		return
	}
	if len(req.Document) == 0 {
		http.Error(w, "document required", http.StatusBadRequest)
		return
	}
	if req.Entity == "" {
		http.Error(w, "entity required", http.StatusBadRequest)
		return
	}
	if err := idgen.ValidateEntity(req.Entity); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	res, err := sess.Store.Enqueue(r.Context(), req, sess.IDGen)
	if err != nil {
		s.stats.Errors.Inc("")
		s.logger.Error("enqueue failed", "err", err)
		http.Error(w, "enqueue failed", http.StatusInternalServerError)
		return
	}
	s.stats.Enqueue.Inc(strconv.Itoa(req.DurableFactor))
	s.stats.EnqueueLatency.Record(time.Since(enqueueStart))

	enqueuedData := []byte(fmt.Sprintf(`{"jobId":%q,"lane":%q}`, res.JobID, req.Lane))
	s.bus.PublishJob(eventbus.Event{
		ID:    res.EnqueuedSeq,
		JobID: res.JobID,
		Type:  protocol.EventEnqueued,
		Data:  enqueuedData,
	})
	s.bus.SignalLane(req.Lane)

	// If the producer asked for durability, watch this enqueue's seq
	// in the ack tracker. checkSyncedJobs will fire when N peers have
	// acked it, and a 'synced' event will land on the producer's SSE
	// stream alongside 'enqueued' / 'claimed' / 'done'.
	if res.DurableFactor != 0 {
		s.markPendingDurable(res.EnqueuedSeq, res.JobID, res.DurableFactor)
	}

	if accepts(r, "text/event-stream") {
		s.streamJob(w, r, sess, res.JobID)
		return
	}
	writeJSON(w, http.StatusOK, protocol.EnqueueResponse{JobID: res.JobID, Status: "queued"})
}

// handleEvents resumes (or freshly subscribes to) the SSE event stream
// for a job. Last-Event-ID, when supplied, is the seq of the last
// event the client successfully received; replay starts from
// seq > Last-Event-ID.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	sess, ok := s.requireSession(w)
	if !ok {
		return
	}
	jobID := r.PathValue("jobId")
	if jobID == "" {
		http.Error(w, "jobId required", http.StatusBadRequest)
		return
	}
	s.streamJob(w, r, sess, jobID)
}

// streamJob is the shared SSE engine used by both POST /enqueue (when
// the client sent Accept: text/event-stream) and GET /events/{jobId}.
// It replays from Last-Event-ID, then tails live events until the job
// reaches a terminal state or the client disconnects.
func (s *Server) streamJob(w http.ResponseWriter, r *http.Request, sess *Session, jobID string) {
	if _, err := sess.Store.GetJob(r.Context(), jobID); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			http.Error(w, "job not found", http.StatusNotFound)
			return
		}
		http.Error(w, "lookup failed", http.StatusInternalServerError)
		return
	}

	sinceSeq := int64(0)
	if h := r.Header.Get("Last-Event-ID"); h != "" {
		if v, err := strconv.ParseInt(h, 10, 64); err == nil {
			sinceSeq = v
		}
	}

	// Subscribe BEFORE replay to avoid the race where a terminal event
	// lands between the historical query and the subscription. We dedupe
	// by seq to drop overlap.
	jobCh, unsub := s.bus.SubscribeJob(jobID)
	defer unsub()

	sse, err := newSSE(w)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	historical, err := sess.Store.EventsSince(r.Context(), jobID, sinceSeq)
	if err != nil {
		s.logger.Error("events replay failed", "job", jobID, "err", err)
		return
	}
	lastSent := sinceSeq
	for _, ev := range historical {
		if err := sse.send(ev.Seq, ev.EventType, ev.Payload); err != nil {
			return
		}
		lastSent = ev.Seq
		if protocol.IsTerminal(ev.EventType) {
			return
		}
	}

	heartbeat := time.NewTicker(s.cfg.HeartbeatInterval)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			if err := sse.heartbeat(); err != nil {
				return
			}
		case ev := <-jobCh:
			if ev.ID <= lastSent {
				continue
			}
			if err := sse.send(ev.ID, ev.Type, ev.Data); err != nil {
				return
			}
			lastSent = ev.ID
			if protocol.IsTerminal(ev.Type) {
				return
			}
		}
	}
}

func (s *Server) handleDequeue(w http.ResponseWriter, r *http.Request) {
	sess, ok := s.requireSession(w)
	if !ok {
		return
	}
	lane := r.PathValue("lane")
	if lane == "" {
		http.Error(w, "lane required", http.StatusBadRequest)
		return
	}

	// Optional capability filter: ?capabilities=email,pdf — substring
	// match against the document JSON, OR-combined. Empty = no filter.
	var capabilities []string
	if raw := r.URL.Query().Get("capabilities"); raw != "" {
		for _, c := range strings.Split(raw, ",") {
			if c = strings.TrimSpace(c); c != "" {
				capabilities = append(capabilities, c)
			}
		}
	}

	workerID := uuid.NewString()

	laneCh, unsubLane := s.bus.SubscribeLane(lane)
	defer unsubLane()

	sse, err := newSSE(w)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := sse.send(0, "welcome", []byte(fmt.Sprintf(`{"worker_id":%q}`, workerID))); err != nil {
		return
	}

	heartbeat := time.NewTicker(s.cfg.HeartbeatInterval)
	defer heartbeat.Stop()
	idle := time.NewTicker(s.cfg.DequeueIdlePoll)
	defer idle.Stop()

	for {
		res, hit, err := sess.Store.Dequeue(r.Context(), lane, workerID, s.cfg.LockDuration, capabilities)
		if err != nil {
			s.logger.Error("dequeue failed", "lane", lane, "err", err)
			return
		}
		if !hit {
			select {
			case <-r.Context().Done():
				return
			case <-laneCh:
			case <-idle.C:
			case <-heartbeat.C:
				if err := sse.heartbeat(); err != nil {
					return
				}
			}
			continue
		}

		// Subscribe to this job's events BEFORE we hand it off so we
		// don't miss the terminal event from a fast worker.
		jobCh, unsubJob := s.bus.SubscribeJob(res.Job.ID)

		// Publish the claim so /events subscribers see it. The dequeue
		// stream itself sends a 'job' event below — those are different
		// audiences.
		s.bus.PublishJob(eventbus.Event{
			ID:    res.ClaimedSeq,
			JobID: res.Job.ID,
			Type:  protocol.EventClaimed,
			Data:  res.ClaimedData,
		})

		payload, err := json.Marshal(res.Job)
		if err != nil {
			unsubJob()
			s.logger.Error("marshal job", "err", err)
			return
		}
		if err := sse.send(res.ClaimedSeq, "job", payload); err != nil {
			unsubJob()
			return
		}
		s.stats.Dequeue.Inc(lane)

		if !s.waitTerminal(r, sse, jobCh, heartbeat) {
			unsubJob()
			return
		}
		unsubJob()
	}
}

// waitTerminal blocks until a terminal event for the currently-claimed
// job arrives on jobCh, the client disconnects, or a write error
// occurs. Returns false on disconnect/error so the dequeue loop knows
// to exit.
func (s *Server) waitTerminal(r *http.Request, sse *sseWriter, jobCh <-chan eventbus.Event, hb *time.Ticker) bool {
	for {
		select {
		case <-r.Context().Done():
			return false
		case <-hb.C:
			if err := sse.heartbeat(); err != nil {
				return false
			}
		case ev, ok := <-jobCh:
			if !ok {
				return false
			}
			if protocol.IsTerminal(ev.Type) {
				return true
			}
		}
	}
}

// handleJobAction dispatches POST /jobs/{id}/{action}. We use a
// catch-all path value here because Go's mux does not support nested
// path patterns with dynamic suffixes; the action lives at the end of
// the path and we split it ourselves.
func (s *Server) handleJobAction(w http.ResponseWriter, r *http.Request) {
	sess, ok := s.requireSession(w)
	if !ok {
		return
	}
	rest := r.PathValue("id")
	if rest == "" {
		http.Error(w, "missing job id", http.StatusNotFound)
		return
	}

	// The path is "<entity>/<ulid>/<action>". Split off the action,
	// keeping the rest as the job id.
	slash := strings.LastIndexByte(rest, '/')
	if slash <= 0 {
		http.Error(w, "missing action", http.StatusNotFound)
		return
	}
	jobID, action := rest[:slash], rest[slash+1:]

	workerID := r.Header.Get("X-Worker-ID")
	if workerID == "" {
		http.Error(w, "X-Worker-ID header required", http.StatusBadRequest)
		return
	}

	switch action {
	case "done":
		s.handleDone(w, r, sess, jobID, workerID)
	case "fail":
		s.handleFail(w, r, sess, jobID, workerID)
	case "release":
		s.handleRelease(w, r, sess, jobID, workerID)
	case "progress":
		s.handleProgress(w, r, sess, jobID, workerID)
	default:
		http.Error(w, "unknown action: "+action, http.StatusNotFound)
	}
}

func (s *Server) handleDone(w http.ResponseWriter, r *http.Request, sess *Session, jobID, workerID string) {
	var req protocol.DoneRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	seq, data, err := sess.Store.Done(r.Context(), jobID, workerID, req.Result)
	if err != nil {
		s.handleStoreErr(w, err)
		return
	}
	s.bus.PublishJob(eventbus.Event{ID: seq, JobID: jobID, Type: protocol.EventDone, Data: data})
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleFail(w http.ResponseWriter, r *http.Request, sess *Session, jobID, workerID string) {
	body, err := readBody(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(body) == 0 {
		body = []byte(`{}`)
	}
	seq, data, err := sess.Store.Fail(r.Context(), jobID, workerID, body)
	if err != nil {
		s.handleStoreErr(w, err)
		return
	}
	s.bus.PublishJob(eventbus.Event{ID: seq, JobID: jobID, Type: protocol.EventFailed, Data: data})
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleRelease(w http.ResponseWriter, r *http.Request, sess *Session, jobID, workerID string) {
	var req protocol.ReleaseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		http.Error(w, fmt.Sprintf("invalid json: %v", err), http.StatusBadRequest)
		return
	}
	delay := time.Duration(req.DelaySeconds) * time.Second
	if delay < 0 {
		delay = 0
	}
	lane, _ := sess.Store.JobLane(r.Context(), jobID)
	seq, data, err := sess.Store.Release(r.Context(), jobID, workerID, delay)
	if err != nil {
		s.handleStoreErr(w, err)
		return
	}
	s.bus.PublishJob(eventbus.Event{ID: seq, JobID: jobID, Type: protocol.EventReleased, Data: data})
	if lane != "" && delay == 0 {
		s.bus.SignalLane(lane)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleProgress(w http.ResponseWriter, r *http.Request, sess *Session, jobID, workerID string) {
	var req protocol.ProgressRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		http.Error(w, fmt.Sprintf("invalid json: %v", err), http.StatusBadRequest)
		return
	}
	data := []byte(req.Data)
	if len(data) == 0 {
		data = []byte(`{}`)
	}
	seq, payload, err := sess.Store.Progress(r.Context(), jobID, workerID, data)
	if err != nil {
		s.handleStoreErr(w, err)
		return
	}
	s.bus.PublishJob(eventbus.Event{ID: seq, JobID: jobID, Type: protocol.EventProgress, Data: payload})
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleStoreErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, storage.ErrNotFound):
		http.Error(w, "job not found", http.StatusNotFound)
	case errors.Is(err, storage.ErrInvalidState):
		http.Error(w, "job not in processing state", http.StatusConflict)
	case errors.Is(err, storage.ErrLockMismatch):
		http.Error(w, "worker does not hold the lock", http.StatusConflict)
	default:
		s.stats.Errors.Inc("")
		s.logger.Error("store error", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func accepts(r *http.Request, mime string) bool {
	for _, h := range r.Header.Values("Accept") {
		if strings.Contains(h, mime) {
			return true
		}
	}
	return false
}

// readBody returns up to 1 MiB of the request body. Anything larger is
// almost certainly accidental — the queue does not parse documents.
func readBody(r *http.Request) ([]byte, error) {
	return io.ReadAll(http.MaxBytesReader(nil, r.Body, 1<<20))
}
