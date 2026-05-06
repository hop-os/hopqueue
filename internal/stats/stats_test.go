package stats

import (
	"testing"
	"time"
)

func TestRateCounter_Total(t *testing.T) {
	t.Parallel()
	r := &RateCounter{}
	for i := 0; i < 10; i++ {
		r.Inc("")
	}
	if got := r.Total(); got != 10 {
		t.Errorf("Total = %d, want 10", got)
	}
}

func TestRateCounter_PerLabel(t *testing.T) {
	t.Parallel()
	r := &RateCounter{}
	r.Inc("a")
	r.Inc("a")
	r.Inc("b")
	got := r.PerLabel()
	if got["a"] != 2 || got["b"] != 1 {
		t.Errorf("PerLabel = %+v, want a=2 b=1", got)
	}
	if total := r.Total(); total != 3 {
		t.Errorf("Total = %d, want 3", total)
	}
}

func TestHistogram_Percentiles(t *testing.T) {
	t.Parallel()
	h := &Histogram{}
	for i := 1; i <= 100; i++ {
		h.Record(time.Duration(i) * time.Millisecond)
	}
	p50, p95, p99 := h.Percentiles()
	// Values are 1..100 ms; p50 ≈ 50ms, p95 ≈ 95ms, p99 ≈ 99ms.
	if p50 < 49*time.Millisecond || p50 > 51*time.Millisecond {
		t.Errorf("p50 = %s, want ~50ms", p50)
	}
	if p95 < 94*time.Millisecond || p95 > 96*time.Millisecond {
		t.Errorf("p95 = %s, want ~95ms", p95)
	}
	if p99 < 98*time.Millisecond || p99 > 100*time.Millisecond {
		t.Errorf("p99 = %s, want ~99ms", p99)
	}
}

func TestHistogram_Empty(t *testing.T) {
	t.Parallel()
	h := &Histogram{}
	p50, p95, p99 := h.Percentiles()
	if p50 != 0 || p95 != 0 || p99 != 0 {
		t.Errorf("empty histogram percentiles non-zero: %s %s %s", p50, p95, p99)
	}
}

func TestRegistry_LeaderInfo(t *testing.T) {
	t.Parallel()
	r := New()
	if _, _, ok := r.LeaderInfo(); ok {
		t.Error("fresh registry reports leader=true")
	}
	now := time.Now()
	r.SetLeader(now, 7)
	since, gen, ok := r.LeaderInfo()
	if !ok || gen != 7 || since.Unix() != now.Unix() {
		t.Errorf("LeaderInfo = (%v, %d, %v), want (~%v, 7, true)", since, gen, ok, now)
	}
	r.SetLeader(time.Time{}, 0)
	if _, _, ok := r.LeaderInfo(); ok {
		t.Error("after demotion, leader still true")
	}
}

func TestRegistry_Retention(t *testing.T) {
	t.Parallel()
	r := New()
	at, done, failed, trimmed := r.RetentionSnapshot()
	if !at.IsZero() || done != 0 || failed != 0 || trimmed != 0 {
		t.Errorf("fresh retention snapshot non-zero: %+v %d %d %d", at, done, failed, trimmed)
	}
	now := time.Now()
	r.RecordRetentionRun(now, 5, 2, 100)
	at, done, failed, trimmed = r.RetentionSnapshot()
	if at != now || done != 5 || failed != 2 || trimmed != 100 {
		t.Errorf("retention snapshot mismatch: at=%v done=%d failed=%d trimmed=%d", at, done, failed, trimmed)
	}
}
