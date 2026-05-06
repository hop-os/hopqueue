// Performance tests for haasqueue.
//
// Two flavours live in this file:
//
//   - Benchmarks (BenchmarkXxx) — go's built-in benchmark framework.
//     Run with `go test -bench=. -run=^$ ./test/integration`. They
//     measure throughput of the enqueue path under different
//     configurations.
//
//   - Sustained-load tests (TestPerf_*) — assert latency percentiles
//     match the spec's acceptance criteria. Skipped under -short so
//     `go test ./...` stays fast.
//
// Importantly, none of these run with -race meaningfully — the race
// detector slows everything down by ~10x. Run them without -race for
// honest numbers:
//
//   go test -run TestPerf -bench=. -benchtime=2s ./test/integration
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"haaslock/mem"

	"haasqueue/pkg/protocol"
)

// enqueueOnce posts a single enqueue with the given durable_factor
// and returns the round-trip duration plus an error. Reuses the
// shared http.Client so connection pooling kicks in.
func enqueueOnce(client *http.Client, base, lane, entity string, durable int) (time.Duration, error) {
	body, _ := json.Marshal(protocol.EnqueueRequest{
		Lane:          lane,
		Entity:        entity,
		DurableFactor: durable,
		Document:      json.RawMessage(`{"hello":"world"}`),
	})
	req, err := http.NewRequest(http.MethodPost, base+"/enqueue", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status %d", resp.StatusCode)
	}
	return time.Since(start), nil
}

// percentiles returns p50, p95, p99 of the input slice. Mutates the
// input by sorting it.
func percentiles(durations []time.Duration) (p50, p95, p99 time.Duration) {
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	idx := func(p int) time.Duration {
		i := len(durations) * p / 100
		if i >= len(durations) {
			i = len(durations) - 1
		}
		return durations[i]
	}
	return idx(50), idx(95), idx(99)
}

func sharedClient() *http.Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = 100
	tr.MaxIdleConns = 100
	return &http.Client{Transport: tr}
}

// BenchmarkEnqueue_SingleNode_Fire is the upper-bound throughput
// number for the queue: one node, durable_factor=0 (fire-and-forget),
// no replication overhead. Measures pure SQLite-write + HTTP-handler
// time.
func BenchmarkEnqueue_SingleNode_Fire(b *testing.B) {
	node := startServer(b)
	client := sharedClient()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := enqueueOnce(client, node.URL, "x", "e", 0); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEnqueue_3Node_Durable1 measures enqueue throughput in a
// 3-node cluster with durable_factor=1. Each enqueue still returns
// without waiting for synced (we use Accept: application/json), so
// the number is "writes-per-second the leader can absorb while
// replication runs in the background".
func BenchmarkEnqueue_3Node_Durable1(b *testing.B) {
	leaseBackend := mem.New()
	nodes := startReplicationCluster(b, 3, leaseBackend, 500*time.Millisecond)
	leader := waitForReplicationLeader(b, nodes, 3*time.Second)
	time.Sleep(300 * time.Millisecond)

	client := sharedClient()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := enqueueOnce(client, leader.URL, "x", "e", 1); err != nil {
			b.Fatal(err)
		}
	}
}

// sustainedConfig describes one open-loop perf scenario: at a fixed
// arrival rate, for a fixed duration, against a target leader URL,
// with a given durable_factor on each enqueue.
type sustainedConfig struct {
	Name      string
	TargetRPS int
	Duration  time.Duration
	Durable   int
	P99SLO    time.Duration // 0 disables the assertion
}

// runSustained drives an open-loop request generator at config.TargetRPS
// for config.Duration and reports latency percentiles. Returns
// (achievedRPS, p50, p95, p99, errCount, totalDispatched).
func runSustained(t *testing.T, leaderURL string, cfg sustainedConfig) (achieved float64, p50, p95, p99 time.Duration, errCount, dispatched int) {
	t.Helper()
	client := sharedClient()
	bufSize := cfg.TargetRPS*int(cfg.Duration/time.Second)*2 + 1024
	durations := make(chan time.Duration, bufSize)
	errors := make(chan error, 1024)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Duration+10*time.Second)
	defer cancel()

	workers := 128
	jobs := make(chan int, workers*4)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range jobs {
				d, err := enqueueOnce(client, leaderURL, "perf", fmt.Sprintf("e-%d", n), cfg.Durable)
				if err != nil {
					select {
					case errors <- err:
					default:
					}
					continue
				}
				durations <- d
			}
		}()
	}

	// Warm-up: fill the connection pool, prime caches.
	for i := 0; i < 200; i++ {
		jobs <- -i
	}
	for len(durations) < 200 {
		time.Sleep(10 * time.Millisecond)
	}
	for len(durations) > 0 {
		<-durations
	}

	tickInterval := time.Second / time.Duration(cfg.TargetRPS)
	tick := time.NewTicker(tickInterval)
	defer tick.Stop()
	end := time.After(cfg.Duration)
	startWall := time.Now()
loop:
	for {
		select {
		case <-tick.C:
			select {
			case jobs <- dispatched:
				dispatched++
			default:
				// Worker pool saturated; the slip shows up in
				// achievedRPS being lower than TargetRPS.
			}
		case <-end:
			break loop
		case <-ctx.Done():
			break loop
		}
	}
	wallDuration := time.Since(startWall)
	close(jobs)
	wg.Wait()
	close(durations)
	close(errors)

	all := make([]time.Duration, 0, dispatched)
	for d := range durations {
		all = append(all, d)
	}
	for range errors {
		errCount++
	}
	if len(all) == 0 {
		t.Fatal("no successful requests recorded")
	}
	p50, p95, p99 = percentiles(all)
	achieved = float64(len(all)) / wallDuration.Seconds()
	return
}

// TestPerf_SustainedAcceptance is the spec acceptance test #8:
// "Sustained 1000 r/s durable_factor=1, p99 < 100ms". A focused
// single-scenario run that fails the test if the SLO is violated.
func TestPerf_SustainedAcceptance(t *testing.T) {
	if testing.Short() {
		t.Skip("perf test skipped under -short")
	}
	t.Parallel()

	leaseBackend := mem.New()
	nodes := startReplicationCluster(t, 3, leaseBackend, 500*time.Millisecond)
	leader := waitForReplicationLeader(t, nodes, 3*time.Second)
	time.Sleep(500 * time.Millisecond)

	achieved, p50, p95, p99, errCount, dispatched := runSustained(t, leader.URL, sustainedConfig{
		Name:      "spec acceptance",
		TargetRPS: 1000,
		Duration:  10 * time.Second,
		Durable:   1,
		P99SLO:    100 * time.Millisecond,
	})
	t.Logf("dispatched=%d achieved=%.0f r/s errors=%d", dispatched, achieved, errCount)
	t.Logf("latency p50=%s p95=%s p99=%s", p50, p95, p99)

	if errCount > dispatched/100 {
		t.Errorf("error rate too high: %d/%d", errCount, dispatched)
	}
	if p99 > 100*time.Millisecond {
		t.Errorf("p99 = %s exceeds 100ms SLO", p99)
	}
}

// TestPerf_RealNumbers reports honest performance across multiple
// rates and configurations. Does not assert SLOs — just measures and
// logs. Run without -race for meaningful numbers:
//
//	go test -run TestPerf_RealNumbers ./test/integration -v
func TestPerf_RealNumbers(t *testing.T) {
	if testing.Short() {
		t.Skip("perf test skipped under -short")
	}

	const reportDuration = 5 * time.Second

	// Single-node configurations probe the SQLite + HTTP write path
	// without replication overhead.
	t.Run("single-node", func(t *testing.T) {
		t.Parallel()
		node := startServer(t)
		for _, target := range []int{1000, 5000, 10000} {
			t.Run(fmt.Sprintf("durable=0_rps=%d", target), func(t *testing.T) {
				achieved, p50, p95, p99, errCount, dispatched := runSustained(t, node.URL, sustainedConfig{
					TargetRPS: target,
					Duration:  reportDuration,
					Durable:   0,
				})
				t.Logf("target=%d achieved=%.0f dispatched=%d errors=%d p50=%s p95=%s p99=%s",
					target, achieved, dispatched, errCount, p50, p95, p99)
			})
		}
	})

	// 3-node cluster with varying durable factors. durable_factor=1
	// = "any one peer ack" — the typical production setting.
	// durable_factor=2 = quorum.
	t.Run("3-node", func(t *testing.T) {
		t.Parallel()
		leaseBackend := mem.New()
		nodes := startReplicationCluster(t, 3, leaseBackend, 500*time.Millisecond)
		leader := waitForReplicationLeader(t, nodes, 3*time.Second)
		time.Sleep(500 * time.Millisecond)

		for _, scenario := range []struct {
			durable int
			rps     []int
		}{
			{0, []int{1000, 3000, 5000}},
			{1, []int{1000, 2000, 3000}},
			{2, []int{1000, 2000, 3000}},
		} {
			for _, target := range scenario.rps {
				durable := scenario.durable
				name := fmt.Sprintf("durable=%d_rps=%d", durable, target)
				t.Run(name, func(t *testing.T) {
					achieved, p50, p95, p99, errCount, dispatched := runSustained(t, leader.URL, sustainedConfig{
						TargetRPS: target,
						Duration:  reportDuration,
						Durable:   durable,
					})
					t.Logf("target=%d achieved=%.0f dispatched=%d errors=%d p50=%s p95=%s p99=%s",
						target, achieved, dispatched, errCount, p50, p95, p99)
				})
			}
		}
	})
}

// TestPerf_BurstThroughput is a closed-loop stress test: hammer the
// leader with as many concurrent enqueues as we can for 2 seconds
// and report aggregate throughput. Useful to spot regressions in
// the SQLite/HTTP critical path.
func TestPerf_BurstThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("perf test skipped under -short")
	}
	t.Parallel()

	node := startServer(t)
	client := sharedClient()

	const (
		concurrency = 32
		duration    = 2 * time.Second
	)

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var sent atomic.Int64
	var failed atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				if _, err := enqueueOnce(client, node.URL, "burst", fmt.Sprintf("w%d", id), 0); err != nil {
					failed.Add(1)
					continue
				}
				sent.Add(1)
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)

	rps := float64(sent.Load()) / elapsed.Seconds()
	t.Logf("concurrency=%d sent=%d failed=%d elapsed=%s rps=%.0f",
		concurrency, sent.Load(), failed.Load(), elapsed, rps)
	if failed.Load() > 0 {
		t.Errorf("had %d failures during burst", failed.Load())
	}
}

