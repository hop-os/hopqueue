# haasqueue

A small, fast, single-binary job queue. SQLite for storage, HTTP/SSE for the
wire, S3 for leader election. Three nodes replicate over plain HTTP. No
brokers, no Zookeeper, no Redis, no Kafka — one Go binary and one bucket.

```
                    ┌──────────────┐
        producers ──▶│   leader     │── replication_log ──▶ followers
                    │              │                        ▲   ▲
        workers   ◀─┤ /enqueue     │                        │   │
        SSE       ◀─┤ /dequeue     │   S3 lease (haaslock) ─┘   │
                    │ /events/{id} │                            │
                    └──────────────┘ ◀── ack stream ────────────┘
```

## Why

Most job queues either embrace a heavy stack (Kafka, RabbitMQ) or run on
single-node Redis with hand-rolled HA. haasqueue sits in the middle: durable
SQLite WAL on every node, a leader chosen by an S3 conditional-write lease,
and replication that is just a tail of an append-only log.

- **One binary, no daemons.** SQLite is embedded; the lease is just an S3
  PUT-If-Match. A node needs a disk and an S3 endpoint, nothing else.
- **HTTP everywhere.** Producers `POST /enqueue`. Workers do
  `GET /dequeue/{lane}` (SSE long-poll). Followers do `GET /replication/stream`
  (SSE tail). Last-Event-ID resumes any stream cleanly.
- **Configurable durability per job.** `durable_factor` of 0 (fire-and-forget),
  1 (any peer ack), 2 (quorum), or -1 (all peers). The leader emits a `synced`
  event when the threshold is reached, alongside the regular lifecycle events.
- **Per-job event stream out of the box.** Every state change is an SSE event
  on `/events/{jobId}` with a stable `seq` for replay. No polling.
- **Group-commit writer.** Concurrent enqueues coalesce into a single
  transaction per `fsync`. Idle = batch of 1; under load = up to 256 per
  commit. Throughput scales with concurrency, not with disk speed.
- **Built-in operations dashboard.** `GET /` renders an auto-refresh HTML
  view with cluster, throughput, lane, replication and retention sections.
  No JavaScript, no external assets.

## Quick start

```bash
# Single node, no clustering, listens on :8080
go run ./cmd/haasqueue

# Submit a job
curl -s -X POST http://localhost:8080/enqueue \
  -H 'Content-Type: application/json' \
  -d '{"lane":"emails","entity":"customer-1","document":{"to":"a@b.c"}}'
# → {"job_id":"customer-1/01HXX...","status":"queued"}

# Long-poll for work on a lane (SSE)
curl -N -H 'Accept: text/event-stream' \
  http://localhost:8080/dequeue/emails

# Open the dashboard
open http://localhost:8080/
```

## Cluster mode

Set `QUEUE_LEASE_URL` to an S3 object that all candidate nodes can
conditionally PUT to. The first node that wins the conditional write becomes
leader; the rest tail it as followers. On lease expiry or a deliberate
demotion, election re-runs.

```bash
export QUEUE_LEASE_URL=s3://my-bucket/haasqueue.lease
export QUEUE_PUBLIC_URL=https://node1.internal:8080
export QUEUE_NODE_ID=node1
go run ./cmd/haasqueue
```

Followers reconcile via the leader's `replication_log` table over an SSE
stream; the leader exposes `GET /replication/stream` and `POST /replication/ack`.
Followers persist their applied seq in `node_state.current_seq` so a restart
resumes from where it left off.

A non-leader request gets `503 Service Unavailable` with an `X-Active-Leader`
header pointing at the current leader — clients retry there.

## Build options

The SQLite driver is selected at build time. The default is pure Go, no CGO.

```bash
go build ./...                      # modernc.org/sqlite (default, pure Go)
go build -tags cgo_sqlite ./...     # github.com/mattn/go-sqlite3 (CGO)
```

The CGO build is opt-in. It needs a C toolchain at build time. In return, p99
latency under sustained 3-node replicated load is roughly 3-15× lower at the
durable=2 working point. For most workloads (durable=1, under ~2k req/s) the
two drivers are indistinguishable; see the numbers below.

## Performance

Hardware: Apple M4 Pro, darwin/arm64. Three nodes co-located in-process. No
`-race`. Each percentile is the worst response over a 30-second
open-loop run with a 128-worker pool.

### 3-node, durable=1 (any peer ack)

| Driver  | RPS  | p50    | p75    | p90    | p95    | p99    |
|---------|------|--------|--------|--------|--------|--------|
| modernc | 1000 | 233µs  | 255µs  | 279µs  | 301µs  | 901µs  |
| CGO     | 1000 | 221µs  | 237µs  | 256µs  | 281µs  | 957µs  |
| modernc | 2000 | 479µs  | 819µs  | 1.49ms | 3.63ms | 36.8ms |
| CGO     | 2000 | 103µs  | 117µs  | 154µs  | 377µs  | 1.36ms |
| modernc | 3000 | 2.07ms | 3.07ms | 4.22ms | 5.14ms | 37.4ms |
| CGO     | 3000 | 1.15ms | 1.88ms | 2.62ms | 3.55ms | 34.7ms |

### 3-node, durable=2 (quorum)

| Driver  | RPS  | p50    | p75    | p90    | p95    | p99     |
|---------|------|--------|--------|--------|--------|---------|
| modernc | 1000 | 2.64ms | 3.58ms | 5.29ms | 27.6ms | 164.9ms |
| CGO     | 1000 | 959µs  | 1.89ms | 2.82ms | 8.52ms | 117.4ms |
| modernc | 2000 | 1.90ms | 2.45ms | 3.24ms | 10.6ms | 49.9ms  |
| CGO     | 2000 | 519µs  | 901µs  | 1.31ms | 1.97ms | 7.82ms  |
| modernc | 3000 | 3.70ms | 4.76ms | 5.95ms | 6.73ms | 28.4ms  |
| CGO     | 3000 | 2.51ms | 3.41ms | 4.36ms | 4.94ms | 16.7ms  |

### Headline numbers

| Scenario                                           | modernc      | CGO          |
|----------------------------------------------------|--------------|--------------|
| Burst (single-node, 32 concurrent, durable=0, 2s)  | 19,873 rps   | 28,094 rps   |
| Spec acceptance (1k rps, durable=1, 10s)           | p99 = 1.61ms | p99 = 1.16ms |
| Single-node sustained ceiling (durable=0)          | ~8,200 rps   | ~9,100 rps   |
| 3-node sustained ceiling (durable=1)               | ~3,000 rps   | ~3,000 rps   |
| Single-thread enqueue benchmark                    | 117µs/op     | 82µs/op      |

### How to read this

- **Pick modernc unless you have a reason not to.** Pure Go, trivial
  cross-compilation, identical to CGO on durable=1 up to ~2k rps. The cliff
  shows up only on durable=2 quorum or above 2k rps.
- **CGO is the right call if you run heavy quorum traffic.** At durable=2,
  every percentile is meaningfully lower under CGO. The `cgo_sqlite` tag is
  one flag away.
- **Both drivers have a 100ms+ p99 tail at durable=2 @ 1k rps.** This is not
  a driver issue — it's a periodic event (lease renewal, WAL checkpoint,
  sweeper) showing up in the open-loop timing. It smears out at higher rps
  and disappears at single-node. Worth investigating before deploying d=2 at
  exactly that working point.
- **The 3-node ceiling is replication-bound, not fsync-bound.** Adding more
  nodes or faster disks won't push past ~3k rps without a different fanout
  strategy. Single-node clears 8k+.

## Architecture

- `internal/storage` — SQLite, migrations, batched-writer enqueue path,
  prepared statements, replication-log helpers.
- `internal/api` — HTTP server, SSE writers, dashboard, sweeper, retention,
  durability watcher.
- `internal/eventbus` — in-process pub/sub for per-job and per-lane fanout.
- `internal/follower` — applies the leader's `replication_log` rows locally.
- `internal/lease` — wraps `haaslock` with the bind/unbind orchestration the
  API server expects.
- `internal/idgen` — `<entity>/<ulid>` IDs, monotonic within a millisecond.
- `internal/scheduler`, `internal/retention` — leader-only background jobs.
- `pkg/protocol` — wire types: requests, responses, replication payloads.
- `cmd/haasqueue` — binary, env-var configuration, leader-task wiring.

## Configuration

All via environment variables. Sensible defaults — single-node mode needs
zero config.

| Variable                    | Default                | Purpose                                  |
|-----------------------------|------------------------|------------------------------------------|
| `QUEUE_NODE_ID`             | `node-1`               | Identifier in `replication_log.origin_node` |
| `QUEUE_DATA_DIR`            | `./data`               | Where `queue.db` lives                   |
| `QUEUE_LISTEN`              | `:8080`                | HTTP listen address                      |
| `QUEUE_LOCK_DURATION`       | `5m`                   | How long a claimed job stays locked      |
| `QUEUE_SWEEP_INTERVAL`      | `5s`                   | How often expired locks are reaped       |
| `QUEUE_RETENTION_DONE`      | `24h`                  | Done jobs older than this are pruned     |
| `QUEUE_RETENTION_FAILED`    | `7d`                   | Failed jobs older than this are pruned   |
| `QUEUE_RETENTION_LOG`       | `7d`                   | replication_log rows older than this go  |
| `QUEUE_RETENTION_INTERVAL`  | `5m`                   | How often retention runs                 |
| `QUEUE_LEASE_URL`           | _(unset)_              | Set to enable cluster mode               |
| `QUEUE_PUBLIC_URL`          | derived from `LISTEN`  | URL peers use to reach this node         |
| `QUEUE_LEASE_TTL`           | `30s`                  | Lease lifetime; renewed at TTL/2         |
| `QUEUE_S3_ENDPOINT`         | AWS default            | For S3-compatible storage (Minio, R2)    |
| `QUEUE_S3_PATH_STYLE`       | _(unset)_              | Override path-style addressing           |
| `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` | from env | S3 credentials |

## API

| Method | Path                        | Notes                                      |
|--------|-----------------------------|--------------------------------------------|
| GET    | `/`                         | Operations dashboard (HTML / JSON)         |
| GET    | `/health`                   | Liveness + leader hint                     |
| POST   | `/enqueue`                  | Submit a job; SSE on `Accept: text/event-stream` |
| GET    | `/events/{jobId}`           | SSE replay + tail of a job's events        |
| GET    | `/dequeue/{lane}`           | SSE long-poll for work on a lane           |
| POST   | `/jobs/{id}/done`           | Mark complete                              |
| POST   | `/jobs/{id}/fail`           | Mark failed                                |
| POST   | `/jobs/{id}/release`        | Return to pending                          |
| POST   | `/jobs/{id}/progress`       | Emit a progress event (no state change)   |
| GET    | `/replication/stream`       | Follower SSE tail (leader only)            |
| POST   | `/replication/ack`          | Follower ack callback                      |
| GET    | `/metrics`                  | Prometheus-style counters and gauges       |

All worker actions require an `X-Worker-ID` header matching the lock holder.

## Testing

```bash
go test ./...                      # everything, default driver
go test -tags cgo_sqlite ./...     # everything under CGO
go test -short ./...               # skip the perf suite
go test -run TestPerf -v ./test/integration/   # numbers + tail latency
go test -bench=. -run=^$ ./test/integration/   # micro-benchmarks
```

The perf tests open-loop a 128-worker pool against an in-process cluster.
They run for five seconds each by default — bump `reportDuration` in
`perf_test.go` for steadier tail-latency numbers.
