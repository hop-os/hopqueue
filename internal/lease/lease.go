// Package lease wires haaslock.Elector to the queue's configuration:
// env-driven config, S3 backing by default (or a caller-supplied
// Backend for tests), and a small surface for the rest of the binary
// to consult ("am I leader?", "what is the current fencing
// generation?", "let me run a function only while I'm leader").
package lease

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"haaslock"
	"haaslock/s3"
)

// Config is the lease-related slice of haasqueue's configuration. It
// maps onto haaslock's Backend and Elector fields without exposing them
// directly, so the rest of the binary only sees haasqueue-shaped values.
type Config struct {
	// NodeID is this node's identifier. Used as the haaslock Owner
	// when Owner is empty. Required.
	NodeID string

	// Owner overrides what gets written into lease.State.Owner.
	// Followers read this string to discover the leader's URL, so in
	// cluster mode Owner should be a URL peers can reach
	// (e.g. "http://node-1.cluster:8080"). When empty, NodeID is
	// used.
	Owner string

	// Backend overrides the auto-constructed S3 backend. When non-nil,
	// all S3 fields below are ignored. Tests use this to inject
	// haaslock/mem.Backend; production leaves it nil and lets New build
	// an s3.Backend from the AWS-style fields below.
	Backend haaslock.Backend

	// LeaseURL is the s3:// URL of the lease object, e.g.
	// "s3://my-bucket/queue/lease.json". Required when Backend is nil.
	LeaseURL string

	// Region is the sigv4 region (e.g. "us-east-1", or "auto" for R2).
	// Required when Backend is nil.
	Region string

	// AccessKeyID and SecretAccessKey are the static S3 credentials.
	// Required when Backend is nil.
	AccessKeyID     string
	SecretAccessKey string

	// SessionToken is supplied as X-Amz-Security-Token when non-empty.
	// Optional.
	SessionToken string

	// Endpoint overrides the default region endpoint. Used for R2,
	// MinIO, or any non-AWS S3-compatible service.
	Endpoint string

	// UsePathStyle requests bucket-in-path addressing. Defaults to
	// true when Endpoint is non-empty, false otherwise.
	UsePathStyle *bool

	// TTL is the lease duration. Default 30s. Worst-case failover time
	// after a leader crash.
	TTL time.Duration
}

// Elector is haasqueue's view of haaslock.Elector. It runs the
// election loop in the background and exposes a snapshot ("am I
// leader?") for other subsystems that need to gate writes on
// leadership.
type Elector struct {
	inner *haaslock.Elector
	owner string

	// Atomic snapshot updated by the loop and read by IsLeader/
	// CurrentGeneration. Holds the lease generation while we're leader,
	// 0 otherwise.
	heldGen atomic.Int64
}

// SelfOwner returns this elector's Owner string — the same value that
// will be written into lease.State.Owner when this node acquires.
// Used by the follower applier to compare against the current owner
// and skip subscribing to itself.
func (e *Elector) SelfOwner() string { return e.owner }

// New constructs an Elector from cfg.
func New(cfg Config) (*Elector, error) {
	if cfg.NodeID == "" {
		return nil, errors.New("lease: NodeID is required")
	}
	ttl := cfg.TTL
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	owner := cfg.Owner
	if owner == "" {
		owner = cfg.NodeID
	}

	backend := cfg.Backend
	if backend == nil {
		var err error
		backend, err = newS3Backend(cfg)
		if err != nil {
			return nil, err
		}
	}

	return &Elector{
		inner: &haaslock.Elector{
			Backend: backend,
			Owner:   owner,
			TTL:     ttl,
		},
		owner: owner,
	}, nil
}

// Run starts the election loop and updates IsLeader/CurrentGeneration
// based on lease events. Blocks until ctx is cancelled. Use Lead
// instead if you want to bind a function to leadership lifecycle.
func (e *Elector) Run(ctx context.Context) error {
	events := e.inner.Run(ctx)
	for ev := range events {
		switch ev.Kind {
		case haaslock.EventAcquired, haaslock.EventRenewed:
			e.heldGen.Store(ev.Lease.Generation)
		case haaslock.EventLost:
			e.heldGen.Store(0)
		}
	}
	return ctx.Err()
}

// Lead acquires the lease and calls fn with a context that is
// cancelled when leadership is lost. fn should return when its ctx is
// done. If fn returns nil, Lead waits for the lease to be acquired
// again and calls fn once more, until the outer ctx is cancelled. If
// fn returns a non-nil error, Lead stops and returns it.
//
// IsLeader/CurrentGeneration reflect the current state for the
// duration of fn — so other goroutines that need to gate on leadership
// can check them without coordinating with fn directly.
func (e *Elector) Lead(ctx context.Context, fn func(ctx context.Context, lease haaslock.Lease) error) error {
	return e.inner.Lead(ctx, func(leaderCtx context.Context, l haaslock.Lease) error {
		e.heldGen.Store(l.Generation)
		defer e.heldGen.Store(0)
		return fn(leaderCtx, l)
	})
}

// IsLeader reports whether this node currently holds the lease. The
// answer is a snapshot — the lease can be lost between the check and
// the action that depends on it. Use CurrentGeneration as a fencing
// token in downstream systems where that race matters.
func (e *Elector) IsLeader() bool {
	return e.heldGen.Load() > 0
}

// CurrentOwner peeks the lease backend and returns the current
// owner string and generation, or ("", 0, nil) if no lease exists.
// Used by the follower applier to discover which peer to subscribe to.
//
// This bypasses the elector's own state — useful from followers that
// never see EventAcquired for the *peer* who became leader, only for
// themselves.
func (e *Elector) CurrentOwner(ctx context.Context) (owner string, generation int64, err error) {
	state, _, err := e.inner.Backend.Read(ctx)
	if errors.Is(err, haaslock.ErrNoLease) {
		return "", 0, nil
	}
	if err != nil {
		return "", 0, err
	}
	if state.IsExpired() {
		return "", 0, nil
	}
	return state.Owner, state.Generation, nil
}

// CurrentGeneration returns the generation of the lease this node is
// currently holding, or 0 if it does not hold one. Strictly monotonic
// across leadership changes for a given lease key.
func (e *Elector) CurrentGeneration() int64 {
	return e.heldGen.Load()
}

// newS3Backend builds an s3.Backend from the Config's S3 fields.
// Called from New when cfg.Backend is nil.
func newS3Backend(cfg Config) (haaslock.Backend, error) {
	bucket, key, err := parseLeaseURL(cfg.LeaseURL)
	if err != nil {
		return nil, err
	}
	if cfg.Region == "" {
		return nil, errors.New("lease: Region is required")
	}
	if cfg.AccessKeyID == "" || cfg.SecretAccessKey == "" {
		return nil, errors.New("lease: AccessKeyID and SecretAccessKey are required")
	}

	endpoint := cfg.Endpoint
	if endpoint == "" {
		endpoint = "https://s3." + cfg.Region + ".amazonaws.com"
	}
	pathStyle := false
	switch {
	case cfg.UsePathStyle != nil:
		pathStyle = *cfg.UsePathStyle
	case cfg.Endpoint != "":
		pathStyle = true
	}

	return &s3.Backend{
		Endpoint:        endpoint,
		Bucket:          bucket,
		Key:             key,
		Region:          cfg.Region,
		AccessKeyID:     cfg.AccessKeyID,
		SecretAccessKey: cfg.SecretAccessKey,
		SessionToken:    cfg.SessionToken,
		UsePathStyle:    pathStyle,
	}, nil
}

func parseLeaseURL(raw string) (bucket, key string, err error) {
	if raw == "" {
		return "", "", errors.New("lease: LeaseURL is required")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", "", fmt.Errorf("lease: parse LeaseURL: %w", err)
	}
	if u.Scheme != "s3" {
		return "", "", fmt.Errorf("lease: LeaseURL must use s3:// scheme, got %q", u.Scheme)
	}
	if u.Host == "" {
		return "", "", fmt.Errorf("lease: LeaseURL missing bucket: %q", raw)
	}
	if u.RawQuery != "" {
		return "", "", fmt.Errorf("lease: LeaseURL must not have a query string: %q", raw)
	}
	key = strings.TrimPrefix(u.Path, "/")
	if key == "" {
		return "", "", fmt.Errorf("lease: LeaseURL missing object key: %q", raw)
	}
	return u.Host, key, nil
}
