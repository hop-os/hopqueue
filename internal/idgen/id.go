// Package idgen produces job IDs of the form "<entity>/<ulid>".
//
// The entity portion is supplied by the client and is a logical namespace
// (e.g. "customer-1234", "import-batch-2026-01-15"). The ULID portion is
// generated server-side and cannot be supplied — the queue rejects any
// client-supplied ID. ULIDs sort lexicographically by time, which the
// pending-job index relies on for FIFO ordering within a lane.
package idgen

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

// MaxEntityLength is the upper bound on the entity portion of a job ID.
// Values above this are rejected by ValidateEntity.
const MaxEntityLength = 128

// Errors returned by ValidateEntity. All are wrapped with the offending
// input so callers can return a useful 400 to the API client.
var (
	ErrEntityEmpty    = errors.New("idgen: entity is empty")
	ErrEntityTooLong  = errors.New("idgen: entity exceeds 128 bytes")
	ErrEntityHasSlash = errors.New("idgen: entity must not contain '/'")
	ErrEntityNotASCII = errors.New("idgen: entity must be printable ASCII")
)

// Generator builds new job IDs. Construct with New (production) or
// NewWithClock (tests). Safe for concurrent use.
type Generator struct {
	now func() time.Time

	mu      sync.Mutex
	entropy ulid.MonotonicReader
}

// New returns a Generator backed by time.Now and crypto/rand. The
// underlying ULID monotonic entropy is shared across all NewID calls,
// guaranteeing that IDs generated within the same millisecond still sort
// in invocation order.
func New() *Generator {
	return &Generator{
		now:     time.Now,
		entropy: ulid.Monotonic(rand.Reader, 0),
	}
}

// NewWithClock is the test constructor. now drives the timestamp half of
// every ULID; rng drives the random half. Pass crypto/rand.Reader for
// real entropy or a deterministic io.Reader for reproducible tests.
func NewWithClock(now func() time.Time, rng io.Reader) *Generator {
	if rng == nil {
		rng = rand.Reader
	}
	return &Generator{
		now:     now,
		entropy: ulid.Monotonic(rng, 0),
	}
}

// NewID produces "<entity>/<ulid>". Returns an error if entity fails
// validation.
func (g *Generator) NewID(entity string) (string, error) {
	if err := ValidateEntity(entity); err != nil {
		return "", err
	}
	g.mu.Lock()
	id, err := ulid.New(ulid.Timestamp(g.now()), g.entropy)
	g.mu.Unlock()
	if err != nil {
		return "", fmt.Errorf("idgen: ulid.New: %w", err)
	}
	return entity + "/" + id.String(), nil
}

// ValidateEntity enforces the wire-format rules for the entity portion of
// a job ID: 1-128 bytes, printable ASCII (0x20 through 0x7E inclusive),
// no '/'.
//
// The "no slash" rule is fundamental — slashes are the separator between
// entity and ULID, so allowing one would make IDs ambiguous. The other
// rules are pragmatic: ASCII keeps URLs and logs readable, the length
// cap keeps storage predictable.
func ValidateEntity(entity string) error {
	if entity == "" {
		return ErrEntityEmpty
	}
	if len(entity) > MaxEntityLength {
		return fmt.Errorf("%w: %d bytes", ErrEntityTooLong, len(entity))
	}
	if strings.ContainsRune(entity, '/') {
		return ErrEntityHasSlash
	}
	for i := 0; i < len(entity); i++ {
		c := entity[i]
		if c < 0x20 || c >= 0x7F {
			return fmt.Errorf("%w: byte 0x%02x at offset %d", ErrEntityNotASCII, c, i)
		}
	}
	return nil
}

// SplitID returns the entity and ULID portions of a job ID. Returns an
// error if id is malformed (no '/', empty entity, ULID portion not 26
// chars). Useful for handlers that want to log the entity or route by it.
func SplitID(id string) (entity, idPart string, err error) {
	slash := strings.IndexByte(id, '/')
	if slash <= 0 {
		return "", "", fmt.Errorf("idgen: id missing '/' separator: %q", id)
	}
	entity = id[:slash]
	idPart = id[slash+1:]
	if len(idPart) != 26 {
		return "", "", fmt.Errorf("idgen: ULID portion has length %d, want 26: %q", len(idPart), id)
	}
	if err := ValidateEntity(entity); err != nil {
		return "", "", err
	}
	return entity, idPart, nil
}
