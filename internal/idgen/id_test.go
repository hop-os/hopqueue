package idgen

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateEntity(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		in      string
		wantErr error
	}{
		{"plain", "customer-1234", nil},
		{"with-dots", "import.batch.2026-01-15", nil},
		{"empty", "", ErrEntityEmpty},
		{"too-long", strings.Repeat("a", MaxEntityLength+1), ErrEntityTooLong},
		{"max-length", strings.Repeat("a", MaxEntityLength), nil},
		{"contains-slash", "foo/bar", ErrEntityHasSlash},
		{"control-char", "foo\nbar", ErrEntityNotASCII},
		{"tab", "foo\tbar", ErrEntityNotASCII},
		{"del", "foo\x7Fbar", ErrEntityNotASCII},
		{"non-ascii", "café", ErrEntityNotASCII},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := ValidateEntity(c.in)
			if c.wantErr == nil {
				if err != nil {
					t.Errorf("ValidateEntity(%q) = %v, want nil", c.in, err)
				}
				return
			}
			if !errors.Is(err, c.wantErr) {
				t.Errorf("ValidateEntity(%q) = %v, want %v", c.in, err, c.wantErr)
			}
		})
	}
}

func TestNewID_FormatAndOrdering(t *testing.T) {
	t.Parallel()
	g := New()

	id1, err := g.NewID("customer-1234")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(id1, "customer-1234/") {
		t.Errorf("id missing entity prefix: %q", id1)
	}
	if len(id1) != len("customer-1234/")+26 {
		t.Errorf("unexpected id length: %q (%d)", id1, len(id1))
	}

	// IDs generated back-to-back must sort lexicographically (ULID
	// monotonic entropy guarantees this even within the same millisecond).
	id2, err := g.NewID("customer-1234")
	if err != nil {
		t.Fatal(err)
	}
	if id1 >= id2 {
		t.Errorf("ids not monotonically ordered: %q >= %q", id1, id2)
	}
}

func TestNewID_RejectsBadEntity(t *testing.T) {
	t.Parallel()
	g := New()
	if _, err := g.NewID("with/slash"); !errors.Is(err, ErrEntityHasSlash) {
		t.Errorf("got %v, want ErrEntityHasSlash", err)
	}
	if _, err := g.NewID(""); !errors.Is(err, ErrEntityEmpty) {
		t.Errorf("got %v, want ErrEntityEmpty", err)
	}
}

func TestSplitID(t *testing.T) {
	t.Parallel()
	g := New()
	id, err := g.NewID("customer-42")
	if err != nil {
		t.Fatal(err)
	}

	entity, ulid, err := SplitID(id)
	if err != nil {
		t.Fatal(err)
	}
	if entity != "customer-42" {
		t.Errorf("entity = %q, want customer-42", entity)
	}
	if len(ulid) != 26 {
		t.Errorf("ulid length = %d, want 26", len(ulid))
	}
}

func TestSplitID_Malformed(t *testing.T) {
	t.Parallel()
	cases := []string{
		"",
		"no-slash",
		"/leading-slash",
		"foo/short",
		"foo/" + strings.Repeat("X", 27),
		"foo bar/" + strings.Repeat("0", 26), // entity has space — that's fine
		"foo\n/01ARZ3NDEKTSV4RRFFQ69G5FAV",
	}
	for _, in := range cases {
		t.Run(in, func(t *testing.T) {
			_, _, err := SplitID(in)
			if err == nil && in != "foo bar/"+strings.Repeat("0", 26) {
				t.Errorf("SplitID(%q): got nil err, want one", in)
			}
		})
	}
}
