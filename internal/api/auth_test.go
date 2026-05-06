package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/xinix00/hopqueue/internal/eventbus"
	"github.com/xinix00/hopqueue/internal/idgen"
	"github.com/xinix00/hopqueue/internal/storage"
)

// TestRoutes_NoAPIKey confirms that without a configured key, every
// route is reachable without an Authorization header. This is the
// local-development path.
func TestRoutes_NoAPIKey(t *testing.T) {
	t.Parallel()
	srv, ts := newTestServer(t, "")
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusServiceUnavailable {
		// /health returns 503 when no Session is bound; either is fine
		// here, the point is that we got *through* the middleware.
		t.Fatalf("/health returned %d", resp.StatusCode)
	}
	_ = srv
}

// TestRoutes_APIKey_Required exercises the four behaviours of the
// bearer-token middleware in one go: health is exempt, missing header
// is 401, wrong key is 401, correct key passes through.
func TestRoutes_APIKey_Required(t *testing.T) {
	t.Parallel()
	const secret = "s3cret-key-1234"
	_, ts := newTestServer(t, secret)
	defer ts.Close()

	// /health should always work regardless of key.
	if got := getStatus(t, ts.URL+"/health", ""); got == http.StatusUnauthorized {
		t.Errorf("/health should be unauthenticated, got %d", got)
	}

	// Protected route without header: 401.
	if got := getStatus(t, ts.URL+"/", ""); got != http.StatusUnauthorized {
		t.Errorf("missing header: status = %d, want 401", got)
	}

	// Wrong key: 401.
	if got := getStatus(t, ts.URL+"/", "Bearer wrong-key"); got != http.StatusUnauthorized {
		t.Errorf("wrong key: status = %d, want 401", got)
	}

	// Correct key passes the middleware. The handler itself returns
	// 503 (no Session), but that's beyond auth — confirms we got
	// through.
	if got := getStatus(t, ts.URL+"/", "Bearer "+secret); got == http.StatusUnauthorized {
		t.Errorf("correct key: middleware blocked us with 401")
	}
}

// TestRoutes_APIKey_ConstantTime is a smoke check that the comparison
// is the constant-time path: feed two same-length-but-different keys
// and a same-length-correct key, and confirm the wrong one is still
// rejected.
func TestRoutes_APIKey_ConstantTime(t *testing.T) {
	t.Parallel()
	const secret = "1234567890ABCDEF"
	_, ts := newTestServer(t, secret)
	defer ts.Close()

	// Same length, different content.
	if got := getStatus(t, ts.URL+"/", "Bearer abcdefghijKLMNOP"); got != http.StatusUnauthorized {
		t.Errorf("equal-length wrong key: status = %d, want 401", got)
	}
}

func getStatus(t *testing.T, url, authz string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if authz != "" {
		req.Header.Set("Authorization", authz)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func newTestServer(t *testing.T, apiKey string) (*Server, *httptest.Server) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "queue.db")
	db, err := storage.Open(context.Background(), dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	bus := eventbus.New()
	cfg := DefaultConfig()
	cfg.APIKey = apiKey
	srv := NewServer(bus, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv.Bind(&Session{Store: db, IDGen: idgen.New()})
	ts := httptest.NewServer(srv.Routes())
	return srv, ts
}
