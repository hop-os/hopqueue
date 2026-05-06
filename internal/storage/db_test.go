package storage

import (
	"context"
	"path/filepath"
	"testing"
)

func TestOpen_AppliesMigrations_Memory(t *testing.T) {
	t.Parallel()
	db, err := Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	assertSchema(t, db)
}

func TestOpen_AppliesMigrations_File(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "queue.db")
	db, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	assertSchema(t, db)

	// Re-opening the same file is a no-op for migrations.
	db2, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	defer db2.Close()

	var count int
	if err := db2.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM schema_migrations`,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("after re-open, schema_migrations rows = %d, want 1", count)
	}
}

func assertSchema(t *testing.T, db *DB) {
	t.Helper()
	tables := []string{"schema_migrations", "jobs", "replication_log", "node_state"}
	for _, want := range tables {
		var got string
		err := db.QueryRowContext(context.Background(),
			`SELECT name FROM sqlite_master WHERE type='table' AND name=?`,
			want,
		).Scan(&got)
		if err != nil {
			t.Errorf("table %q: %v", want, err)
			continue
		}
		if got != want {
			t.Errorf("table = %q, want %q", got, want)
		}
	}

	// Spot-check that the partial indexes exist.
	indexes := []string{"idx_jobs_pending", "idx_jobs_processing", "idx_jobs_completed", "idx_replog_recent", "idx_replog_job"}
	for _, want := range indexes {
		var got string
		err := db.QueryRowContext(context.Background(),
			`SELECT name FROM sqlite_master WHERE type='index' AND name=?`,
			want,
		).Scan(&got)
		if err != nil || got != want {
			t.Errorf("index %q missing: got %q err %v", want, got, err)
		}
	}
}

func TestStripDuplicateSchemaMigrations(t *testing.T) {
	t.Parallel()
	in := `CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at INTEGER NOT NULL
);

CREATE TABLE jobs (id TEXT PRIMARY KEY);`
	out := stripDuplicateSchemaMigrations(in)
	if got := out; len(got) >= len(in) {
		t.Fatalf("expected output shorter than input; got len=%d in=%d", len(got), len(in))
	}
	// CREATE TABLE jobs must remain.
	wantSubstr := "CREATE TABLE jobs"
	if !contains(out, wantSubstr) {
		t.Errorf("output missing %q: %q", wantSubstr, out)
	}
	// CREATE TABLE schema_migrations must be gone.
	bad := "CREATE TABLE schema_migrations"
	if contains(out, bad) {
		t.Errorf("output still contains %q: %q", bad, out)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
