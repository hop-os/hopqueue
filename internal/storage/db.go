// Package storage owns the SQLite connection and the migration runner.
// All other internal packages use *DB rather than touching database/sql
// directly; that keeps connection settings, PRAGMAs, and migration order
// in one place.
package storage

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// DB is a thin wrapper around *sql.DB that exposes the SQLite connection
// to the rest of the binary. It is goroutine-safe in the same way *sql.DB
// is — concurrent callers share the underlying connection pool.
//
// nodeID is recorded as origin_node on every replication_log row this
// instance writes. Set via SetNodeID after Open; defaults to "local".
//
// OnReplog, if set, is invoked after each successful commit with the
// replog rows that were appended. Callers wire this to fan rows out
// onto the eventbus so /replication/stream subscribers see live
// events. The callback runs synchronously on the writer's goroutine,
// so implementations must not block: a full bus channel drops on the
// floor (consumers reconnect with a since cursor).
type DB struct {
	*sql.DB
	nodeID   string
	onReplog func(rows ...Replog)

	// Prepared statements for the two hot INSERTs on the enqueue path.
	// Allocated in Open; closed in (*DB).Close.
	stmtInsertJob    *sql.Stmt
	stmtInsertReplog *sql.Stmt

	// Group-commit writer state. Producers send pre-validated, pre-
	// marshaled ops on enqueueIn; the writer goroutine drains a batch,
	// runs all inserts in one transaction, fsyncs once, and fans
	// per-op responses back. Closing writerStop drains the writer; it
	// signals exit by closing writerDone.
	enqueueIn  chan enqueueOp
	writerStop chan struct{}
	writerDone chan struct{}
	closeOnce  sync.Once
}

// SetReplogPublisher installs the post-commit replog hook. Pass nil
// to disable publishing (used by tests that don't care about
// replication fanout).
func (db *DB) SetReplogPublisher(fn func(rows ...Replog)) {
	db.onReplog = fn
}

// NodeID returns the origin_node string this DB tags replication_log
// rows with. Used by the api layer when constructing event metadata.
func (db *DB) NodeID() string {
	if db.nodeID == "" {
		return "local"
	}
	return db.nodeID
}

// Open opens (or creates) a SQLite database at path, applies the WAL-mode
// PRAGMAs the queue relies on, and runs any pending migrations.
//
// path may be ":memory:" for an in-memory database (used in tests).
func Open(ctx context.Context, path string) (*DB, error) {
	// _txlock=immediate makes every BeginTx take the write lock up front,
	// turning concurrent enqueue/dequeue/sweep into a clean queue rather
	// than a flurry of SQLITE_BUSY retries. The cost — read-only txns also
	// take the lock — is acceptable for a queue's write-heavy mix.
	//
	// cache_size, mmap_size, temp_store, wal_autocheckpoint are pure
	// performance knobs: bigger page cache + mmap for reads keeps the
	// hot path off syscalls, temp_store=memory keeps spill files out of
	// the picture, and a higher autocheckpoint threshold reduces the
	// frequency of WAL→main rolls under sustained write load.
	driverName, dsn := sqliteDSN(path)
	sqlDB, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("storage.Open: %w", err)
	}

	// In-memory databases collapse to a single connection's view of the
	// data; multiple connections see different empty databases. Pinning to
	// one connection avoids that surprise in tests.
	if path == ":memory:" {
		sqlDB.SetMaxOpenConns(1)
	}

	if err := sqlDB.PingContext(ctx); err != nil {
		sqlDB.Close()
		return nil, fmt.Errorf("storage.Open ping: %w", err)
	}

	db := &DB{
		DB:         sqlDB,
		enqueueIn:  make(chan enqueueOp, 1024),
		writerStop: make(chan struct{}),
		writerDone: make(chan struct{}),
	}
	if err := db.migrate(ctx); err != nil {
		sqlDB.Close()
		return nil, fmt.Errorf("storage.Open migrate: %w", err)
	}

	if err := db.prepareStatements(ctx); err != nil {
		sqlDB.Close()
		return nil, fmt.Errorf("storage.Open prepare: %w", err)
	}

	go db.writerLoop()
	return db, nil
}

// prepareStatements compiles the two hot inserts that the batched
// writer hammers. Called once after migrations finish.
func (db *DB) prepareStatements(ctx context.Context) error {
	var err error
	db.stmtInsertJob, err = db.DB.PrepareContext(ctx, `
		INSERT INTO jobs (id, lane, durable_factor, document, state, attempts, available_at, created_at, updated_at)
		VALUES (?, ?, ?, ?, 'pending', 0, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("prepare insert job: %w", err)
	}
	db.stmtInsertReplog, err = db.DB.PrepareContext(ctx, `
		INSERT INTO replication_log (event_type, job_id, payload, created_at, origin_node)
		VALUES (?, ?, ?, ?, ?)
		RETURNING seq
	`)
	if err != nil {
		return fmt.Errorf("prepare insert replog: %w", err)
	}
	return nil
}

// Close stops the batched writer, releases prepared statements, then
// closes the underlying *sql.DB. Safe to call multiple times.
//
// Callers must ensure no further Enqueue calls happen after Close —
// the writer goroutine is the only thing draining the enqueueIn
// channel, and once it has exited any in-flight ops would block
// forever. main.go enforces this by shutting down the HTTP server
// (and waiting for handlers) before deferring store.Close().
func (db *DB) Close() error {
	var err error
	db.closeOnce.Do(func() {
		close(db.writerStop)
		<-db.writerDone
		if db.stmtInsertJob != nil {
			db.stmtInsertJob.Close()
		}
		if db.stmtInsertReplog != nil {
			db.stmtInsertReplog.Close()
		}
		err = db.DB.Close()
	})
	return err
}

// migrate applies any embedded migration whose version is greater than
// the highest version already recorded in schema_migrations. Migrations
// run in a single transaction each so a failed migration leaves the DB
// at the previous version, never partial.
func (db *DB) migrate(ctx context.Context) error {
	if err := db.ensureMigrationsTable(ctx); err != nil {
		return err
	}

	current, err := db.currentVersion(ctx)
	if err != nil {
		return err
	}

	pending, err := loadMigrations()
	if err != nil {
		return err
	}

	for _, m := range pending {
		if m.version <= current {
			continue
		}
		if err := db.applyMigration(ctx, m); err != nil {
			return fmt.Errorf("apply migration %d (%s): %w", m.version, m.name, err)
		}
	}
	return nil
}

func (db *DB) ensureMigrationsTable(ctx context.Context) error {
	// schema_migrations may not exist on a fresh database. Create it
	// outside the migrations themselves so the very first migration can
	// also use it.
	_, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    INTEGER PRIMARY KEY,
			applied_at INTEGER NOT NULL
		)
	`)
	return err
}

func (db *DB) currentVersion(ctx context.Context) (int, error) {
	var v sql.NullInt64
	err := db.QueryRowContext(ctx, `SELECT MAX(version) FROM schema_migrations`).Scan(&v)
	if err != nil {
		return 0, err
	}
	if !v.Valid {
		return 0, nil
	}
	return int(v.Int64), nil
}

func (db *DB) applyMigration(ctx context.Context, m migration) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, m.body); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)`,
		m.version, time.Now().Unix(),
	); err != nil {
		return err
	}
	return tx.Commit()
}

type migration struct {
	version int
	name    string
	body    string
}

// loadMigrations reads the embedded migrations/*.sql files. Filenames
// must be of the form NNNN_name.sql; NNNN is the version. The 0001_init
// migration's body declares schema_migrations itself, but ensureMigrationsTable
// has already created it, so we skip the duplicate definition by stripping
// any CREATE TABLE schema_migrations statement from the body.
func loadMigrations() ([]migration, error) {
	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return nil, err
	}
	var ms []migration
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		base := strings.TrimSuffix(entry.Name(), ".sql")
		parts := strings.SplitN(base, "_", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("storage: bad migration filename %q (want NNNN_name.sql)", entry.Name())
		}
		v, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, fmt.Errorf("storage: bad version in %q: %w", entry.Name(), err)
		}
		body, err := migrationsFS.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return nil, err
		}
		ms = append(ms, migration{
			version: v,
			name:    parts[1],
			body:    stripDuplicateSchemaMigrations(string(body)),
		})
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i].version < ms[j].version })
	return ms, nil
}

// stripDuplicateSchemaMigrations removes the CREATE TABLE schema_migrations
// statement from migration bodies; the storage layer already creates that
// table via ensureMigrationsTable. Keeping the declaration in 0001_init.sql
// makes the SQL file readable on its own.
func stripDuplicateSchemaMigrations(body string) string {
	lower := strings.ToLower(body)
	idx := strings.Index(lower, "create table schema_migrations")
	if idx < 0 {
		return body
	}
	end := strings.Index(body[idx:], ";")
	if end < 0 {
		return body
	}
	return body[:idx] + body[idx+end+1:]
}
