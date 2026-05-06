//go:build cgo_sqlite

// Opt-in build: CGO SQLite via github.com/mattn/go-sqlite3. Requires a
// C toolchain at build time (clang/gcc + sqlite3 headers). Trades
// cross-compilation simplicity for ~3-15× lower p99 latency under
// sustained 3-node replicated load. Build with:
//
//	go build -tags cgo_sqlite ./...
package storage

import _ "github.com/mattn/go-sqlite3"

// sqliteDSN returns the database/sql driver name and DSN for mattn's
// driver. mattn uses underscore-prefixed query parameters
// (e.g. _busy_timeout=5000) rather than the _pragma=name(value) form.
func sqliteDSN(path string) (driverName, dsn string) {
	if path == ":memory:" {
		// WAL is incompatible with :memory:. Tests don't need durability.
		return "sqlite3", path + "?_busy_timeout=5000" +
			"&_foreign_keys=on" +
			"&_journal_mode=MEMORY" +
			"&_synchronous=OFF" +
			"&_temp_store=memory" +
			"&_txlock=immediate"
	}
	return "sqlite3", path + "?_busy_timeout=5000" +
		"&_foreign_keys=on" +
		"&_journal_mode=WAL" +
		"&_synchronous=NORMAL" +
		"&_cache_size=-65536" +
		"&_mmap_size=268435456" +
		"&_temp_store=memory" +
		"&_wal_autocheckpoint=2000" +
		"&_txlock=immediate"
}
