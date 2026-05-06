//go:build !cgo_sqlite

// Default build: pure-Go SQLite via modernc.org/sqlite. No CGO toolchain
// required, cross-compiles cleanly. Slower under sustained replicated
// load than the CGO driver — use the cgo_sqlite build tag if you need
// the extra headroom.
package storage

import _ "modernc.org/sqlite"

// sqliteDSN returns the database/sql driver name and DSN for modernc's
// driver. modernc uses the `_pragma=name(value)` query-string syntax.
func sqliteDSN(path string) (driverName, dsn string) {
	if path == ":memory:" {
		// WAL is incompatible with :memory:. Tests don't need durability.
		return "sqlite", path + "?_pragma=busy_timeout(5000)" +
			"&_pragma=foreign_keys(on)" +
			"&_pragma=journal_mode(memory)" +
			"&_pragma=synchronous(off)" +
			"&_pragma=temp_store(memory)" +
			"&_txlock=immediate"
	}
	return "sqlite", path + "?_pragma=busy_timeout(5000)" +
		"&_pragma=foreign_keys(on)" +
		"&_pragma=journal_mode(wal)" +
		"&_pragma=synchronous(normal)" +
		"&_pragma=cache_size(-65536)" +
		"&_pragma=mmap_size(268435456)" +
		"&_pragma=temp_store(memory)" +
		"&_pragma=wal_autocheckpoint(2000)" +
		"&_txlock=immediate"
}
