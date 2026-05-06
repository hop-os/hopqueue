module haasqueue

go 1.24

require (
	github.com/google/uuid v1.6.0
	github.com/mattn/go-sqlite3 v1.14.44
	github.com/oklog/ulid/v2 v2.1.1
	github.com/robfig/cron/v3 v3.0.1
	haaslock v0.0.0-00010101000000-000000000000
)

replace haaslock => ../haaslock
