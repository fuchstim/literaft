package driver

import "time"

// Option configures an optional Driver setting; see the WithXxx functions
// below. Required settings (the raft/FSM instances and the db path) are
// New's direct parameters instead (CLAUDE.md "Public API style").
type Option func(*options)

type options struct {
	pageSize           uint32
	applyTimeout       time.Duration
	checkpointInterval time.Duration
	name               string
}

func defaultOptions() options {
	return options{
		applyTimeout:       5 * time.Second,
		checkpointInterval: 2 * time.Second,
	}
}

// WithPageSize sets the cluster-wide fixed page size (CLAUDE.md invariant):
// every applied frame's page image must be exactly this many bytes. Unset
// (0) disables the check.
func WithPageSize(pageSize uint32) Option {
	return func(o *options) { o.pageSize = pageSize }
}

// WithApplyTimeout bounds each hraft.Apply call the Gate makes. Defaults to
// 5s.
func WithApplyTimeout(d time.Duration) Option {
	return func(o *options) { o.applyTimeout = d }
}

// WithCheckpointInterval controls how often this Driver runs
// PRAGMA wal_checkpoint(PASSIVE) on its dedicated connection while its raft
// isn't the leader (docs/DESIGN.md §checkpoint -- followers have no local
// writer to trigger autocheckpoint). Defaults to 2s.
func WithCheckpointInterval(d time.Duration) Option {
	return func(o *options) { o.checkpointInterval = d }
}

// WithName sets a readable prefix for this Driver's generated VFS name. It
// does not need to be unique -- New always appends a random suffix
// regardless (see nextVFSName) -- and today it has no other effect.
// Reserved for a future feature: matching sql.Open's dbName argument to a
// specific Driver instance sharing a raft cluster with others
// (docs/ROADMAP.md "Deferred").
func WithName(name string) Option {
	return func(o *options) { o.name = name }
}
