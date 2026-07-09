package driver

import "time"

// Config configures one Driver instance. Unlike internal/node.Config, it
// carries no raft transport/store/bootstrap settings -- those stay entirely
// on the caller's own *hraft.Raft, built exactly as internal/node.Start
// builds one (internal/node/node.go).
type Config struct {
	// DBPath is the path to the SQLite database file this driver serves.
	// Required.
	DBPath string

	// PageSize is the cluster-wide fixed page size (CLAUDE.md invariant):
	// every applied frame's page image must be exactly this many bytes.
	// 0 disables the check, same convention as internal/node.Config.
	PageSize uint32

	// ApplyTimeout bounds each hraft.Apply call the Gate makes. Defaults to
	// 5s if zero.
	ApplyTimeout time.Duration

	// CheckpointInterval controls how often this Driver runs
	// PRAGMA wal_checkpoint(PASSIVE) on its dedicated connection while its
	// raft isn't the leader (docs/DESIGN.md §checkpoint -- followers have
	// no local writer to trigger autocheckpoint). Defaults to 2s if zero.
	CheckpointInterval time.Duration

	// Name, if set, is used as a readable prefix for this Driver's
	// generated VFS name. It does not need to be unique -- New always
	// appends a random suffix regardless (see nextVFSName) -- and today it
	// has no other effect. Reserved for a future feature: matching
	// sql.Open's dbName argument to a specific Driver instance sharing a
	// raft cluster with others (docs/ROADMAP.md "Deferred").
	Name string
}

func (c Config) withDefaults() Config {
	if c.ApplyTimeout == 0 {
		c.ApplyTimeout = 5 * time.Second
	}
	if c.CheckpointInterval == 0 {
		c.CheckpointInterval = 2 * time.Second
	}
	return c
}
