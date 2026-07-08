package node

import (
	"io"
	"time"

	hraft "github.com/hashicorp/raft"
)

// Config configures one literaft node process (docs/ROADMAP.md M4).
type Config struct {
	// ID uniquely identifies this node across the cluster's lifetime and
	// becomes its hraft.ServerID.
	ID string

	// BindAddr is the TCP address the raft transport listens on (e.g.
	// "127.0.0.1:9001", or "127.0.0.1:0" for an OS-assigned port in tests).
	BindAddr string

	// DataDir holds this node's raft log/stable store and snapshot store.
	// Created if it doesn't exist.
	DataDir string

	// DBPath is the path to the SQLite database file this node serves.
	DBPath string

	// PageSize is the cluster-wide fixed page size (CLAUDE.md invariant):
	// every applied frame's page image must be exactly this many bytes.
	PageSize uint32

	// Bootstrap lists the initial cluster configuration. Non-nil only on
	// the node(s) forming a brand new cluster; a node joining an existing
	// one leaves this nil and is added via the leader's raft.AddVoter
	// instead.
	Bootstrap []hraft.Server

	// ApplyTimeout bounds each hraft.Apply call the Gate makes. Defaults to
	// 5s if zero.
	ApplyTimeout time.Duration

	// CheckpointInterval controls how often a non-leader node runs
	// PRAGMA wal_checkpoint(PASSIVE) (docs/DESIGN.md §checkpoint --
	// followers have no local writer to trigger autocheckpoint). Defaults
	// to 2s if zero.
	CheckpointInterval time.Duration

	// LogOutput receives hraft's log output. Defaults to io.Discard if nil.
	LogOutput io.Writer
}

func (c Config) withDefaults() Config {
	if c.ApplyTimeout == 0 {
		c.ApplyTimeout = 5 * time.Second
	}
	if c.CheckpointInterval == 0 {
		c.CheckpointInterval = 2 * time.Second
	}
	if c.LogOutput == nil {
		c.LogOutput = io.Discard
	}
	return c
}
