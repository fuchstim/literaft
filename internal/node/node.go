// Package node wires the RAFT-backed VFS (docs/DESIGN.md) into one running
// process: a real hraft.Raft cluster member, the literaft VFS registered
// against it, and the "keep >=1 RW connection open" + follower checkpoint
// driver invariants CLAUDE.md calls for (docs/ROADMAP.md M4).
package node

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ncruces/go-sqlite3"
	sqlite3vfs "github.com/ncruces/go-sqlite3/vfs"

	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"github.com/fuchstim/literaft/apply"
	raftadapter "github.com/fuchstim/literaft/raft"
	"github.com/fuchstim/literaft/vfs"
)

// snapshotThreshold is set high enough that hraft's periodic automatic
// snapshot check never actually fires in normal M4 operation --
// snapshot-based log compaction and very-behind-follower catch-up
// (InstallSnapshot) are deferred to docs/ROADMAP.md M5 (see raft.FSM's
// Snapshot/Restore, which error out if ever actually invoked).
const snapshotThreshold = 1 << 30

// Node is one running literaft cluster member.
type Node struct {
	cfg       Config
	transport *hraft.NetworkTransport
	boltStore *raftboltdb.BoltStore
	applier   *apply.Applier
	raft      *hraft.Raft
	fsm       *raftadapter.FSM
	gate      *raftadapter.Gate
	vfsName   string
	keeper    *sqlite3.Conn

	// checkpointer is a connection dedicated to runCheckpointDriver, kept
	// separate from keeper because a *sqlite3.Conn isn't safe for
	// concurrent use from multiple goroutines -- sharing keeper with the
	// checkpoint driver's own timer goroutine would race against whatever
	// callers are doing with DB() (multiple RW connections against the
	// same VFS are exactly what WAL mode supports; sharing one connection
	// across goroutines is not).
	checkpointer *sqlite3.Conn

	stopCheckpoint chan struct{}
	checkpointDone chan struct{}
	shutdownOnce   sync.Once
	shutdownErr    error
}

// Start brings up one node: raft transport/stores, the FSM/Gate over
// cfg.DBPath, VFS registration, and the kept-alive RW connection + follower
// checkpoint driver. If cfg.Bootstrap is set, this node bootstraps a new
// cluster with that configuration; otherwise it expects to be joined into
// an existing cluster via the leader's raft.AddVoter.
func Start(cfg Config) (*Node, error) {
	cfg = cfg.withDefaults()

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("node: creating data dir %s: %w", cfg.DataDir, err)
	}

	// Establish this db's WAL-mode identity (page 1's WAL marker) before
	// apply.Open ever touches -wal/-shm -- apply.Applier materializes
	// frames without a SQLite connection in the loop at all, and a fresh,
	// schema-less main db file gives it nothing to be recognized as
	// WAL-mode against (apply/README.md's documented gap). This connection
	// only runs a PRAGMA (never a gated write), so it uses the plain
	// default-registered VFS (vfs.Name) and closes immediately after --
	// apply.Applier.Apply is independently robust to whatever
	// half-initialized wal-index state that leaves behind (see the
	// pageSize==0 check in apply.Apply's doc).
	priming, err := sqlite3.Open("file:" + cfg.DBPath + "?vfs=" + vfs.Name)
	if err != nil {
		return nil, fmt.Errorf("node: opening %s to establish WAL mode: %w", cfg.DBPath, err)
	}
	if err := priming.Exec("PRAGMA journal_mode=WAL"); err != nil {
		priming.Close()
		return nil, fmt.Errorf("node: enabling WAL mode on %s: %w", cfg.DBPath, err)
	}
	if err := priming.Close(); err != nil {
		return nil, fmt.Errorf("node: closing priming connection to %s: %w", cfg.DBPath, err)
	}

	transport, err := hraft.NewTCPTransport(cfg.BindAddr, nil, 3, 10*time.Second, cfg.LogOutput)
	if err != nil {
		return nil, fmt.Errorf("node: starting raft transport on %s: %w", cfg.BindAddr, err)
	}

	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft.db"))
	if err != nil {
		transport.Close()
		return nil, fmt.Errorf("node: opening raft log store: %w", err)
	}

	snapshotStore, err := hraft.NewFileSnapshotStore(cfg.DataDir, 2, cfg.LogOutput)
	if err != nil {
		boltStore.Close()
		transport.Close()
		return nil, fmt.Errorf("node: opening raft snapshot store: %w", err)
	}

	applier, err := apply.Open(cfg.DBPath, cfg.PageSize)
	if err != nil {
		boltStore.Close()
		transport.Close()
		return nil, fmt.Errorf("node: opening applier for %s: %w", cfg.DBPath, err)
	}

	fsm := raftadapter.NewFSM(applier)

	raftConfig := hraft.DefaultConfig()
	raftConfig.LocalID = hraft.ServerID(cfg.ID)
	raftConfig.LogOutput = cfg.LogOutput
	raftConfig.SnapshotThreshold = snapshotThreshold

	r, err := hraft.NewRaft(raftConfig, fsm, boltStore, boltStore, snapshotStore, transport)
	if err != nil {
		applier.Close()
		boltStore.Close()
		transport.Close()
		return nil, fmt.Errorf("node: starting raft: %w", err)
	}

	if cfg.Bootstrap != nil {
		err := r.BootstrapCluster(hraft.Configuration{Servers: cfg.Bootstrap}).Error()
		if err != nil && !errors.Is(err, hraft.ErrCantBootstrap) {
			r.Shutdown()
			applier.Close()
			boltStore.Close()
			transport.Close()
			return nil, fmt.Errorf("node: bootstrapping cluster: %w", err)
		}
	}

	gate := raftadapter.NewGate(r, fsm, cfg.ApplyTimeout)
	vfsName := "literaft-node-" + cfg.ID
	vfs.RegisterGate(vfsName, sqlite3vfs.Find(""), gate)

	// Journal mode is already WAL (set persistently by the priming
	// connection above; verified not to need re-setting per connection);
	// synchronous is a per-connection setting, so it's set again on every
	// new connection below.
	keeper, err := sqlite3.Open("file:" + cfg.DBPath + "?vfs=" + vfsName)
	if err != nil {
		r.Shutdown()
		applier.Close()
		boltStore.Close()
		transport.Close()
		return nil, fmt.Errorf("node: opening kept-alive connection to %s: %w", cfg.DBPath, err)
	}
	if err := keeper.Exec("PRAGMA synchronous=NORMAL"); err != nil {
		keeper.Close()
		r.Shutdown()
		applier.Close()
		boltStore.Close()
		transport.Close()
		return nil, fmt.Errorf("node: setting synchronous=NORMAL on %s: %w", cfg.DBPath, err)
	}

	checkpointer, err := sqlite3.Open("file:" + cfg.DBPath + "?vfs=" + vfsName)
	if err != nil {
		keeper.Close()
		r.Shutdown()
		applier.Close()
		boltStore.Close()
		transport.Close()
		return nil, fmt.Errorf("node: opening checkpoint connection to %s: %w", cfg.DBPath, err)
	}

	n := &Node{
		cfg:            cfg,
		transport:      transport,
		boltStore:      boltStore,
		applier:        applier,
		raft:           r,
		fsm:            fsm,
		gate:           gate,
		vfsName:        vfsName,
		keeper:         keeper,
		checkpointer:   checkpointer,
		stopCheckpoint: make(chan struct{}),
		checkpointDone: make(chan struct{}),
	}
	go n.runCheckpointDriver()

	return n, nil
}

// runCheckpointDriver periodically checkpoints the kept-alive connection
// while this node isn't the leader (docs/DESIGN.md §checkpoint: followers
// have no local writer to trigger autocheckpoint). It's harmless to also
// run on the leader's brief non-leader windows; checking State() on every
// tick (rather than a static leader/follower branch decided once at Start)
// keeps this correct across role changes.
func (n *Node) runCheckpointDriver() {
	defer close(n.checkpointDone)
	ticker := time.NewTicker(n.cfg.CheckpointInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCheckpoint:
			return
		case <-ticker.C:
			if n.raft.State() != hraft.Leader {
				_ = n.checkpointer.Exec("PRAGMA wal_checkpoint(PASSIVE)")
			}
		}
	}
}

// Raft returns the underlying hraft.Raft handle, e.g. for AddVoter calls
// when joining a new node into an existing cluster.
func (n *Node) Raft() *hraft.Raft { return n.raft }

// DB returns the kept-alive RW connection. On the leader, client writes
// should go through this connection (or another opened against the same
// VFS name) so they flow through the commit-frame gate.
func (n *Node) DB() *sqlite3.Conn { return n.keeper }

// VFSName returns the name this node registered its literaft VFS instance
// under.
func (n *Node) VFSName() string { return n.vfsName }

// Shutdown stops the checkpoint driver, the raft node, and closes every
// resource Start opened, returning the first error encountered (if any)
// while still attempting every step. Idempotent: a second call is a no-op
// returning the same result, so callers (e.g. a test killing a specific
// node and then unconditionally tearing down the whole cluster) don't need
// to track which nodes they already shut down.
func (n *Node) Shutdown() error {
	n.shutdownOnce.Do(func() { n.shutdownErr = n.shutdown() })
	return n.shutdownErr
}

func (n *Node) shutdown() error {
	close(n.stopCheckpoint)
	<-n.checkpointDone

	var errs []error
	if err := n.raft.Shutdown().Error(); err != nil {
		errs = append(errs, fmt.Errorf("raft shutdown: %w", err))
	}
	if err := n.keeper.Close(); err != nil {
		errs = append(errs, fmt.Errorf("closing kept-alive connection: %w", err))
	}
	if err := n.checkpointer.Close(); err != nil {
		errs = append(errs, fmt.Errorf("closing checkpoint connection: %w", err))
	}
	if err := n.applier.Close(); err != nil {
		errs = append(errs, fmt.Errorf("closing applier: %w", err))
	}
	if err := n.boltStore.Close(); err != nil {
		errs = append(errs, fmt.Errorf("closing raft log store: %w", err))
	}
	if err := n.transport.Close(); err != nil {
		errs = append(errs, fmt.Errorf("closing raft transport: %w", err))
	}
	return errors.Join(errs...)
}
