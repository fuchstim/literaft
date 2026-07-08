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

// Node is one running literaft cluster member.
type Node struct {
	cfg       Config
	transport *hraft.NetworkTransport
	boltStore *raftboltdb.BoltStore
	raft      *hraft.Raft
	fsm       *raftadapter.FSM
	gate      *raftadapter.Gate
	vfsName   string

	// backend owns the applier and the two kept-alive SQLite connections
	// (keeper, checkpointer) -- everything a snapshot install
	// (docs/ROADMAP.md M6) must close and reopen together as a unit. See
	// internal/node/backend.go.
	backend *dbBackend

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

	// Discard any -wal/-shm left over from a previous life of this process
	// (docs/ROADMAP.md M7 "crash/restart recovery") before anything else
	// touches them. Local WAL fsync is skippable (docs/DESIGN.md
	// §durability), so this node's own on-disk WAL tail is never trusted as
	// durable; the RAFT log is. Must run before the priming connection
	// below: that's a real SQLite connection, and if it saw a non-empty,
	// non-corrupt -wal it could run SQLite's own walIndexRecover and
	// resurrect the discarded tail's committed frames on its own --
	// exactly the untrusted local-recovery path apply/README.md says is
	// out of scope, defeating the RAFT-log-driven rebuild below. With a
	// clean -wal, hraft's own startup replay (from its last local snapshot
	// index, or from log index 1 if none -- restoreSnapshot/processLogs in
	// vendor/github.com/hashicorp/raft) re-materializes everything via
	// FSM.Apply -> apply.Applier.Apply. That's safe to replay more of than
	// strictly necessary: RAFT entries are full page images, not deltas
	// (CLAUDE.md), so re-applying an already-applied entry converges to the
	// same state instead of corrupting it.
	if err := removeWALFiles(cfg.DBPath); err != nil {
		return nil, fmt.Errorf("node: clearing stale -wal/-shm for %s: %w", cfg.DBPath, err)
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

	vfsName := "literaft-node-" + cfg.ID
	backend := &dbBackend{cfg: cfg, vfsName: vfsName, applier: applier}
	fsm := raftadapter.NewFSM(backend)
	// Must be wired before hraft.NewRaft below: NewRaft synchronously calls
	// restoreSnapshot on startup, which -- if this node has any locally
	// stored snapshot from a previous life (self-taken or received via a
	// past InstallSnapshot) -- calls FSM.Restore immediately. Restore
	// requires a snapshotter; setting it any later makes NewRaft itself
	// fail on every restart of a node that ever snapshotted.
	fsm.SetSnapshotter(backend)

	raftConfig := hraft.DefaultConfig()
	raftConfig.LocalID = hraft.ServerID(cfg.ID)
	raftConfig.LogOutput = cfg.LogOutput
	raftConfig.SnapshotThreshold = cfg.SnapshotThreshold
	raftConfig.SnapshotInterval = cfg.SnapshotInterval
	raftConfig.TrailingLogs = cfg.TrailingLogs

	r, err := hraft.NewRaft(raftConfig, fsm, boltStore, boltStore, snapshotStore, transport)
	if err != nil {
		backend.closeAll()
		boltStore.Close()
		transport.Close()
		return nil, fmt.Errorf("node: starting raft: %w", err)
	}

	if cfg.Bootstrap != nil {
		err := r.BootstrapCluster(hraft.Configuration{Servers: cfg.Bootstrap}).Error()
		if err != nil && !errors.Is(err, hraft.ErrCantBootstrap) {
			r.Shutdown()
			backend.closeAll()
			boltStore.Close()
			transport.Close()
			return nil, fmt.Errorf("node: bootstrapping cluster: %w", err)
		}
	}

	gate := raftadapter.NewGate(r, fsm, cfg.ApplyTimeout)
	vfs.RegisterGatePageSize(vfsName, sqlite3vfs.Find(""), gate, cfg.PageSize)

	// Journal mode is already WAL (set persistently by the priming
	// connection above; verified not to need re-setting per connection);
	// synchronous is a per-connection setting, so it's set again on every
	// new connection below.
	keeper, err := sqlite3.Open("file:" + cfg.DBPath + "?vfs=" + vfsName)
	if err != nil {
		r.Shutdown()
		backend.closeAll()
		boltStore.Close()
		transport.Close()
		return nil, fmt.Errorf("node: opening kept-alive connection to %s: %w", cfg.DBPath, err)
	}
	if err := keeper.Exec("PRAGMA synchronous=NORMAL"); err != nil {
		keeper.Close()
		r.Shutdown()
		backend.closeAll()
		boltStore.Close()
		transport.Close()
		return nil, fmt.Errorf("node: setting synchronous=NORMAL on %s: %w", cfg.DBPath, err)
	}

	checkpointer, err := sqlite3.Open("file:" + cfg.DBPath + "?vfs=" + vfsName)
	if err != nil {
		keeper.Close()
		r.Shutdown()
		backend.closeAll()
		boltStore.Close()
		transport.Close()
		return nil, fmt.Errorf("node: opening checkpoint connection to %s: %w", cfg.DBPath, err)
	}

	backend.attachConns(keeper, checkpointer)

	n := &Node{
		cfg:            cfg,
		transport:      transport,
		boltStore:      boltStore,
		raft:           r,
		fsm:            fsm,
		gate:           gate,
		vfsName:        vfsName,
		backend:        backend,
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
				_ = n.backend.checkpointPassive()
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
func (n *Node) DB() *sqlite3.Conn { return n.backend.DB() }

// Ready reports whether this node is currently the raft leader and has
// finished draining its apply backlog for the current term (docs/DESIGN.md
// §conflicts "gaining leadership"). A client write attempted before Ready
// returns true will fail its gate with a raftadapter.CatchingUpError; unlike
// a NotLeaderError there's no other node to redirect to, so callers should
// just retry shortly.
func (n *Node) Ready() bool { return n.gate.Ready() }

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
	n.gate.Close()

	var errs []error
	if err := n.raft.Shutdown().Error(); err != nil {
		errs = append(errs, fmt.Errorf("raft shutdown: %w", err))
	}
	if err := n.backend.closeAll(); err != nil {
		errs = append(errs, err)
	}
	if err := n.boltStore.Close(); err != nil {
		errs = append(errs, fmt.Errorf("closing raft log store: %w", err))
	}
	if err := n.transport.Close(); err != nil {
		errs = append(errs, fmt.Errorf("closing raft transport: %w", err))
	}
	return errors.Join(errs...)
}
