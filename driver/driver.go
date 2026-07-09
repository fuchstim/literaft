// Package driver exports a database/sql-compatible driver over an
// already-running literaft-replicated SQLite database. Unlike
// internal/node.Node, it does not build a raft transport, log store,
// or FSM itself: the caller constructs its own *hraft.Raft and
// *raftadapter.FSM exactly as internal/node.Start does today
// (internal/node/node.go), including wiring the FSM's Materializer and
// Snapshotter -- that plumbing is orthogonal to what this package does and
// stays entirely caller-owned. Given those two objects, New builds the
// commit-frame Gate, registers a process-unique gated+page-size-enforcing
// VFS, keeps one dedicated connection alive (CLAUDE.md's "keep >=1 RW
// connection open" invariant) to drive the follower checkpoint loop, and
// the resulting *Driver implements database/sql/driver.Driver and
// driver.DriverContext by delegating to ncruces/go-sqlite3/driver's SQLite
// type for all connection/statement/row machinery.
//
// # Usage
//
//	r, fsm := /* build your own *hraft.Raft + *raftadapter.FSM; see
//	   internal/node/node.go:53-163 for the reference pattern, including
//	   the stale -wal/-shm discard and WAL-mode priming steps at
//	   node.go:56-101 -- see the WAL-priming ordering note below */
//	drv, err := driver.New(r, fsm, driver.Config{DBPath: "mydb.db", PageSize: 4096})
//	sql.Register("mydb", drv)
//	db, err := sql.Open("mydb", "") // second arg is reserved, unused for now
//	defer db.Close()                // does NOT close drv -- see Driver.Close
//	defer drv.Close()
//
// database/sql has no notion of a stateful, runtime-constructed driver:
// sql.Register expects a driver.Driver value that already exists, so unlike
// ncruces/go-sqlite3/driver's own package (which registers a stateless
// singleton in an init()), this package deliberately does not call
// sql.Register itself -- there is no live raft/FSM/Config to build a Driver
// from at package-init time. Callers register the instance New returns,
// under whatever alias they choose, once it exists. sql.Register panics on
// a duplicate name, so a caller that rebuilds its Driver (e.g. on a config
// reload) needs a fresh alias each time.
//
// # WAL-mode priming and hraft.NewRaft ordering
//
// New primes cfg.DBPath's WAL-mode identity (mirroring
// internal/node/node.go:91-101), but by the time New runs, the caller's
// *hraft.Raft already exists -- and hraft.NewRaft starts a background
// goroutine that can immediately begin replaying any already-committed-but-
// unapplied backlog from a prior life of a persisted raft log store, with
// no ordering guarantee relative to New's priming call. For a fresh
// single-node bootstrap (empty log store) this can't happen, so New's
// priming call is sufficient on its own in that case. For a
// restart-persistent deployment, though, it is only a defensive
// re-assertion: callers must replicate internal/node/node.go's full 56-101
// range (discard stale -wal/-shm, then prime WAL mode) themselves, before
// calling hraft.NewRaft -- not rely on New to do it.
package driver

import (
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ncruces/go-sqlite3"
	sqlite3vfs "github.com/ncruces/go-sqlite3/vfs"

	hraft "github.com/hashicorp/raft"

	raftadapter "github.com/fuchstim/literaft/raft"
	"github.com/fuchstim/literaft/vfs"
)

// Driver is a database/sql/driver.Driver and driver.DriverContext exposing
// one running literaft database. See the package doc comment for the
// construction flow.
type Driver struct {
	cfg     Config
	raft    *hraft.Raft
	gate    *raftadapter.Gate
	vfsName string

	// keepAlive is this Driver's own dedicated connection: it exists only
	// to (a) keep >=1 RW connection open against vfsName so the -shm
	// wal-index stays live (CLAUDE.md invariant) and (b) run the follower
	// checkpoint driver. It never serves client writes -- those go through
	// database/sql's own pool via Open/OpenConnector -- so unlike
	// internal/node's "keeper" it doesn't need PRAGMA synchronous=NORMAL,
	// and unlike internal/node's dbBackend it needs no mutex: only the
	// checkpoint-driver goroutine ever touches it, and close() waits for
	// that goroutine to exit before closing it (see Close).
	keepAlive *sqlite3.Conn

	stopCheckpoint chan struct{}
	checkpointDone chan struct{}
	closeOnce      sync.Once
	closeErr       error
}

// nextVFSName returns a name guaranteed unique among every Driver built in
// this process. sqlite3vfs.Register silently overwrites any existing
// registration under the same name with no error
// (vendor/github.com/ncruces/go-sqlite3/vfs/registry.go), so a collision
// would be a silent, hard-to-diagnose VFS swap rather than a startup
// failure -- hence a random suffix, not just deduplication of hint. hint
// (Config.Name) is a readability prefix only.
func nextVFSName(hint string) (string, error) {
	if hint == "" {
		hint = "literaft-driver"
	}
	var suffix [16]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return "", fmt.Errorf("driver: generating VFS name: %w", err)
	}
	return fmt.Sprintf("%s-%x", hint, suffix), nil
}

// primeWAL establishes dbPath's WAL-mode identity (internal/node/node.go:
// 91-101) via the plain default-wrapping VFS. Safe to call on an
// already-WAL-mode db: PRAGMA journal_mode=WAL against an already-WAL db is
// a no-op.
func primeWAL(dbPath string) error {
	priming, err := sqlite3.Open("file:" + dbPath + "?vfs=" + vfs.Name)
	if err != nil {
		return fmt.Errorf("driver: opening %s to establish WAL mode: %w", dbPath, err)
	}
	if err := priming.Exec("PRAGMA journal_mode=WAL"); err != nil {
		priming.Close()
		return fmt.Errorf("driver: enabling WAL mode on %s: %w", dbPath, err)
	}
	if err := priming.Close(); err != nil {
		return fmt.Errorf("driver: closing priming connection to %s: %w", dbPath, err)
	}
	return nil
}

// New builds a Driver gating writes through r, whose FSM must be fsm (the
// same instance passed to hraft.NewRaft) and whose Materializer/Snapshotter
// the caller has already wired in. See the package doc comment for the
// required caller-side WAL-priming/discard ordering relative to
// hraft.NewRaft.
func New(r *hraft.Raft, fsm *raftadapter.FSM, cfg Config) (*Driver, error) {
	if r == nil {
		return nil, errors.New("driver: raft is nil")
	}
	if fsm == nil {
		return nil, errors.New("driver: fsm is nil")
	}
	if cfg.DBPath == "" {
		return nil, errors.New("driver: Config.DBPath is required")
	}
	cfg = cfg.withDefaults()

	if err := primeWAL(cfg.DBPath); err != nil {
		return nil, err
	}

	vfsName, err := nextVFSName(cfg.Name)
	if err != nil {
		return nil, err
	}

	gate := raftadapter.NewGate(r, fsm, cfg.ApplyTimeout)
	vfs.RegisterGatePageSize(vfsName, sqlite3vfs.Find(""), gate, cfg.PageSize)

	keepAlive, err := sqlite3.Open("file:" + cfg.DBPath + "?vfs=" + vfsName)
	if err != nil {
		gate.Close()
		sqlite3vfs.Unregister(vfsName)
		return nil, fmt.Errorf("driver: opening keep-alive connection to %s: %w", cfg.DBPath, err)
	}

	d := &Driver{
		cfg:            cfg,
		raft:           r,
		gate:           gate,
		vfsName:        vfsName,
		keepAlive:      keepAlive,
		stopCheckpoint: make(chan struct{}),
		checkpointDone: make(chan struct{}),
	}
	go d.runCheckpointDriver()
	return d, nil
}

// runCheckpointDriver periodically checkpoints the dedicated connection
// while this Driver's raft isn't the leader (docs/DESIGN.md §checkpoint:
// followers have no local writer to trigger autocheckpoint). It's harmless
// to also run during the leader's brief non-leader windows; checking
// State() on every tick keeps this correct across role changes. Mirrors
// internal/node/node.go's runCheckpointDriver exactly.
func (d *Driver) runCheckpointDriver() {
	defer close(d.checkpointDone)
	ticker := time.NewTicker(d.cfg.CheckpointInterval)
	defer ticker.Stop()
	for {
		select {
		case <-d.stopCheckpoint:
			return
		case <-ticker.C:
			if d.raft.State() != hraft.Leader {
				_ = d.keepAlive.Exec("PRAGMA wal_checkpoint(PASSIVE)")
			}
		}
	}
}

// Ready reports whether this Driver's raft is currently the leader and has
// finished draining its apply backlog for the current term (docs/DESIGN.md
// §conflicts "gaining leadership"). A client write attempted before Ready
// returns true will fail its gate with a raftadapter.CatchingUpError.
func (d *Driver) Ready() bool { return d.gate.Ready() }

// LastRejection returns the concrete error from this Driver's most recently
// completed write proposal (nil if it succeeded, or if none has been made
// yet). This is the reliable way to recover whether a rejected write was a
// *raftadapter.NotLeaderError or a raftadapter.CatchingUpError: that
// distinction doesn't reliably survive the round trip back through
// database/sql, which by then may report only a generic error (see
// raft/gate.go's Propose doc and vfs/file.go:277-291; mapping that error to
// the right SQLite result code in the first place is a separate, larger
// change to vfs/File's write path, not something this package attempts).
func (d *Driver) LastRejection() error { return d.gate.LastRejection() }

// VFSName returns the name this Driver registered its gated VFS under.
func (d *Driver) VFSName() string { return d.vfsName }

// Close stops the checkpoint driver, closes the dedicated connection,
// unregisters this Driver's VFS name, and closes the Gate. Idempotent.
//
// database/sql/driver.Driver has no Close contract, so sql.DB.Close() never
// calls this -- callers must call it themselves, and should do so only
// after sql.DB.Close() has returned: unregistering vfsName while
// database/sql's pool still has (or is opening) a connection against it
// races Open/Connect's VFS lookup.
func (d *Driver) Close() error {
	d.closeOnce.Do(func() { d.closeErr = d.close() })
	return d.closeErr
}

func (d *Driver) close() error {
	close(d.stopCheckpoint)
	<-d.checkpointDone
	d.gate.Close()

	err := d.keepAlive.Close()
	sqlite3vfs.Unregister(d.vfsName)
	if err != nil {
		return fmt.Errorf("driver: closing keep-alive connection: %w", err)
	}
	return nil
}
