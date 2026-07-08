package node

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ncruces/go-sqlite3"

	"github.com/fuchstim/literaft/apply"
	raftadapter "github.com/fuchstim/literaft/raft"
	"github.com/fuchstim/literaft/vfs"
)

// checkpointRetryDelay/checkpointRetryTimeout bound how long Snapshot waits
// for a full TRUNCATE checkpoint (docs/ROADMAP.md M6): TRUNCATE only fully
// backfills and truncates the -wal file once no reader still needs older WAL
// content, so a lagging reader can make one attempt only partially succeed.
const (
	checkpointRetryDelay   = 20 * time.Millisecond
	checkpointRetryTimeout = 5 * time.Second
)

// dbBackend owns every piece of this node's on-disk/SQLite state that a
// snapshot install (docs/ROADMAP.md M6) must swap out as a unit: the
// follower-apply handle and the two kept-alive SQLite connections
// (CLAUDE.md's "keep >=1 RW connection open"). Before M6
// nothing ever replaced these mid-flight, so Node held them as plain
// fields; Restore needs to close and reopen all three together, so they're
// centralized here behind one mutex.
//
// One plain Mutex, not an RWMutex, guards applier/checkpointer: Apply,
// Snapshot, Restore, and the checkpoint driver's tick all touch one or both,
// and a *sqlite3.Conn is not safe for concurrent goroutine use (the same
// reason checkpointer was split from keeper in the first place). Contention
// is negligible -- every one of these operations is brief except Snapshot's
// checkpoint+copy, which is expected to be infrequent.
//
// keeper is guarded by a separate connMu instead: it used to be handed out
// via a DB() *sqlite3.Conn accessor, but that only protected the field read,
// not the caller's subsequent use of the returned pointer -- a concurrent
// Restore (driven by hraft's InstallSnapshot, which can fire on a follower
// serving reads at any time) could close and replace keeper while a caller
// was still calling Prepare/Exec on the connection object it got back
// (confirmed reproducible: ginkgo -race caught a genuine use-after-free,
// since Conn.Close on the WASM-backed driver frees the connection's linear
// memory). WithDB replaces DB(): it holds connMu for the whole call instead
// of just a field read, so Restore (which takes connMu exclusively) always
// waits for in-flight users to finish before closing keeper. connMu is a
// plain Mutex, not an RWMutex: a *sqlite3.Conn is not safe for concurrent
// goroutine use (ncruces/go-sqlite3's own doc on Conn -- the same reason
// checkpointer was split from keeper in the first place), so two concurrent
// WithDB callers must never both be running fn(keeper) at once, only ever
// serialized behind each other and behind Restore.
type dbBackend struct {
	mu sync.Mutex

	connMu sync.Mutex
	keeper *sqlite3.Conn

	cfg     Config
	vfsName string

	applier      *apply.Applier
	checkpointer *sqlite3.Conn
}

// attachConns wires the two kept-alive SQLite connections in after they're
// opened (Start creates them only after the raft node -- and therefore this
// backend's FSM -- already exists, so they can't be set at construction).
func (b *dbBackend) attachConns(keeper, checkpointer *sqlite3.Conn) {
	b.connMu.Lock()
	b.keeper = keeper
	b.connMu.Unlock()

	b.mu.Lock()
	b.checkpointer = checkpointer
	b.mu.Unlock()
}

// Apply implements raftadapter.Materializer.
func (b *dbBackend) Apply(e vfs.Entry) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.applier.Apply(e)
}

// WithDB runs fn with the kept-alive RW connection client writes should
// use, held safe both from a concurrent Restore closing/replacing it out
// from under fn and from a second, concurrent WithDB caller running fn on
// the same *sqlite3.Conn at the same time (see dbBackend's doc comment).
func (b *dbBackend) WithDB(fn func(*sqlite3.Conn) error) error {
	b.connMu.Lock()
	defer b.connMu.Unlock()
	return fn(b.keeper)
}

// checkpointPassive runs the periodic follower checkpoint
// (docs/DESIGN.md §checkpoint). Kept behind the same lock as Snapshot's own
// checkpoint call so the two never drive checkpointer concurrently.
func (b *dbBackend) checkpointPassive() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.checkpointer.Exec("PRAGMA wal_checkpoint(PASSIVE)")
}

// closeAll closes the applier and both SQLite connections, returning the
// first error encountered (if any) while still attempting every step.
func (b *dbBackend) closeAll() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.connMu.Lock()
	defer b.connMu.Unlock()
	return b.closeAllLocked()
}

// closeAllLocked requires both mu and connMu already held.
func (b *dbBackend) closeAllLocked() error {
	var errs []error
	if b.keeper != nil {
		if err := b.keeper.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing kept-alive connection: %w", err))
		}
	}
	if b.checkpointer != nil {
		if err := b.checkpointer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing checkpoint connection: %w", err))
		}
	}
	if b.applier != nil {
		if err := b.applier.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing applier: %w", err))
		}
	}
	return errors.Join(errs...)
}

// Snapshot implements raftadapter.Snapshotter (docs/ROADMAP.md M6). It
// drives a TRUNCATE checkpoint -- the natural snapshot cut point
// (docs/DESIGN.md §checkpoint) -- so the .db file alone becomes a complete,
// self-contained copy of the current state, then hands back a private copy
// of those bytes.
//
// The lock is held for this whole call, but only across a local checkpoint
// + file copy, not the (potentially much slower, over-the-network) transfer
// hraft does later via the returned ReadCloser's consumer -- copying now is
// exactly what decouples the two, so Apply/Restore/checkpointPassive aren't
// blocked while a slow follower is still catching up.
func (b *dbBackend) Snapshot() (io.ReadCloser, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := b.checkpointTruncateLocked(); err != nil {
		return nil, err
	}

	src, err := os.Open(b.cfg.DBPath)
	if err != nil {
		return nil, fmt.Errorf("snapshot: opening %s: %w", b.cfg.DBPath, err)
	}
	defer src.Close()

	tmp, err := os.CreateTemp("", "literaft-snapshot-*")
	if err != nil {
		return nil, fmt.Errorf("snapshot: creating temp copy: %w", err)
	}
	if _, err := io.Copy(tmp, src); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return nil, fmt.Errorf("snapshot: copying %s: %w", b.cfg.DBPath, err)
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return nil, fmt.Errorf("snapshot: rewinding temp copy: %w", err)
	}

	return &tempFileReader{File: tmp}, nil
}

// checkpointTruncateLocked retries a TRUNCATE checkpoint until the -wal
// file is actually truncated to zero bytes -- the real signal that every
// frame was backfilled and no reader is still pinning older WAL content
// (a busy reader can make a single attempt only partially succeed).
//
// This goes through Exec("PRAGMA wal_checkpoint(TRUNCATE)") rather than the
// Conn.WALCheckpoint API: verified empirically that calling the latter as
// the very first operation on a freshly opened connection (exactly
// checkpointer's usage pattern -- it's opened once at Start and otherwise
// only ever runs PASSIVE checkpoints) returns nLog=nCkpt=-1 with a nil
// error ("checkpoint could not run"), because that path skips whatever
// per-connection WAL setup a normal SQL statement lazily triggers. Going
// through the SQL layer sidesteps it; the -wal size check below is what
// actually confirms success either way.
func (b *dbBackend) checkpointTruncateLocked() error {
	deadline := time.Now().Add(checkpointRetryTimeout)
	for {
		err := b.checkpointer.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
		if err == nil {
			fi, statErr := os.Stat(b.cfg.DBPath + "-wal")
			if statErr != nil && !os.IsNotExist(statErr) {
				return fmt.Errorf("snapshot: checking -wal size: %w", statErr)
			}
			if statErr != nil || fi.Size() == 0 {
				return nil
			}
		}
		if time.Now().After(deadline) {
			if err != nil {
				return fmt.Errorf("snapshot: TRUNCATE checkpoint never completed: %w", err)
			}
			return errors.New("snapshot: TRUNCATE checkpoint never fully backfilled the WAL")
		}
		time.Sleep(checkpointRetryDelay)
	}
}

// Restore implements raftadapter.Snapshotter (docs/ROADMAP.md M6). It
// replaces this node's entire .db with r's bytes and resets local WAL/apply
// state to match, swapping in fresh handles for the applier and both
// kept-alive SQLite connections.
//
// A failure partway through leaves this node's local state genuinely
// broken (there is no in-place rollback of a half-completed file swap);
// that's surfaced as an error, and hraft will simply retry InstallSnapshot
// -- the same collect-errors-don't-half-fix posture Node.shutdown already
// takes, just without a way to keep serving from the old state once the
// swap has begun.
//
// Restore can also run synchronously inside hraft.NewRaft's startup
// restoreSnapshot, before Node.Start has opened keeper/checkpointer or
// registered this node's VFS name (docs/ROADMAP.md M7 "crash/restart
// recovery") -- attached distinguishes that case, since reopening against
// an unregistered VFS name would fail. Start's own post-NewRaft code
// unconditionally opens both connections and calls attachConns regardless
// of whether a restore happened, so leaving them nil here is safe.
//
// connMu is held for this whole call (not just around closing/reassigning
// keeper) alongside mu: hraft's own contract already guarantees Restore
// never runs concurrently with Apply or with itself, so the only new
// exclusion this needs is against a concurrent WithDB caller still using
// the connection this call is about to close out from under it.
func (b *dbBackend) Restore(r io.Reader) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.connMu.Lock()
	defer b.connMu.Unlock()
	attached := b.keeper != nil

	dir := filepath.Dir(b.cfg.DBPath)
	tmp, err := os.CreateTemp(dir, ".literaft-restore-*")
	if err != nil {
		return fmt.Errorf("restore: creating temp file: %w", err)
	}
	tmpPath := tmp.Name()
	if _, err := io.Copy(tmp, r); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("restore: writing incoming snapshot: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("restore: syncing incoming snapshot: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("restore: closing incoming snapshot: %w", err)
	}

	// Close everything before touching files on disk: SQLite's own
	// mmap/fd state (and the vendored shm handle inside applier) must not
	// outlive the swap.
	if err := b.closeAllLocked(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("restore: closing existing state: %w", err)
	}

	if err := os.Rename(tmpPath, b.cfg.DBPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("restore: installing new db file: %w", err)
	}
	// The incoming .db is already fully checkpointed (self-contained), so
	// the old -wal/-shm belong to a superseded generation. Leaving them
	// would hand apply.Open's bootstrap a nonzero-size -wal it explicitly
	// refuses to touch (apply/apply.go's bootstrap doc comment).
	if err := removeWALFiles(b.cfg.DBPath); err != nil {
		return fmt.Errorf("restore: %w", err)
	}

	applier, err := apply.Open(b.cfg.DBPath, b.cfg.PageSize)
	if err != nil {
		return fmt.Errorf("restore: reopening applier: %w", err)
	}
	b.applier = applier

	if !attached {
		return nil
	}

	keeper, err := sqlite3.Open("file:" + b.cfg.DBPath + "?vfs=" + b.vfsName)
	if err != nil {
		return fmt.Errorf("restore: reopening kept-alive connection: %w", err)
	}
	if err := keeper.Exec("PRAGMA synchronous=NORMAL"); err != nil {
		keeper.Close()
		return fmt.Errorf("restore: setting synchronous=NORMAL: %w", err)
	}

	checkpointer, err := sqlite3.Open("file:" + b.cfg.DBPath + "?vfs=" + b.vfsName)
	if err != nil {
		keeper.Close()
		return fmt.Errorf("restore: reopening checkpoint connection: %w", err)
	}

	b.keeper = keeper
	b.checkpointer = checkpointer
	return nil
}

// removeWALFiles removes dbPath's -wal and -shm siblings, tolerating either
// not existing. Shared by Node.Start (docs/ROADMAP.md M7 "crash/restart
// recovery": every process start discards its local WAL tail) and Restore
// (docs/ROADMAP.md M6: an installed snapshot's .db is already self-
// contained, so any -wal/-shm on disk belongs to a superseded generation).
func removeWALFiles(dbPath string) error {
	if err := os.Remove(dbPath + "-wal"); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("removing -wal: %w", err)
	}
	if err := os.Remove(dbPath + "-shm"); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("removing -shm: %w", err)
	}
	return nil
}

// tempFileReader wraps a temp file so Close both closes and removes it,
// tying the private snapshot copy's lifetime to its consumer (fsmSnapshot's
// Release, or an error path that never gets that far).
type tempFileReader struct {
	*os.File
}

func (t *tempFileReader) Close() error {
	closeErr := t.File.Close()
	if err := os.Remove(t.File.Name()); err != nil && !os.IsNotExist(err) {
		if closeErr == nil {
			closeErr = err
		}
	}
	return closeErr
}

var (
	_ raftadapter.Materializer = (*dbBackend)(nil)
	_ raftadapter.Snapshotter  = (*dbBackend)(nil)
)
