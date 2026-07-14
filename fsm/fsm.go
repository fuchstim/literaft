package fsm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/fuchstim/literaft/internal/fsm/snapshotter"
	"github.com/fuchstim/literaft/internal/fsm/walappender"
	raftproto "github.com/fuchstim/literaft/internal/raft/proto"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/ncruces/go-sqlite3"
	"google.golang.org/protobuf/proto"
)

// Must not implement hraft's BatchingFSM: forwarded-write acceptance relies on
// each entry's future resolving only after that same entry's Apply, which a
// batching apply would not preserve.
var _ raft.FSM = (*FSM)(nil)

type FSM struct {
	dbPath      string
	db          *sqlite3.Conn
	dbLock      *os.File
	pageSize    uint32
	walAppender *walappender.WALAppender
	snapshotter *snapshotter.Snapshotter
	logger      hclog.Logger

	// lastApplied is the raft index of the last command entry materialized
	// into this node's local database. It is advanced under the WAL write
	// lock, or at skip-marker consumption, and set (including downward) from
	// the snapshot header on Restore. Not raft's own applied index, which
	// advances at dispatch, before Apply runs. Atomic only for a clean
	// cross-goroutine read; the file-lock serialization is what keeps the
	// value exact.
	lastApplied atomic.Uint64

	// skipEntriesMu guards skipEntries: the proposing goroutine mutates it,
	// the apply goroutine reads and consumes it.
	skipEntriesMu sync.Mutex
	skipEntries   map[string]*skipEntry

	// loansMu guards loans (id -> held WAL write lock). A separate, short-held
	// lock, never the write lock a forward handler holds across its round
	// trip: Apply must look a loan up while that handler is still blocked, so
	// sharing the held lock here would deadlock.
	loansMu sync.Mutex
	loans   map[string]*walappender.HeldLock
}

// skipEntry tracks one in-flight self-proposal so its outcome can be decided
// by whichever proves commitment first: the transport response or local
// marker consumption.
type skipEntry struct {
	state skipState
	// done is closed exactly once, when state becomes consumed.
	done chan struct{}
}

type skipState int

const (
	// pending: marker created, entry not yet applied. consumed: applied while
	// pending, materialization skipped, and this node must publish it via its
	// own write path. abandoned: the proposer gave up before consumption, so
	// the entry -- if it ever arrives -- is materialized normally.
	skipPending skipState = iota
	skipConsumed
	skipAbandoned
)

func New(dbPath string, opts ...Option) (*FSM, error) {
	if dbPath == "" {
		return nil, fmt.Errorf("dbPath is required")
	}

	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}

	db, err := sqlite3.Open("file:" + dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database at path `%s`: %w", dbPath, err)
	}

	if err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		return nil, fmt.Errorf("failed to enable WAL mode on database at path `%s`: %w", dbPath, err)
	}

	stmt, _, err := db.Prepare("PRAGMA page_size;")
	if err != nil {
		return nil, fmt.Errorf("failed to prepare PRAGMA page_size statement: %w", err)
	}
	defer stmt.Close()

	var pageSize uint32
	for stmt.Step() {
		pageSize = uint32(stmt.ColumnInt(0))
	}

	if err := stmt.Reset(); err != nil {
		return nil, fmt.Errorf("failed to reset PRAGMA page_size statement: %w", err)
	}

	if pageSize <= 0 {
		return nil, fmt.Errorf("invalid page size %d returned from PRAGMA page_size", pageSize)
	}

	// Acquired only after WAL mode is enabled (enabling WAL mode requires exclusive lock)
	dbLock, err := acquireSharedDBLock(dbPath)
	if err != nil {
		return nil, err
	}

	// Order matters: db is opened and put in WAL mode above, and stays open,
	// before the WALAppender opens. A publish-after-commit failure on the
	// leader is fatal (internal/vfs panics), so a node can restart on a
	// crash image whose -wal holds committed frames but whose wal-index was
	// never published. walappender.Open can't rebuild the wal-index from
	// such a -wal on its own; it relies on this db connection having already
	// run SQLite's own WAL recovery (during the WAL-mode PRAGMA above) and
	// holding the shm alive so the appender joins the recovered mapping
	// rather than re-initializing it.
	walAppender, err := walappender.Open(dbPath, pageSize, o.checkpointThresholdPages, o.checkpointInterval, o.logger.Named("walappender"))
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL appender: %w", err)
	}

	snapshotter := snapshotter.New(dbPath, pageSize, o.logger.Named("snapshotter"))

	logger := o.logger.Named("fsm")
	logger.Info("opened FSM", "dbPath", dbPath, "pageSize", pageSize)

	return &FSM{
		dbPath:      dbPath,
		db:          db,
		dbLock:      dbLock,
		pageSize:    uint32(pageSize),
		walAppender: walAppender,
		snapshotter: snapshotter,
		logger:      logger,

		skipEntries: make(map[string]*skipEntry),
		loans:       make(map[string]*walappender.HeldLock),
	}, nil
}

func (f *FSM) Close() error {
	return errors.Join(f.db.Close(), f.walAppender.Close(), f.dbLock.Close())
}

func (f *FSM) DBPath() string {
	return f.dbPath
}

func (f *FSM) PageSize() uint32 {
	return f.pageSize
}

// LastApplied returns the raft index of the last entry materialized into this
// node's local database.
func (f *FSM) LastApplied() uint64 {
	return f.lastApplied.Load()
}

// SkipEntry registers an in-flight self-proposal's marker (pending),
// immediately before the proposing call.
func (f *FSM) SkipEntry(id string) {
	f.skipEntriesMu.Lock()
	defer f.skipEntriesMu.Unlock()
	f.skipEntries[id] = &skipEntry{state: skipPending, done: make(chan struct{})}
}

// UnskipEntry deletes the marker whatever its terminal state -- bookkeeping,
// run once the proposing call has fully resolved.
func (f *FSM) UnskipEntry(id string) {
	f.skipEntriesMu.Lock()
	defer f.skipEntriesMu.Unlock()
	delete(f.skipEntries, id)
}

// AwaitEntryApplied blocks until id's marker is consumed (returns nil), or
// until ctx expires. On expiry it resolves the marker: an already-consumed
// one still returns nil (commitment proven locally, so the txn must publish);
// a still-pending one becomes abandoned and returns ctx.Err(), so the entry,
// if it ever arrives, is materialized normally rather than double-published.
func (f *FSM) AwaitEntryApplied(ctx context.Context, id string) error {
	f.skipEntriesMu.Lock()
	e := f.skipEntries[id]
	f.skipEntriesMu.Unlock()
	if e == nil {
		return fmt.Errorf("no in-flight proposal with id %s", id)
	}

	select {
	case <-e.done:
		return nil
	case <-ctx.Done():
		f.skipEntriesMu.Lock()
		defer f.skipEntriesMu.Unlock()
		if e.state == skipConsumed {
			// Consumed between ctx firing and this lock: consumed wins.
			return nil
		}
		e.state = skipAbandoned
		return ctx.Err()
	}
}

// BeginHeldApply acquires this node's WAL write lock and registers a loan
// under id, so Apply materializes the accepted forwarded entry under that
// same lock rather than re-acquiring it (which would deadlock against a
// caller holding it across a round trip). release is mandatory on every path:
// it deregisters the loan and releases the lock.
func (f *FSM) BeginHeldApply(ctx context.Context, id string) (func(), error) {
	h, err := f.walAppender.AcquireWriteLock(ctx)
	if err != nil {
		return nil, err
	}

	f.loansMu.Lock()
	f.loans[id] = h
	f.loansMu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			f.loansMu.Lock()
			delete(f.loans, id)
			f.loansMu.Unlock()
			h.Release()
		})
	}, nil
}

// tryConsumeSkip consumes a pending marker for id: it skips materialization,
// advances lastApplied to index (the proposer still holds the write lock at
// this point), signals waiters, and returns true. A missing or abandoned
// marker returns false, so Apply materializes the entry normally.
func (f *FSM) tryConsumeSkip(id string, index uint64) bool {
	f.skipEntriesMu.Lock()
	defer f.skipEntriesMu.Unlock()
	e, ok := f.skipEntries[id]
	if !ok || e.state != skipPending {
		return false
	}
	e.state = skipConsumed
	f.lastApplied.Store(index)
	close(e.done)
	return true
}

// takeLoan returns the held write lock loaned for id, if any. It does not
// remove the loan; the loan's owner releases it, and only after the apply it
// covers has run.
func (f *FSM) takeLoan(id string) *walappender.HeldLock {
	f.loansMu.Lock()
	defer f.loansMu.Unlock()
	return f.loans[id]
}

// Apply implements hraft.FSM.
//
// A decode or materialization failure panics rather than returning the
// error as hraft's generic response value. hraft advances past a failed
// entry unconditionally with no way to signal the failure back, so a
// returned error here would be silently discarded while every later entry
// applies on top of a base state this node never actually reached --
// permanently and silently diverging it from the cluster. An FSM that
// can't apply a committed entry deterministically must stop the process
// rather than limp on with corrupted state.
func (f *FSM) Apply(log *raft.Log) any {
	if log.Type != raft.LogCommand {
		return nil
	}

	entry := &raftproto.Entry{}
	if err := proto.Unmarshal(log.Data, entry); err != nil {
		f.logger.Error("failed to unmarshal committed entry", "index", log.Index, "error", err)
		panic(fmt.Sprintf("failed to unmarshal committed entry at index %d: %v", log.Index, err))
	}

	index := log.Index
	id := entry.GetHeader().GetId()

	// This node's own in-flight proposal, already published via its own write
	// path: consume the marker, don't re-materialize.
	if f.tryConsumeSkip(id, index) {
		f.logger.Debug("consumed skip marker; entry already published locally", "index", index, "id", id)
		return nil
	}

	txn := entry.GetTransaction()
	if txn == nil {
		return nil
	}

	// An accepted forwarded entry whose write lock a handler is still holding
	// across its round trip: materialize under that loaned lock.
	if loan := f.takeLoan(id); loan != nil {
		f.logger.Debug("applying forwarded entry under loaned lock",
			"index", index, "id", id, "pages", len(txn.Pages), "nTruncate", txn.NTruncate)
		if err := f.walAppender.AppendTransactionUnderLock(loan, txn, func() { f.lastApplied.Store(index) }); err != nil {
			f.logger.Error("failed to append forwarded entry", "index", index, "id", id, "error", err)
			panic(fmt.Sprintf("failed to append forwarded entry at index %d: %v", index, err))
		}
		return nil
	}

	// Otherwise materialize under our own write lock.
	f.logger.Debug("applying entry",
		"index", index, "id", id, "pages", len(txn.Pages), "nTruncate", txn.NTruncate)
	if err := f.walAppender.AppendTransaction(txn, func() { f.lastApplied.Store(index) }); err != nil {
		f.logger.Error("failed to append entry", "index", index, "id", id, "error", err)
		panic(fmt.Sprintf("failed to append entry at index %d: %v", index, err))
	}

	return nil
}

// Snapshot implements hraft.FSM. It runs serialized with Apply, so
// lastApplied is exactly the index this snapshot's bytes reflect, and that
// index travels in the snapshot's own header for the restoring node to
// recover.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	rc, err := f.snapshotter.Snapshot(f.lastApplied.Load())
	if err != nil {
		return nil, fmt.Errorf("failed to capture snapshot: %w", err)
	}

	return &fsmSnapshot{rc}, nil
}

// Restore implements hraft.FSM. It sets lastApplied to the snapshot's index
// unconditionally, including downward (a restore to an older snapshot), after
// which replay advances it forward again.
func (f *FSM) Restore(rc io.ReadCloser) error {
	index, err := f.snapshotter.Restore(rc)
	if err != nil {
		return fmt.Errorf("failed to restore snapshot: %w", err)
	}
	f.lastApplied.Store(index)

	return nil
}

// fsmSnapshot adapts a Snapshotter's io.ReadCloser to hraft.FSMSnapshot.
var _ raft.FSMSnapshot = (*fsmSnapshot)(nil)

type fsmSnapshot struct {
	rc io.ReadCloser
}

// Persist implements hraft.FSMSnapshot, streaming the already-captured
// state to sink. Safe to run long after Snapshot() returned and
// concurrently with further FSM.Apply calls, since the ReadCloser is a
// private copy the Snapshotter took up front.
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.rc); err != nil {
		return errors.Join(sink.Cancel(), fmt.Errorf("failed to persist snapshot: %w", err))
	}

	return sink.Close()
}

// Release implements hraft.FSMSnapshot.
func (s *fsmSnapshot) Release() {
	s.rc.Close()
}
