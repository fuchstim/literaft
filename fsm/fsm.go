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

// Must not implement raft.BatchingFSM; each entry must be applied separately as the writer's
// Propose call blocks until the entry was materialized by the FSM.
var _ raft.FSM = (*FSM)(nil)

type FSM struct {
	dbPath      string
	db          *sqlite3.Conn
	dbLock      *os.File
	pageSize    uint32
	walAppender *walappender.WALAppender
	snapshotter *snapshotter.Snapshotter
	logger      hclog.Logger

	// raft.LastApplied is incremented before fsm.Apply is called, so it may
	// be ahead of fsm.lastApplied if Apply is still running. We're keeping
	// our own lastApplied to track the last index that was actually materialized.
	lastApplied atomic.Uint64

	skipMarkers   map[string]*skipMarker
	skipMarkersMu sync.Mutex

	loans   map[string]*walappender.HeldLock
	loansMu sync.Mutex
}

// skipMarker tracks in-flight self-proposals so that when they are committed,
// the FSM can skip materializing them (they were already materialized by the proposer).
type skipMarker struct {
	state skipMarkerState
	done  chan struct{}
}

type skipMarkerState int

const (
	// pending: entry not yet applied
	skipMarkerPending skipMarkerState = iota
	// skipped: applied through self-write path, FSM skipped materialization
	skipMarkerSkipped
	// abandoned: proposer gave up before consumption, so FSM materializes normally
	skipMarkerAbandoned
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

	pageSize, err := getPageSize(db)
	if err != nil {
		return nil, fmt.Errorf("failed to get page size: %w", err)
	}

	// Acquired only after WAL mode is enabled (enabling WAL mode requires exclusive lock)
	dbLock, err := acquireSharedDBLock(dbPath)
	if err != nil {
		return nil, err
	}

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

		skipMarkers: make(map[string]*skipMarker),
		loans:       make(map[string]*walappender.HeldLock),
	}, nil
}

func (f *FSM) Close() error {
	return errors.Join(f.db.Close(), f.walAppender.Close(), f.dbLock.Close())
}

func (f *FSM) DBPath() string {
	return f.dbPath
}

func (f *FSM) CreateSkipMarker(entryID string) {
	f.skipMarkersMu.Lock()
	defer f.skipMarkersMu.Unlock()
	f.skipMarkers[entryID] = &skipMarker{state: skipMarkerPending, done: make(chan struct{})}
}

func (f *FSM) DeleteSkipMarker(entryID string) {
	f.skipMarkersMu.Lock()
	defer f.skipMarkersMu.Unlock()
	delete(f.skipMarkers, entryID)
}

func (f *FSM) AwaitSkipMarkerConsumed(ctx context.Context, entryID string) error {
	f.skipMarkersMu.Lock()
	e := f.skipMarkers[entryID]
	f.skipMarkersMu.Unlock()
	if e == nil {
		return fmt.Errorf("no skip marker found with id %s", entryID)
	}

	select {
	case <-e.done:
		return nil
	case <-ctx.Done():
		f.skipMarkersMu.Lock()
		defer f.skipMarkersMu.Unlock()
		if e.state == skipMarkerSkipped {
			return nil
		}
		e.state = skipMarkerAbandoned
		return ctx.Err()
	}
}

func (f *FSM) BeginHeldApply(ctx context.Context, entryID string) (func(), error) {
	h, err := f.walAppender.AcquireWriteLock(ctx)
	if err != nil {
		return nil, err
	}

	f.loansMu.Lock()
	defer f.loansMu.Unlock()
	f.loans[entryID] = h

	var once sync.Once
	return func() {
		once.Do(func() {
			f.loansMu.Lock()
			defer f.loansMu.Unlock()
			delete(f.loans, entryID)
			h.Release()
		})
	}, nil
}

func (f *FSM) tryConsumeSkipMarker(entryID string, index uint64) bool {
	f.skipMarkersMu.Lock()
	defer f.skipMarkersMu.Unlock()

	m, ok := f.skipMarkers[entryID]
	if !ok || m.state != skipMarkerPending {
		return false
	}

	m.state = skipMarkerSkipped
	f.lastApplied.Store(index)
	close(m.done)
	return true
}

func (f *FSM) getLoan(id string) *walappender.HeldLock {
	f.loansMu.Lock()
	defer f.loansMu.Unlock()
	return f.loans[id]
}

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
	entryID := entry.GetHeader().GetId()

	// This node's own in-flight proposal, already published via its own write
	// path: consume the marker, don't re-materialize.
	if f.tryConsumeSkipMarker(entryID, index) {
		f.logger.Debug("consumed skip marker; entry already published locally", "index", index, "id", entryID)
		return nil
	}

	txn := entry.GetTransaction()
	if txn == nil {
		return nil
	}

	// An accepted forwarded entry whose write lock a handler is still holding
	// across its round trip: materialize under that loaned lock.
	if loan := f.getLoan(entryID); loan != nil {
		f.logger.Debug("applying forwarded entry under loaned lock",
			"index", index, "id", entryID, "pages", len(txn.Pages), "nTruncate", txn.NTruncate)
		if err := f.walAppender.AppendTransactionUnderLock(loan, txn, func() { f.lastApplied.Store(index) }); err != nil {
			f.logger.Error("failed to append forwarded entry", "index", index, "id", entryID, "error", err)
			panic(fmt.Sprintf("failed to append forwarded entry at index %d: %v", index, err))
		}
		return nil
	}

	// Otherwise materialize under our own write lock.
	f.logger.Debug("applying entry",
		"index", index, "id", entryID, "pages", len(txn.Pages), "nTruncate", txn.NTruncate)
	if err := f.walAppender.AppendTransaction(txn, func() { f.lastApplied.Store(index) }); err != nil {
		f.logger.Error("failed to append entry", "index", index, "id", entryID, "error", err)
		panic(fmt.Sprintf("failed to append entry at index %d: %v", index, err))
	}

	return nil
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	rc, err := f.snapshotter.Snapshot(f.lastApplied.Load())
	if err != nil {
		return nil, fmt.Errorf("failed to capture snapshot: %w", err)
	}

	return &fsmSnapshot{rc}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	header, err := f.snapshotter.Restore(rc)
	if err != nil {
		return fmt.Errorf("failed to restore snapshot: %w", err)
	}
	f.lastApplied.Store(header.LastAppliedIndex)

	return nil
}

var _ raft.FSMSnapshot = (*fsmSnapshot)(nil)

type fsmSnapshot struct {
	rc io.ReadCloser
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.rc); err != nil {
		return errors.Join(sink.Cancel(), fmt.Errorf("failed to persist snapshot: %w", err))
	}

	return sink.Close()
}

func (s *fsmSnapshot) Release() {
	s.rc.Close()
}

func getPageSize(db *sqlite3.Conn) (uint32, error) {
	stmt, _, err := db.Prepare("PRAGMA page_size;")
	if err != nil {
		return 0, fmt.Errorf("failed to prepare PRAGMA page_size statement: %w", err)
	}
	defer stmt.Close()

	var pageSize uint32
	for stmt.Step() {
		pageSize = uint32(stmt.ColumnInt(0))
	}

	if err := stmt.Reset(); err != nil {
		return 0, fmt.Errorf("failed to reset PRAGMA page_size statement: %w", err)
	}

	if pageSize <= 0 {
		return 0, fmt.Errorf("invalid page size %d returned from PRAGMA page_size", pageSize)
	}

	return pageSize, nil
}
