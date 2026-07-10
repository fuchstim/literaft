package fsm

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/fuchstim/literaft/internal/fsm/snapshotter"
	"github.com/fuchstim/literaft/internal/fsm/walappender"
	raftproto "github.com/fuchstim/literaft/internal/raft/proto"
	"github.com/hashicorp/raft"
	"github.com/ncruces/go-sqlite3"
)

var _ raft.FSM = (*FSM)(nil)

type FSM struct {
	nodeID, dbPath string
	db             *sqlite3.Conn
	dbLock         *os.File
	pageSize       uint32
	walAppender    *walappender.WALAppender
	snapshotter    *snapshotter.Snapshotter
}

func New(nodeID, dbPath string, opts ...Option) (*FSM, error) {
	if nodeID == "" {
		return nil, fmt.Errorf("nodeID is required")
	}

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

	walAppender, err := walappender.Open(dbPath, pageSize, o.checkpointThresholdPages, o.checkpointInterval)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL appender: %w", err)
	}

	snapshotter := snapshotter.New(dbPath, pageSize)

	return &FSM{
		nodeID:      nodeID,
		dbPath:      dbPath,
		db:          db,
		dbLock:      dbLock,
		pageSize:    uint32(pageSize),
		walAppender: walAppender,
		snapshotter: snapshotter,
	}, nil
}

func (f *FSM) Close() error {
	return errors.Join(f.db.Close(), f.walAppender.Close(), f.dbLock.Close())
}

func (f *FSM) NodeID() string {
	return f.nodeID
}

func (f *FSM) DBPath() string {
	return f.dbPath
}

func (f *FSM) PageSize() uint32 {
	return f.pageSize
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

	entry, err := raftproto.DecodeEntry(log.Data)
	if err != nil {
		panic(fmt.Sprintf("failed to decode committed entry at index %d: %v", log.Index, err))
	}

	if entry.NodeID == f.NodeID() {
		return nil
	}

	if err := f.walAppender.AppendEntry(entry); err != nil {
		panic(fmt.Sprintf("failed to append entry at index %d: %v", log.Index, err))
	}

	return nil
}

// Snapshot implements hraft.FSM. It delegates the
// actual state capture to the Snapshotter and wraps the result
// for hraft's snapshot machinery.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	rc, err := f.snapshotter.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("failed to capture snapshot: %w", err)
	}

	return &fsmSnapshot{rc}, nil
}

// Restore implements hraft.FSM. See Snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	if err := f.snapshotter.Restore(rc); err != nil {
		return fmt.Errorf("failed to restore snapshot: %w", err)
	}

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
