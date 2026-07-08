package raft

import (
	"errors"
	"fmt"
	"io"
	"sync"

	hraft "github.com/hashicorp/raft"

	"github.com/fuchstim/literaft/vfs"
)

// Materializer applies a captured write transaction into a follower's local
// WAL + wal-index. apply.Applier satisfies this; kept as an interface here
// so this package's tests can inject a spy instead of touching real files.
type Materializer interface {
	Apply(vfs.Entry) error
}

// FSM adapts a Materializer to hraft.FSM.
//
// Leader/follower asymmetry (docs/DECISIONS.md ADR-005): hraft calls Apply
// for every committed entry on every node, including the leader that
// proposed it. But the leader must publish only through SQLite's own commit
// path -- vfs/file.go releases the withheld commit frame once Gate.Propose
// returns, and that write, not a second one driven by Materializer, is what
// must reach disk. So Gate marks its own outstanding proposal
// "self-pending" before calling hraft's Apply; this FSM's Apply consumes
// that marker and skips materialization for exactly that entry.
//
// That boolean marker is only safe because Gate never lets a *new*
// self-proposal start until it has proven there's no leftover backlog from
// an earlier leadership stint (docs/ROADMAP.md M5 "gaining leadership",
// implemented by Gate.drain). Without that drain, a node could accumulate
// backlog against itself, with no other leader involved, via hraft's
// Figure-8 rule: an entry it appended while leader but never got committed
// (e.g. it lost leadership mid-proposal, the scenario Gate.Propose's
// ambiguous-commit error covers) can be retroactively committed later, once
// it regains leadership and a *subsequent* entry in its new term commits
// and covers it. If a regained-leadership node's next self-proposal started
// before its own FSM had caught up through that stale entry, the
// self-pending flag could attach to the wrong one (dropping the stale
// entry, or double-materializing the new one). Gate.drain closes this gap
// structurally: the stale entry necessarily has a lower log index than the
// drain's own barrier, so it is always applied *during* the drain, while
// the gate is still closed and no self-proposal can be racing for the
// marker. See docs/DESIGN.md §conflicts.
type FSM struct {
	materializer Materializer

	mu          sync.Mutex
	selfPending bool
	snapshotter Snapshotter
}

// Snapshotter captures and restores this node's full database state for
// RAFT snapshot-based (very-behind follower) catch-up (docs/ROADMAP.md M6).
// internal/node's dbBackend satisfies this by driving a TRUNCATE checkpoint
// (docs/DESIGN.md §checkpoint's "natural cut point") and swapping the
// resulting .db file in as a unit.
type Snapshotter interface {
	// Snapshot returns a reader over a complete, self-contained, private
	// copy of the current state. It must be unaffected by any Apply calls
	// that happen after Snapshot returns but before the reader is fully
	// consumed and closed -- hraft streams it (FSMSnapshot.Persist) from a
	// separate goroutine, possibly much later and concurrently with
	// further log application.
	Snapshot() (io.ReadCloser, error)
	// Restore replaces all local state with r's bytes (as produced by
	// another node's Snapshot) and resets any local WAL/apply state to
	// match. r is exhausted but not closed by Restore.
	Restore(r io.Reader) error
}

// SetSnapshotter wires s in as the FSM's Snapshotter. Must be called once,
// before hraft can plausibly issue a Snapshot/InstallSnapshot against this
// FSM (node.Start does this as part of bringing the node up, after the
// backend owning the swappable SQLite state exists).
func (f *FSM) SetSnapshotter(s Snapshotter) {
	f.mu.Lock()
	f.snapshotter = s
	f.mu.Unlock()
}

// NewFSM returns an FSM that materializes non-self-originated entries via m.
func NewFSM(m Materializer) *FSM {
	return &FSM{materializer: m}
}

// beginSelfApply marks the next Apply call as this node's own proposal,
// about to be published by the caller's own SQLite write path rather than
// by the Materializer. Must be paired with endSelfApply (deferred by the
// caller) so a proposal that fails before Apply ever runs (e.g.
// ErrLeadershipLost or a timeout) can't leave the flag set for some later,
// unrelated entry.
func (f *FSM) beginSelfApply() {
	f.mu.Lock()
	f.selfPending = true
	f.mu.Unlock()
}

// endSelfApply unconditionally clears the self-pending marker.
func (f *FSM) endSelfApply() {
	f.mu.Lock()
	f.selfPending = false
	f.mu.Unlock()
}

// takeSelfApply atomically reads and clears the self-pending marker.
func (f *FSM) takeSelfApply() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	v := f.selfPending
	f.selfPending = false
	return v
}

// Apply implements hraft.FSM.
//
// A decode or materialization failure panics rather than returning the error
// as hraft's generic response value. hraft has no "apply failed, stop
// advancing" concept: processLogs advances lastApplied unconditionally after
// dispatching a batch, and every follower-received entry (plus any
// retroactively-committed Figure-8 entry from an earlier leadership stint) is
// applied with no local future to receive a response at all -- so a returned
// error here is silently discarded forever, while lastApplied has already
// moved past the entry that failed. Every later entry then applies on top of
// a base state this node never actually reached, permanently and silently
// diverging it from the cluster (CLAUDE.md: "apply must be strictly in-order
// and gapless"). An FSM that can't apply a committed entry deterministically
// has no safe way to keep participating, so it must stop the process instead
// of limping on with corrupted state.
func (f *FSM) Apply(log *hraft.Log) interface{} {
	// Defensive, not load-bearing against the real library: hraft's own
	// applySingle only ever calls FSM.Apply for LogCommand entries (verified
	// against vendor/github.com/hashicorp/raft/fsm.go) -- LogBarrier and
	// LogConfiguration entries never reach here at all. That's actually
	// load-bearing context for Gate.drain's ordering argument: a Barrier's
	// future resolves without ever touching the self-apply marker below,
	// which is what lets drain distinguish "the barrier itself completed"
	// from "an entry got materialized."
	if log.Type != hraft.LogCommand {
		return nil
	}
	if f.takeSelfApply() {
		return nil
	}
	entry, err := DecodeEntry(log.Data)
	if err != nil {
		panic(fmt.Sprintf("raft: decoding committed entry at index %d: %v", log.Index, err))
	}
	if err := f.materializer.Apply(entry); err != nil {
		panic(fmt.Sprintf("raft: materializing entry at index %d: %v", log.Index, err))
	}
	return nil
}

// Snapshot implements hraft.FSM (docs/ROADMAP.md M6). It delegates the
// actual state capture to the configured Snapshotter and wraps the result
// for hraft's snapshot machinery.
func (f *FSM) Snapshot() (hraft.FSMSnapshot, error) {
	f.mu.Lock()
	s := f.snapshotter
	f.mu.Unlock()
	if s == nil {
		return nil, errors.New("raft: snapshotting not configured yet")
	}
	rc, err := s.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("raft: capturing snapshot: %w", err)
	}
	return &fsmSnapshot{rc: rc}, nil
}

// Restore implements hraft.FSM. See Snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	f.mu.Lock()
	s := f.snapshotter
	f.mu.Unlock()
	if s == nil {
		return errors.New("raft: snapshotting not configured yet")
	}
	if err := s.Restore(rc); err != nil {
		return fmt.Errorf("raft: installing snapshot: %w", err)
	}
	return nil
}

// fsmSnapshot adapts a Snapshotter's io.ReadCloser to hraft.FSMSnapshot.
type fsmSnapshot struct {
	rc io.ReadCloser
}

// Persist implements hraft.FSMSnapshot, streaming the already-captured
// state to sink. Safe to run long after Snapshot() returned and
// concurrently with further FSM.Apply calls, since the ReadCloser is a
// private copy the Snapshotter took up front.
func (s *fsmSnapshot) Persist(sink hraft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.rc); err != nil {
		sink.Cancel()
		return fmt.Errorf("raft: persisting snapshot: %w", err)
	}
	return sink.Close()
}

// Release implements hraft.FSMSnapshot.
func (s *fsmSnapshot) Release() {
	s.rc.Close()
}

var _ hraft.FSM = (*FSM)(nil)
var _ hraft.FSMSnapshot = (*fsmSnapshot)(nil)
