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
// M4-scoped limitation: this assumes a freshly-leading node has no apply
// backlog when it starts accepting local proposals (true for the bootstrap
// / small-cluster scenarios M4 targets). The general fix -- drain
// lastApplied == commitIndex before opening the gate on a new leader -- is
// docs/ROADMAP.md M5 ("gaining leadership"); see docs/DESIGN.md §conflicts.
// Without that drain, a backlog entry could be wrongly treated as
// self-originated (skipped, losing data) or vice versa. Concretely: a node
// can accumulate exactly this kind of backlog against itself, with no other
// leader involved, via hraft's Figure-8 rule -- an entry it appended while
// leader but never got committed (e.g. it lost leadership mid-proposal, the
// scenario Gate.Propose's ambiguous-commit error covers) can be retroactively
// committed later, once it regains leadership and a *subsequent* entry in
// its new term commits and covers it. If that regained-leadership node's
// next self-proposal starts before its own FSM has caught up through that
// stale entry, the self-pending flag can attach to the wrong one.
type FSM struct {
	materializer Materializer

	mu          sync.Mutex
	selfPending bool
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
func (f *FSM) Apply(log *hraft.Log) interface{} {
	if log.Type != hraft.LogCommand {
		return nil
	}
	if f.takeSelfApply() {
		return nil
	}
	entry, err := DecodeEntry(log.Data)
	if err != nil {
		return fmt.Errorf("raft: decoding committed entry at index %d: %w", log.Index, err)
	}
	if err := f.materializer.Apply(entry); err != nil {
		return fmt.Errorf("raft: materializing entry at index %d: %w", log.Index, err)
	}
	return nil
}

// Snapshot implements hraft.FSM. Snapshot-based log compaction and
// very-behind-follower catch-up (InstallSnapshot) are deferred to
// docs/ROADMAP.md M5; node wiring configures hraft to avoid triggering this
// in normal M4 operation (high SnapshotThreshold/SnapshotInterval).
// Returning an error here -- rather than a snapshot of nothing -- means a
// forced snapshot attempt fails loudly instead of silently discarding state.
func (f *FSM) Snapshot() (hraft.FSMSnapshot, error) {
	return nil, errors.New("raft: snapshotting not implemented yet (docs/ROADMAP.md M5)")
}

// Restore implements hraft.FSM. See Snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	rc.Close()
	return errors.New("raft: snapshot restore not implemented yet (docs/ROADMAP.md M5)")
}

var _ hraft.FSM = (*FSM)(nil)
