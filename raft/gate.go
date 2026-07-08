package raft

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	hraft "github.com/hashicorp/raft"

	"github.com/fuchstim/literaft/vfs"
)

// NotLeaderError is returned by Gate.Propose when this node isn't the raft
// leader. Per docs/DECISIONS.md ADR-007, a follower rejects the write
// outright rather than forwarding it; Leader carries the current leader's
// address (empty if unknown) so a caller can redirect.
type NotLeaderError struct {
	Leader hraft.ServerAddress
}

func (e *NotLeaderError) Error() string {
	if e.Leader == "" {
		return "raft: not the leader (leader unknown)"
	}
	return fmt.Sprintf("raft: not the leader (leader hint: %s)", e.Leader)
}

// CatchingUpError is returned by Gate.Propose when this node has just won
// an election but hasn't yet finished draining its apply backlog
// (docs/ROADMAP.md M5 "gaining leadership"; docs/DESIGN.md §conflicts
// "apply-behind"). The node genuinely is the raft leader -- unlike
// NotLeaderError there's no other node to redirect to -- but its local
// SQLite state may not yet reflect every entry the cluster has already
// committed, so serving a write now could silently drop or reorder
// causally-prior data. Callers should retry shortly rather than redirect.
type CatchingUpError struct{}

func (CatchingUpError) Error() string {
	return "raft: elected leader but still draining the apply backlog"
}

// Gate adapts a real hraft.Raft cluster to vfs.Gate (docs/ROADMAP.md M4).
type Gate struct {
	raft    *hraft.Raft
	fsm     *FSM
	timeout time.Duration
	ready   atomic.Bool

	stop      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// NewGate returns a Gate proposing through r, whose FSM must be fsm (the
// same FSM instance passed to hraft.NewRaft) -- Propose drives fsm's
// self-apply marker so the leader's own entries aren't double-materialized
// (see FSM's doc comment). timeout bounds each hraft.Apply call.
//
// NewGate immediately starts a background watcher tracking r's leadership
// transitions (docs/ROADMAP.md M5 "role transitions"); callers must Close
// the Gate to stop it.
func NewGate(r *hraft.Raft, fsm *FSM, timeout time.Duration) *Gate {
	g := &Gate{raft: r, fsm: fsm, timeout: timeout, stop: make(chan struct{})}
	g.wg.Add(1)
	go g.watchLeadership()
	return g
}

// Close stops the leadership watcher and any in-flight drain, waiting for
// both to exit. Idempotent.
func (g *Gate) Close() {
	g.closeOnce.Do(func() { close(g.stop) })
	g.wg.Wait()
}

// Ready reports whether this node is currently the raft leader *and* has
// finished draining its apply backlog for the current term, i.e. whether
// Propose is expected to succeed barring the usual runtime failure modes.
func (g *Gate) Ready() bool {
	return g.raft.State() == hraft.Leader && g.ready.Load()
}

// watchLeadership keeps ready in sync with this node's leadership state
// (docs/DESIGN.md §conflicts "role transitions"). hraft resolves every
// in-flight local proposal with ErrLeadershipLost *before* it flips
// LeaderCh to false (runLeader's step-down path), so by the time a
// step-down is observed here, Propose calls already in flight have (or are
// about to have) surfaced their own error -- no separate "abort in-flight
// writes" step is needed on the losing side. The gaining side needs an
// active drain, handled by drain.
func (g *Gate) watchLeadership() {
	defer g.wg.Done()
	for {
		select {
		case <-g.stop:
			return
		case isLeader := <-g.raft.LeaderCh():
			if !isLeader {
				g.ready.Store(false)
				continue
			}
			// Closed until proven otherwise, even if some earlier drain
			// already left it open -- a fresh term starts undrained.
			g.ready.Store(false)
			term := g.raft.CurrentTerm()
			g.wg.Add(1)
			go func() {
				defer g.wg.Done()
				g.drain(term)
			}()
		}
	}
}

// drain implements docs/ROADMAP.md M5's "gaining leadership" step: commit a
// current-term barrier (a no-op that only resolves once every
// already-committed entry up to and including it has been sent through
// FSM.Apply on this node) and only then open the gate. This is also what
// makes the boolean self-apply marker in fsm.go safe: hraft's Figure-8 rule
// can retroactively commit an entry this node proposed in an earlier,
// unfinished leadership stint (raft/fsm.go's doc comment) -- but that entry
// necessarily has a lower log index than this term's barrier, so it is
// applied *during this drain*, while the gate is still closed and no new
// self-proposal can be racing to (mis)claim the self-apply marker.
//
// term pins this call to the leadership stint that spawned it: if the node
// has since lost leadership or moved to a later term, drain bails without
// touching ready, so a slow, superseded drain can never re-open a gate a
// newer transition already closed (or claim credit for a later one).
func (g *Gate) drain(term uint64) {
	const retryDelay = 50 * time.Millisecond
	for {
		if g.raft.State() != hraft.Leader || g.raft.CurrentTerm() != term {
			return
		}
		// Barrier's own timeout only bounds enqueueing it, not waiting for
		// the FSM to catch up -- exactly what we want here: however long a
		// real backlog takes to drain, we wait for it rather than giving up
		// and opening the gate on stale state.
		if err := g.raft.Barrier(g.timeout).Error(); err == nil {
			if g.raft.State() == hraft.Leader && g.raft.CurrentTerm() == term {
				g.ready.Store(true)
			}
			return
		}
		select {
		case <-g.stop:
			return
		case <-time.After(retryDelay):
		}
	}
}

// Propose implements vfs.Gate. A rejected or ambiguous proposal (including
// ErrLeadershipLost -- "proposed, outcome unknown") surfaces as a plain
// error here, which vfs/file.go turns into an IOERR_WRITE, leaving no valid
// commit frame on disk (docs/CLAUDE.md "Ambiguous commit"; already exercised
// by the M2 abort-path tests).
func (g *Gate) Propose(e vfs.Entry) error {
	if g.raft.State() != hraft.Leader {
		leader, _ := g.raft.LeaderWithID()
		return &NotLeaderError{Leader: leader}
	}
	if !g.ready.Load() {
		return CatchingUpError{}
	}

	// Mark this proposal self-originated before calling Apply: FSM.Apply
	// may run (on this same node) before Apply's future resolves, and it
	// must skip materialization for this entry -- vfs/file.go is about to
	// write these exact bytes itself once Propose returns (ADR-005).
	g.fsm.beginSelfApply()
	defer g.fsm.endSelfApply()

	future := g.raft.Apply(EncodeEntry(e), g.timeout)
	if err := future.Error(); err != nil {
		return fmt.Errorf("raft: proposal failed: %w", err)
	}
	if resp := future.Response(); resp != nil {
		if err, ok := resp.(error); ok && err != nil {
			return fmt.Errorf("raft: proposal committed but the FSM rejected it: %w", err)
		}
	}
	return nil
}

var _ vfs.Gate = (*Gate)(nil)
