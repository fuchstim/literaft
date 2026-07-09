package raftgate

import (
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/raft"

	raftproto "github.com/fuchstim/literaft/internal/raft/proto"
	"github.com/fuchstim/literaft/internal/vfs"
)

var _ vfs.Gate = (*Gate)(nil)

// Gate adapts a real raft.Raft cluster to vfs.Gate.
type Gate struct {
	raft    *raft.Raft
	nodeID  string
	timeout time.Duration

	readyMu sync.RWMutex
	ready   bool

	stop chan struct{}
	wg   sync.WaitGroup
}

// New returns a Gate proposing through r, whose FSM must be fsm (the
// same FSM instance passed to hraft.NewRaft) -- Propose drives fsm's
// self-apply marker so the leader's own entries aren't double-materialized.
// timeout bounds each hraft.Apply call.
//
// New immediately starts a background watcher tracking r's leadership
// transitions; callers must Close the Gate to stop it.
func New(r *raft.Raft, nodeID string, timeout time.Duration) *Gate {
	g := &Gate{raft: r, nodeID: nodeID, timeout: timeout, stop: make(chan struct{})}
	g.wg.Go(g.watchLeadership)

	return g
}

// Close stops the leadership watcher and any in-flight drain, waiting for
// both to exit.
func (g *Gate) Close() {
	close(g.stop)
	g.wg.Wait()
}

// Ready reports whether this node is currently the raft leader *and* has
// finished draining its apply backlog for the current term, i.e. whether
// Propose is expected to succeed barring the usual runtime failure modes.
func (g *Gate) Ready() bool {
	g.readyMu.RLock()
	defer g.readyMu.RUnlock()

	return g.raft.State() == raft.Leader && g.ready
}

// watchLeadership keeps ready in sync with this node's leadership state.
// hraft resolves every in-flight local proposal with ErrLeadershipLost *before* it flips
// LeaderCh to false (runLeader's step-down path), so by the time a
// step-down is observed here, Propose calls already in flight have (or are
// about to have) surfaced their own error -- no separate "abort in-flight
// writes" step is needed on the losing side. The gaining side needs an
// active drain, handled by drain.
func (g *Gate) watchLeadership() {
	for {
		select {
		case <-g.stop:
			return
		case isLeader := <-g.raft.LeaderCh():
			g.readyMu.Lock()
			g.ready = false
			g.readyMu.Unlock()

			if !isLeader {
				continue
			}

			// Closed until proven otherwise, even if some earlier drain
			// already left it open -- a fresh term starts undrained.
			term := g.raft.CurrentTerm()
			g.wg.Go(func() { g.drain(term) })
		}
	}
}

// drain implements the "gaining leadership" step: commit a current-term
// barrier (a no-op that only resolves once every
// already-committed entry up to and including it has been sent through
// FSM.Apply on this node) and only then open the gate. This is also what
// makes the boolean self-apply marker in fsm.go safe: hraft's Figure-8 rule
// can retroactively commit an entry this node proposed in an earlier,
// unfinished leadership stint -- but that entry
// necessarily has a lower log index than this term's barrier, so it is
// applied *during this drain*, while the gate is still closed and no new
// self-proposal can be racing to (mis)claim the self-apply marker.
//
// term pins this call to the leadership stint that spawned it: if the node
// has since lost leadership or moved to a later term, drain bails without
// touching ready, so a slow, superseded drain can never re-open a gate a
// newer transition already closed (or claim credit for a later one).
//
// The post-Barrier check-and-set holds readyMu across both halves: a plain
// "check state+term, then Store(true)" would leave a window where
// watchLeadership's step-down write for this exact loss could land between
// the check and the set, and drain's Store(true) would silently clobber it
// back to true. No concrete way to trigger it was found (Ready/Propose both
// independently re-check State() == Leader before consulting ready, and any
// later leadership regain unconditionally resets ready first), but the race
// itself is real, so it's closed properly rather than left as a documented
// coincidence.
func (g *Gate) drain(term uint64) {
	const retryDelay = 50 * time.Millisecond
	for {
		if g.raft.State() != raft.Leader || g.raft.CurrentTerm() != term {
			return
		}

		// Barrier's own timeout only bounds enqueueing it, not waiting for
		// the FSM to catch up
		if err := g.raft.Barrier(g.timeout).Error(); err == nil {
			g.readyMu.Lock()
			defer g.readyMu.Unlock()
			if g.raft.State() == raft.Leader && g.raft.CurrentTerm() == term {
				g.ready = true
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
// ErrLeadershipLost -- "proposed, outcome unknown") surfaces as an error.
func (g *Gate) Propose(frames []*vfs.Frame, nTruncate uint32) error {
	if g.raft.State() != raft.Leader {
		leader, _ := g.raft.LeaderWithID()
		return &NotLeaderError{Leader: leader}
	}

	if !g.Ready() {
		return CatchingUpError{}
	}

	e := &raftproto.Entry{g.nodeID, frames, nTruncate}

	future := g.raft.Apply(e.Encode(), g.timeout)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to apply change: %w", err)
	}

	if resp := future.Response(); resp != nil {
		if err, ok := resp.(error); ok && err != nil {
			return fmt.Errorf("proposal committed but the FSM rejected it: %w", err)
		}
	}

	return nil
}
