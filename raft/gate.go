package raft

import (
	"fmt"
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

// Gate adapts a real hraft.Raft cluster to vfs.Gate (docs/ROADMAP.md M4).
type Gate struct {
	raft    *hraft.Raft
	fsm     *FSM
	timeout time.Duration
}

// NewGate returns a Gate proposing through r, whose FSM must be fsm (the
// same FSM instance passed to hraft.NewRaft) -- Propose drives fsm's
// self-apply marker so the leader's own entries aren't double-materialized
// (see FSM's doc comment). timeout bounds each hraft.Apply call.
func NewGate(r *hraft.Raft, fsm *FSM, timeout time.Duration) *Gate {
	return &Gate{raft: r, fsm: fsm, timeout: timeout}
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
