package log

import (
	"errors"
	"sync"
	"time"

	"github.com/hashicorp/raft"

	raftgate "github.com/fuchstim/literaft/internal/raft/gate"
	rafterrors "github.com/fuchstim/literaft/internal/raft/gate/errors"
)

var _ raftgate.LogAdapter = (*SingleWriterLog)(nil)

// SingleWriterLog adapts a real raft.Raft cluster to raftgate.LogAdapter.
type SingleWriterLog struct {
	raft    *raft.Raft
	timeout time.Duration

	readyMu sync.RWMutex
	ready   bool

	stop      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// NewSingleWriterLog returns a SingleWriterLog applying entries through r.
// WithApplyTimeout bounds each hraft.Apply call; see its doc for the
// default.
//
// NewSingleWriterLog immediately starts a background watcher tracking r's
// leadership transitions; callers must Close the SingleWriterLog to stop
// it.
func NewSingleWriterLog(r *raft.Raft, opts ...Option) *SingleWriterLog {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}

	g := &SingleWriterLog{raft: r, timeout: o.applyTimeout, stop: make(chan struct{})}
	g.wg.Go(g.watchLeadership)

	return g
}

// Close stops the leadership watcher and any in-flight drain, waiting for
// both to exit. Idempotent.
func (g *SingleWriterLog) Close() {
	g.closeOnce.Do(func() {
		close(g.stop)
		g.wg.Wait()
	})
}

// Ready reports whether this node is currently the raft leader *and* has
// finished draining its apply backlog for the current term, i.e. whether
// Apply is expected to succeed barring the usual runtime failure modes.
func (g *SingleWriterLog) Ready() bool {
	g.readyMu.RLock()
	defer g.readyMu.RUnlock()

	return g.raft.State() == raft.Leader && g.ready
}

// IsLeader reports whether this node is currently the raft leader, regardless
// of whether it has finished draining.
func (g *SingleWriterLog) IsLeader() bool {
	return g.raft.State() == raft.Leader
}

// LeaderAddr returns the address this node currently believes is the leader
// (empty if unknown).
func (g *SingleWriterLog) LeaderAddr() raft.ServerAddress {
	addr, _ := g.raft.LeaderWithID()
	return addr
}

// watchLeadership keeps ready in sync with this node's leadership state.
// hraft resolves every in-flight local proposal with ErrLeadershipLost *before* it flips
// LeaderCh to false (runLeader's step-down path), so by the time a
// step-down is observed here, Apply calls already in flight have (or are
// about to have) surfaced their own error -- no separate "abort in-flight
// writes" step is needed on the losing side. The gaining side needs an
// active drain, handled by drain.
func (g *SingleWriterLog) watchLeadership() {
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
// barrier (a no-op that only resolves once every already-committed entry up
// to and including it has been applied on this node) and only then mark
// this node ready. This is also what makes the self-apply marker safe:
// hraft's Figure-8 rule can retroactively commit an entry this node
// proposed in an earlier, unfinished leadership stint -- but that entry
// necessarily has a lower log index than this term's barrier, so it is
// applied *during this drain*, while this node is still not ready and no
// new self-proposal can be racing to (mis)claim the marker.
//
// term pins this call to the leadership stint that spawned it: if the node
// has since lost leadership or moved to a later term, drain bails without
// touching ready, so a slow, superseded drain can never mark the node
// ready again after a newer transition already closed it.
//
// The post-Barrier check-and-set holds readyMu across both halves: a plain
// "check state+term, then Store(true)" would leave a window where
// watchLeadership's step-down write for this exact loss could land between
// the check and the set, silently clobbering it back to true. Closed
// properly rather than left as a documented coincidence.
func (g *SingleWriterLog) drain(term uint64) {
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

// Apply implements raftgate.LogAdapter, translating every rejection into the
// shared rafterrors taxonomy. A rejected or ambiguous proposal (including
// ErrLeadershipLost -- "proposed, outcome unknown") surfaces as an error.
func (g *SingleWriterLog) Apply(e []byte) error {
	if g.raft.State() != raft.Leader {
		leader, _ := g.raft.LeaderWithID()
		return rafterrors.NewNotLeaderError(string(leader))
	}

	if !g.Ready() {
		return rafterrors.NewCatchingUpError()
	}

	future := g.raft.Apply(e, g.timeout)
	if err := future.Error(); err != nil {
		// Classify so callers can tell a definitively-not-proposed failure
		// from an ambiguous one. An enqueue timeout never entered the log;
		// any other failure leaves the outcome unknown.
		if errors.Is(err, raft.ErrEnqueueTimeout) {
			return rafterrors.NewNotAppliedError("proposal not enqueued before timeout", err)
		}
		return rafterrors.NewAmbiguousError(err)
	}

	return nil
}
