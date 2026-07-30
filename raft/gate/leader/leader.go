package leadergate

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"

	"github.com/fuchstim/literaft/internal/vfs"
	"github.com/fuchstim/literaft/internal/wal"
	rafterrors "github.com/fuchstim/literaft/raft/errors"
	"github.com/fuchstim/literaft/raft/fsm"
	raftproto "github.com/fuchstim/literaft/raft/proto"
)

var _ vfs.Gate = (*Gate)(nil)

// Gate implements a vfs.Gate that only accepts writes on the current RAFT cluster leader
type Gate struct {
	raft   *raft.Raft
	fsm    *fsm.FSM
	logger hclog.Logger

	timeout time.Duration

	readyMu sync.RWMutex
	ready   bool

	stop      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
}

func New(r *raft.Raft, f *fsm.FSM, opts ...Option) *Gate {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}

	g := &Gate{
		raft:    r,
		fsm:     f,
		timeout: o.applyTimeout,
		logger:  o.logger.Named("leadergate"),
		stop:    make(chan struct{}),
	}
	g.wg.Go(g.watchLeadership)

	return g
}

func (g *Gate) ProposeTransaction(frames []*wal.Frame) error {
	e := &raftproto.LogEntry{
		Header: &raftproto.LogEntry_Header{Id: uuid.NewString()},
		Payload: &raftproto.LogEntry_Transaction_{
			Transaction: raftproto.NewLogEntryTransaction(frames),
		},
	}

	return g.ProposeEntry(e)
}

func (g *Gate) ProposeEntry(e *raftproto.LogEntry) error {
	if g.raft.State() != raft.Leader {
		leader, _ := g.raft.LeaderWithID()
		return rafterrors.NewNotLeaderError(leader)
	}

	if !g.Ready() {
		return rafterrors.NewCatchingUpError()
	}

	b, err := proto.Marshal(e)
	if err != nil {
		return fmt.Errorf("failed to marshal entry: %w", err)
	}

	g.fsm.CreateSkipMarker(e.GetHeader().GetId())
	defer g.fsm.DeleteSkipMarker(e.GetHeader().GetId())

	g.logger.Debug("proposing transaction",
		"id", e.GetHeader().GetId(), "pages", len(e.GetTransaction().GetPages()), "nTruncate", e.GetTransaction().GetNTruncate())

	future := g.raft.Apply(b, g.timeout)
	if err := future.Error(); err != nil {
		g.logger.Debug("transaction proposal rejected", "id", e.GetHeader().GetId(), "error", err)

		if errors.Is(err, raft.ErrEnqueueTimeout) {
			return rafterrors.NewNotAppliedError("proposal not enqueued before timeout", err)
		}
		return rafterrors.NewAmbiguousError(err)
	}

	return nil
}

func (g *Gate) Close() {
	g.closeOnce.Do(func() {
		close(g.stop)
		g.wg.Wait()
	})
}

// Ready reports whether this node is currently the raft leader *and* has
// finished draining its apply backlog for the current term, i.e. whether
// Apply is expected to succeed barring the usual runtime failure modes.
func (g *Gate) Ready() bool {
	g.readyMu.RLock()
	defer g.readyMu.RUnlock()

	return g.raft.State() == raft.Leader && g.ready
}

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
				g.logger.Info("lost leadership", "term", g.raft.CurrentTerm())
				continue
			}

			term := g.raft.CurrentTerm()
			g.logger.Info("gained leadership; draining apply backlog", "term", term)
			g.wg.Go(func() { g.drain(term) })
		}
	}
}

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
				g.logger.Info("drained apply backlog; ready to serve writes", "term", term)
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
