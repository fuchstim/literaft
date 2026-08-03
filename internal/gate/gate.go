package gate

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"

	"github.com/fuchstim/literaft/fsm"
	"github.com/fuchstim/literaft/internal/vfs"
	"github.com/fuchstim/literaft/internal/wal"
	raftproto "github.com/fuchstim/literaft/proto"
	protoerrors "github.com/fuchstim/literaft/proto/errors"
)

var _ vfs.Gate = (*Gate)(nil)

type Gate struct {
	raft            *raft.Raft
	fsm             *fsm.FSM
	leaderTransport raftproto.LeaderTransport
	logger          hclog.Logger

	applyTimeout       time.Duration
	forwardTimeout     time.Duration
	handlerLockTimeout time.Duration

	readyMu sync.RWMutex
	ready   bool

	stop      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
}

func New(
	r *raft.Raft,
	f *fsm.FSM,
	logger hclog.Logger,
	leaderTransport raftproto.LeaderTransport,
	applyTimeout, forwardTimeout, handlerLockTimeout time.Duration,
) *Gate {
	g := &Gate{
		raft:            r,
		fsm:             f,
		leaderTransport: leaderTransport,
		logger:          logger.Named("gate"),

		applyTimeout:       applyTimeout,
		forwardTimeout:     forwardTimeout,
		handlerLockTimeout: handlerLockTimeout,

		stop: make(chan struct{}),
	}
	g.wg.Go(g.watchLeadership)

	if g.leaderTransport != nil {
		g.leaderTransport.Handle(g.handleRequest)
	}

	return g
}

func (g *Gate) ProposeTransaction(frames []*wal.Frame) error {
	e := &raftproto.LogEntry{
		Header: &raftproto.LogEntry_Header{Id: uuid.NewString()},
		Payload: &raftproto.LogEntry_Transaction_{
			Transaction: raftproto.NewLogEntryTransaction(frames),
		},
	}

	g.fsm.CreateSkipMarker(e.GetHeader().GetId())
	defer g.fsm.DeleteSkipMarker(e.GetHeader().GetId())

	err := g.proposeEntry(e)
	if g.leaderTransport == nil {
		return err
	}

	var nle *protoerrors.NotLeaderError
	if err == nil || !errors.As(err, &nle) {
		return err
	}

	if nle.Leader == "" {
		// No known leader to forward to
		return err
	}

	req := &raftproto.LeaderRequest{
		Header:  &raftproto.LeaderRequest_Header{LastAppliedIndex: g.fsm.LastAppliedIndex()},
		Payload: &raftproto.LeaderRequest_LogEntry{LogEntry: e},
	}

	g.logger.Debug("forwarding follower write to leader",
		"id", e.GetHeader().GetId(), "lastAppliedIndex", req.GetHeader().GetLastAppliedIndex(), "leader", nle.Leader)

	ctx, cancel := context.WithTimeout(context.Background(), g.forwardTimeout)
	defer cancel()

	return g.forwardToLeader(ctx, req, nle.Leader)
}

func (g *Gate) Close() {
	g.closeOnce.Do(func() {
		close(g.stop)
		g.wg.Wait()
	})
}

func (g *Gate) Ready() bool {
	if g.raft.State() != raft.Leader {
		return g.leaderTransport != nil
	}

	return g.leaderReady()
}

// leaderReady reports whether this node is a leader that has finished
// draining its apply backlog. Unlike Ready, it never takes the
// forwarding-follower shortcut, so proposeEntry can't be fooled by a
// leadership loss into treating a non-leader as ready.
func (g *Gate) leaderReady() bool {
	g.readyMu.RLock()
	defer g.readyMu.RUnlock()
	return g.raft.State() == raft.Leader && g.ready
}

func (g *Gate) proposeEntry(e *raftproto.LogEntry) error {
	if g.raft.State() != raft.Leader {
		leader, _ := g.raft.LeaderWithID()
		return protoerrors.NewNotLeaderError(leader)
	}

	if !g.leaderReady() {
		return protoerrors.NewCatchingUpError()
	}

	b, err := proto.Marshal(e)
	if err != nil {
		return fmt.Errorf("failed to marshal entry: %w", err)
	}

	g.logger.Debug("proposing transaction",
		"id", e.GetHeader().GetId(), "pages", len(e.GetTransaction().GetPages()), "nTruncate", e.GetTransaction().GetNTruncate())

	future := g.raft.Apply(b, g.applyTimeout)
	if err := future.Error(); err != nil {
		g.logger.Debug("transaction proposal rejected", "id", e.GetHeader().GetId(), "error", err)

		if errors.Is(err, raft.ErrEnqueueTimeout) {
			return protoerrors.NewNotAppliedError("proposal not enqueued before timeout", err)
		}
		return protoerrors.NewAmbiguousError(err)
	}

	return nil
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

		if err := g.raft.Barrier(g.applyTimeout).Error(); err == nil {
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

func (g *Gate) forwardToLeader(ctx context.Context, req *raftproto.LeaderRequest, leader raft.ServerAddress) error {
	id := req.GetLogEntry().GetHeader().GetId()

	for attempt := 0; ; attempt++ {
		if leader == "" {
			return protoerrors.NewNotLeaderError("")
		}

		res, err := g.leaderTransport.Propose(ctx, leader, req)
		if err != nil {
			var notApplied *protoerrors.NotAppliedError
			if errors.As(err, &notApplied) {
				return err // Definitely not applied, return immediately
			}

			// Maybe applied, wait and see if FSM consumes it.
			return g.awaitFSMApplied(ctx, id, err)
		}

		switch res.GetStatus() {
		case raftproto.LeaderResponse_STATUS_OK:
			g.logger.Debug("leader accepted forwarded write", "id", id)
			return g.awaitFSMApplied(ctx, id, nil)
		case raftproto.LeaderResponse_STATUS_STALE_BASE:
			g.logger.Debug("leader rejected forwarded write: stale base",
				"id", id, "leaderApplied", res.GetLastAppliedIndex())
			return protoerrors.NewNotAppliedError(
				fmt.Sprintf("forwarded write lost to a concurrent write (leader applied index %d)", res.GetLastAppliedIndex()), nil)
		case raftproto.LeaderResponse_STATUS_CATCHING_UP:
			return protoerrors.NewCatchingUpError()
		case raftproto.LeaderResponse_STATUS_BUSY:
			reason := "leader busy"
			if d := res.GetDetail(); d != "" {
				reason = "leader busy: " + d
			}
			return protoerrors.NewNotAppliedError(reason, nil)
		case raftproto.LeaderResponse_STATUS_NOT_LEADER:
			if attempt == 0 && res.GetLeaderAddr() != "" {
				leader = raft.ServerAddress(res.GetLeaderAddr())
				continue
			}
			return protoerrors.NewNotLeaderError(raft.ServerAddress(res.GetLeaderAddr()))
		case raftproto.LeaderResponse_STATUS_AMBIGUOUS:
			// Status unknown, wait and see if FSM consumes it.
			return g.awaitFSMApplied(ctx, id, errors.New(res.GetDetail()))
		default:
			return g.awaitFSMApplied(ctx, id, fmt.Errorf("unknown forward status %v (%s)", res.GetStatus(), res.GetStatus().String()))
		}
	}
}

func (g *Gate) awaitFSMApplied(ctx context.Context, id string, cause error) error {
	if err := g.fsm.AwaitSkipMarkerConsumed(ctx, id); err != nil {
		if cause != nil {
			return protoerrors.NewAmbiguousError(cause)
		}
		return protoerrors.NewAmbiguousError(err)
	}
	return nil
}

func (g *Gate) handleRequest(ctx context.Context, req *raftproto.LeaderRequest) *raftproto.LeaderResponse {
	if err := g.validateRequest(req); err != nil {
		return &raftproto.LeaderResponse{Status: raftproto.LeaderResponse_STATUS_INVALID, Detail: err.Error()}
	}

	id := req.GetLogEntry().GetHeader().GetId()

	if g.raft.State() != raft.Leader {
		g.logger.Info("redirecting mis-routed forwarded write", "id", id, "leader", g.raft.Leader())
		return &raftproto.LeaderResponse{
			Status:     raftproto.LeaderResponse_STATUS_NOT_LEADER,
			LeaderAddr: string(g.raft.Leader()),
		}
	}

	lockCtx, cancel := context.WithTimeout(ctx, g.handlerLockTimeout)
	defer cancel()

	release, err := g.fsm.BeginHeldApply(lockCtx, id)
	if err != nil {
		return &raftproto.LeaderResponse{Status: raftproto.LeaderResponse_STATUS_BUSY, Detail: "failed to acquire write lock: " + err.Error()}
	}
	defer release()

	// Base check under the held lock. Nothing has been proposed yet.
	if la := g.fsm.LastAppliedIndex(); req.GetHeader().GetLastAppliedIndex() != la {
		g.logger.Info("rejecting forwarded write: stale base",
			"id", id, "baseIndex", req.GetHeader().GetLastAppliedIndex(), "lastApplied", la)
		return &raftproto.LeaderResponse{Status: raftproto.LeaderResponse_STATUS_STALE_BASE, LastAppliedIndex: la}
	}

	g.logger.Debug("forwarded write base-index check passed", "id", id, "baseIndex", req.GetHeader().GetLastAppliedIndex())

	if err := g.proposeEntry(req.GetLogEntry()); err != nil {
		g.logger.Info("forwarded write proposal failed", "id", id, "error", err)
		return g.classifyApplyErr(err)
	}

	g.logger.Info("accepted forwarded write", "id", id, "lastApplied", g.fsm.LastAppliedIndex())
	return &raftproto.LeaderResponse{Status: raftproto.LeaderResponse_STATUS_OK, LastAppliedIndex: g.fsm.LastAppliedIndex()}
}

func (g *Gate) validateRequest(req *raftproto.LeaderRequest) error {
	txn := req.GetLogEntry().GetTransaction()
	if txn == nil {
		return fmt.Errorf("forwarded entry has no transaction payload")
	}

	if txn.GetNTruncate() == 0 {
		return fmt.Errorf("forwarded transaction has nTruncate == 0 (not a whole committed txn)")
	}

	pages := txn.GetPages()
	if len(pages) == 0 {
		return fmt.Errorf("forwarded transaction has no pages")
	}

	for _, p := range pages {
		if p.GetPgNo() == 0 {
			return fmt.Errorf("forwarded transaction has a zero page number")
		}
		if len(p.GetData()) == 0 {
			return fmt.Errorf("forwarded transaction has a page with no data")
		}
	}

	return nil
}

func (g *Gate) classifyApplyErr(err error) *raftproto.LeaderResponse {
	var notLeader *protoerrors.NotLeaderError
	if errors.As(err, &notLeader) {
		return &raftproto.LeaderResponse{
			Status:     raftproto.LeaderResponse_STATUS_NOT_LEADER,
			LeaderAddr: string(notLeader.Leader),
			Detail:     err.Error(),
		}
	}
	var catchingUp *protoerrors.CatchingUpError
	if errors.As(err, &catchingUp) {
		return &raftproto.LeaderResponse{Status: raftproto.LeaderResponse_STATUS_CATCHING_UP, Detail: err.Error()}
	}
	var notApplied *protoerrors.NotAppliedError
	if errors.As(err, &notApplied) {
		return &raftproto.LeaderResponse{Status: raftproto.LeaderResponse_STATUS_BUSY, Detail: err.Error()}
	}
	return &raftproto.LeaderResponse{Status: raftproto.LeaderResponse_STATUS_AMBIGUOUS, Detail: err.Error()}
}
