package forwardinggate

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"

	"github.com/fuchstim/literaft/internal/vfs"
	"github.com/fuchstim/literaft/internal/wal"
	rafterrors "github.com/fuchstim/literaft/raft/errors"
	"github.com/fuchstim/literaft/raft/fsm"
	leadergate "github.com/fuchstim/literaft/raft/gate/leader"
	raftproto "github.com/fuchstim/literaft/raft/proto"
)

type LeaderTransport interface {
	Propose(ctx context.Context, leader raft.ServerAddress, request *raftproto.ForwardRequest) (*raftproto.ForwardResponse, error)
	Handle(handler func(ctx context.Context, request *raftproto.ForwardRequest) *raftproto.ForwardResponse)
}

var _ vfs.Gate = (*Gate)(nil)

// Gate implements a vfs.Gate that accepts writes on the current RAFT cluster leader,
// and forwards writes to the leader on followers.
type Gate struct {
	raft      *raft.Raft
	fsm       *fsm.FSM
	transport LeaderTransport
	logger    hclog.Logger

	forwardTimeout     time.Duration
	handlerLockTimeout time.Duration

	baseGate *leadergate.Gate
}

func New(r *raft.Raft, f *fsm.FSM, transport LeaderTransport, opts ...Option) *Gate {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	o.baseGateOptions = append(o.baseGateOptions, leadergate.WithLogger(o.logger))

	g := &Gate{
		raft:      r,
		fsm:       f,
		transport: transport,
		logger:    o.logger.Named("forwardinggate"),

		forwardTimeout:     o.forwardTimeout,
		handlerLockTimeout: o.handlerLockTimeout,

		baseGate: leadergate.New(r, f, o.baseGateOptions...),
	}
	transport.Handle(g.handleRequest)
	return g
}

func (g *Gate) ProposeTransaction(frames []*wal.Frame) error {
	e := &raftproto.LogEntry{
		Header: &raftproto.LogEntry_Header{Id: uuid.NewString()},
		Payload: &raftproto.LogEntry_Transaction_{
			Transaction: raftproto.NewLogEntryTransaction(frames),
		},
	}

	err := g.baseGate.ProposeEntry(e)
	var nle *rafterrors.NotLeaderError
	if err == nil || !errors.As(err, &nle) {
		return err
	}

	if nle.Leader == "" {
		// No known leader to forward to
		return err
	}

	req := &raftproto.ForwardRequest{
		Header: &raftproto.ForwardRequest_Header{LastAppliedIndex: g.fsm.LastAppliedIndex()},
		Entry:  e,
	}

	g.logger.Debug("forwarding follower write to leader",
		"id", e.GetHeader().GetId(), "lastAppliedIndex", req.GetHeader().GetLastAppliedIndex(), "leader", nle.Leader)

	ctx, cancel := context.WithTimeout(context.Background(), g.forwardTimeout)
	defer cancel()

	g.fsm.CreateSkipMarker(e.GetHeader().GetId())
	defer g.fsm.DeleteSkipMarker(e.GetHeader().GetId())

	return g.forwardToLeader(ctx, req, nle.Leader)
}

func (g *Gate) forwardToLeader(ctx context.Context, req *raftproto.ForwardRequest, leader raft.ServerAddress) error {
	id := req.GetEntry().GetHeader().GetId()

	for attempt := 0; ; attempt++ {
		if leader == "" {
			return rafterrors.NewNotLeaderError("")
		}

		res, err := g.transport.Propose(ctx, leader, req)
		if err != nil {
			var notApplied *rafterrors.NotAppliedError
			if errors.As(err, &notApplied) {
				return err // Definitely not applied, return immediately
			}

			// Maybe applied, wait and see if FSM consumes it.
			return g.awaitFSMApplied(ctx, id, err)
		}

		switch res.GetStatus() {
		case raftproto.ForwardResponse_STATUS_OK:
			g.logger.Debug("leader accepted forwarded write", "id", id)
			return g.awaitFSMApplied(ctx, id, nil)
		case raftproto.ForwardResponse_STATUS_STALE_BASE:
			g.logger.Debug("leader rejected forwarded write: stale base",
				"id", id, "leaderApplied", res.GetLastAppliedIndex())
			return rafterrors.NewNotAppliedError(
				fmt.Sprintf("forwarded write lost to a concurrent write (leader applied index %d)", res.GetLastAppliedIndex()), nil)
		case raftproto.ForwardResponse_STATUS_CATCHING_UP:
			return rafterrors.NewCatchingUpError()
		case raftproto.ForwardResponse_STATUS_BUSY:
			reason := "leader busy"
			if d := res.GetDetail(); d != "" {
				reason = "leader busy: " + d
			}
			return rafterrors.NewNotAppliedError(reason, nil)
		case raftproto.ForwardResponse_STATUS_NOT_LEADER:
			if attempt == 0 && res.GetLeaderAddr() != "" {
				leader = raft.ServerAddress(res.GetLeaderAddr())
				continue
			}
			return rafterrors.NewNotLeaderError(raft.ServerAddress(res.GetLeaderAddr()))
		case raftproto.ForwardResponse_STATUS_AMBIGUOUS:
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
			return rafterrors.NewAmbiguousError(cause)
		}
		return rafterrors.NewAmbiguousError(err)
	}
	return nil
}

func (g *Gate) handleRequest(ctx context.Context, req *raftproto.ForwardRequest) *raftproto.ForwardResponse {
	if err := g.validateRequest(req); err != nil {
		return &raftproto.ForwardResponse{Status: raftproto.ForwardResponse_STATUS_INVALID, Detail: err.Error()}
	}

	id := req.GetEntry().GetHeader().GetId()

	if g.raft.State() == raft.Leader {
		g.logger.Info("redirecting mis-routed forwarded write", "id", id, "leader", g.raft.Leader())
		return &raftproto.ForwardResponse{
			Status:     raftproto.ForwardResponse_STATUS_NOT_LEADER,
			LeaderAddr: string(g.raft.Leader()),
		}
	}

	// Acquire the leader's write lock (a loan the apply materializes under),
	// with a deadline -> BUSY on timeout. Held across the raft round trip.
	lockCtx, cancel := context.WithTimeout(ctx, g.handlerLockTimeout)
	defer cancel()

	release, err := g.fsm.BeginHeldApply(lockCtx, id)
	if err != nil {
		return &raftproto.ForwardResponse{Status: raftproto.ForwardResponse_STATUS_BUSY, Detail: "failed to acquire write lock: " + err.Error()}
	}
	defer release()

	// Base check under the held lock. Nothing has been proposed yet.
	if la := g.fsm.LastAppliedIndex(); req.GetHeader().GetLastAppliedIndex() != la {
		g.logger.Info("rejecting forwarded write: stale base",
			"id", id, "baseIndex", req.GetHeader().GetLastAppliedIndex(), "lastApplied", la)
		return &raftproto.ForwardResponse{Status: raftproto.ForwardResponse_STATUS_STALE_BASE, LastAppliedIndex: la}
	}

	g.logger.Debug("forwarded write base-index check passed", "id", id, "baseIndex", req.GetHeader().GetLastAppliedIndex())

	if err := g.baseGate.ProposeEntry(req.GetEntry()); err != nil {
		g.logger.Info("forwarded write proposal failed", "id", id, "error", err)
		return g.classifyApplyErr(err)
	}

	g.logger.Info("accepted forwarded write", "id", id, "lastApplied", g.fsm.LastAppliedIndex())
	return &raftproto.ForwardResponse{Status: raftproto.ForwardResponse_STATUS_OK, LastAppliedIndex: g.fsm.LastAppliedIndex()}
}

func (g *Gate) validateRequest(req *raftproto.ForwardRequest) error {
	txn := req.GetEntry().GetTransaction()
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

func (g *Gate) classifyApplyErr(err error) *raftproto.ForwardResponse {
	var notLeader *rafterrors.NotLeaderError
	if errors.As(err, &notLeader) {
		return &raftproto.ForwardResponse{
			Status:     raftproto.ForwardResponse_STATUS_NOT_LEADER,
			LeaderAddr: string(notLeader.Leader),
			Detail:     err.Error(),
		}
	}
	var catchingUp *rafterrors.CatchingUpError
	if errors.As(err, &catchingUp) {
		return &raftproto.ForwardResponse{Status: raftproto.ForwardResponse_STATUS_CATCHING_UP, Detail: err.Error()}
	}
	var notApplied *rafterrors.NotAppliedError
	if errors.As(err, &notApplied) {
		return &raftproto.ForwardResponse{Status: raftproto.ForwardResponse_STATUS_BUSY, Detail: err.Error()}
	}
	return &raftproto.ForwardResponse{Status: raftproto.ForwardResponse_STATUS_AMBIGUOUS, Detail: err.Error()}
}
