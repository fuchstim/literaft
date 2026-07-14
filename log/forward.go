package log

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"

	raftgate "github.com/fuchstim/literaft/internal/raft/gate"
	rafterrors "github.com/fuchstim/literaft/internal/raft/gate/errors"
	raftproto "github.com/fuchstim/literaft/internal/raft/proto"
)

// LeaderTransport ships opaque byte blobs between nodes (the blobs are
// proto-encoded forward request/response messages). Implementations own
// listeners, dialing, connection-level retries, auth, and lifecycle. Propose
// returns the leader's response bytes, or an error if the leader can't be
// reached or didn't answer. Handle registers the single callback invoked for
// each inbound request on every node (only the current leader accepts work).
type LeaderTransport interface {
	Propose(ctx context.Context, leader raft.ServerAddress, request []byte) ([]byte, error)
	Handle(handler func(ctx context.Context, request []byte) ([]byte, error))
}

// ForwardTarget is what a ForwardingLog needs from the local FSM.
type ForwardTarget interface {
	// LastApplied is the raft index of the last entry materialized locally.
	LastApplied() uint64
	// PageSize is the fixed cluster-wide page size, for shape validation.
	PageSize() uint32
	// AwaitEntryApplied blocks until id's skip marker is consumed (returns
	// nil), or until ctx expires -- resolving the marker: consumed -> nil,
	// pending -> abandoned + error.
	AwaitEntryApplied(ctx context.Context, id string) error
	// BeginHeldApply acquires this node's WAL write lock and registers a loan
	// so the entry is materialized under it. release is mandatory on all paths.
	BeginHeldApply(ctx context.Context, id string) (release func(), err error)
}

var _ raftgate.LogAdapter = (*ForwardingLog)(nil)

// innerLog is the slice of *SingleWriterLog a ForwardingLog drives, kept as an
// interface so the follower and handler logic can be unit-tested against a
// fake; the exported constructor still takes the concrete type.
type innerLog interface {
	Apply(entry []byte) error
	IsLeader() bool
	LeaderAddr() raft.ServerAddress
}

var _ innerLog = (*SingleWriterLog)(nil)

// ForwardingLog is an alternative raftgate.LogAdapter: on a leader it behaves
// exactly like the inner SingleWriterLog; on a follower it forwards the write
// to the leader under a base-index check instead of rejecting it.
type ForwardingLog struct {
	inner     innerLog
	transport LeaderTransport
	target    ForwardTarget

	forwardTimeout     time.Duration
	handlerLockTimeout time.Duration
}

// NewForwardingLog wraps inner so follower-originated writes are forwarded to
// the leader over transport, with target supplying the local applied index,
// page size, marker resolution, and loaned-apply lock. It registers the
// leader-side handler with transport immediately.
func NewForwardingLog(inner *SingleWriterLog, transport LeaderTransport, target ForwardTarget, opts ...Option) *ForwardingLog {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	f := &ForwardingLog{
		inner:              inner,
		transport:          transport,
		target:             target,
		forwardTimeout:     o.forwardTimeout,
		handlerLockTimeout: o.handlerLockTimeout,
	}
	transport.Handle(f.handle)
	return f
}

// Apply implements raftgate.LogAdapter. It first tries the inner adapter. A
// nil or non-NotLeaderError result is returned as-is (on a leader, unchanged
// behavior). A *NotLeaderError -- returned strictly before anything was
// proposed -- is the forward trigger.
func (f *ForwardingLog) Apply(entry []byte) error {
	err := f.inner.Apply(entry)
	var notLeader *rafterrors.NotLeaderError
	if err == nil || !errors.As(err, &notLeader) {
		return err
	}

	if notLeader.Leader == "" {
		// No known leader to forward to: fall back to the redirect.
		return err
	}

	e := &raftproto.Entry{}
	if uerr := proto.Unmarshal(entry, e); uerr != nil {
		return fmt.Errorf("failed to decode entry for forwarding: %w", uerr)
	}
	id := e.GetHeader().GetId()

	req := &raftproto.ForwardRequest{Entry: entry, BaseIndex: f.target.LastApplied()}
	reqBytes, merr := proto.Marshal(req)
	if merr != nil {
		return fmt.Errorf("failed to marshal forward request: %w", merr)
	}

	ctx, cancel := context.WithTimeout(context.Background(), f.forwardTimeout)
	defer cancel()
	return f.forward(ctx, id, reqBytes, raft.ServerAddress(notLeader.Leader))
}

// forward sends reqBytes to the leader and interprets the response, retrying
// a NOT_LEADER with a fresh hint at most once (safe: nothing was proposed,
// and the base check re-validates at the new leader). It never re-sends the
// same request after a possibly-proposed outcome.
func (f *ForwardingLog) forward(ctx context.Context, id string, reqBytes []byte, leader raft.ServerAddress) error {
	for attempt := 0; ; attempt++ {
		if leader == "" {
			return rafterrors.NewNotLeaderError("")
		}
		respBytes, err := f.transport.Propose(ctx, leader, reqBytes)
		if err != nil {
			// A proven non-delivery (nothing proposed) is a clean retryable
			// rejection: re-running is safe, the base check re-validates. A
			// transport signals it by returning a rafterrors.NotAppliedError,
			// which is already retryable, so surface it as-is. Any other
			// failure could have been delivered and its answer lost, so resolve
			// via the marker CAS -- consumed if it committed, abandoned on
			// timeout otherwise.
			var notApplied *rafterrors.NotAppliedError
			if errors.As(err, &notApplied) {
				return err
			}
			return f.resolve(ctx, id, err)
		}

		resp := &raftproto.ForwardResponse{}
		if err := proto.Unmarshal(respBytes, resp); err != nil {
			return f.resolve(ctx, id, fmt.Errorf("decoding forward response: %w", err))
		}

		switch resp.GetStatus() {
		case raftproto.ForwardResponse_OK:
			// Dual-wait: block until the entry replicates back and is locally
			// consumed, then publish. Consumption proves commitment.
			return f.resolve(ctx, id, nil)
		case raftproto.ForwardResponse_STALE_BASE:
			return rafterrors.NewNotAppliedError(
				fmt.Sprintf("forwarded write lost to a concurrent write (leader applied index %d)", resp.GetLastApplied()), nil)
		case raftproto.ForwardResponse_CATCHING_UP:
			return rafterrors.NewCatchingUpError()
		case raftproto.ForwardResponse_BUSY:
			reason := "leader busy"
			if d := resp.GetDetail(); d != "" {
				reason = "leader busy: " + d
			}
			return rafterrors.NewNotAppliedError(reason, nil)
		case raftproto.ForwardResponse_NOT_LEADER:
			if attempt == 0 && resp.GetLeaderAddr() != "" {
				leader = raft.ServerAddress(resp.GetLeaderAddr())
				continue
			}
			return rafterrors.NewNotLeaderError(resp.GetLeaderAddr())
		case raftproto.ForwardResponse_AMBIGUOUS:
			return f.resolve(ctx, id, errors.New(resp.GetDetail()))
		default:
			return f.resolve(ctx, id, fmt.Errorf("unknown forward status %v", resp.GetStatus()))
		}
	}
}

// resolve completes a forwarded proposal through the marker CAS: it waits for
// local consumption (returning nil -> publish) or for ctx to expire (marker
// abandoned -> a non-retryable ambiguous error wrapping cause, or the await
// error if cause is nil). It never re-proposes.
func (f *ForwardingLog) resolve(ctx context.Context, id string, cause error) error {
	if err := f.target.AwaitEntryApplied(ctx, id); err != nil {
		if cause != nil {
			return rafterrors.NewAmbiguousError(cause)
		}
		return rafterrors.NewAmbiguousError(err)
	}
	return nil
}

// handle decodes an inbound ForwardRequest and encodes the ForwardResponse.
func (f *ForwardingLog) handle(ctx context.Context, reqBytes []byte) ([]byte, error) {
	req := &raftproto.ForwardRequest{}
	if err := proto.Unmarshal(reqBytes, req); err != nil {
		return nil, fmt.Errorf("failed to decode forward request: %w", err)
	}
	return proto.Marshal(f.handleRequest(ctx, req))
}

func (f *ForwardingLog) handleRequest(ctx context.Context, req *raftproto.ForwardRequest) *raftproto.ForwardResponse {
	e := &raftproto.Entry{}
	if err := proto.Unmarshal(req.GetEntry(), e); err != nil {
		return &raftproto.ForwardResponse{Status: raftproto.ForwardResponse_BUSY, Detail: "malformed entry: " + err.Error()}
	}
	if err := f.validateShape(e); err != nil {
		return &raftproto.ForwardResponse{Status: raftproto.ForwardResponse_BUSY, Detail: err.Error()}
	}
	id := e.GetHeader().GetId()

	// Answer a mis-routed request with NOT_LEADER (+ best hint) before
	// touching the write lock, so the follower re-resolves rather than
	// re-running its whole txn on a STALE_BASE.
	if !f.inner.IsLeader() {
		return &raftproto.ForwardResponse{
			Status:     raftproto.ForwardResponse_NOT_LEADER,
			LeaderAddr: string(f.inner.LeaderAddr()),
		}
	}

	// Acquire the leader's write lock (a loan the apply materializes under),
	// with a deadline -> BUSY on timeout. Held across the raft round trip.
	lockCtx, cancel := context.WithTimeout(ctx, f.handlerLockTimeout)
	defer cancel()
	release, err := f.target.BeginHeldApply(lockCtx, id)
	if err != nil {
		return &raftproto.ForwardResponse{Status: raftproto.ForwardResponse_BUSY, Detail: "write lock: " + err.Error()}
	}
	defer release()

	// Base check under the held lock. Nothing has been proposed yet.
	if la := f.target.LastApplied(); req.GetBaseIndex() != la {
		return &raftproto.ForwardResponse{Status: raftproto.ForwardResponse_STALE_BASE, LastApplied: la}
	}

	// Propose through the inner adapter. On nil the entry is committed and --
	// via the loan -- already applied on this leader.
	if err := f.inner.Apply(req.GetEntry()); err != nil {
		return f.classifyApplyErr(err)
	}

	return &raftproto.ForwardResponse{Status: raftproto.ForwardResponse_OK, LastApplied: f.target.LastApplied()}
}

// validateShape checks a forwarded entry's structure -- a whole committed
// transaction of full, non-zero page images at the cluster page size. Not
// content validation: forwarded images are trusted (RAFT is non-Byzantine),
// so the transport's authentication is the real trust boundary.
func (f *ForwardingLog) validateShape(e *raftproto.Entry) error {
	txn := e.GetTransaction()
	if txn == nil {
		return fmt.Errorf("forwarded entry has no transaction payload")
	}
	if txn.GetNTruncate() == 0 {
		return fmt.Errorf("forwarded transaction has nTruncate == 0 (not a whole committed txn)")
	}
	pageSize := int(f.target.PageSize())
	pages := txn.GetPages()
	if len(pages) == 0 {
		return fmt.Errorf("forwarded transaction has no pages")
	}
	for _, p := range pages {
		if p.GetPgno() == 0 {
			return fmt.Errorf("forwarded transaction has a zero page number")
		}
		if len(p.GetData()) != pageSize {
			return fmt.Errorf("forwarded page %d is %d bytes, cluster page size is %d", p.GetPgno(), len(p.GetData()), pageSize)
		}
	}
	return nil
}

// classifyApplyErr maps the inner adapter's Apply error to a ForwardResponse.
// NOT_LEADER/CATCHING_UP/BUSY are pre-propose (the entry did not enter the
// log); AMBIGUOUS means it was proposed but its outcome is unknown.
func (f *ForwardingLog) classifyApplyErr(err error) *raftproto.ForwardResponse {
	var notLeader *rafterrors.NotLeaderError
	if errors.As(err, &notLeader) {
		return &raftproto.ForwardResponse{
			Status:     raftproto.ForwardResponse_NOT_LEADER,
			LeaderAddr: notLeader.Leader,
			Detail:     err.Error(),
		}
	}
	var catchingUp *rafterrors.CatchingUpError
	if errors.As(err, &catchingUp) {
		return &raftproto.ForwardResponse{Status: raftproto.ForwardResponse_CATCHING_UP, Detail: err.Error()}
	}
	var notApplied *rafterrors.NotAppliedError
	if errors.As(err, &notApplied) {
		return &raftproto.ForwardResponse{Status: raftproto.ForwardResponse_BUSY, Detail: err.Error()}
	}
	return &raftproto.ForwardResponse{Status: raftproto.ForwardResponse_AMBIGUOUS, Detail: err.Error()}
}
