package testutils

import (
	"context"
	"sync"

	"github.com/hashicorp/raft"

	rafterrors "github.com/fuchstim/literaft/raft/errors"
	forwardinggate "github.com/fuchstim/literaft/raft/gate/forwarding"
	raftproto "github.com/fuchstim/literaft/raft/proto"
)

// InmemForwardHub is an in-process forwardinggate.LeaderTransport router for
// cluster tests: it dispatches a follower's forwarded Propose straight to the
// target node's registered handler, no network involved. A test cluster runs
// in one process, so this exercises the whole forwarding gate end to end.
type InmemForwardHub struct {
	mu       sync.RWMutex
	handlers map[raft.ServerAddress]func(ctx context.Context, req *raftproto.ForwardRequest) *raftproto.ForwardResponse
}

func NewInmemForwardHub() *InmemForwardHub {
	return &InmemForwardHub{
		handlers: make(map[raft.ServerAddress]func(context.Context, *raftproto.ForwardRequest) *raftproto.ForwardResponse),
	}
}

// Transport returns a forwardinggate.LeaderTransport bound to the node at
// self: Handle registers self's handler in the hub, and Propose dispatches to
// the target address's handler.
func (h *InmemForwardHub) Transport(self raft.ServerAddress) forwardinggate.LeaderTransport {
	return &inmemForwardTransport{hub: h, self: self}
}

type inmemForwardTransport struct {
	hub  *InmemForwardHub
	self raft.ServerAddress
}

func (t *inmemForwardTransport) Handle(handler func(ctx context.Context, req *raftproto.ForwardRequest) *raftproto.ForwardResponse) {
	t.hub.mu.Lock()
	defer t.hub.mu.Unlock()
	t.hub.handlers[t.self] = handler
}

func (t *inmemForwardTransport) Propose(ctx context.Context, leader raft.ServerAddress, req *raftproto.ForwardRequest) (*raftproto.ForwardResponse, error) {
	t.hub.mu.RLock()
	handler := t.hub.handlers[leader]
	t.hub.mu.RUnlock()
	if handler == nil {
		// No handler for the target: nothing was delivered anywhere, a proven
		// non-delivery (clean retryable rejection).
		return nil, rafterrors.NewNotAppliedError("no forward handler registered for "+string(leader), nil)
	}
	return handler(ctx, req), nil
}
