package testutils

import (
	"context"
	"fmt"
	"sync"

	"github.com/hashicorp/raft"

	"github.com/fuchstim/literaft/log"
)

// InmemForwardHub is an in-process log.LeaderTransport router for cluster
// tests: it dispatches a follower's forwarded Propose straight to the target
// node's registered handler, no network involved. A test cluster runs in one
// process, so this exercises the whole forwarding adapter end to end.
type InmemForwardHub struct {
	mu       sync.RWMutex
	handlers map[raft.ServerAddress]func(ctx context.Context, req []byte) ([]byte, error)
}

func NewInmemForwardHub() *InmemForwardHub {
	return &InmemForwardHub{handlers: make(map[raft.ServerAddress]func(context.Context, []byte) ([]byte, error))}
}

// Transport returns a log.LeaderTransport bound to the node at self: Handle
// registers self's handler in the hub, and Propose dispatches to the target
// address's handler.
func (h *InmemForwardHub) Transport(self raft.ServerAddress) log.LeaderTransport {
	return &inmemForwardTransport{hub: h, self: self}
}

type inmemForwardTransport struct {
	hub  *InmemForwardHub
	self raft.ServerAddress
}

func (t *inmemForwardTransport) Handle(handler func(ctx context.Context, req []byte) ([]byte, error)) {
	t.hub.mu.Lock()
	defer t.hub.mu.Unlock()
	t.hub.handlers[t.self] = handler
}

func (t *inmemForwardTransport) Propose(ctx context.Context, leader raft.ServerAddress, req []byte) ([]byte, error) {
	t.hub.mu.RLock()
	handler := t.hub.handlers[leader]
	t.hub.mu.RUnlock()
	if handler == nil {
		// No handler for the target: nothing was delivered anywhere.
		return nil, &log.NotDeliveredError{Err: fmt.Errorf("no forward handler registered for %s", leader)}
	}
	return handler(ctx, req)
}
