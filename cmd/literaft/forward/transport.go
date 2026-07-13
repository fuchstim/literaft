// Package forward is a reference log.LeaderTransport: a small gRPC service
// that ships opaque byte blobs from a follower to the leader for write
// forwarding. It runs on every node; only the current leader accepts work.
//
// A caller-supplied resolver maps a leader's raft address to its forward dial
// address. When the raft transport already is gRPC on that address, register
// this service on the same server and use an identity resolver; otherwise run
// a dedicated forward server per node and map addresses to it.
package forward

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	forwardpb "github.com/fuchstim/literaft/cmd/literaft/forward/proto"
	"github.com/fuchstim/literaft/log"
)

var _ log.LeaderTransport = (*Transport)(nil)

// Transport implements log.LeaderTransport over gRPC.
type Transport struct {
	// resolve maps a leader's raft.ServerAddress to the dial address of that
	// node's forward gRPC service. For a node whose raft transport already is
	// gRPC on that address, this is the identity.
	resolve     func(raft.ServerAddress) string
	dialOptions []grpc.DialOption

	handlerMu sync.RWMutex
	handler   func(ctx context.Context, request []byte) ([]byte, error)

	connMu sync.Mutex
	conns  map[string]*grpc.ClientConn
	closed bool
}

// New returns a Transport that reaches a leader's forward service at
// resolve(leaderAddr), dialing with dialOptions (typically the same options
// the raft transport uses).
func New(resolve func(raft.ServerAddress) string, dialOptions []grpc.DialOption) *Transport {
	return &Transport{
		resolve:     resolve,
		dialOptions: dialOptions,
		conns:       make(map[string]*grpc.ClientConn),
	}
}

// Register installs the Forwarding service on a gRPC server. The service
// dispatches to whatever Handle last registered; a request that arrives
// before Handle is set is answered Unavailable.
func (t *Transport) Register(g grpc.ServiceRegistrar) {
	forwardpb.RegisterForwardingServer(g, &server{t: t})
}

// Handle implements log.LeaderTransport.
func (t *Transport) Handle(handler func(ctx context.Context, request []byte) ([]byte, error)) {
	t.handlerMu.Lock()
	t.handler = handler
	t.handlerMu.Unlock()
}

// Propose implements log.LeaderTransport: it dials the leader's forward
// service and issues one Propose RPC, returning the response payload bytes.
func (t *Transport) Propose(ctx context.Context, leader raft.ServerAddress, request []byte) ([]byte, error) {
	conn, err := t.conn(t.resolve(leader))
	if err != nil {
		return nil, err
	}
	resp, err := forwardpb.NewForwardingClient(conn).Propose(ctx, &forwardpb.Envelope{Payload: request})
	if err != nil {
		return nil, err
	}
	return resp.GetPayload(), nil
}

// Close closes all cached client connections.
func (t *Transport) Close() error {
	t.connMu.Lock()
	defer t.connMu.Unlock()
	t.closed = true
	var errs []error
	for _, c := range t.conns {
		if err := c.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	t.conns = nil
	return errors.Join(errs...)
}

// conn returns a cached client connection to addr (gRPC connections are meant
// to be long-lived and reused), creating one on first use.
func (t *Transport) conn(addr string) (*grpc.ClientConn, error) {
	t.connMu.Lock()
	defer t.connMu.Unlock()
	if t.closed {
		return nil, errors.New("forward transport closed")
	}
	if c, ok := t.conns[addr]; ok {
		return c, nil
	}
	c, err := grpc.NewClient(addr, t.dialOptions...)
	if err != nil {
		return nil, fmt.Errorf("dial forward target %s: %w", addr, err)
	}
	t.conns[addr] = c
	return c, nil
}

// server adapts the registered handler to the Forwarding gRPC service.
type server struct {
	forwardpb.UnimplementedForwardingServer
	t *Transport
}

func (s *server) Propose(ctx context.Context, env *forwardpb.Envelope) (*forwardpb.Envelope, error) {
	s.t.handlerMu.RLock()
	handler := s.t.handler
	s.t.handlerMu.RUnlock()
	if handler == nil {
		return nil, status.Error(codes.Unavailable, "forward handler not registered")
	}
	respBytes, err := handler(ctx, env.GetPayload())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "forward handler: %v", err)
	}
	return &forwardpb.Envelope{Payload: respBytes}, nil
}
