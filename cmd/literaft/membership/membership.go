// Package membership is the cluster join/leave control plane: a small gRPC
// service that runs on every node's gRPC server (alongside the raft
// transport) and lets a node be added to or removed from the raft
// configuration by contacting any existing member. A member that isn't the
// leader forwards the call, unchanged, to the current leader.
package membership

import (
	"context"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	membershippb "github.com/fuchstim/literaft/cmd/literaft/membership/proto"
)

// Raft is the subset of *raft.Raft the control plane drives. *raft.Raft
// satisfies it.
type Raft interface {
	State() raft.RaftState
	LeaderWithID() (raft.ServerAddress, raft.ServerID)
	AddVoter(id raft.ServerID, address raft.ServerAddress, prevIndex uint64, timeout time.Duration) raft.IndexFuture
	RemoveServer(id raft.ServerID, prevIndex uint64, timeout time.Duration) raft.IndexFuture
}

var _ Raft = (*raft.Raft)(nil)

// Server implements the Membership gRPC service against a local raft node.
type Server struct {
	membershippb.UnimplementedMembershipServer

	raft        Raft
	dialOptions []grpc.DialOption
	timeout     time.Duration
}

// NewServer returns a Membership server driving r, dialing forwarded calls
// with dialOptions (the same options used for the raft transport, so leader
// forwarding reaches the leader's gRPC address).
func NewServer(r Raft, dialOptions []grpc.DialOption, opts ...Option) *Server {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	return &Server{raft: r, dialOptions: dialOptions, timeout: o.timeout}
}

// Register installs the Membership service on a gRPC server.
func (s *Server) Register(g grpc.ServiceRegistrar) {
	membershippb.RegisterMembershipServer(g, s)
}

// AddVoter adds (req.Id, req.Address) to the cluster as a voter. On the
// leader it applies the configuration change directly; on any other node it
// forwards the request, unchanged, to the current leader.
func (s *Server) AddVoter(ctx context.Context, req *membershippb.AddVoterRequest) (*membershippb.AddVoterResponse, error) {
	if s.raft.State() != raft.Leader {
		client, closeConn, err := s.leaderClient()
		if err != nil {
			return nil, err
		}
		defer closeConn()
		return client.AddVoter(ctx, req)
	}

	if err := s.raft.AddVoter(raft.ServerID(req.GetId()), raft.ServerAddress(req.GetAddress()), 0, s.timeout).Error(); err != nil {
		return nil, status.Errorf(codes.Internal, "add voter %s: %v", req.GetId(), err)
	}
	return &membershippb.AddVoterResponse{}, nil
}

// RemoveVoter removes req.Id from the cluster, forwarding to the leader when
// this node isn't it.
func (s *Server) RemoveVoter(ctx context.Context, req *membershippb.RemoveVoterRequest) (*membershippb.RemoveVoterResponse, error) {
	if s.raft.State() != raft.Leader {
		client, closeConn, err := s.leaderClient()
		if err != nil {
			return nil, err
		}
		defer closeConn()
		return client.RemoveVoter(ctx, req)
	}

	if err := s.raft.RemoveServer(raft.ServerID(req.GetId()), 0, s.timeout).Error(); err != nil {
		return nil, status.Errorf(codes.Internal, "remove voter %s: %v", req.GetId(), err)
	}
	return &membershippb.RemoveVoterResponse{}, nil
}

// leaderClient dials the current leader's Membership service. The returned
// close func releases the connection. It fails with Unavailable if this node
// doesn't yet know a leader.
func (s *Server) leaderClient() (membershippb.MembershipClient, func() error, error) {
	addr, _ := s.raft.LeaderWithID()
	if addr == "" {
		return nil, nil, status.Error(codes.Unavailable, "no known leader to forward to")
	}
	conn, err := grpc.NewClient(string(addr), s.dialOptions...)
	if err != nil {
		return nil, nil, status.Errorf(codes.Unavailable, "dial leader %s: %v", addr, err)
	}
	return membershippb.NewMembershipClient(conn), conn.Close, nil
}
