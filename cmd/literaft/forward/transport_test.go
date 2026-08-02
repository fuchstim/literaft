package forward_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/fuchstim/literaft/cmd/literaft/forward"
	raftproto "github.com/fuchstim/literaft/proto"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestForward(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "forward transport Suite")
}

// startServer brings up a gRPC server hosting t's Forwarding service on a
// fresh loopback address, returning the address and a stop func.
func startServer(tr *forward.Transport) (string, func()) {
	GinkgoHelper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	Expect(err).NotTo(HaveOccurred())
	g := grpc.NewServer()
	tr.Register(g)
	go func() { _ = g.Serve(lis) }()
	return lis.Addr().String(), g.GracefulStop
}

var dialOpts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

func testRequest(id string) *raftproto.LeaderRequest {
	return &raftproto.LeaderRequest{
		Header:  &raftproto.LeaderRequest_Header{LastAppliedIndex: 7},
		Payload: &raftproto.LeaderRequest_LogEntry{LogEntry: &raftproto.LogEntry{Header: &raftproto.LogEntry_Header{Id: id}}},
	}
}

var _ = Describe("forward.Transport", func() {
	It("round-trips a request through the leader's handler", func() {
		// identity resolver: the forward address is the raft address.
		leaderTr := forward.New(func(a raft.ServerAddress) string { return string(a) }, dialOpts)
		var gotReq *raftproto.LeaderRequest
		leaderTr.Handle(func(_ context.Context, req *raftproto.LeaderRequest) *raftproto.LeaderResponse {
			gotReq = req
			return &raftproto.LeaderResponse{Status: raftproto.LeaderResponse_STATUS_OK, LastAppliedIndex: 8}
		})
		addr, stop := startServer(leaderTr)
		defer stop()

		follower := forward.New(func(a raft.ServerAddress) string { return string(a) }, dialOpts)
		defer follower.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req := testRequest("hello")
		resp, err := follower.Propose(ctx, raft.ServerAddress(addr), req)
		Expect(err).NotTo(HaveOccurred())
		Expect(gotReq.GetLogEntry().GetHeader().GetId()).To(Equal("hello"))
		Expect(resp.GetStatus()).To(Equal(raftproto.LeaderResponse_STATUS_OK))
		Expect(resp.GetLastAppliedIndex()).To(Equal(uint64(8)))
	})

	It("surfaces a not-leader response to the caller", func() {
		leaderTr := forward.New(func(a raft.ServerAddress) string { return string(a) }, dialOpts)
		leaderTr.Handle(func(context.Context, *raftproto.LeaderRequest) *raftproto.LeaderResponse {
			return &raftproto.LeaderResponse{Status: raftproto.LeaderResponse_STATUS_NOT_LEADER, LeaderAddr: "elsewhere"}
		})
		addr, stop := startServer(leaderTr)
		defer stop()

		follower := forward.New(func(a raft.ServerAddress) string { return string(a) }, dialOpts)
		defer follower.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp, err := follower.Propose(ctx, raft.ServerAddress(addr), testRequest("x"))
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.GetStatus()).To(Equal(raftproto.LeaderResponse_STATUS_NOT_LEADER))
		Expect(resp.GetLeaderAddr()).To(Equal("elsewhere"))
	})

	It("answers Unavailable before a handler is registered", func() {
		leaderTr := forward.New(func(a raft.ServerAddress) string { return string(a) }, dialOpts)
		addr, stop := startServer(leaderTr)
		defer stop()

		follower := forward.New(func(a raft.ServerAddress) string { return string(a) }, dialOpts)
		defer follower.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := follower.Propose(ctx, raft.ServerAddress(addr), testRequest("x"))
		Expect(err).To(HaveOccurred())
	})
})
