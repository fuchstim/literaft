package forward_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/fuchstim/literaft/cmd/literaft/forward"

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

var _ = Describe("forward.Transport", func() {
	It("round-trips a request through the leader's handler", func() {
		// identity resolver: the forward address is the raft address.
		leaderTr := forward.New(func(a raft.ServerAddress) string { return string(a) }, dialOpts)
		var gotReq []byte
		leaderTr.Handle(func(_ context.Context, req []byte) ([]byte, error) {
			gotReq = req
			return append([]byte("resp:"), req...), nil
		})
		addr, stop := startServer(leaderTr)
		defer stop()

		follower := forward.New(func(a raft.ServerAddress) string { return string(a) }, dialOpts)
		defer follower.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp, err := follower.Propose(ctx, raft.ServerAddress(addr), []byte("hello"))
		Expect(err).NotTo(HaveOccurred())
		Expect(gotReq).To(Equal([]byte("hello")))
		Expect(resp).To(Equal([]byte("resp:hello")))
	})

	It("surfaces a handler error to the caller", func() {
		leaderTr := forward.New(func(a raft.ServerAddress) string { return string(a) }, dialOpts)
		leaderTr.Handle(func(context.Context, []byte) ([]byte, error) {
			return nil, errors.New("nope")
		})
		addr, stop := startServer(leaderTr)
		defer stop()

		follower := forward.New(func(a raft.ServerAddress) string { return string(a) }, dialOpts)
		defer follower.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := follower.Propose(ctx, raft.ServerAddress(addr), []byte("x"))
		Expect(err).To(HaveOccurred())
	})

	It("answers Unavailable before a handler is registered", func() {
		leaderTr := forward.New(func(a raft.ServerAddress) string { return string(a) }, dialOpts)
		addr, stop := startServer(leaderTr)
		defer stop()

		follower := forward.New(func(a raft.ServerAddress) string { return string(a) }, dialOpts)
		defer follower.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := follower.Propose(ctx, raft.ServerAddress(addr), []byte("x"))
		Expect(err).To(HaveOccurred())
	})
})
