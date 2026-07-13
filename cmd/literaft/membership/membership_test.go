package membership_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/fuchstim/literaft/cmd/literaft/membership"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestMembership(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "cmd/literaft/membership Suite")
}

var dialOptions = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

type addVoterCall struct {
	id   raft.ServerID
	addr raft.ServerAddress
}

// fakeRaft is a membership.Raft that records the configuration changes asked
// of it instead of running a real cluster.
type fakeRaft struct {
	mu sync.Mutex

	state      raft.RaftState
	leaderAddr raft.ServerAddress

	addErr    error
	removeErr error

	addVoterCalls []addVoterCall
	removeCalls   []raft.ServerID
}

func (f *fakeRaft) State() raft.RaftState { return f.state }

func (f *fakeRaft) LeaderWithID() (raft.ServerAddress, raft.ServerID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.leaderAddr, ""
}

func (f *fakeRaft) AddVoter(id raft.ServerID, addr raft.ServerAddress, _ uint64, _ time.Duration) raft.IndexFuture {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.addVoterCalls = append(f.addVoterCalls, addVoterCall{id, addr})
	return fakeFuture{err: f.addErr}
}

func (f *fakeRaft) RemoveServer(id raft.ServerID, _ uint64, _ time.Duration) raft.IndexFuture {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removeCalls = append(f.removeCalls, id)
	return fakeFuture{err: f.removeErr}
}

func (f *fakeRaft) adds() []addVoterCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]addVoterCall(nil), f.addVoterCalls...)
}

func (f *fakeRaft) removes() []raft.ServerID {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]raft.ServerID(nil), f.removeCalls...)
}

type fakeFuture struct{ err error }

func (f fakeFuture) Error() error  { return f.err }
func (f fakeFuture) Index() uint64 { return 0 }

// startServer brings up a membership gRPC server backed by fake on a loopback
// port and returns its address plus a stop func.
func startServer(fake *fakeRaft) (string, func()) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	Expect(err).NotTo(HaveOccurred())

	s := grpc.NewServer()
	membership.NewServer(fake, dialOptions).Register(s)
	go func() { _ = s.Serve(lis) }()

	return lis.Addr().String(), s.Stop
}

var _ = Describe("Membership", func() {
	var ctx context.Context
	var cancel context.CancelFunc

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		DeferCleanup(cancel)
	})

	Describe("on the leader", func() {
		It("applies AddVoter directly", func() {
			leader := &fakeRaft{state: raft.Leader}
			addr, stop := startServer(leader)
			defer stop()

			Expect(membership.Join(ctx, addr, "n2", "127.0.0.1:9002", dialOptions)).To(Succeed())
			Expect(leader.adds()).To(Equal([]addVoterCall{{id: "n2", addr: "127.0.0.1:9002"}}))
		})

		It("applies RemoveVoter directly", func() {
			leader := &fakeRaft{state: raft.Leader}
			addr, stop := startServer(leader)
			defer stop()

			Expect(membership.Leave(ctx, addr, "n2", dialOptions)).To(Succeed())
			Expect(leader.removes()).To(Equal([]raft.ServerID{"n2"}))
		})
	})

	Describe("on a follower", func() {
		It("forwards AddVoter to the leader", func() {
			leader := &fakeRaft{state: raft.Leader}
			leaderAddr, stopLeader := startServer(leader)
			defer stopLeader()

			follower := &fakeRaft{state: raft.Follower, leaderAddr: raft.ServerAddress(leaderAddr)}
			followerAddr, stopFollower := startServer(follower)
			defer stopFollower()

			Expect(membership.Join(ctx, followerAddr, "n3", "127.0.0.1:9003", dialOptions)).To(Succeed())
			Expect(leader.adds()).To(Equal([]addVoterCall{{id: "n3", addr: "127.0.0.1:9003"}}))
			Expect(follower.adds()).To(BeEmpty())
		})

		It("forwards RemoveVoter to the leader", func() {
			leader := &fakeRaft{state: raft.Leader}
			leaderAddr, stopLeader := startServer(leader)
			defer stopLeader()

			follower := &fakeRaft{state: raft.Follower, leaderAddr: raft.ServerAddress(leaderAddr)}
			followerAddr, stopFollower := startServer(follower)
			defer stopFollower()

			Expect(membership.Leave(ctx, followerAddr, "n3", dialOptions)).To(Succeed())
			Expect(leader.removes()).To(Equal([]raft.ServerID{"n3"}))
			Expect(follower.removes()).To(BeEmpty())
		})

		It("errors when no leader is ever known", func() {
			follower := &fakeRaft{state: raft.Follower}
			addr, stop := startServer(follower)
			defer stop()

			// A permanently leaderless follower keeps returning Unavailable,
			// which Join retries; bound it tightly so the spec fails fast.
			shortCtx, shortCancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
			defer shortCancel()
			Expect(membership.Join(shortCtx, addr, "n3", "127.0.0.1:9003", dialOptions)).NotTo(Succeed())
		})
	})
})
