package raft_test

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	hraft "github.com/hashicorp/raft"

	raftadapter "github.com/fuchstim/literaft/raft"
	"github.com/fuchstim/literaft/vfs"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// spyMaterializer records every entry passed to Apply, standing in for
// apply.Applier so these tests don't need real WAL/shm files (raft.FSM
// depends only on the raftadapter.Materializer interface). failWith, if set,
// makes Apply fail instead of recording -- for exercising FSM.Apply's
// materialization-failure path directly (see fsm_test.go).
type spyMaterializer struct {
	mu       sync.Mutex
	entries  []vfs.Entry
	failWith error
}

func (m *spyMaterializer) Apply(e vfs.Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failWith != nil {
		return m.failWith
	}
	m.entries = append(m.entries, e)
	return nil
}

func (m *spyMaterializer) snapshot() []vfs.Entry {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]vfs.Entry(nil), m.entries...)
}

// testNode bundles one in-memory hraft.Raft node with the FSM/Gate this
// repo wires it to.
type testNode struct {
	id    hraft.ServerID
	addr  hraft.ServerAddress
	trans *hraft.InmemTransport
	raft  *hraft.Raft
	fsm   *raftadapter.FSM
	spy   *spyMaterializer
	gate  *raftadapter.Gate
}

// fastConfig shrinks hraft's election/heartbeat timing so tests don't spend
// real wall-clock seconds waiting for a leader, while staying loose enough
// (vs. e.g. 50ms) that scheduling jitter under test load doesn't itself
// trigger spurious re-elections. Logs go to io.Discard: they're noisy and
// the extra I/O only adds to that jitter.
func fastConfig(id hraft.ServerID) *hraft.Config {
	cfg := hraft.DefaultConfig()
	cfg.LocalID = id
	cfg.HeartbeatTimeout = 150 * time.Millisecond
	cfg.ElectionTimeout = 150 * time.Millisecond
	cfg.LeaderLeaseTimeout = 75 * time.Millisecond
	cfg.CommitTimeout = 10 * time.Millisecond
	cfg.LogOutput = io.Discard
	return cfg
}

// newCluster wires n hraft nodes together over InmemTransport, each with
// its own spy-backed FSM/Gate, and bootstraps them as a single cluster.
// Callers must shutdown() the result.
func newCluster(n int) []*testNode {
	GinkgoHelper()
	nodes := make([]*testNode, n)
	for i := range nodes {
		id := hraft.ServerID(fmt.Sprintf("node%d", i))
		// A short per-RPC timeout (vs. the 500ms default) so a killed peer
		// is detected quickly and deterministically in tests.
		addr, trans := hraft.NewInmemTransportWithTimeout("", 50*time.Millisecond)
		spy := &spyMaterializer{}
		nodes[i] = &testNode{id: id, addr: addr, trans: trans, fsm: raftadapter.NewFSM(spy), spy: spy}
	}
	for _, a := range nodes {
		for _, b := range nodes {
			if a != b {
				a.trans.Connect(b.addr, b.trans)
			}
		}
	}

	servers := make([]hraft.Server, len(nodes))
	for i, n := range nodes {
		servers[i] = hraft.Server{Suffrage: hraft.Voter, ID: n.id, Address: n.addr}
	}

	for _, n := range nodes {
		r, err := hraft.NewRaft(fastConfig(n.id), n.fsm,
			hraft.NewInmemStore(), hraft.NewInmemStore(), hraft.NewInmemSnapshotStore(), n.trans)
		Expect(err).NotTo(HaveOccurred())
		n.raft = r
		n.gate = raftadapter.NewGate(r, n.fsm, time.Second)
	}
	Expect(nodes[0].raft.BootstrapCluster(hraft.Configuration{Servers: servers}).Error()).To(Succeed())

	return nodes
}

func shutdownCluster(nodes []*testNode) {
	for _, n := range nodes {
		_ = n.raft.Shutdown().Error()
	}
}

// waitForLeader waits for a leader that has also finished its M5
// gaining-leadership drain (Gate.Ready), i.e. one that's actually ready to
// accept a proposal -- not just one hraft currently reports as State() ==
// Leader, which can be true for a moment before the drain completes.
func waitForLeader(nodes []*testNode) *testNode {
	GinkgoHelper()
	var leader *testNode
	Eventually(func() bool {
		for _, n := range nodes {
			if n.raft.State() == hraft.Leader && n.gate.Ready() {
				leader = n
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond).Should(BeTrue())
	return leader
}

func otherThan(nodes []*testNode, skip *testNode) *testNode {
	for _, n := range nodes {
		if n != skip {
			return n
		}
	}
	return nil
}

// proposeOnCurrentLeader retries against whichever node currently reports
// itself as leader until a proposal succeeds, tolerating the transient
// re-elections these tests' short (if still jittery under load) timeouts
// can produce between finding a leader and acting on it. It returns the
// node that actually accepted the proposal.
func proposeOnCurrentLeader(nodes []*testNode, entry vfs.Entry) *testNode {
	GinkgoHelper()
	var proposer *testNode
	Eventually(func() error {
		leader := findLeader(nodes)
		if leader == nil {
			return fmt.Errorf("no leader currently")
		}
		if err := leader.gate.Propose(entry); err != nil {
			return err
		}
		proposer = leader
		return nil
	}, 5*time.Second, 20*time.Millisecond).Should(Succeed())
	return proposer
}

func findLeader(nodes []*testNode) *testNode {
	for _, n := range nodes {
		if n.raft.State() == hraft.Leader {
			return n
		}
	}
	return nil
}

var _ = Describe("Gate", func() {
	It("returns a NotLeaderError with a leader hint from a follower", func() {
		nodes := newCluster(2)
		defer shutdownCluster(nodes)
		waitForLeader(nodes)

		var hint hraft.ServerAddress
		Eventually(func() error {
			leader := findLeader(nodes)
			if leader == nil {
				return fmt.Errorf("no leader currently")
			}
			follower := otherThan(nodes, leader)
			err := follower.gate.Propose(vfs.Entry{Frames: []vfs.Frame{{Pgno: 1, Page: []byte("x")}}, NTruncate: 1})
			if err == nil {
				return fmt.Errorf("follower unexpectedly accepted the proposal")
			}
			var notLeader *raftadapter.NotLeaderError
			if !errors.As(err, &notLeader) {
				return fmt.Errorf("got %v (%T), not a NotLeaderError", err, err)
			}
			if notLeader.Leader != leader.addr {
				return fmt.Errorf("leader hint %q doesn't match current leader %q yet", notLeader.Leader, leader.addr)
			}
			hint = notLeader.Leader
			return nil
		}, 5*time.Second, 20*time.Millisecond).Should(Succeed())
		Expect(hint).NotTo(BeEmpty())
	})

	It("commits a leader's proposal without the leader materializing its own entry, while the follower does", func() {
		nodes := newCluster(2)
		defer shutdownCluster(nodes)
		waitForLeader(nodes)

		entry := vfs.Entry{Frames: []vfs.Frame{{Pgno: 1, Page: []byte("hello")}}, NTruncate: 1}
		proposer := proposeOnCurrentLeader(nodes, entry)
		follower := otherThan(nodes, proposer)

		// ADR-005: the leader publishes via its own SQLite write path (out
		// of scope for this unit test), never via the Materializer.
		Consistently(proposer.spy.snapshot, 200*time.Millisecond, 10*time.Millisecond).Should(BeEmpty())
		Eventually(follower.spy.snapshot).Should(ConsistOf(entry))
	})

	It("surfaces a lost-leadership proposal as an error", func() {
		nodes := newCluster(2)
		defer shutdownCluster(nodes)
		leader := waitForLeader(nodes)
		follower := otherThan(nodes, leader)

		// Kill the follower (rather than disconnecting the transport --
		// hraft's leader-side replication can hold a pipeline object with
		// its own direct reference to the peer, obtained before the
		// disconnect, so severing the transport's peer-address table alone
		// doesn't reliably stop in-flight replication) so the leader's
		// in-flight proposal can never reach quorum; it must eventually
		// give up once its leader lease expires (fastConfig's
		// LeaderLeaseTimeout), resolving Propose's blocking call with an
		// error -- the "ambiguous commit" case (CLAUDE.md).
		Expect(follower.raft.Shutdown().Error()).To(Succeed())

		// This test deliberately stops at the error rather than reviving the
		// follower and checking a follow-up proposal: if this same node
		// regained leadership later, hraft's Figure-8 rule could
		// retroactively commit this stale, uncommitted "lost" entry (by
		// covering it with a later current-term commit) -- exactly the
		// apply-backlog-on-a-regained-leader scenario raft.FSM's self-apply
		// flag is documented not to handle yet (docs/ROADMAP.md M5,
		// "gaining leadership" drain). Asserting anything past the error
		// here would be testing a guarantee M4 doesn't make.
		entry := vfs.Entry{Frames: []vfs.Frame{{Pgno: 1, Page: []byte("lost")}}, NTruncate: 1}
		errCh := make(chan error, 1)
		go func() { errCh <- leader.gate.Propose(entry) }()
		Eventually(errCh, 5*time.Second, 10*time.Millisecond).Should(Receive(HaveOccurred()))
	})
})
