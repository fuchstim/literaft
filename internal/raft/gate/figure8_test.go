package raftgate_test

import (
	"time"

	"github.com/hashicorp/raft"

	"github.com/fuchstim/literaft/internal/testutils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// hraft's Figure-8 rule can retroactively commit an entry a node proposed
// during an earlier, unfinished leadership stint, once that same node
// regains leadership and a later entry in its new term commits and covers
// it. FSM.Apply's self-skip (entry.NodeID == f.NodeID()) is a static,
// permanent property of the entry, not scoped to the one proposal that
// originally published it -- so a node's own stale entry, retroactively
// committed after it regains leadership, gets skipped forever, silently
// and permanently dropping it from this node's own local disk even though
// the rest of the cluster considers it committed.
//
// This is a known, tracked regression: the test below reliably
// demonstrates it, which is why it's committed as PIt (pending) rather
// than a normal It -- flip it back to It once the self-skip is made
// transient (scoped to the specific in-flight proposal) instead of
// permanent, and this should pass.
var _ = PDescribe("Figure-8 self-apply safety", func() {
	It("materializes a node's own stale entry from an earlier leadership stint during its own later drain, exactly once", func() {
		c := newGatedCluster(GinkgoT(), 3, time.Second)
		defer c.Shutdown()

		// leader is whichever of the three wins the initial election --
		// doesn't matter which, the test isolates it with its own stale
		// entry regardless. helper is any one of the other two, reconnected
		// alone with leader below so leader deterministically regains
		// leadership without racing an election against the third.
		leader, leaderGate := c.ReadyLeader(GinkgoT())
		var helper, thirdWheel *testutils.Node
		for _, n := range c.Nodes() {
			if n == leader {
				continue
			}
			if helper == nil {
				helper = n
			} else {
				thirdWheel = n
			}
		}

		pageSize := leader.FSM.PageSize()
		stale := captureEntries(pageSize, "CREATE TABLE stale (id INTEGER PRIMARY KEY)")[0]
		fresh := captureEntries(pageSize, "CREATE TABLE fresh (id INTEGER PRIMARY KEY)")[0]

		// Fully isolate all three from each other: whatever leader appends
		// to its own log next must survive only there (with no quorum
		// reachable anywhere, nobody can win an election and overwrite it),
		// while leader itself steps down once it can no longer contact a
		// quorum.
		for _, n := range c.Nodes() {
			n.Transport.DisconnectAll()
		}

		proposeErr := make(chan error, 1)
		go func() { proposeErr <- leaderGate.Propose(stale.frames, stale.nTruncate) }()

		testutils.Eventually(GinkgoT(), 5*time.Second, 10*time.Millisecond, func() bool {
			return leader.Raft.State() == raft.Follower
		}, "leader to step down once it can't reach a quorum")
		Eventually(proposeErr, 5*time.Second, 10*time.Millisecond).Should(Receive(HaveOccurred()))

		// With all three mutually unreachable, no quorum exists anywhere, so
		// no election can complete and nobody's log (including leader's
		// stale entry) can be overwritten by a rival leader.
		testutils.Consistently(GinkgoT(), 300*time.Millisecond, 20*time.Millisecond, func() bool {
			return helper.Raft.State() != raft.Leader && thirdWheel.Raft.State() != raft.Leader
		}, "no rival leader must emerge while all three are isolated")

		// Reconnect only leader and helper, leaving thirdWheel isolated a
		// while longer: otherwise helper and thirdWheel's plain majority
		// could elect one of themselves without ever needing leader's vote
		// (safe in general, since stale was never committed, but would make
		// this test flaky/non-deterministic about which node ends up
		// leading again -- with only leader and helper able to talk, helper
		// (whose log lacks stale) can never win a vote from leader, so
		// leader is the only one that can possibly regain leadership here).
		leader.Transport.Connect(helper.Addr, helper.Transport)
		helper.Transport.Connect(leader.Addr, leader.Transport)
		testutils.Eventually(GinkgoT(), 5*time.Second, 10*time.Millisecond, func() bool {
			return leader.Raft.State() == raft.Leader && leaderGate.Ready()
		}, "leader to regain leadership with only helper reachable")

		// Now bring thirdWheel back in to catch up normally; leader is
		// already the established leader at this point, so this is just
		// replication, no further election dynamics.
		leader.Transport.Connect(thirdWheel.Addr, thirdWheel.Transport)
		thirdWheel.Transport.Connect(leader.Addr, leader.Transport)
		helper.Transport.Connect(thirdWheel.Addr, thirdWheel.Transport)
		thirdWheel.Transport.Connect(helper.Addr, helper.Transport)

		// The Figure-8 case itself: stale was never committed during
		// leader's first term, but is still in leader's log, so this new
		// term's Barrier (Gate.drain) commits and covers it -- and it must
		// be materialized through an ordinary FSM.Apply on leader itself,
		// not lost, not double-applied.
		testutils.Eventually(GinkgoT(), 5*time.Second, 10*time.Millisecond, func() bool {
			_, okHelper := tryNodeQueryInt(helper, "SELECT count(*) FROM stale")
			_, okThird := tryNodeQueryInt(thirdWheel, "SELECT count(*) FROM stale")
			return okHelper && okThird
		}, "stale to actually commit and materialize cluster-wide (sanity check, not the regression itself)")

		testutils.Eventually(GinkgoT(), 5*time.Second, 10*time.Millisecond, func() bool {
			_, ok := tryNodeQueryInt(leader, "SELECT count(*) FROM stale")
			return ok
		}, "leader to materialize its own stale entry during its own later drain")

		// A fresh self-proposal after the drain must still be materialized
		// elsewhere, not by leader itself -- proving the drain didn't leave
		// the self-apply check confused about which entry it applies to.
		Expect(leaderGate.Propose(fresh.frames, fresh.nTruncate)).To(Succeed())
		testutils.Consistently(GinkgoT(), 200*time.Millisecond, 10*time.Millisecond, func() bool {
			_, ok := tryNodeQueryInt(leader, "SELECT count(*) FROM fresh")
			return !ok
		}, "leader must not materialize its own fresh entry")
		testutils.Eventually(GinkgoT(), 5*time.Second, 10*time.Millisecond, func() bool {
			_, okHelper := tryNodeQueryInt(helper, "SELECT count(*) FROM fresh")
			_, okThird := tryNodeQueryInt(thirdWheel, "SELECT count(*) FROM fresh")
			return okHelper && okThird
		}, "both other nodes to materialize the fresh entry")
	})
})
