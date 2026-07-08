package raft_test

import (
	"time"

	hraft "github.com/hashicorp/raft"

	raftadapter "github.com/fuchstim/literaft/raft"
	"github.com/fuchstim/literaft/vfs"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// docs/DECISIONS.md ADR-010 / raft/fsm.go's longest doc comment: hraft's
// Figure-8 rule can retroactively commit an entry a node proposed during an
// earlier, unfinished leadership stint once that SAME node regains
// leadership and a subsequent entry in its new term commits and covers it.
// Gate.drain (M5) exists specifically to make that safe: the stale entry
// necessarily has a lower log index than the new term's Barrier, so it's
// applied *during* the drain, while the gate is still closed and no new
// self-proposal can be racing to (mis)claim the self-apply marker. Neither
// gate_test.go's "surfaces a lost-leadership proposal as an error" test
// (stops at the error, never revives the node) nor leadership_test.go's
// drain test (a different node draining another's backlog) exercises this
// end to end -- this test constructs it directly.
var _ = Describe("Figure-8 self-apply safety", func() {
	It("materializes a node's own stale entry from an earlier leadership stint during its own later drain, exactly once", func() {
		aID, bID, cID := hraft.ServerID("a"), hraft.ServerID("b"), hraft.ServerID("c")
		aAddr, aTrans := hraft.NewInmemTransportWithTimeout("", 50*time.Millisecond)
		bAddr, bTrans := hraft.NewInmemTransportWithTimeout("", 50*time.Millisecond)
		cAddr, cTrans := hraft.NewInmemTransportWithTimeout("", 50*time.Millisecond)

		connectAll := func() {
			aTrans.Connect(bAddr, bTrans)
			bTrans.Connect(aAddr, aTrans)
			aTrans.Connect(cAddr, cTrans)
			cTrans.Connect(aAddr, aTrans)
			bTrans.Connect(cAddr, cTrans)
			cTrans.Connect(bAddr, bTrans)
		}
		disconnectAll := func() {
			aTrans.DisconnectAll()
			bTrans.DisconnectAll()
			cTrans.DisconnectAll()
		}
		connectAll()

		// A gets a much shorter election timeout than B/C so that, once the
		// full partition below heals, A is the one that calls the next
		// election first -- otherwise which of the three regains leadership
		// would be non-deterministic, and the whole point of this test is
		// that it must specifically be A, with its own stale entry still in
		// its own log.
		aCfg := fastConfig(aID)
		aCfg.HeartbeatTimeout = 60 * time.Millisecond
		aCfg.ElectionTimeout = 60 * time.Millisecond
		aCfg.LeaderLeaseTimeout = 30 * time.Millisecond

		aSpy := &spyMaterializer{}
		aFSM := raftadapter.NewFSM(aSpy)
		aRaft, err := hraft.NewRaft(aCfg, aFSM,
			hraft.NewInmemStore(), hraft.NewInmemStore(), hraft.NewInmemSnapshotStore(), aTrans)
		Expect(err).NotTo(HaveOccurred())
		aGate := raftadapter.NewGate(aRaft, aFSM, time.Second)
		defer aGate.Close()

		bSpy := &spyMaterializer{}
		bFSM := raftadapter.NewFSM(bSpy)
		bRaft, err := hraft.NewRaft(fastConfig(bID), bFSM,
			hraft.NewInmemStore(), hraft.NewInmemStore(), hraft.NewInmemSnapshotStore(), bTrans)
		Expect(err).NotTo(HaveOccurred())
		bGate := raftadapter.NewGate(bRaft, bFSM, time.Second)
		defer bGate.Close()

		cSpy := &spyMaterializer{}
		cFSM := raftadapter.NewFSM(cSpy)
		cRaft, err := hraft.NewRaft(fastConfig(cID), cFSM,
			hraft.NewInmemStore(), hraft.NewInmemStore(), hraft.NewInmemSnapshotStore(), cTrans)
		Expect(err).NotTo(HaveOccurred())
		cGate := raftadapter.NewGate(cRaft, cFSM, time.Second)
		defer cGate.Close()

		Expect(aRaft.BootstrapCluster(hraft.Configuration{Servers: []hraft.Server{
			{Suffrage: hraft.Voter, ID: aID, Address: aAddr},
		}}).Error()).To(Succeed())
		Eventually(func() bool { return aRaft.State() == hraft.Leader && aGate.Ready() },
			5*time.Second, 10*time.Millisecond).Should(BeTrue())
		Expect(aRaft.AddVoter(bID, bAddr, 0, 5*time.Second).Error()).To(Succeed())
		Expect(aRaft.AddVoter(cID, cAddr, 0, 5*time.Second).Error()).To(Succeed())

		// Fully isolate all three from each other: whatever A appends to its
		// own log next must survive only there (with no quorum reachable
		// anywhere, nobody can win an election and overwrite it), while A
		// itself loses leadership once it can no longer contact a quorum
		// (checkLeaderLease's step-down, vendor/.../raft.go).
		disconnectAll()

		stale := vfs.Entry{Frames: []vfs.Frame{{Pgno: 1, Page: []byte("stale")}}, NTruncate: 1}
		proposeErr := make(chan error, 1)
		go func() { proposeErr <- aGate.Propose(stale) }()

		Eventually(func() hraft.RaftState { return aRaft.State() },
			5*time.Second, 10*time.Millisecond).Should(Equal(hraft.Follower))
		Eventually(proposeErr, 5*time.Second, 10*time.Millisecond).Should(Receive(HaveOccurred()))

		// With all three mutually unreachable, no quorum exists anywhere, so
		// no election can complete and nobody's log (including A's stale
		// entry) can be overwritten by a rival leader.
		Consistently(func() hraft.RaftState { return bRaft.State() },
			300*time.Millisecond, 20*time.Millisecond).ShouldNot(Equal(hraft.Leader))
		Consistently(func() hraft.RaftState { return cRaft.State() },
			300*time.Millisecond, 20*time.Millisecond).ShouldNot(Equal(hraft.Leader))

		// Reconnect only A and B, leaving C isolated a while longer. This is
		// deliberate, not just a speed trick: with all three reconnected at
		// once, RAFT's plain majority rule would let B and C elect one of
		// themselves leader using only each other's votes, without ever
		// needing A's -- which is actually safe in general (stale was never
		// committed, so Leader Completeness doesn't obligate the next leader
		// to have it) but would make the test flaky, sometimes electing B or
		// C instead of exercising the Figure-8 case at all (observed
		// directly: ~35% of runs). With only A and B able to talk, the
		// 3-member configuration's quorum of 2 can only be reached by A+B
		// together, and B -- whose log lacks stale -- can never win a vote
		// from A, so A is the only one that can possibly become leader here.
		aTrans.Connect(bAddr, bTrans)
		bTrans.Connect(aAddr, aTrans)
		Eventually(func() bool { return aRaft.State() == hraft.Leader && aGate.Ready() },
			5*time.Second, 10*time.Millisecond).Should(BeTrue())

		// Now bring C back in to catch up normally; A is already the
		// established leader at this point, so this is just replication, no
		// further election dynamics.
		aTrans.Connect(cAddr, cTrans)
		cTrans.Connect(aAddr, aTrans)
		bTrans.Connect(cAddr, cTrans)
		cTrans.Connect(bAddr, bTrans)

		// The Figure-8 case itself: stale was never committed during A's
		// first term, but is still in A's log, so this new term's Barrier
		// (Gate.drain) commits and covers it -- and it must be materialized
		// through an ordinary FSM.Apply (it's not self-pending by the time
		// this runs: Propose's own deferred endSelfApply already cleared
		// that marker back when it returned the ambiguous-commit error
		// above), not lost, not double-applied.
		Eventually(aSpy.snapshot, 5*time.Second, 10*time.Millisecond).Should(ConsistOf(stale))
		Consistently(aSpy.snapshot, 200*time.Millisecond, 10*time.Millisecond).Should(ConsistOf(stale))

		// A fresh self-proposal after the drain must still follow ADR-005
		// (materialized elsewhere, not by A itself) -- proving the drain
		// didn't leave the self-apply marker attached to the wrong entry.
		fresh := vfs.Entry{Frames: []vfs.Frame{{Pgno: 2, Page: []byte("fresh")}}, NTruncate: 2}
		Expect(aGate.Propose(fresh)).To(Succeed())
		Consistently(aSpy.snapshot, 200*time.Millisecond, 10*time.Millisecond).Should(ConsistOf(stale))
		Eventually(bSpy.snapshot, 5*time.Second, 10*time.Millisecond).Should(ConsistOf(stale, fresh))
		Eventually(cSpy.snapshot, 5*time.Second, 10*time.Millisecond).Should(ConsistOf(stale, fresh))
	})
})
