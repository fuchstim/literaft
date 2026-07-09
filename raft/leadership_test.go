package raft_test

import (
	"errors"
	"time"

	hraft "github.com/hashicorp/raft"

	raftadapter "github.com/fuchstim/literaft/raft"
	"github.com/fuchstim/literaft/vfs"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// blockableMaterializer wraps a spyMaterializer with a gate that Apply
// blocks on until release is called, standing in for a follower whose own
// FSM.Apply is slow. It exists so the test below can force a real,
// deterministic apply backlog -- durably replicated but not yet
// materialized -- without racing hraft's actual wall-clock commit timing.
type blockableMaterializer struct {
	spy   *spyMaterializer
	block chan struct{}
}

func newBlockableMaterializer() *blockableMaterializer {
	return &blockableMaterializer{spy: &spyMaterializer{}, block: make(chan struct{})}
}

func (m *blockableMaterializer) Apply(e vfs.Entry) error {
	<-m.block
	return m.spy.Apply(e)
}

func (m *blockableMaterializer) release() { close(m.block) }

var _ = Describe("Gate gaining-leadership drain", func() {
	It("closes the gate until a newly elected leader drains its apply backlog, and applies the backlog exactly once", func() {
		// Two real hraft nodes over InmemTransport. A is bootstrapped alone
		// so it's deterministically the first leader. B's materializer is
		// blockable, letting the test force a real, unapplied apply backlog
		// on B while A is still leader (entries commit via A+B's log
		// storage, which is independent of B's FSM-apply queue) -- the
		// "apply-behind but log-current" case docs/DESIGN.md §conflicts
		// describes. A then explicitly hands leadership to B via
		// LeadershipTransferToServer: this sidesteps hraft's
		// leader-stickiness vote-rejection heuristic (a follower refuses to
		// vote for a challenger while it still believes a legitimate leader
		// is live), which makes forcing a specific winner via a plain
		// election-timeout race non-deterministic in a real cluster.
		aID, bID := hraft.ServerID("a"), hraft.ServerID("b")
		aAddr, aTrans := hraft.NewInmemTransportWithTimeout("", 50*time.Millisecond)
		bAddr, bTrans := hraft.NewInmemTransportWithTimeout("", 50*time.Millisecond)
		aTrans.Connect(bAddr, bTrans)
		bTrans.Connect(aAddr, aTrans)

		aSpy := &spyMaterializer{}
		aFSM := raftadapter.NewFSM(aSpy)
		aRaft, err := hraft.NewRaft(fastConfig(aID), aFSM,
			hraft.NewInmemStore(), hraft.NewInmemStore(), hraft.NewInmemSnapshotStore(), aTrans)
		Expect(err).NotTo(HaveOccurred())
		aGate := raftadapter.NewGate(aRaft, aFSM, time.Second)
		defer aGate.Close()

		bMat := newBlockableMaterializer()
		bFSM := raftadapter.NewFSM(bMat)
		bRaft, err := hraft.NewRaft(fastConfig(bID), bFSM,
			hraft.NewInmemStore(), hraft.NewInmemStore(), hraft.NewInmemSnapshotStore(), bTrans)
		Expect(err).NotTo(HaveOccurred())
		bGate := raftadapter.NewGate(bRaft, bFSM, time.Second)
		defer bGate.Close()

		Expect(aRaft.BootstrapCluster(hraft.Configuration{Servers: []hraft.Server{
			{Suffrage: hraft.Voter, ID: aID, Address: aAddr},
		}}).Error()).To(Succeed())
		Eventually(func() bool { return aRaft.State() == hraft.Leader && aGate.Ready() },
			5*time.Second, 10*time.Millisecond).Should(BeTrue())

		Expect(aRaft.AddVoter(bID, bAddr, 0, 5*time.Second).Error()).To(Succeed())

		// B is caught up so far (nothing proposed yet). Engage its block,
		// then propose 3 entries through A: committing them only needs a
		// majority of 2 (A + B), and B's AppendEntries handler stores each
		// frame to its own log/log-store immediately regardless of whether
		// its FSM-apply queue is stuck -- storage and FSM application are
		// decoupled in hraft. So these commit even though B's FSM never
		// gets to run for them yet.
		entries := []vfs.Entry{
			{Frames: []vfs.Frame{{Pgno: 1, Page: []byte("one")}}, NTruncate: 1},
			{Frames: []vfs.Frame{{Pgno: 2, Page: []byte("two")}}, NTruncate: 2},
			{Frames: []vfs.Frame{{Pgno: 3, Page: []byte("three")}}, NTruncate: 3},
		}
		for _, e := range entries {
			Expect(aGate.Propose(e)).To(Succeed())
		}
		Consistently(bMat.spy.snapshot, 200*time.Millisecond, 10*time.Millisecond).Should(BeEmpty())

		// Hand leadership to B. hraft only transfers once it believes the
		// target's log is caught up (true here: B has stored, but not yet
		// applied, all 3 entries), so this succeeds despite B's backlog.
		Expect(aRaft.LeadershipTransferToServer(bID, bAddr).Error()).To(Succeed())
		Eventually(func() hraft.RaftState { return bRaft.State() },
			5*time.Second, 10*time.Millisecond).Should(Equal(hraft.Leader))

		// B is the raft leader but must not be Ready yet -- its backlog is
		// still blocked -- and must reject a new local write accordingly.
		Consistently(bGate.Ready, 300*time.Millisecond, 20*time.Millisecond).Should(BeFalse())
		proposeErr := bGate.Propose(vfs.Entry{Frames: []vfs.Frame{{Pgno: 4, Page: []byte("premature")}}, NTruncate: 4})
		var catchingUp raftadapter.CatchingUpError
		Expect(errors.As(proposeErr, &catchingUp)).To(BeTrue(), "got %v (%T), not a CatchingUpError", proposeErr, proposeErr)

		// Release the backlog: the drain's Barrier can now complete, B
		// applies exactly the 3 backlog entries (no loss, no duplication),
		// and Ready flips true.
		bMat.release()
		Eventually(bMat.spy.snapshot, 5*time.Second, 10*time.Millisecond).Should(ConsistOf(entries))
		Eventually(bGate.Ready, 5*time.Second, 10*time.Millisecond).Should(BeTrue())

		// A fresh write through the new leader must still follow ADR-005
		// (materialized elsewhere, not by B itself). This is the Figure-8
		// race the drain exists to prevent: without it, the self-apply
		// marker could misfire against the just-drained backlog instead of
		// this new entry, either losing the backlog or double-materializing
		// this write.
		fresh := vfs.Entry{Frames: []vfs.Frame{{Pgno: 5, Page: []byte("fresh")}}, NTruncate: 5}
		Expect(bGate.Propose(fresh)).To(Succeed())
		Consistently(bMat.spy.snapshot, 200*time.Millisecond, 10*time.Millisecond).Should(ConsistOf(entries))
		Eventually(aSpy.snapshot, 5*time.Second, 10*time.Millisecond).Should(ConsistOf(fresh))
	})
})
