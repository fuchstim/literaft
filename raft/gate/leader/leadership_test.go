package leadergate_test

import (
	"errors"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"

	"github.com/fuchstim/literaft/internal/fsm/walappender/shm"
	"github.com/fuchstim/literaft/internal/testutils"
	"github.com/fuchstim/literaft/internal/wal"
	rafterrors "github.com/fuchstim/literaft/raft/errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("leadergate.Gate gaining-leadership drain", func() {
	It("closes the gate until a newly elected leader drains its apply backlog, and applies the backlog exactly once", func() {
		c := newGatedCluster(GinkgoT(), 2, time.Second)
		defer c.Shutdown()

		oldLeader, oldGate := c.ReadyLeader(GinkgoT())
		newLeader := c.Other(oldLeader)
		newGate := c.Gate(newLeader)

		// Force a real, deterministic apply backlog on newLeader by holding
		// its own -shm's WAL_WRITE_LOCK externally, via a second raw handle
		// on the same path (the same protocol newLeader's own walappender
		// uses). newLeader's FSM.Apply calls AppendFrames for every
		// non-self-originated committed entry, and AppendFrames blocks
		// acquiring this exact lock -- so while it's held externally,
		// newLeader's FSM genuinely cannot apply anything, however fast
		// hraft itself replicates.
		lock, err := shm.Open(newLeader.DBPath+"-shm", hclog.NewNullLogger())
		Expect(err).NotTo(HaveOccurred())
		defer lock.Close()
		Expect(lock.Lock(shm.WriteLock)).To(Succeed())

		txns := captureTransactions(
			"CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
			"INSERT INTO t (id, v) VALUES (1, 'one')",
			"INSERT INTO t (id, v) VALUES (2, 'two')",
		)
		// Captured independently (its own local db, own schema) rather
		// than continuing off txns above: txns never materializes on
		// oldLeader at all (oldLeader authored them, so its own FSM
		// self-skips them; real usage instead publishes them via
		// oldLeader's own SQLite write path, out of scope for this
		// direct-ProposeTransaction test). A "fresh" entry that referenced
		// table t would therefore hit a nonexistent table once
		// materialized on oldLeader; a self-contained CREATE TABLE has no
		// such dependency.
		fresh := captureTransactions("CREATE TABLE fresh (id INTEGER PRIMARY KEY, v TEXT)")[0]

		// Committing only needs a majority of 2 (both nodes): newLeader's
		// AppendEntries handler stores each entry to its own log/log-store
		// immediately regardless of whether its FSM-apply queue is stuck --
		// storage and FSM application are decoupled in hraft. So these
		// commit even though newLeader's FSM never gets to run for them yet.
		for _, txn := range txns {
			Expect(oldGate.ProposeTransaction(txn.frames)).To(Succeed())
		}
		testutils.Consistently(GinkgoT(), 200*time.Millisecond, 10*time.Millisecond, func() bool {
			_, ok := tryNodeQueryInt(newLeader, "SELECT count(*) FROM t")
			return !ok
		}, "newLeader's backlog must stay unapplied while its WAL_WRITE_LOCK is held externally")

		// Hand leadership to newLeader. hraft only transfers once it
		// believes the target's log is caught up (true here: newLeader has
		// stored, but not yet applied, all 3 entries), so this succeeds
		// despite its backlog.
		Expect(oldLeader.Raft.LeadershipTransferToServer(raft.ServerID(newLeader.ID), newLeader.Addr).Error()).To(Succeed())
		testutils.Eventually(GinkgoT(), 5*time.Second, 10*time.Millisecond, func() bool {
			return newLeader.Raft.State() == raft.Leader
		}, "newLeader to become raft leader")

		// newLeader is the raft leader but must not be Ready yet -- its
		// backlog is still blocked -- and must reject a new local write
		// accordingly.
		testutils.Consistently(GinkgoT(), 300*time.Millisecond, 20*time.Millisecond, func() bool {
			return !newGate.Ready()
		}, "newLeader's Gate must stay not-Ready while its backlog is blocked")

		// Rejected by the Ready() check before ever reaching raft.Apply, so
		// the frame content here is never validated -- unlike txns above,
		// it doesn't need to be real, valid page content.
		proposeErr := newGate.ProposeTransaction([]*wal.Frame{frame(1, []byte("premature"))})
		var catchingUp *rafterrors.CatchingUpError
		Expect(errors.As(proposeErr, &catchingUp)).To(BeTrue(), "got %v (%T), not a CatchingUpError", proposeErr, proposeErr)

		// Release the backlog: the drain's Barrier can now complete,
		// newLeader applies exactly the 3 backlog entries (no loss, no
		// duplication), and Ready flips true.
		Expect(lock.Unlock(shm.WriteLock)).To(Succeed())
		testutils.Eventually(GinkgoT(), 5*time.Second, 10*time.Millisecond, func() bool {
			n, ok := tryNodeQueryInt(newLeader, "SELECT count(*) FROM t")
			return ok && n == 2
		}, "newLeader to apply its full backlog exactly once")
		testutils.Eventually(GinkgoT(), 5*time.Second, 10*time.Millisecond, func() bool {
			return newGate.Ready()
		}, "newLeader's Gate to become Ready once the drain completes")

		// A fresh write through the new leader must still be materialized
		// elsewhere, not by newLeader itself. This is the Figure-8 race the
		// drain exists to prevent: without it, the self-apply skip could
		// misfire against the just-drained backlog instead of this new
		// entry, either losing the backlog or double-materializing this
		// write.
		Expect(newGate.ProposeTransaction(fresh.frames)).To(Succeed())

		testutils.Eventually(GinkgoT(), 5*time.Second, 10*time.Millisecond, func() bool {
			_, ok := tryNodeQueryInt(oldLeader, "SELECT count(*) FROM fresh")
			return ok
		}, "the other node to materialize the fresh entry")
		testutils.Consistently(GinkgoT(), 200*time.Millisecond, 10*time.Millisecond, func() bool {
			_, ok := tryNodeQueryInt(newLeader, "SELECT count(*) FROM fresh")
			return !ok
		}, "newLeader must not materialize its own fresh entry")
	})
})
