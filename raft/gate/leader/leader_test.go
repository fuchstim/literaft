package leadergate_test

import (
	"errors"
	"time"

	"github.com/hashicorp/raft"
	"github.com/ncruces/go-sqlite3"

	"github.com/fuchstim/literaft/internal/testutils"
	"github.com/fuchstim/literaft/internal/wal"
	rafterrors "github.com/fuchstim/literaft/raft/errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func findLeader(c *gatedCluster) *testutils.Node {
	for _, n := range c.Nodes() {
		if n.Raft.State() == raft.Leader {
			return n
		}
	}
	return nil
}

// frame builds a minimal, non-commit wal.Frame carrying pgno/data -- enough
// to drive a rejection that never reaches the FSM (Gate rejects on the
// leader/ready check before the entry's content is ever validated).
func frame(pgno uint32, data []byte) *wal.Frame {
	h := &wal.FrameHeader{}
	h.SetPgNo(pgno)
	return &wal.Frame{Header: h, Data: data}
}

var _ = Describe("leadergate.Gate", func() {
	It("returns a NotLeaderError with a leader hint from a follower", func() {
		c := newGatedCluster(GinkgoT(), 2, time.Second)
		defer c.Shutdown()
		c.ReadyLeader(GinkgoT())

		var hint raft.ServerAddress
		testutils.Eventually(GinkgoT(), 5*time.Second, 20*time.Millisecond, func() bool {
			leader := findLeader(c)
			if leader == nil {
				return false
			}
			follower := c.Other(leader)
			err := c.Gate(follower).ProposeTransaction([]*wal.Frame{frame(1, []byte("x"))})
			if err == nil {
				return false
			}
			var notLeader *rafterrors.NotLeaderError
			if !errors.As(err, &notLeader) || notLeader.Leader != leader.Addr {
				return false
			}
			hint = notLeader.Leader
			return true
		}, "a NotLeaderError carrying the current leader's address")
		Expect(hint).NotTo(BeEmpty())
	})

	It("commits a leader's proposal without the leader materializing its own entry, while the follower does", func() {
		c := newGatedCluster(GinkgoT(), 2, time.Second)
		defer c.Shutdown()
		leader, gate := c.ReadyLeader(GinkgoT())
		follower := c.Other(leader)

		txns := captureTransactions(
			"CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
			"INSERT INTO t (id, v) VALUES (1, 'hello')",
		)
		for _, txn := range txns {
			Expect(gate.ProposeTransaction(txn.frames)).To(Succeed())
		}

		// The leader publishes via its own SQLite write path in real usage
		// (out of scope for this direct-ProposeTransaction unit test);
		// either way, its own fsm.FSM must never materialize its own entry
		// via AppendFrames -- there is nothing at all on the leader's disk
		// for this table, since this test never opened a real gated
		// connection on the leader to publish it any other way.
		leaderConn, err := sqlite3.Open("file:" + leader.DBPath)
		Expect(err).NotTo(HaveOccurred())
		defer leaderConn.Close()
		_, _, err = leaderConn.Prepare("SELECT count(*) FROM t")
		Expect(err).To(HaveOccurred(), "the leader must not have materialized its own entry at all")

		testutils.Eventually(GinkgoT(), 5*time.Second, 20*time.Millisecond, func() bool {
			// Not nodeQueryInt: the table may not exist yet if the
			// CREATE TABLE entry hasn't materialized on the follower yet,
			// and that must retry, not fail the spec outright.
			n, ok := tryNodeQueryInt(follower, "SELECT count(*) FROM t")
			return ok && n == 1
		}, "the follower to materialize the leader's proposal")
		Expect(nodeQueryText(follower, "SELECT v FROM t WHERE id = 1")).To(Equal("hello"))
	})

	It("surfaces a lost-leadership proposal as an error", func() {
		c := newGatedCluster(GinkgoT(), 2, time.Second)
		defer c.Shutdown()
		leader, gate := c.ReadyLeader(GinkgoT())
		follower := c.Other(leader)

		// Kill the follower so the leader's in-flight proposal can never
		// reach quorum; it must eventually give up once its leader lease
		// expires, resolving ProposeTransaction's blocking call with an
		// error -- the "ambiguous commit" case.
		Expect(follower.Raft.Shutdown().Error()).To(Succeed())

		errCh := make(chan error, 1)
		go func() {
			errCh <- gate.ProposeTransaction([]*wal.Frame{frame(1, []byte("lost"))})
		}()

		Eventually(errCh, 5*time.Second, 10*time.Millisecond).Should(Receive(HaveOccurred()))
	})
})
