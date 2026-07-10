package raftgate_test

import (
	"errors"
	"time"

	"github.com/hashicorp/raft"
	"github.com/ncruces/go-sqlite3"

	raftgate "github.com/fuchstim/literaft/internal/raft/gate"
	raftproto "github.com/fuchstim/literaft/internal/raft/proto"
	"github.com/fuchstim/literaft/internal/testutils"

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

var _ = Describe("Gate", func() {
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
			err := c.Gate(follower).Propose(&raftproto.Transaction{Pages: []*raftproto.Page{{Pgno: 1, Data: []byte("x")}}, NTruncate: 1})
			if err == nil {
				return false
			}
			var notLeader *raftgate.NotLeaderError
			if !errors.As(err, &notLeader) || notLeader.Leader != leader.Addr {
				return false
			}
			hint = notLeader.Leader
			return true
		}, "a NotLeaderError carrying the current leader's address")
		Expect(hint).NotTo(BeEmpty())
	})

	// Propose's concrete error doesn't reliably survive the round trip back
	// through *sqlite3.Conn.Exec/Stmt.Step, so LastRejection is the
	// mechanism a caller holding the Gate directly should actually use.
	It("exposes the most recent rejection via LastRejection, clearing it on the next success", func() {
		c := newGatedCluster(GinkgoT(), 2, time.Second)
		defer c.Shutdown()
		leader, _ := c.ReadyLeader(GinkgoT())
		follower := c.Other(leader)

		Expect(c.Gate(follower).LastRejection()).To(BeNil(), "no proposal attempted yet")

		err := c.Gate(follower).Propose(&raftproto.Transaction{Pages: []*raftproto.Page{{Pgno: 1, Data: []byte("x")}}, NTruncate: 1})
		Expect(err).To(HaveOccurred())
		var notLeader *raftgate.NotLeaderError
		Expect(errors.As(c.Gate(follower).LastRejection(), &notLeader)).To(BeTrue(),
			"LastRejection must return the same concrete error Propose returned")

		pageSize := leader.FSM.PageSize()
		entries := captureEntries(pageSize, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")

		var proposer *testutils.Node
		testutils.Eventually(GinkgoT(), 5*time.Second, 20*time.Millisecond, func() bool {
			l, g := c.ReadyLeader(GinkgoT())
			if err := g.Propose(entries[0]); err != nil {
				return false
			}
			proposer = l
			return true
		}, "a successful proposal on the current leader")

		Expect(c.Gate(proposer).LastRejection()).To(BeNil(), "a successful proposal must clear the previous rejection")
	})

	It("commits a leader's proposal without the leader materializing its own entry, while the follower does", func() {
		c := newGatedCluster(GinkgoT(), 2, time.Second)
		defer c.Shutdown()
		leader, gate := c.ReadyLeader(GinkgoT())
		follower := c.Other(leader)

		entries := captureEntries(leader.FSM.PageSize(),
			"CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
			"INSERT INTO t (id, v) VALUES (1, 'hello')",
		)
		for _, e := range entries {
			Expect(gate.Propose(e)).To(Succeed())
		}

		// The leader publishes via its own SQLite write path in real usage
		// (out of scope for this direct-Propose unit test); either way, its
		// own fsm.FSM must never materialize its own entry via
		// AppendTransaction -- there is nothing at all on the leader's disk for
		// this table, since this test never opened a real gated connection
		// on the leader to publish it any other way.
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
		// expires, resolving Propose's blocking call with an error -- the
		// "ambiguous commit" case.
		Expect(follower.Raft.Shutdown().Error()).To(Succeed())

		errCh := make(chan error, 1)
		go func() {
			errCh <- gate.Propose(&raftproto.Transaction{Pages: []*raftproto.Page{{Pgno: 1, Data: []byte("lost")}}, NTruncate: 1})
		}()

		Eventually(errCh, 5*time.Second, 10*time.Millisecond).Should(Receive(HaveOccurred()))
	})
})
