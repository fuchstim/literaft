package gate_test

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/ncruces/go-sqlite3"

	"github.com/fuchstim/literaft/internal/testutils"
	raftproto "github.com/fuchstim/literaft/proto"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("gate.Gate forwarding", func() {
	It("still lets the leader write directly without forwarding", func() {
		c := newFwdCluster(GinkgoT(), 2, time.Second)
		defer c.Shutdown()
		leader, gate := c.ReadyLeader(GinkgoT())
		follower := c.Other(leader)

		txn := captureTransactions("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")[0]
		Expect(gate.ProposeTransaction(txn.frames)).To(Succeed())

		testutils.Eventually(GinkgoT(), 5*time.Second, 20*time.Millisecond, func() bool {
			_, ok := tryNodeQueryInt(follower, "SELECT count(*) FROM t")
			return ok
		}, "the follower to materialize the leader's direct write")
	})

	It("accepts a follower's forwarded write and materializes it on the leader, without the follower re-materializing its own entry", func() {
		c := newFwdCluster(GinkgoT(), 2, time.Second)
		defer c.Shutdown()
		leader, _ := c.ReadyLeader(GinkgoT())
		follower := c.Other(leader)

		txn := captureTransactions("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")[0]
		Expect(c.Gate(follower).ProposeTransaction(txn.frames)).To(Succeed())

		testutils.Eventually(GinkgoT(), 5*time.Second, 20*time.Millisecond, func() bool {
			_, ok := tryNodeQueryInt(leader, "SELECT count(*) FROM t")
			return ok
		}, "the leader to materialize the forwarded entry")

		// The originating follower's own skip marker consumes the entry
		// instead of re-materializing it through AppendFrames -- mirroring
		// the leader's own self-apply skip on a direct write, there is
		// nothing at all on the follower's disk for this table.
		followerConn, err := sqlite3.Open("file:" + follower.DBPath)
		Expect(err).NotTo(HaveOccurred())
		defer followerConn.Close()
		_, _, err = followerConn.Prepare("SELECT count(*) FROM t")
		Expect(err).To(HaveOccurred(), "the originating follower must not have materialized its own forwarded entry")
	})

	It("rejects a forwarded write whose base index is stale, without proposing it", func() {
		c := newFwdCluster(GinkgoT(), 2, time.Second)
		defer c.Shutdown()
		leader, _ := c.ReadyLeader(GinkgoT())
		follower := c.Other(leader)

		// Prime the leader's applied index past 0, so a request stamped
		// with the original (now stale) base index is rejected.
		primer := captureTransactions("CREATE TABLE t (id INTEGER PRIMARY KEY)")[0]
		Expect(c.Gate(leader).ProposeTransaction(primer.frames)).To(Succeed())
		testutils.Eventually(GinkgoT(), 5*time.Second, 10*time.Millisecond, func() bool {
			return leader.FSM.LastAppliedIndex() > 0
		}, "the leader to apply the priming write")

		stale := captureTransactions("CREATE TABLE stale (id INTEGER PRIMARY KEY)")[0]
		entry := &raftproto.LogEntry{
			Header:  &raftproto.LogEntry_Header{Id: uuid.NewString()},
			Payload: &raftproto.LogEntry_Transaction_{Transaction: raftproto.NewLogEntryTransaction(stale.frames)},
		}
		req := &raftproto.LeaderRequest{
			Header:  &raftproto.LeaderRequest_Header{LastAppliedIndex: 0},
			Payload: &raftproto.LeaderRequest_LogEntry{LogEntry: entry},
		}

		resp, err := c.hub.Transport(follower.Addr).Propose(context.Background(), leader.Addr, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.GetStatus()).To(Equal(raftproto.LeaderResponse_STATUS_STALE_BASE))
		Expect(resp.GetLastAppliedIndex()).To(Equal(leader.FSM.LastAppliedIndex()))

		testutils.Consistently(GinkgoT(), 200*time.Millisecond, 10*time.Millisecond, func() bool {
			_, ok := tryNodeQueryInt(leader, "SELECT count(*) FROM stale")
			return !ok
		}, "a stale-base request must never be proposed")
	})

	It("rejects a malformed transaction shape without proposing it", func() {
		c := newFwdCluster(GinkgoT(), 2, time.Second)
		defer c.Shutdown()
		leader, _ := c.ReadyLeader(GinkgoT())
		follower := c.Other(leader)

		entry := &raftproto.LogEntry{
			Header: &raftproto.LogEntry_Header{Id: uuid.NewString()},
			Payload: &raftproto.LogEntry_Transaction_{Transaction: &raftproto.LogEntry_Transaction{
				Pages:     []*raftproto.LogEntry_Transaction_Page{{PgNo: 1, Data: []byte("x")}},
				NTruncate: 0, // not a whole committed txn
			}},
		}
		req := &raftproto.LeaderRequest{
			Header:  &raftproto.LeaderRequest_Header{LastAppliedIndex: 0},
			Payload: &raftproto.LeaderRequest_LogEntry{LogEntry: entry},
		}

		resp, err := c.hub.Transport(follower.Addr).Propose(context.Background(), leader.Addr, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.GetStatus()).To(Equal(raftproto.LeaderResponse_STATUS_INVALID))

		testutils.Consistently(GinkgoT(), 200*time.Millisecond, 10*time.Millisecond, func() bool {
			return leader.FSM.LastAppliedIndex() == 0
		}, "a malformed transaction must never be proposed")
	})

	It("answers a request routed to a non-leader with NOT_LEADER, before touching the write lock", func() {
		c := newFwdCluster(GinkgoT(), 2, time.Second)
		defer c.Shutdown()
		leader, _ := c.ReadyLeader(GinkgoT())
		follower := c.Other(leader)

		txn := captureTransactions("CREATE TABLE t (id INTEGER PRIMARY KEY)")[0]
		entry := &raftproto.LogEntry{
			Header:  &raftproto.LogEntry_Header{Id: uuid.NewString()},
			Payload: &raftproto.LogEntry_Transaction_{Transaction: raftproto.NewLogEntryTransaction(txn.frames)},
		}
		req := &raftproto.LeaderRequest{
			Header:  &raftproto.LeaderRequest_Header{LastAppliedIndex: 0},
			Payload: &raftproto.LeaderRequest_LogEntry{LogEntry: entry},
		}

		// Routed straight to the follower's own registered handler, not the
		// leader's -- a mis-route the real gRPC transport should never
		// produce, but the handler itself must still redirect cleanly.
		resp, err := c.hub.Transport(leader.Addr).Propose(context.Background(), follower.Addr, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.GetStatus()).To(Equal(raftproto.LeaderResponse_STATUS_NOT_LEADER))
		Expect(resp.GetLeaderAddr()).To(Equal(string(leader.Addr)))
	})
})
