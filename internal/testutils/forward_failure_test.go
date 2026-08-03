package testutils_test

import (
	"fmt"
	"time"

	"github.com/hashicorp/raft"

	"github.com/fuchstim/literaft/internal/testutils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Two forwarding failure scenarios that need a real multi-node stack rather
// than fakes: a leadership change while a forward is in flight, and a snapshot
// install interleaved with forwarding. Both hold forwarding to the same bar as
// everything else -- every node converges to identical state with a clean
// integrity_check, and no accepted write is lost or duplicated.
//
// Both drive idempotent, always-frame-producing writes (INSERT OR REPLACE with
// fixed content), retried through every transient or ambiguous outcome: an
// ambiguous forward may have committed, so a blind re-run is only safe when the
// statement is idempotent, and that in turn lets the tests assert a
// deterministic final state despite the churn.

// currentLeader returns the node currently reporting itself as raft leader, or
// nil if there isn't one this instant (mid-election).
func currentLeader(c *testutils.TCPCluster) *testutils.Node {
	for _, n := range c.Nodes() {
		if n.Raft.State() == raft.Leader {
			return n
		}
	}
	return nil
}

// forwardUntilAccepted runs an idempotent write on n, retrying through every
// transient/ambiguous outcome (leadership churn, stale-base rejection, a lost
// response after a possibly-committed forward) until it is accepted or deadline
// passes. Safe to re-run only because the statement is idempotent.
func forwardUntilAccepted(n *testutils.Node, deadline time.Time, sql string, args ...any) bool {
	for time.Now().Before(deadline) {
		if _, err := n.DB.Exec(sql, args...); err == nil {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// nodeDigest is an order-stable fingerprint of table t's full contents, so two
// nodes can be compared for byte-for-byte-equal logical state in one query.
func nodeDigest(n *testutils.Node) (string, error) {
	var d string
	err := n.DB.QueryRow(
		"SELECT COALESCE(group_concat(id || ':' || v, '|'), '') FROM (SELECT id, v FROM t ORDER BY id)",
	).Scan(&d)
	return d, err
}

var _ = Describe("write forwarding failure matrix", func() {
	It("keeps forwarded writes consistent across leadership churn", func() {
		c := testutils.NewTCPCluster(GinkgoT(), GinkgoT().TempDir(), 3,
			testutils.WithForwarding(), testutils.WithApplyTimeout(5*time.Second))
		defer c.Shutdown()

		leader := c.ReadyLeader()
		Expect(nodeExec(leader, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")).To(Succeed())

		// Originate every write from one fixed follower, so it is always
		// forwarding (never the leader). Leadership only ever moves between the
		// other two nodes below.
		writer := followers(c, leader)[0]
		var others []*testutils.Node
		for _, n := range c.Nodes() {
			if n != writer {
				others = append(others, n)
			}
		}
		testutils.Eventually(GinkgoT(), 5*time.Second, 20*time.Millisecond, func() bool {
			var count int64
			return writer.DB.QueryRow("SELECT count(*) FROM t").Scan(&count) == nil
		}, "the writer follower to materialize the schema")

		const total = 40
		writeDone := make(chan int, 1) // how many keys were accepted
		go func() {
			defer GinkgoRecover()
			accepted := 0
			for k := 1; k <= total; k++ {
				if !forwardUntilAccepted(writer, time.Now().Add(30*time.Second),
					"INSERT OR REPLACE INTO t (id, v) VALUES (?, ?)", k, fmt.Sprintf("v%d", k)) {
					break
				}
				accepted++
			}
			writeDone <- accepted
		}()

		// While the writer hammers forwards, bounce leadership between the two
		// non-writer nodes so some forwards are in flight across a term change.
		// Transfers are best-effort: a target that isn't caught up (or a
		// momentary lack of leader) just means this attempt is skipped and the
		// cluster re-elects on its own.
		churn := time.After(3 * time.Second)
	churnLoop:
		for i := 0; ; i++ {
			select {
			case <-churn:
				break churnLoop
			default:
			}
			if l := currentLeader(c); l != nil {
				target := others[i%len(others)]
				if target != l {
					_ = l.Raft.LeadershipTransferToServer(raft.ServerID(target.ID), target.Addr).Error()
				}
			}
			time.Sleep(200 * time.Millisecond)
		}

		var accepted int
		Eventually(writeDone, 40*time.Second, 50*time.Millisecond).Should(Receive(&accepted))
		Expect(accepted).To(Equal(total), "every idempotent forward must eventually be accepted despite churn")

		// Every node converges to the identical full set, with clean integrity
		// and no lost or duplicated write.
		want, err := nodeDigest(writer)
		Expect(err).NotTo(HaveOccurred())
		for _, n := range c.Nodes() {
			node := n
			testutils.Eventually(GinkgoT(), 15*time.Second, 50*time.Millisecond, func() bool {
				got, err := nodeDigest(node)
				return err == nil && got == want
			}, fmt.Sprintf("node %s to converge to the identical forwarded state", node.ID))
			cnt, err := rowCount(node)
			Expect(err).NotTo(HaveOccurred())
			Expect(cnt).To(Equal(int64(total)))
			Expect(nodeQueryText(node, "PRAGMA integrity_check")).To(Equal("ok"))
			Expect(externalQueryText(node.DBPath, "PRAGMA integrity_check")).To(Equal("ok"))
		}
	})

	It("keeps forwarding consistent while a far-behind joiner catches up via InstallSnapshot", func() {
		// Low snapshot thresholds so the leader compacts its log well past the
		// row count -- a brand-new joiner then cannot catch up by log replay and
		// must take an InstallSnapshot, all while writes keep arriving via
		// forwarding.
		c := testutils.NewTCPCluster(GinkgoT(), GinkgoT().TempDir(), 3,
			testutils.WithForwarding(),
			testutils.WithApplyTimeout(5*time.Second),
			testutils.WithSnapshotThreshold(30),
			testutils.WithTrailingLogs(10),
			testutils.WithSnapshotInterval(200*time.Millisecond),
		)
		defer c.Shutdown()

		leader := c.ReadyLeader()
		Expect(nodeExec(leader, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")).To(Succeed())

		writer := followers(c, leader)[0]
		testutils.Eventually(GinkgoT(), 5*time.Second, 20*time.Millisecond, func() bool {
			var count int64
			return writer.DB.QueryRow("SELECT count(*) FROM t").Scan(&count) == nil
		}, "the writer follower to materialize the schema")

		const preJoin = 120
		for k := 1; k <= preJoin; k++ {
			Expect(forwardUntilAccepted(writer, time.Now().Add(30*time.Second),
				"INSERT OR REPLACE INTO t (id, v) VALUES (?, ?)", k, fmt.Sprintf("v%d", k))).
				To(BeTrue(), "pre-join forwarded write %d to be accepted", k)
		}

		// Let the snapshot goroutine fire and compact the leader's log past the
		// row count before the joiner asks for anything, so its catch-up is
		// InstallSnapshot, not lucky replay.
		testutils.Eventually(GinkgoT(), 5*time.Second, 50*time.Millisecond, func() bool {
			leader = c.ReadyLeader()
			return leader.Raft.LastIndex() > uint64(preJoin)
		}, "the leader's log to compact past the row count")
		time.Sleep(1 * time.Second)

		joiner := c.Join(GinkgoT(), "joiner")
		Expect(leader.Raft.AddVoter(raft.ServerID(joiner.ID), joiner.Addr, 0, 5*time.Second).Error()).To(Succeed())

		// Keep forwarding while the joiner installs the snapshot and then
		// applies the tail via ordinary replication.
		const postJoin = 20
		for k := preJoin + 1; k <= preJoin+postJoin; k++ {
			Expect(forwardUntilAccepted(writer, time.Now().Add(30*time.Second),
				"INSERT OR REPLACE INTO t (id, v) VALUES (?, ?)", k, fmt.Sprintf("v%d", k))).
				To(BeTrue(), "post-join forwarded write %d to be accepted", k)
		}

		want, err := nodeDigest(writer)
		Expect(err).NotTo(HaveOccurred())
		for _, n := range c.Nodes() {
			node := n
			testutils.Eventually(GinkgoT(), 15*time.Second, 50*time.Millisecond, func() bool {
				got, err := nodeDigest(node)
				return err == nil && got == want
			}, fmt.Sprintf("node %s (incl. the snapshot-restored joiner) to converge", node.ID))
			cnt, err := rowCount(node)
			Expect(err).NotTo(HaveOccurred())
			Expect(cnt).To(Equal(int64(preJoin + postJoin)))
			Expect(nodeQueryText(node, "PRAGMA integrity_check")).To(Equal("ok"))
			Expect(externalQueryText(node.DBPath, "PRAGMA integrity_check")).To(Equal("ok"))
		}
	})
})
