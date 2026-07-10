package testutils_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/ncruces/go-sqlite3"

	"github.com/fuchstim/literaft/internal/testutils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// This suite runs the same real, disk-backed, TCP-transport stack a real
// node process wires up in production (testutils.NewTCPCluster), not a
// test-only stand-in: a multi-node cluster replicates writes, followers
// serve (possibly stale) reads, and killing/adding nodes converges.

func TestCluster(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "testutils cluster Suite")
}

func nodeExec(n *testutils.Node, sql string) error {
	_, err := n.DB.Exec(sql)
	return err
}

func rowCount(n *testutils.Node) (int64, error) {
	var count int64
	err := n.DB.QueryRow("SELECT count(*) FROM t").Scan(&count)
	return count, err
}

func nodeQueryText(n *testutils.Node, sql string) string {
	GinkgoHelper()
	var v string
	Expect(n.DB.QueryRow(sql).Scan(&v)).To(Succeed())
	return v
}

// externalQueryText/Int run sql against dbPath through a completely plain,
// unmodified-VFS connection (no "?vfs=" at all) -- proving the on-disk
// files are actually correct, not just readable through the same VFS that
// wrote them.
func externalQueryText(dbPath, sql string) string {
	GinkgoHelper()
	c, err := sqlite3.Open(dbPath)
	Expect(err).NotTo(HaveOccurred())
	defer c.Close()
	stmt, _, err := c.Prepare(sql)
	Expect(err).NotTo(HaveOccurred())
	defer stmt.Close()
	Expect(stmt.Step()).To(BeTrue(), "no rows for %q", sql)
	return stmt.ColumnText(0)
}

func externalQueryInt(dbPath, sql string) int64 {
	GinkgoHelper()
	c, err := sqlite3.Open(dbPath)
	Expect(err).NotTo(HaveOccurred())
	defer c.Close()
	stmt, _, err := c.Prepare(sql)
	Expect(err).NotTo(HaveOccurred())
	defer stmt.Close()
	Expect(stmt.Step()).To(BeTrue(), "no rows for %q", sql)
	return stmt.ColumnInt64(0)
}

var _ = Describe("cluster", func() {
	It("replicates writes to followers, and survives killing and adding a node", func() {
		c := testutils.NewTCPCluster(GinkgoT(), GinkgoT().TempDir(), 3)
		defer c.Shutdown()

		leader := c.ReadyLeader()
		Expect(nodeExec(leader, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")).To(Succeed())
		for i := 0; i < 20; i++ {
			Expect(nodeExec(leader, fmt.Sprintf("INSERT INTO t (v) VALUES ('row%d')", i))).To(Succeed())
		}

		var followers []*testutils.Node
		for _, n := range c.Nodes() {
			if n != leader {
				followers = append(followers, n)
			}
		}
		Expect(followers).To(HaveLen(2))

		for _, f := range followers {
			testutils.Eventually(GinkgoT(), 5*time.Second, 20*time.Millisecond, func() bool {
				n, err := rowCount(f)
				return err == nil && n == 20
			}, "follower to catch up to 20 rows")
			Expect(nodeQueryText(f, "PRAGMA integrity_check")).To(Equal("ok"))
		}

		// Kill one follower: the cluster (now 2/3) must still accept and
		// replicate writes to the remaining follower.
		killed := followers[0]
		survivor := followers[1]
		Expect(killed.Shutdown()).To(Succeed())

		testutils.Eventually(GinkgoT(), 5*time.Second, 20*time.Millisecond, func() bool {
			leader = c.ReadyLeader()
			return nodeExec(leader, "INSERT INTO t (v) VALUES ('after-kill')") == nil
		}, "the surviving majority to keep accepting writes")

		testutils.Eventually(GinkgoT(), 5*time.Second, 20*time.Millisecond, func() bool {
			n, err := rowCount(survivor)
			return err == nil && n == 21
		}, "the survivor to catch up to 21 rows")

		// Add a brand new node into the running cluster; it must catch up
		// via normal replication (no snapshot needed at this log size) to
		// the same state the rest of the cluster has.
		joiner := c.Join(GinkgoT(), "joiner")
		Expect(leader.Raft.AddVoter(raft.ServerID(joiner.ID), joiner.Addr, 0, 5*time.Second).Error()).To(Succeed())

		testutils.Eventually(GinkgoT(), 5*time.Second, 20*time.Millisecond, func() bool {
			n, err := rowCount(joiner)
			return err == nil && n == 21
		}, "the joiner to catch up to 21 rows")
		Expect(nodeQueryText(joiner, "PRAGMA integrity_check")).To(Equal("ok"))
	})
})
