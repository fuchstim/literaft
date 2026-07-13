package testutils_test

import (
	"fmt"
	"time"

	"github.com/fuchstim/literaft/internal/testutils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// followers returns every node in c that isn't leader.
func followers(c *testutils.TCPCluster, leader *testutils.Node) []*testutils.Node {
	var fs []*testutils.Node
	for _, n := range c.Nodes() {
		if n != leader {
			fs = append(fs, n)
		}
	}
	return fs
}

var _ = Describe("write forwarding (WithForwarding)", func() {
	It("accepts a write issued on a follower connection and replicates it everywhere", func() {
		c := testutils.NewTCPCluster(GinkgoT(), GinkgoT().TempDir(), 3, testutils.WithForwarding())
		defer c.Shutdown()

		leader := c.ReadyLeader()
		Expect(nodeExec(leader, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")).To(Succeed())

		follower := followers(c, leader)[0]
		// The follower must have the table before it can compute a write on
		// top of it.
		testutils.Eventually(GinkgoT(), 5*time.Second, 20*time.Millisecond, func() bool {
			_, err := follower.DB.Exec("SELECT 1")
			if err != nil {
				return false
			}
			var n int64
			return follower.DB.QueryRow("SELECT count(*) FROM t").Scan(&n) == nil
		}, "the follower to materialize the schema")

		// A write on the follower connection succeeds via forwarding. Retry
		// through the retryable rejections (STALE_BASE / CATCHING_UP / BUSY);
		// INSERT OR REPLACE keeps the retry idempotent.
		testutils.Eventually(GinkgoT(), 10*time.Second, 20*time.Millisecond, func() bool {
			return nodeExec(follower, "INSERT OR REPLACE INTO t (id, v) VALUES (1, 'from-follower')") == nil
		}, "the follower write to be accepted by the leader")

		// Read-your-writes on the originating follower: the row is visible
		// immediately after COMMIT returned.
		Expect(nodeQueryText(follower, "SELECT v FROM t WHERE id = 1")).To(Equal("from-follower"))

		// It replicated to every node, on disk (external plain-VFS reader).
		for _, n := range c.Nodes() {
			node := n
			testutils.Eventually(GinkgoT(), 5*time.Second, 20*time.Millisecond, func() bool {
				var got int64
				return node.DB.QueryRow("SELECT count(*) FROM t WHERE id = 1").Scan(&got) == nil && got == 1
			}, fmt.Sprintf("node %s to see the forwarded row", node.ID))
			Expect(externalQueryText(node.DBPath, "SELECT v FROM t WHERE id = 1")).To(Equal("from-follower"))
			Expect(nodeQueryText(node, "PRAGMA integrity_check")).To(Equal("ok"))
		}
	})

	It("still lets the leader write directly with forwarding enabled", func() {
		c := testutils.NewTCPCluster(GinkgoT(), GinkgoT().TempDir(), 3, testutils.WithForwarding())
		defer c.Shutdown()

		leader := c.ReadyLeader()
		Expect(nodeExec(leader, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")).To(Succeed())
		for i := 0; i < 10; i++ {
			Expect(nodeExec(leader, fmt.Sprintf("INSERT INTO t (v) VALUES ('leader%d')", i))).To(Succeed())
		}

		for _, f := range followers(c, leader) {
			node := f
			testutils.Eventually(GinkgoT(), 5*time.Second, 20*time.Millisecond, func() bool {
				n, err := rowCount(node)
				return err == nil && n == 10
			}, "follower to catch up to 10 leader-written rows")
		}
	})
})
