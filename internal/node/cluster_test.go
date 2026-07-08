package node_test

import (
	"fmt"
	"net"
	"path/filepath"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/ncruces/go-sqlite3"

	"github.com/fuchstim/literaft/internal/node"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// docs/ROADMAP.md M4 "done when": a multi-node cluster replicates writes,
// followers serve (possibly stale) reads, and killing/adding nodes
// converges. This suite runs a real hraft cluster over TCP loopback (the
// same node.Start production path a real deployment uses, not a
// test-only stand-in) to exercise that end to end.

// freeTCPAddr grabs an OS-assigned port by briefly binding and releasing
// it, then returns "127.0.0.1:<port>" so every node's initial bootstrap
// configuration can name every peer's address up front (hraft's
// multi-node bootstrap requires every initial voter to be listed by every
// other, before any of them have a leader to learn it from).
func freeTCPAddr() string {
	GinkgoHelper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	Expect(err).NotTo(HaveOccurred())
	addr := l.Addr().String()
	Expect(l.Close()).To(Succeed())
	return addr
}

type clusterHarness struct {
	dir      string
	pageSize uint32
	nodes    []*node.Node
}

// startNode starts and returns one node under h.dir, bootstrapping it with
// servers if non-nil (nil for a node meant to join later via AddVoter).
func (h *clusterHarness) startNode(id string, addr string, servers []hraft.Server) *node.Node {
	GinkgoHelper()
	cfg := node.Config{
		ID:                 id,
		BindAddr:           addr,
		DataDir:            filepath.Join(h.dir, id),
		DBPath:             filepath.Join(h.dir, id+".db"),
		PageSize:           h.pageSize,
		Bootstrap:          servers,
		ApplyTimeout:       2 * time.Second,
		CheckpointInterval: 200 * time.Millisecond,
	}
	n, err := node.Start(cfg)
	Expect(err).NotTo(HaveOccurred())
	h.nodes = append(h.nodes, n)
	return n
}

func (h *clusterHarness) shutdown() {
	for _, n := range h.nodes {
		_ = n.Shutdown()
	}
}

func (h *clusterHarness) leader() *node.Node {
	GinkgoHelper()
	var leader *node.Node
	Eventually(func() bool {
		for _, n := range h.nodes {
			if n.Raft().State() == hraft.Leader {
				leader = n
				return true
			}
		}
		return false
	}, 10*time.Second, 20*time.Millisecond).Should(BeTrue())
	return leader
}

// startCluster brings up an n-node cluster, all bootstrapped together
// (docs' "sane approach" of a single bootstrapper + AddVoter is the
// alternative; this repo's test pre-allocates every address up front
// instead, which is equally standard and keeps startNode uniform for both
// initial members and later joiners).
func startCluster(n int) *clusterHarness {
	GinkgoHelper()
	h := &clusterHarness{dir: GinkgoT().TempDir(), pageSize: pageSizeProbe()}

	addrs := make([]string, n)
	for i := range addrs {
		addrs[i] = freeTCPAddr()
	}
	servers := make([]hraft.Server, n)
	for i, addr := range addrs {
		servers[i] = hraft.Server{Suffrage: hraft.Voter, ID: hraft.ServerID(fmt.Sprintf("n%d", i)), Address: hraft.ServerAddress(addr)}
	}
	for i, addr := range addrs {
		h.startNode(fmt.Sprintf("n%d", i), addr, servers)
	}
	return h
}

// pageSizeProbe returns SQLite's actual default page size by asking a
// throwaway in-memory connection, rather than assuming a value -- the same
// caution apply_test.go takes (CLAUDE.md: verify, don't assume).
func pageSizeProbe() uint32 {
	GinkgoHelper()
	c, err := sqlite3.Open(":memory:")
	Expect(err).NotTo(HaveOccurred())
	defer c.Close()
	return uint32(queryInt(c, "PRAGMA page_size"))
}

func queryInt(c *sqlite3.Conn, sql string) int64 {
	GinkgoHelper()
	stmt, _, err := c.Prepare(sql)
	Expect(err).NotTo(HaveOccurred())
	defer stmt.Close()
	Expect(stmt.Step()).To(BeTrue(), "no rows for %q", sql)
	return stmt.ColumnInt64(0)
}

func rowCount(n *node.Node) (int64, error) {
	stmt, _, err := n.DB().Prepare("SELECT count(*) FROM t")
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	if !stmt.Step() {
		return 0, fmt.Errorf("no rows")
	}
	return stmt.ColumnInt64(0), nil
}

var _ = Describe("cluster", func() {
	It("replicates writes to followers, and survives killing and adding a node", func() {
		h := startCluster(3)
		defer h.shutdown()

		leader := h.leader()
		Expect(leader.DB().Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")).To(Succeed())
		for i := 0; i < 20; i++ {
			Expect(leader.DB().Exec(fmt.Sprintf("INSERT INTO t (v) VALUES ('row%d')", i))).To(Succeed())
		}

		var followers []*node.Node
		for _, n := range h.nodes {
			if n != leader {
				followers = append(followers, n)
			}
		}
		Expect(followers).To(HaveLen(2))

		for _, f := range followers {
			f := f
			Eventually(func() (int64, error) { return rowCount(f) }, 5*time.Second, 20*time.Millisecond).Should(Equal(int64(20)))
			Expect(queryText(f.DB(), "PRAGMA integrity_check")).To(Equal("ok"))
		}

		// Kill one follower: the cluster (now 2/3) must still accept and
		// replicate writes to the remaining follower.
		killed := followers[0]
		survivor := followers[1]
		Expect(killed.Shutdown()).To(Succeed())

		Eventually(func() error {
			leader = h.leader()
			return leader.DB().Exec("INSERT INTO t (v) VALUES ('after-kill')")
		}, 5*time.Second, 20*time.Millisecond).Should(Succeed())

		Eventually(func() (int64, error) { return rowCount(survivor) }, 5*time.Second, 20*time.Millisecond).Should(Equal(int64(21)))

		// Add a brand new node into the running cluster; it must catch up
		// via normal replication (no snapshot needed at this log size) to
		// the same state the rest of the cluster has.
		joinerAddr := freeTCPAddr()
		joiner := h.startNode("joiner", joinerAddr, nil)

		Expect(leader.Raft().AddVoter(hraft.ServerID("joiner"), hraft.ServerAddress(joinerAddr), 0, 5*time.Second).Error()).
			To(Succeed())

		Eventually(func() (int64, error) { return rowCount(joiner) }, 5*time.Second, 20*time.Millisecond).Should(Equal(int64(21)))
		Expect(queryText(joiner.DB(), "PRAGMA integrity_check")).To(Equal("ok"))
	})
})

func queryText(c *sqlite3.Conn, sql string) string {
	GinkgoHelper()
	stmt, _, err := c.Prepare(sql)
	Expect(err).NotTo(HaveOccurred())
	defer stmt.Close()
	Expect(stmt.Step()).To(BeTrue(), "no rows for %q", sql)
	return stmt.ColumnText(0)
}
