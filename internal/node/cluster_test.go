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

// A multi-node cluster replicates writes, followers serve (possibly
// stale) reads, and killing/adding nodes converges. This suite runs a
// real hraft cluster over TCP loopback (the same node.Start production
// path a real deployment uses, not a test-only stand-in) to exercise
// that end to end.

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
	cfgs     []node.Config

	// snapshotThreshold/trailingLogs/snapshotInterval, when non-zero,
	// override node.Config's own (hraft-default) values -- snapshot_test.go
	// sets these low enough to force real, fast InstallSnapshot catch-up
	// instead of waiting on production-sized thresholds. Left zero,
	// startNode gets node.Config's normal defaults.
	snapshotThreshold uint64
	trailingLogs      uint64
	snapshotInterval  time.Duration
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
		SnapshotThreshold:  h.snapshotThreshold,
		TrailingLogs:       h.trailingLogs,
		SnapshotInterval:   h.snapshotInterval,
	}
	n, err := node.Start(cfg)
	Expect(err).NotTo(HaveOccurred())
	h.nodes = append(h.nodes, n)
	h.cfgs = append(h.cfgs, cfg)
	return n
}

func (h *clusterHarness) shutdown() {
	for _, n := range h.nodes {
		_ = n.Shutdown()
	}
}

// restart simulates a process restart of node i: shuts it down and starts
// a fresh node.Node against its exact original Config, in particular the
// same BindAddr -- the rest of the cluster's raft.Configuration (and its
// own persisted log) still names that address, so a rebind to a new one
// would leave this member permanently unreachable instead of rejoined.
func (h *clusterHarness) restart(i int) *node.Node {
	GinkgoHelper()
	Expect(h.nodes[i].Shutdown()).To(Succeed())
	n, err := node.Start(h.cfgs[i])
	Expect(err).NotTo(HaveOccurred())
	h.nodes[i] = n
	return n
}

// indexOf returns n's position in h.nodes, for passing to restart.
func (h *clusterHarness) indexOf(n *node.Node) int {
	GinkgoHelper()
	for i, hn := range h.nodes {
		if hn == n {
			return i
		}
	}
	Fail("node not found in harness")
	return -1
}

// leader waits for a node that's both the raft leader and Ready (the
// gaining-leadership drain) -- callers immediately issue writes against the
// result, which would otherwise race the drain and fail with a
// raftadapter.CatchingUpError.
func (h *clusterHarness) leader() *node.Node {
	GinkgoHelper()
	var leader *node.Node
	Eventually(func() bool {
		for _, n := range h.nodes {
			if n.Raft().State() == hraft.Leader && n.Ready() {
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

// nodeExec runs sql against n's kept-alive connection via WithDB, rather
// than holding a *sqlite3.Conn returned from a since-removed DB() accessor
// across the call -- exactly the pattern a concurrent snapshot install
// could race.
func nodeExec(n *node.Node, sql string) error {
	GinkgoHelper()
	return n.WithDB(func(c *sqlite3.Conn) error { return c.Exec(sql) })
}

// nodeQueryText/nodeQueryInt are queryText/queryInt for a *node.Node instead
// of an already-open *sqlite3.Conn, for the same reason as nodeExec.
func nodeQueryText(n *node.Node, sql string) string {
	GinkgoHelper()
	var result string
	Expect(n.WithDB(func(c *sqlite3.Conn) error {
		result = queryText(c, sql)
		return nil
	})).To(Succeed())
	return result
}

func nodeQueryInt(n *node.Node, sql string) int64 {
	GinkgoHelper()
	var result int64
	Expect(n.WithDB(func(c *sqlite3.Conn) error {
		result = queryInt(c, sql)
		return nil
	})).To(Succeed())
	return result
}

func rowCount(n *node.Node) (int64, error) {
	var count int64
	err := n.WithDB(func(c *sqlite3.Conn) error {
		stmt, _, err := c.Prepare("SELECT count(*) FROM t")
		if err != nil {
			return err
		}
		defer stmt.Close()
		if !stmt.Step() {
			return fmt.Errorf("no rows")
		}
		count = stmt.ColumnInt64(0)
		return nil
	})
	return count, err
}

var _ = Describe("cluster", func() {
	It("replicates writes to followers, and survives killing and adding a node", func() {
		h := startCluster(3)
		defer h.shutdown()

		leader := h.leader()
		Expect(nodeExec(leader, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")).To(Succeed())
		for i := 0; i < 20; i++ {
			Expect(nodeExec(leader, fmt.Sprintf("INSERT INTO t (v) VALUES ('row%d')", i))).To(Succeed())
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
			Expect(nodeQueryText(f, "PRAGMA integrity_check")).To(Equal("ok"))
		}

		// Kill one follower: the cluster (now 2/3) must still accept and
		// replicate writes to the remaining follower.
		killed := followers[0]
		survivor := followers[1]
		Expect(killed.Shutdown()).To(Succeed())

		Eventually(func() error {
			leader = h.leader()
			return nodeExec(leader, "INSERT INTO t (v) VALUES ('after-kill')")
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
		Expect(nodeQueryText(joiner, "PRAGMA integrity_check")).To(Equal("ok"))
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
