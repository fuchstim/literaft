package gate_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/ncruces/go-sqlite3"
	sqlite3vfs "github.com/ncruces/go-sqlite3/vfs"

	"github.com/fuchstim/literaft/internal/gate"
	"github.com/fuchstim/literaft/internal/testutils"
	"github.com/fuchstim/literaft/internal/vfs"
	"github.com/fuchstim/literaft/internal/wal"
	raftproto "github.com/fuchstim/literaft/proto"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestGate(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "gate Suite")
}

func queryInt(c *sqlite3.Conn, sql string) int64 {
	GinkgoHelper()
	stmt, _, err := c.Prepare(sql)
	Expect(err).NotTo(HaveOccurred())
	defer stmt.Close()
	Expect(stmt.Step()).To(BeTrue(), "no rows for %q", sql)
	return stmt.ColumnInt64(0)
}

func queryText(c *sqlite3.Conn, sql string) string {
	GinkgoHelper()
	stmt, _, err := c.Prepare(sql)
	Expect(err).NotTo(HaveOccurred())
	defer stmt.Close()
	Expect(stmt.Step()).To(BeTrue(), "no rows for %q", sql)
	return stmt.ColumnText(0)
}

// capturedTxn is one call to funcGate's ProposeTransaction, recorded
// verbatim.
type capturedTxn struct {
	frames []*wal.Frame
}

// funcGate adapts a function to vfs.Gate.
type funcGate func(frames []*wal.Frame) error

func (fn funcGate) ProposeTransaction(frames []*wal.Frame) error { return fn(frames) }

// captureTransactions runs each of stmts as its own committed transaction
// against a fresh, local, single-node connection through internal/vfs --
// never touching any real cluster -- via a gate that only records what it
// captures. It returns realistic, valid WAL frames in commit order, ready
// to feed straight into a real Gate.ProposeTransaction call so these tests
// can exercise gate.Gate's leader/ready/timeout/forwarding logic in
// isolation. Callers proposing more than one of the returned captures onto
// the same node must do so in this same order, since later statements
// build on earlier ones' schema/rows.
func captureTransactions(stmts ...string) []capturedTxn {
	GinkgoHelper()
	var got []capturedTxn
	gate := funcGate(func(frames []*wal.Frame) error {
		got = append(got, capturedTxn{frames})
		return nil
	})

	name := "literaft-gate-test-capture-" + uuid.NewString()
	vfs.Register(name, sqlite3vfs.Find(""), gate, hclog.NewNullLogger())

	path := filepath.Join(GinkgoT().TempDir(), "capture.db")
	c, err := sqlite3.Open("file:" + path + "?vfs=" + name)
	Expect(err).NotTo(HaveOccurred())
	defer c.Close()
	Expect(c.Exec("PRAGMA journal_mode=WAL")).To(Succeed())
	Expect(c.Exec("PRAGMA synchronous=NORMAL")).To(Succeed())

	for _, s := range stmts {
		Expect(c.Exec(s)).To(Succeed())
	}
	return got
}

// nodeQueryInt/nodeQueryText open a plain, unwrapped connection directly to
// a testutils.Node's db file (bypassing any VFS/gate) and run a query --
// the only way to observe whether the FSM materialized something.
func nodeQueryInt(n *testutils.Node, sql string) int64 {
	GinkgoHelper()
	c, err := sqlite3.Open("file:" + n.DBPath)
	Expect(err).NotTo(HaveOccurred())
	defer c.Close()
	return queryInt(c, sql)
}

func nodeQueryText(n *testutils.Node, sql string) string {
	GinkgoHelper()
	c, err := sqlite3.Open("file:" + n.DBPath)
	Expect(err).NotTo(HaveOccurred())
	defer c.Close()
	return queryText(c, sql)
}

// tryNodeQueryInt is nodeQueryInt's non-failing counterpart, for polling a
// query inside an Eventually condition where the underlying table may not
// exist yet -- that must make the condition retry, not fail the spec
// outright.
func tryNodeQueryInt(n *testutils.Node, sql string) (result int64, ok bool) {
	c, err := sqlite3.Open("file:" + n.DBPath)
	if err != nil {
		return 0, false
	}
	defer c.Close()
	stmt, _, err := c.Prepare(sql)
	if err != nil {
		return 0, false
	}
	defer stmt.Close()
	if !stmt.Step() {
		return 0, false
	}
	return stmt.ColumnInt64(0), true
}

// gatedCluster bundles a testutils.Cluster (Inmem tier) with a real
// gate.Gate per node. hub is non-nil only when built by newFwdCluster, in
// which case every node's Gate forwards follower writes to the leader
// through it instead of rejecting them.
type gatedCluster struct {
	*testutils.Cluster
	hub   *testutils.InmemForwardHub
	gates map[*testutils.Node]*gate.Gate
}

func newGatedCluster(t testutils.TB, n int, timeout time.Duration) *gatedCluster {
	GinkgoHelper()
	return newCluster(t, n, timeout, false)
}

// newFwdCluster is newGatedCluster with forwarding enabled: every node's
// Gate is wired to a shared testutils.InmemForwardHub, the same pairing a
// real node process wires together with -forward-writes.
func newFwdCluster(t testutils.TB, n int, timeout time.Duration) *gatedCluster {
	GinkgoHelper()
	return newCluster(t, n, timeout, true)
}

func newCluster(t testutils.TB, n int, timeout time.Duration, forwarding bool) *gatedCluster {
	GinkgoHelper()
	c := testutils.NewInmemCluster(t, n)

	var hub *testutils.InmemForwardHub
	if forwarding {
		hub = testutils.NewInmemForwardHub()
	}

	gates := make(map[*testutils.Node]*gate.Gate, n)
	for _, node := range c.Nodes() {
		var leaderTransport raftproto.LeaderTransport
		if hub != nil {
			leaderTransport = hub.Transport(node.Addr)
		}

		gates[node] = gate.New(node.Raft, node.FSM, hclog.NewNullLogger(), leaderTransport, timeout, timeout, timeout)
	}
	return &gatedCluster{c, hub, gates}
}

func (gc *gatedCluster) Gate(n *testutils.Node) *gate.Gate { return gc.gates[n] }

func (gc *gatedCluster) Shutdown() {
	for _, g := range gc.gates {
		g.Close()
	}
	gc.Cluster.Shutdown()
}

// ReadyLeader waits for a node that's both the raft leader and has
// finished its Gate's gaining-leadership drain -- safe to immediately
// propose against.
func (gc *gatedCluster) ReadyLeader(t testutils.TB) (*testutils.Node, *gate.Gate) {
	t.Helper()
	var leader *testutils.Node
	testutils.Eventually(t, 5*time.Second, 10*time.Millisecond, func() bool {
		for _, n := range gc.Nodes() {
			if n.Raft.State() == raft.Leader && gc.gates[n].Ready() {
				leader = n
				return true
			}
		}
		return false
	}, "a ready leader")
	return leader, gc.gates[leader]
}
