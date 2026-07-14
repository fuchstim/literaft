package log_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/ncruces/go-sqlite3"
	sqlite3vfs "github.com/ncruces/go-sqlite3/vfs"

	raftgate "github.com/fuchstim/literaft/internal/raft/gate"
	"github.com/fuchstim/literaft/internal/testutils"
	"github.com/fuchstim/literaft/internal/vfs"
	"github.com/fuchstim/literaft/log"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestLog(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "log Suite")
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
	frames    []*vfs.Frame
	nTruncate uint32
}

// funcGate adapts a function to vfs.Gate.
type funcGate func(frames []*vfs.Frame, nTruncate uint32) error

func (fn funcGate) ProposeTransaction(frames []*vfs.Frame, nTruncate uint32) error {
	return fn(frames, nTruncate)
}

// captureTransactions runs each of stmts as its own committed transaction
// against a fresh, local, single-node connection through internal/vfs --
// never touching any real cluster -- via a gate that only records what it
// captures. It returns realistic, valid SQLite page content in commit
// order, ready to feed straight into a real Gate.ProposeTransaction call so
// these tests can exercise SingleWriterLog's leader/ready/timeout logic in
// isolation. Callers proposing more than one of the returned captures onto
// the same node must do so in this same order, since later statements
// build on earlier ones' schema/rows.
func captureTransactions(pageSize uint32, stmts ...string) []capturedTxn {
	GinkgoHelper()
	var got []capturedTxn
	gate := funcGate(func(frames []*vfs.Frame, nTruncate uint32) error {
		got = append(got, capturedTxn{frames, nTruncate})
		return nil
	})

	name := "literaft-log-test-capture-" + uuid.NewString()
	vfs.Register(name, sqlite3vfs.Find(""), gate, pageSize, hclog.NewNullLogger())

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

// nodeGate bundles one testutils.Node's log.SingleWriterLog with the
// raftgate.Gate adapting it -- the same pairing a real node process wires
// together (cmd/literaft/main.go), and the object under test in this
// package.
type nodeGate struct {
	log  *log.SingleWriterLog
	gate *raftgate.Gate
}

// gatedCluster bundles a testutils.Cluster (Inmem tier) with a real
// log.SingleWriterLog + raftgate.Gate per node.
type gatedCluster struct {
	*testutils.Cluster
	nodes map[*testutils.Node]nodeGate
}

func newGatedCluster(t testutils.TB, n int, timeout time.Duration) *gatedCluster {
	GinkgoHelper()
	c := testutils.NewInmemCluster(t, n)
	nodes := make(map[*testutils.Node]nodeGate, n)
	for _, node := range c.Nodes() {
		l := log.NewSingleWriterLog(node.Raft, log.WithApplyTimeout(timeout))
		nodes[node] = nodeGate{log: l, gate: raftgate.New(node.FSM, l, hclog.NewNullLogger())}
	}
	return &gatedCluster{c, nodes}
}

func (gc *gatedCluster) Gate(n *testutils.Node) *raftgate.Gate      { return gc.nodes[n].gate }
func (gc *gatedCluster) Log(n *testutils.Node) *log.SingleWriterLog { return gc.nodes[n].log }

func (gc *gatedCluster) Shutdown() {
	for _, ng := range gc.nodes {
		ng.log.Close()
	}
	gc.Cluster.Shutdown()
}

// ReadyLeader waits for a node that's both the raft leader and has
// finished its Log's gaining-leadership drain -- safe to immediately
// propose against.
func (gc *gatedCluster) ReadyLeader(t testutils.TB) (*testutils.Node, *raftgate.Gate) {
	t.Helper()
	var leader *testutils.Node
	testutils.Eventually(t, 5*time.Second, 10*time.Millisecond, func() bool {
		for _, n := range gc.Nodes() {
			if n.Raft.State() == raft.Leader && gc.nodes[n].log.Ready() {
				leader = n
				return true
			}
		}
		return false
	}, "a ready leader")
	return leader, gc.nodes[leader].gate
}
