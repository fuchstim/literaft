package forwardinggate_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/ncruces/go-sqlite3"
	sqlite3vfs "github.com/ncruces/go-sqlite3/vfs"

	"github.com/fuchstim/literaft/internal/testutils"
	"github.com/fuchstim/literaft/internal/vfs"
	"github.com/fuchstim/literaft/internal/wal"
	forwardinggate "github.com/fuchstim/literaft/raft/gate/forwarding"
	leadergate "github.com/fuchstim/literaft/raft/gate/leader"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestForwardingGate(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "forwardinggate Suite")
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
// to feed straight into a real Gate.ProposeTransaction call.
func captureTransactions(stmts ...string) []capturedTxn {
	GinkgoHelper()
	var got []capturedTxn
	gate := funcGate(func(frames []*wal.Frame) error {
		got = append(got, capturedTxn{frames})
		return nil
	})

	name := "literaft-forwardinggate-test-capture-" + uuid.NewString()
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

// nodeQueryInt opens a plain, unwrapped connection directly to a
// testutils.Node's db file (bypassing any VFS/gate) and runs a query --
// the only way to observe whether the FSM materialized something.
func nodeQueryInt(n *testutils.Node, sql string) int64 {
	GinkgoHelper()
	c, err := sqlite3.Open("file:" + n.DBPath)
	Expect(err).NotTo(HaveOccurred())
	defer c.Close()
	stmt, _, err := c.Prepare(sql)
	Expect(err).NotTo(HaveOccurred())
	defer stmt.Close()
	Expect(stmt.Step()).To(BeTrue(), "no rows for %q", sql)
	return stmt.ColumnInt64(0)
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

// fwdCluster bundles a testutils.Cluster (Inmem tier) with a real
// leadergate.Gate + forwardinggate.Gate per node, wired through a shared
// testutils.InmemForwardHub -- the same pairing a real node process wires
// together with -forward-writes.
type fwdCluster struct {
	*testutils.Cluster
	hub    *testutils.InmemForwardHub
	base   map[*testutils.Node]*leadergate.Gate
	fwdGts map[*testutils.Node]*forwardinggate.Gate
}

func newFwdCluster(t testutils.TB, n int, timeout time.Duration) *fwdCluster {
	GinkgoHelper()
	c := testutils.NewInmemCluster(t, n)
	hub := testutils.NewInmemForwardHub()
	base := make(map[*testutils.Node]*leadergate.Gate, n)
	fwdGts := make(map[*testutils.Node]*forwardinggate.Gate, n)
	for _, node := range c.Nodes() {
		lg := leadergate.New(node.Raft, node.FSM, leadergate.WithApplyTimeout(timeout))
		base[node] = lg
		fwdGts[node] = forwardinggate.New(node.Raft, node.FSM, hub.Transport(node.Addr), forwardinggate.WithBaseGate(lg))
	}
	return &fwdCluster{c, hub, base, fwdGts}
}

func (fc *fwdCluster) Gate(n *testutils.Node) *forwardinggate.Gate   { return fc.fwdGts[n] }
func (fc *fwdCluster) LeaderGate(n *testutils.Node) *leadergate.Gate { return fc.base[n] }

func (fc *fwdCluster) Shutdown() {
	for _, g := range fc.fwdGts {
		g.Close()
	}
	fc.Cluster.Shutdown()
}

// ReadyLeader waits for a node that's both the raft leader and has
// finished its base leadergate.Gate's gaining-leadership drain -- safe to
// immediately propose against.
func (fc *fwdCluster) ReadyLeader(t testutils.TB) (*testutils.Node, *forwardinggate.Gate) {
	t.Helper()
	var leader *testutils.Node
	testutils.Eventually(t, 5*time.Second, 10*time.Millisecond, func() bool {
		for _, n := range fc.Nodes() {
			if n.Raft.State() == raft.Leader && fc.base[n].Ready() {
				leader = n
				return true
			}
		}
		return false
	}, "a ready leader")
	return leader, fc.fwdGts[leader]
}
