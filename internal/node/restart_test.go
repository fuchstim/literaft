package node_test

import (
	"fmt"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/ncruces/go-sqlite3"

	"github.com/fuchstim/literaft/internal/node"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// docs/ROADMAP.md M7 "done when" (crash/restart recovery): a node whose
// process restarts against its own prior DataDir/DBPath -- distinct from
// cluster_test.go's "killed and permanently retired" or snapshot_test.go's
// "brand new joiner" cases, neither of which reuses existing on-disk state
// -- ends up logically identical to the rest of the cluster, including to
// an external reader, regardless of whether it was a follower or the
// leader, and regardless of whether it had ever taken a local RAFT
// snapshot yet.
//
// assertRestarted checks the bar every scenario below holds itself to:
// correct row count on the restarted node itself, a clean integrity_check,
// and the same from a completely plain, unmodified-VFS external reader
// (backend_test.go's pattern) -- proving the on-disk files are actually
// correct, not just readable through the same VFS that wrote them.
func assertRestarted(restarted *node.Node, dbPath string, wantRows int64) {
	GinkgoHelper()
	Eventually(func() (int64, error) { return rowCount(restarted) }, 10*time.Second, 20*time.Millisecond).Should(Equal(wantRows))
	Expect(nodeQueryText(restarted, "PRAGMA integrity_check")).To(Equal("ok"))

	external, err := sqlite3.Open(dbPath)
	Expect(err).NotTo(HaveOccurred())
	defer external.Close()
	Expect(queryText(external, "PRAGMA integrity_check")).To(Equal("ok"))
	Expect(queryInt(external, "SELECT count(*) FROM t")).To(Equal(wantRows))
}

var _ = Describe("node restart (M7)", func() {
	It("recovers a follower restarted before any local snapshot has been taken", func() {
		h := startCluster(3)
		defer h.shutdown()

		leader := h.leader()
		Expect(nodeExec(leader, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")).To(Succeed())
		for i := 0; i < 20; i++ {
			Expect(nodeExec(leader, fmt.Sprintf("INSERT INTO t (v) VALUES ('row%d')", i))).To(Succeed())
		}

		var follower *node.Node
		for _, n := range h.nodes {
			if n != leader {
				follower = n
				break
			}
		}
		Eventually(func() (int64, error) { return rowCount(follower) }, 5*time.Second, 20*time.Millisecond).Should(Equal(int64(20)))

		restarted := h.restart(h.indexOf(follower))
		assertRestarted(restarted, h.cfgs[h.indexOf(restarted)].DBPath, 20)
	})

	It("recovers a leader restarted before any local snapshot has been taken", func() {
		h := startCluster(3)
		defer h.shutdown()

		leader := h.leader()
		Expect(nodeExec(leader, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")).To(Succeed())
		for i := 0; i < 20; i++ {
			Expect(nodeExec(leader, fmt.Sprintf("INSERT INTO t (v) VALUES ('row%d')", i))).To(Succeed())
		}

		// The leader's own pre-restart -wal holds real-SQLite-written
		// commit-path frames that were never previously touched by
		// Materializer at all (ADR-005 self-skip) -- restart must rebuild
		// those via replay just as faithfully as a follower's.
		idx := h.indexOf(leader)
		restarted := h.restart(idx)
		assertRestarted(restarted, h.cfgs[idx].DBPath, 20)
	})

	// startSnapshottingCluster mirrors snapshot_test.go's low thresholds so
	// a local RAFT snapshot reliably exists by the time restart runs --
	// the case that specifically exercises FSM.Snapshotter being wired
	// before hraft.NewRaft (docs/ROADMAP.md M7's fix alongside the
	// -wal/-shm discard: hraft.NewRaft synchronously restores this node's
	// latest local snapshot on startup).
	startSnapshottingCluster := func() *clusterHarness {
		GinkgoHelper()
		h := &clusterHarness{
			dir:               GinkgoT().TempDir(),
			pageSize:          pageSizeProbe(),
			snapshotThreshold: 30,
			trailingLogs:      10,
			snapshotInterval:  200 * time.Millisecond,
		}
		addrs := make([]string, 3)
		for i := range addrs {
			addrs[i] = freeTCPAddr()
		}
		servers := make([]hraft.Server, 3)
		for i, addr := range addrs {
			servers[i] = hraft.Server{Suffrage: hraft.Voter, ID: hraft.ServerID(fmt.Sprintf("n%d", i)), Address: hraft.ServerAddress(addr)}
		}
		for i, addr := range addrs {
			h.startNode(fmt.Sprintf("n%d", i), addr, servers)
		}
		return h
	}

	writeRowsAndForceSnapshot := func(h *clusterHarness, rows int) *node.Node {
		GinkgoHelper()
		leader := h.leader()
		Expect(nodeExec(leader, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")).To(Succeed())
		for i := 0; i < rows; i++ {
			Eventually(func() error {
				leader = h.leader()
				return nodeExec(leader, fmt.Sprintf("INSERT INTO t (v) VALUES ('row%d')", i))
			}, 5*time.Second, 20*time.Millisecond).Should(Succeed())
		}
		// Give the snapshot goroutine (SnapshotInterval ticks every 200ms)
		// time to actually fire at least once, on every node, before
		// restarting anything -- see snapshot_test.go for the same wait.
		Eventually(func() uint64 {
			return leader.Raft().LastIndex()
		}, 5*time.Second, 50*time.Millisecond).Should(BeNumerically(">", uint64(rows)))
		time.Sleep(1 * time.Second)
		return leader
	}

	It("recovers a follower restarted after it has taken a local snapshot", func() {
		h := startSnapshottingCluster()
		defer h.shutdown()

		leader := writeRowsAndForceSnapshot(h, 40)

		var follower *node.Node
		for _, n := range h.nodes {
			if n != leader {
				follower = n
				break
			}
		}
		Eventually(func() (int64, error) { return rowCount(follower) }, 5*time.Second, 20*time.Millisecond).Should(Equal(int64(40)))

		idx := h.indexOf(follower)
		restarted := h.restart(idx)
		assertRestarted(restarted, h.cfgs[idx].DBPath, 40)
	})

	It("recovers a leader restarted after it has taken a local snapshot", func() {
		h := startSnapshottingCluster()
		defer h.shutdown()

		leader := writeRowsAndForceSnapshot(h, 40)

		idx := h.indexOf(leader)
		restarted := h.restart(idx)
		assertRestarted(restarted, h.cfgs[idx].DBPath, 40)
	})
})
