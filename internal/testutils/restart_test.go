package testutils_test

import (
	"fmt"
	"time"

	"github.com/fuchstim/literaft/internal/testutils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Crash/restart recovery: a node whose process restarts against its own
// prior data dir/db path -- distinct from a node that's killed and
// permanently retired, or a brand new joiner, neither of which reuses
// existing on-disk state -- ends up logically identical to the rest of
// the cluster, including to an external reader, regardless of whether it
// was a follower or the leader, and regardless of whether it had ever
// taken a local RAFT snapshot yet.

// assertRestarted checks the bar every scenario below holds itself to:
// correct row count on the restarted node itself, a clean integrity_check,
// and the same from a completely plain, unmodified-VFS external reader --
// proving the on-disk files are actually correct, not just readable
// through the same VFS that wrote them.
//
// It waits for the node's applied index to reach minApplied, not just for the
// row count to hit wantRows. A restart recovers the row count almost at once
// from SQLite's own WAL recovery, but the replay that follows re-materializes
// every already-committed transaction as fresh physical-redo frames, each of
// which briefly reverts its pages to an older image -- so the visible count
// dips and climbs back, for local and external readers alike, until replay
// drains. minApplied is where replay ends; the row count alone is already
// right in the recovered-but-not-yet-replayed state, so it can't tell that
// window from a settled one.
func assertRestarted(restarted *testutils.Node, wantRows int64, minApplied uint64) {
	GinkgoHelper()
	testutils.Eventually(GinkgoT(), 10*time.Second, 20*time.Millisecond, func() bool {
		if restarted.FSM.LastAppliedIndex() < minApplied {
			return false
		}
		n, err := rowCount(restarted)
		return err == nil && n == wantRows
	}, "restarted node to finish replaying and recover its rows")
	Expect(nodeQueryText(restarted, "PRAGMA integrity_check")).To(Equal("ok"))

	Expect(externalQueryText(restarted.DBPath, "PRAGMA integrity_check")).To(Equal("ok"))
	Expect(externalQueryInt(restarted.DBPath, "SELECT count(*) FROM t")).To(Equal(wantRows))
}

var _ = Describe("node restart", func() {
	It("recovers a follower restarted before any local snapshot has been taken", func() {
		c := testutils.NewTCPCluster(GinkgoT(), GinkgoT().TempDir(), 3)
		defer c.Shutdown()

		leader := c.ReadyLeader()
		Expect(nodeExec(leader, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")).To(Succeed())
		for i := 0; i < 20; i++ {
			Expect(nodeExec(leader, fmt.Sprintf("INSERT INTO t (v) VALUES ('row%d')", i))).To(Succeed())
		}

		var follower *testutils.Node
		for _, n := range c.Nodes() {
			if n != leader {
				follower = n
				break
			}
		}
		testutils.Eventually(GinkgoT(), 5*time.Second, 20*time.Millisecond, func() bool {
			n, err := rowCount(follower)
			return err == nil && n == 20
		}, "follower to catch up")

		// The applied index the restart must replay back up to, captured
		// while quiescent so no in-flight write moves it.
		target := follower.FSM.LastAppliedIndex()
		restarted := c.RestartNode(GinkgoT(), c.IndexOf(follower))
		assertRestarted(restarted, 20, target)
	})

	It("recovers a leader restarted before any local snapshot has been taken", func() {
		c := testutils.NewTCPCluster(GinkgoT(), GinkgoT().TempDir(), 3)
		defer c.Shutdown()

		leader := c.ReadyLeader()
		Expect(nodeExec(leader, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")).To(Succeed())
		for i := 0; i < 20; i++ {
			Expect(nodeExec(leader, fmt.Sprintf("INSERT INTO t (v) VALUES ('row%d')", i))).To(Succeed())
		}

		// The leader's own pre-restart -wal holds commit frames SQLite wrote
		// directly, never routed through the follower-apply write path --
		// restart must rebuild those via replay just as faithfully as a
		// follower's.
		target := leader.FSM.LastAppliedIndex()
		idx := c.IndexOf(leader)
		restarted := c.RestartNode(GinkgoT(), idx)
		assertRestarted(restarted, 20, target)
	})

	// snapshottingCluster uses low enough snapshot thresholds that a local
	// RAFT snapshot reliably exists by the time restart runs -- the case
	// that specifically exercises snapshot capture and restore being wired
	// correctly across a real process restart, which synchronously restores
	// this node's latest local snapshot on startup.
	snapshottingCluster := func() *testutils.TCPCluster {
		GinkgoHelper()
		return testutils.NewTCPCluster(GinkgoT(), GinkgoT().TempDir(), 3,
			testutils.WithSnapshotThreshold(30),
			testutils.WithTrailingLogs(10),
			testutils.WithSnapshotInterval(200*time.Millisecond),
		)
	}

	writeRowsAndForceSnapshot := func(c *testutils.TCPCluster, rows int) *testutils.Node {
		GinkgoHelper()
		leader := c.ReadyLeader()
		Expect(nodeExec(leader, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")).To(Succeed())
		for i := 0; i < rows; i++ {
			testutils.Eventually(GinkgoT(), 5*time.Second, 20*time.Millisecond, func() bool {
				leader = c.ReadyLeader()
				return nodeExec(leader, fmt.Sprintf("INSERT INTO t (v) VALUES ('row%d')", i)) == nil
			}, "leader to accept the next insert")
		}
		// Give the snapshot goroutine (it ticks on the configured 200ms
		// interval) time to actually fire at least once, on every node,
		// before restarting anything.
		testutils.Eventually(GinkgoT(), 5*time.Second, 50*time.Millisecond, func() bool {
			return leader.Raft.LastIndex() > uint64(rows)
		}, "the leader's log to grow past the row count (snapshot compaction happened)")
		time.Sleep(1 * time.Second)
		return leader
	}

	It("recovers a follower restarted after it has taken a local snapshot", func() {
		c := snapshottingCluster()
		defer c.Shutdown()

		leader := writeRowsAndForceSnapshot(c, 40)

		var follower *testutils.Node
		for _, n := range c.Nodes() {
			if n != leader {
				follower = n
				break
			}
		}
		testutils.Eventually(GinkgoT(), 5*time.Second, 20*time.Millisecond, func() bool {
			n, err := rowCount(follower)
			return err == nil && n == 40
		}, "follower to catch up")

		target := follower.FSM.LastAppliedIndex()
		restarted := c.RestartNode(GinkgoT(), c.IndexOf(follower))
		assertRestarted(restarted, 40, target)
	})

	// The snapshot restore that runs on startup whenever a local snapshot
	// exists resets local state back to the snapshot, so every self-authored
	// log entry after it must replay normally on restart rather than being
	// skipped as already-applied.
	It("recovers a leader restarted after it has taken a local snapshot", func() {
		c := snapshottingCluster()
		defer c.Shutdown()

		leader := writeRowsAndForceSnapshot(c, 40)

		target := leader.FSM.LastAppliedIndex()
		idx := c.IndexOf(leader)
		restarted := c.RestartNode(GinkgoT(), idx)
		assertRestarted(restarted, 40, target)
	})
})
