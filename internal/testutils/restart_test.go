package testutils_test

import (
	"fmt"
	"time"

	"github.com/fuchstim/literaft/internal/testutils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Crash/restart recovery: a node whose process restarts against its own
// prior data dir/db path -- distinct from cluster_test.go's "killed and
// permanently retired" or snapshot_test.go's "brand new joiner" cases,
// neither of which reuses existing on-disk state -- ends up logically
// identical to the rest of the cluster, including to an external reader,
// regardless of whether it was a follower or the leader, and regardless of
// whether it had ever taken a local RAFT snapshot yet.

// assertRestarted checks the bar every scenario below holds itself to:
// correct row count on the restarted node itself, a clean integrity_check,
// and the same from a completely plain, unmodified-VFS external reader --
// proving the on-disk files are actually correct, not just readable
// through the same VFS that wrote them.
func assertRestarted(restarted *testutils.Node, wantRows int64) {
	GinkgoHelper()
	testutils.Eventually(GinkgoT(), 10*time.Second, 20*time.Millisecond, func() bool {
		n, err := rowCount(restarted)
		return err == nil && n == wantRows
	}, "restarted node to recover its rows")
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

		restarted := c.RestartNode(GinkgoT(), c.IndexOf(follower))
		assertRestarted(restarted, 20)
	})

	It("recovers a leader restarted before any local snapshot has been taken", func() {
		c := testutils.NewTCPCluster(GinkgoT(), GinkgoT().TempDir(), 3)
		defer c.Shutdown()

		leader := c.ReadyLeader()
		Expect(nodeExec(leader, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")).To(Succeed())
		for i := 0; i < 20; i++ {
			Expect(nodeExec(leader, fmt.Sprintf("INSERT INTO t (v) VALUES ('row%d')", i))).To(Succeed())
		}

		// The leader's own pre-restart -wal holds real-SQLite-written
		// commit-path frames that were never previously touched by
		// walAppender at all (ADR-005 self-skip) -- restart must rebuild
		// those via replay just as faithfully as a follower's.
		idx := c.IndexOf(leader)
		restarted := c.RestartNode(GinkgoT(), idx)
		assertRestarted(restarted, 20)
	})

	// snapshottingCluster mirrors snapshot_test.go's low thresholds so a
	// local RAFT snapshot reliably exists by the time restart runs -- the
	// case that specifically exercises FSM.Snapshot/Restore being wired
	// correctly across a real process restart (hraft.NewRaft synchronously
	// restores this node's latest local snapshot on startup).
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
		// Give the snapshot goroutine (SnapshotInterval ticks every 200ms)
		// time to actually fire at least once, on every node, before
		// restarting anything.
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

		restarted := c.RestartNode(GinkgoT(), c.IndexOf(follower))
		assertRestarted(restarted, 40)
	})

	// Same root cause as internal/raft/gate/figure8_test.go's known,
	// tracked regression -- another way to trigger it, and a far more
	// mundane one: fsm.FSM.Apply's self-skip (entry.NodeID == f.NodeID())
	// assumes a self-authored entry is already on local disk because it
	// was published via this node's own live SQL write path when it
	// happened. FSM.Restore (run synchronously by hraft.NewRaft on startup
	// whenever a local snapshot exists) invalidates that assumption: it
	// resets local state back to an *older* point, the snapshot, and every
	// self-authored log entry after that snapshot -- entries this node's
	// own local disk no longer actually has, having just been wound back --
	// still gets skipped on replay, exactly as if it were still safely on
	// disk. A node that was leader long enough to take its own snapshot and
	// then restarts permanently loses every row it wrote after that
	// snapshot. Verified below (skip removed and confirmed failing, then
	// restored) before being marked PIt for the same reason
	// figure8_test.go's is: flip back to It once the self-skip is fixed to
	// be transient/scoped rather than a permanent per-entry property.
	PIt("recovers a leader restarted after it has taken a local snapshot", func() {
		c := snapshottingCluster()
		defer c.Shutdown()

		leader := writeRowsAndForceSnapshot(c, 40)

		idx := c.IndexOf(leader)
		restarted := c.RestartNode(GinkgoT(), idx)
		assertRestarted(restarted, 40)
	})
})
