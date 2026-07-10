package testutils_test

import (
	"fmt"
	"time"

	"github.com/hashicorp/raft"

	"github.com/fuchstim/literaft/internal/testutils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// A follower too far behind for normal log replication catches up via a
// snapshot instead, and ends up logically-equivalent to the leader.
//
// WithTrailingLogs and WithSnapshotThreshold are set low enough that, after
// the leader's automatic snapshot fires and compacts its log, a brand-new
// joiner (starting from index 0) cannot possibly catch up via normal
// AppendEntries replay -- its needed starting index no longer exists in the
// leader's log store. Only hraft's InstallSnapshot RPC (driving
// raft.FSM.Snapshot on the leader and raft.FSM.Restore on the joiner) can
// succeed, so a converging joiner here is real, end-to-end proof of the
// snapshot mechanism, not just "eventually caught up somehow".
var _ = Describe("snapshot-based catch-up", func() {
	It("catches up a far-behind joiner via InstallSnapshot instead of log replay", func() {
		c := testutils.NewTCPCluster(GinkgoT(), GinkgoT().TempDir(), 3,
			testutils.WithSnapshotThreshold(30),
			testutils.WithTrailingLogs(10),
			testutils.WithSnapshotInterval(200*time.Millisecond),
		)
		defer c.Shutdown()

		leader := c.ReadyLeader()
		Expect(nodeExec(leader, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")).To(Succeed())

		const rows = 120
		for i := 0; i < rows; i++ {
			testutils.Eventually(GinkgoT(), 5*time.Second, 20*time.Millisecond, func() bool {
				leader = c.ReadyLeader()
				return nodeExec(leader, fmt.Sprintf("INSERT INTO t (v) VALUES ('row%d')", i)) == nil
			}, "leader to accept the next insert")
		}

		// Give the leader's automatic snapshot goroutine time to fire and
		// compact its log well past the row count before the joiner ever
		// asks for anything -- otherwise the joiner might get lucky and
		// catch up via plain replay before compaction happens, which would
		// prove nothing about InstallSnapshot specifically.
		testutils.Eventually(GinkgoT(), 5*time.Second, 50*time.Millisecond, func() bool {
			return leader.Raft.LastIndex() > uint64(rows)
		}, "the leader's log to grow past the row count (snapshot compaction happened)")
		time.Sleep(1 * time.Second)

		joiner := c.Join(GinkgoT(), "joiner")
		Expect(leader.Raft.AddVoter(raft.ServerID(joiner.ID), joiner.Addr, 0, 5*time.Second).Error()).To(Succeed())

		testutils.Eventually(GinkgoT(), 10*time.Second, 20*time.Millisecond, func() bool {
			n, err := rowCount(joiner)
			return err == nil && n == int64(rows)
		}, "the joiner to catch up via InstallSnapshot")
		Expect(nodeQueryText(joiner, "PRAGMA integrity_check")).To(Equal("ok"))
	})
})
