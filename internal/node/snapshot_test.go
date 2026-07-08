package node_test

import (
	"fmt"
	"time"

	hraft "github.com/hashicorp/raft"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// docs/ROADMAP.md M6 "done when": a follower too far behind for normal log
// replication catches up via a snapshot instead, and ends up
// logically-equivalent to the leader.
//
// TrailingLogs and SnapshotThreshold are set low enough that, after the
// leader's automatic snapshot fires and compacts its log, a brand-new
// joiner (starting from index 0) cannot possibly catch up via normal
// AppendEntries replay -- its needed starting index no longer exists in the
// leader's log store. Only hraft's InstallSnapshot RPC (driving
// raft.FSM.Snapshot on the leader and raft.FSM.Restore on the joiner) can
// succeed, so a converging joiner here is real, end-to-end proof of the M6
// mechanism, not just "eventually caught up somehow".
var _ = Describe("snapshot-based catch-up (M6)", func() {
	It("catches up a far-behind joiner via InstallSnapshot instead of log replay", func() {
		h := &clusterHarness{
			dir:               GinkgoT().TempDir(),
			pageSize:          pageSizeProbe(),
			snapshotThreshold: 30,
			trailingLogs:      10,
			snapshotInterval:  200 * time.Millisecond,
		}
		defer h.shutdown()

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

		leader := h.leader()
		Expect(leader.DB().Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")).To(Succeed())

		const rows = 120
		for i := 0; i < rows; i++ {
			Eventually(func() error {
				leader = h.leader()
				return leader.DB().Exec(fmt.Sprintf("INSERT INTO t (v) VALUES ('row%d')", i))
			}, 5*time.Second, 20*time.Millisecond).Should(Succeed())
		}

		// Give the leader's automatic snapshot goroutine (SnapshotInterval
		// ticks every 100ms above) time to fire and compact its log well
		// past the row count before the joiner ever asks for anything --
		// otherwise the joiner might get lucky and catch up via plain
		// replay before compaction happens, which would prove nothing about
		// InstallSnapshot specifically.
		Eventually(func() uint64 {
			return leader.Raft().LastIndex()
		}, 5*time.Second, 50*time.Millisecond).Should(BeNumerically(">", uint64(rows)))
		time.Sleep(1 * time.Second)

		joinerAddr := freeTCPAddr()
		joiner := h.startNode("joiner", joinerAddr, nil)

		Expect(leader.Raft().AddVoter(hraft.ServerID("joiner"), hraft.ServerAddress(joinerAddr), 0, 5*time.Second).Error()).
			To(Succeed())

		Eventually(func() (int64, error) { return rowCount(joiner) }, 10*time.Second, 20*time.Millisecond).Should(Equal(int64(rows)))
		Expect(queryText(joiner.DB(), "PRAGMA integrity_check")).To(Equal("ok"))
	})
})
