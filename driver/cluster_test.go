package driver_test

import (
	"fmt"
	"io"
	"path/filepath"
	"time"

	hraft "github.com/hashicorp/raft"

	"github.com/fuchstim/literaft/apply"
	raftadapter "github.com/fuchstim/literaft/raft"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// raftNode bundles one in-memory hraft.Raft node with a real
// apply.Applier-backed FSM, mirroring raft/gate_test.go's testNode but
// using a real Materializer (not a spy) so these tests exercise actual
// on-disk SQLite behavior through driver.Driver end to end, not just raft
// plumbing. No Snapshotter is wired (SetSnapshotter is never called): these
// clusters are short-lived and write far fewer entries than hraft's default
// SnapshotThreshold, so FSM.Snapshot is never invoked -- same assumption
// raft/gate_test.go's own tests already make.
type raftNode struct {
	id     hraft.ServerID
	addr   hraft.ServerAddress
	trans  *hraft.InmemTransport
	raft   *hraft.Raft
	fsm    *raftadapter.FSM
	dbPath string
}

// fastConfig shrinks hraft's election/heartbeat timing so tests don't spend
// real wall-clock seconds waiting for a leader, mirroring
// raft/gate_test.go's fastConfig exactly.
func fastConfig(id hraft.ServerID) *hraft.Config {
	cfg := hraft.DefaultConfig()
	cfg.LocalID = id
	cfg.HeartbeatTimeout = 150 * time.Millisecond
	cfg.ElectionTimeout = 150 * time.Millisecond
	cfg.LeaderLeaseTimeout = 75 * time.Millisecond
	cfg.CommitTimeout = 10 * time.Millisecond
	cfg.LogOutput = io.Discard
	return cfg
}

// newRaftCluster builds n hraft nodes over InmemTransport, each with a real
// apply.Applier-backed FSM against its own file under dir, bootstrapped as
// a single cluster. Callers must shutdownRaftCluster(...) the result.
func newRaftCluster(dir string, n int, pageSize uint32) []*raftNode {
	GinkgoHelper()
	nodes := make([]*raftNode, n)
	for i := range nodes {
		id := hraft.ServerID(fmt.Sprintf("node%d", i))
		addr, trans := hraft.NewInmemTransportWithTimeout("", 50*time.Millisecond)
		dbPath := filepath.Join(dir, string(id)+".db")
		applier, err := apply.Open(dbPath, pageSize)
		Expect(err).NotTo(HaveOccurred())
		fsm := raftadapter.NewFSM(applier)
		nodes[i] = &raftNode{id: id, addr: addr, trans: trans, fsm: fsm, dbPath: dbPath}
	}
	for _, a := range nodes {
		for _, b := range nodes {
			if a != b {
				a.trans.Connect(b.addr, b.trans)
			}
		}
	}

	servers := make([]hraft.Server, len(nodes))
	for i, n := range nodes {
		servers[i] = hraft.Server{Suffrage: hraft.Voter, ID: n.id, Address: n.addr}
	}

	for _, n := range nodes {
		r, err := hraft.NewRaft(fastConfig(n.id), n.fsm,
			hraft.NewInmemStore(), hraft.NewInmemStore(), hraft.NewInmemSnapshotStore(), n.trans)
		Expect(err).NotTo(HaveOccurred())
		n.raft = r
	}
	Expect(nodes[0].raft.BootstrapCluster(hraft.Configuration{Servers: servers}).Error()).To(Succeed())

	return nodes
}

func shutdownRaftCluster(nodes []*raftNode) {
	for _, n := range nodes {
		_ = n.raft.Shutdown().Error()
	}
}

func findRaftLeader(nodes []*raftNode) *raftNode {
	for _, n := range nodes {
		if n.raft.State() == hraft.Leader {
			return n
		}
	}
	return nil
}

func otherRaftNode(nodes []*raftNode, skip *raftNode) *raftNode {
	for _, n := range nodes {
		if n != skip {
			return n
		}
	}
	return nil
}

// waitForRaftLeader waits for some node to report itself as raft leader
// (hraft.State() only -- driver-level Ready() is checked separately once a
// node is wrapped in a Driver, since that adds the gaining-leadership
// drain on top).
func waitForRaftLeader(nodes []*raftNode) *raftNode {
	GinkgoHelper()
	var leader *raftNode
	Eventually(func() bool {
		leader = findRaftLeader(nodes)
		return leader != nil
	}, 5*time.Second, 10*time.Millisecond).Should(BeTrue())
	return leader
}
