// Package testutils builds real, disk-backed literaft node clusters for
// tests. A real literaft node process wires a real raft transport/store, a
// real fsm.FSM, and a real driver.Driver into one process; this package
// generalizes that same wiring to n nodes across two tiers so package tests
// don't have to hand-wire raft themselves:
//
//   - NewInmemCluster: in-memory transport/log/stable/snapshot store, real
//     fsm.FSM per node under its own temp-dir SQLite file. Fast; no real
//     process restart is possible (nothing durable to restart from). For
//     Gate/FSM/driver-level tests that build their own gate.Gate or Driver
//     on top of the raw nodes.
//   - NewTCPCluster: real TCP transport and file snapshot store on disk
//     under a caller-provided directory, each node already wrapped in a
//     real driver.Driver + *sql.DB. The raft log/stable store is an
//     in-memory raftsqlite.Store by default (WithOnDiskRaftStore switches
//     it to a real file, for tests -- like sustained-throughput
//     benchmarks -- that would otherwise grow the raft log large enough to
//     exhaust the in-process WASM SQLite engine's memory). Supports
//     RestartNode. For cluster/restart/snapshot/REPL-level tests that want
//     the full production stack already assembled.
package testutils

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/hashicorp/raft"

	"github.com/fuchstim/literaft/driver"
	"github.com/fuchstim/literaft/fsm"
)

// TB is the subset of testing.TB (and Ginkgo's GinkgoTInterface, which
// satisfies it structurally) this package needs. Kept minimal and local
// rather than depending on either the testing or ginkgo package directly.
type TB interface {
	Helper()
	TempDir() string
	Fatalf(format string, args ...any)
}

// Node is one cluster member built by NewInmemCluster or NewTCPCluster.
type Node struct {
	ID     string
	Addr   raft.ServerAddress
	Raft   *raft.Raft
	FSM    *fsm.FSM
	DBPath string

	// Driver and DB are only populated by NewTCPCluster; NewInmemCluster
	// leaves them nil since its callers build their own Gate or Driver on top
	// of Raft/FSM with test-specific options.
	Driver *driver.Driver
	DB     *sql.DB

	// Transport is only populated by NewInmemCluster, for tests that need
	// to simulate a network partition (Connect/DisconnectAll) -- there is
	// no equivalent for NewTCPCluster's real raft.NetworkTransport.
	Transport *raft.InmemTransport

	shutdown func() error
}

// Shutdown tears this node down. Idempotent only if the caller makes it so;
// mirrors raft.Raft.Shutdown's own non-idempotent contract.
func (n *Node) Shutdown() error { return n.shutdown() }

// Cluster is a set of Nodes built by NewInmemCluster or NewTCPCluster.
type Cluster struct {
	t     TB
	nodes []*Node
}

// Nodes returns every member of the cluster, in the order they were
// started (initial members first, joiners appended in join order).
func (c *Cluster) Nodes() []*Node { return c.nodes }

// Leader waits for some node to report itself as raft leader (State()
// only -- it does not know about any Gate/Driver drain a caller may have
// layered on top; see Eventually for building a readiness check that does).
func (c *Cluster) Leader() *Node {
	c.t.Helper()
	var leader *Node
	Eventually(c.t, 10*time.Second, 20*time.Millisecond, func() bool {
		for _, n := range c.nodes {
			if n.Raft.State() == raft.Leader {
				leader = n
				return true
			}
		}
		return false
	}, "a raft leader to be elected")
	return leader
}

// Other returns some node in the cluster other than skip.
func (c *Cluster) Other(skip *Node) *Node {
	for _, n := range c.nodes {
		if n != skip {
			return n
		}
	}
	return nil
}

// Shutdown tears down every node in the cluster, best-effort (mirrors old
// test harnesses: teardown errors here are not the thing under test).
func (c *Cluster) Shutdown() {
	for _, n := range c.nodes {
		_ = n.Shutdown()
	}
}

// Eventually polls cond every interval until it returns true, failing the
// test via t.Fatalf if timeout elapses first. A small, dependency-free
// stand-in for Gomega's Eventually so this package doesn't force a testing
// framework on its callers.
func Eventually(t TB, timeout, interval time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("testutils: timed out after %s waiting for %s", timeout, msg)
		}
		time.Sleep(interval)
	}
}

// Consistently polls cond every interval for the whole of duration, failing
// the test via t.Fatalf on the first false. The dependency-free counterpart
// to Eventually, for asserting something stays true.
func Consistently(t TB, duration, interval time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		if !cond() {
			t.Fatalf("testutils: %s stopped holding", msg)
		}
		time.Sleep(interval)
	}
}

// options configures both cluster tiers; see the WithXxx functions below.
type Option func(*options)

type options struct {
	fsmOpts []fsm.Option

	// TCP tier only.
	applyTimeout      time.Duration
	snapshotThreshold uint64
	snapshotInterval  time.Duration
	trailingLogs      uint64
	logOutput         io.Writer
	onDiskRaftStore   bool
	forwarding        bool
}

func defaultOptions() options {
	return options{
		applyTimeout: 2 * time.Second,
		logOutput:    io.Discard,
	}
}

// WithFSMOptions passes opts straight through to every node's fsm.New call.
func WithFSMOptions(opts ...fsm.Option) Option {
	return func(o *options) { o.fsmOpts = opts }
}

// WithApplyTimeout bounds each raft.Apply call the cluster's Logs/Drivers
// (NewTCPCluster only) make. Defaults to 2s.
func WithApplyTimeout(d time.Duration) Option {
	return func(o *options) { o.applyTimeout = d }
}

// WithSnapshotThreshold, WithSnapshotInterval, and WithTrailingLogs
// (NewTCPCluster only) are wired straight onto raft.Config, overriding
// raft's own defaults so a test can force real, fast snapshotting instead
// of waiting on production-sized thresholds.
func WithSnapshotThreshold(n uint64) Option { return func(o *options) { o.snapshotThreshold = n } }
func WithSnapshotInterval(d time.Duration) Option {
	return func(o *options) { o.snapshotInterval = d }
}
func WithTrailingLogs(n uint64) Option { return func(o *options) { o.trailingLogs = n } }

// WithLogOutput sets where raft's own log output goes (both tiers).
// Defaults to io.Discard.
func WithLogOutput(w io.Writer) Option {
	return func(o *options) { o.logOutput = w }
}

// WithOnDiskRaftStore (NewTCPCluster only) backs the raft log/stable store
// with a real file under the node's data dir instead of the default
// in-memory raftsqlite.Store. Use this for tests that sustain heavy write
// volume for a while (e.g. a throughput benchmark): an in-memory store
// holds every raft log entry in the WASM SQLite engine's own memory with
// nothing to spill to disk, which a long enough burst can exhaust --
// observed as ncruces' driver panicking on SQLITE_NOMEM mid-statement and
// the node hanging forever afterward, not a clean error.
func WithOnDiskRaftStore() Option {
	return func(o *options) { o.onDiskRaftStore = true }
}

// WithForwarding (NewTCPCluster only) configures every node's gate with a
// leader transport, routing follower-originated writes to the leader over an
// in-process InmemForwardHub. Follower connections then accept writes (under
// the base-index check) instead of rejecting them.
func WithForwarding() Option {
	return func(o *options) { o.forwarding = true }
}

// fastRaftConfig shrinks raft's election/heartbeat timing so
// NewInmemCluster tests don't spend real wall-clock seconds waiting for a
// leader, while staying loose enough that scheduling jitter under test
// load doesn't itself trigger spurious re-elections.
func fastRaftConfig(id raft.ServerID, o options) *raft.Config {
	cfg := raft.DefaultConfig()
	cfg.LocalID = id
	cfg.HeartbeatTimeout = 150 * time.Millisecond
	cfg.ElectionTimeout = 150 * time.Millisecond
	cfg.LeaderLeaseTimeout = 75 * time.Millisecond
	cfg.CommitTimeout = 10 * time.Millisecond
	cfg.LogOutput = o.logOutput
	if o.snapshotThreshold != 0 {
		cfg.SnapshotThreshold = o.snapshotThreshold
	}
	if o.snapshotInterval != 0 {
		cfg.SnapshotInterval = o.snapshotInterval
	}
	if o.trailingLogs != 0 {
		cfg.TrailingLogs = o.trailingLogs
	}
	return cfg
}

// NewInmemCluster builds n literaft nodes sharing an in-memory RAFT
// transport/log/stable/snapshot store apiece, each backed by a real
// fsm.FSM under its own temp-dir SQLite file. Returns the bootstrapped
// cluster; callers should wait on Leader() before proposing anything.
func NewInmemCluster(t TB, n int, opts ...Option) *Cluster {
	t.Helper()
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}

	dir := t.TempDir()
	nodes := make([]*Node, n)
	for i := range nodes {
		id := fmt.Sprintf("n%d", i)
		addr, trans := raft.NewInmemTransportWithTimeout("", 50*time.Millisecond)
		dbPath := dir + "/" + id + ".db"
		f, err := fsm.New(dbPath, o.fsmOpts...)
		if err != nil {
			t.Fatalf("testutils: fsm.New(%s): %v", id, err)
		}
		nodes[i] = &Node{ID: id, Addr: addr, FSM: f, DBPath: dbPath, Transport: trans}
	}
	for _, a := range nodes {
		for _, b := range nodes {
			if a != b {
				a.Transport.Connect(b.Addr, b.Transport)
			}
		}
	}

	servers := make([]raft.Server, n)
	for i, node := range nodes {
		servers[i] = raft.Server{Suffrage: raft.Voter, ID: raft.ServerID(node.ID), Address: node.Addr}
	}

	for _, node := range nodes {
		r, err := raft.NewRaft(fastRaftConfig(raft.ServerID(node.ID), o), node.FSM,
			raft.NewInmemStore(), raft.NewInmemStore(), raft.NewInmemSnapshotStore(), node.Transport)
		if err != nil {
			t.Fatalf("testutils: raft.NewRaft(%s): %v", node.ID, err)
		}
		node.Raft = r
		fsmRef := node.FSM
		node.shutdown = func() error {
			return errors.Join(r.Shutdown().Error(), fsmRef.Close())
		}
	}
	if err := nodes[0].Raft.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil {
		t.Fatalf("testutils: BootstrapCluster: %v", err)
	}

	return &Cluster{t: t, nodes: nodes}
}
