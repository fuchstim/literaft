package testutils

import (
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"

	"github.com/fuchstim/literaft/driver"
	"github.com/fuchstim/literaft/raft/fsm"
	forwardinggate "github.com/fuchstim/literaft/raft/gate/forwarding"
	leadergate "github.com/fuchstim/literaft/raft/gate/leader"
	"github.com/fuchstim/literaft/raftsqlite"
)

// TCPCluster is a Cluster built by NewTCPCluster; it additionally supports
// Join and RestartNode, which need the on-disk spec NewInmemCluster's
// in-memory tier has no equivalent of. dir is the directory every node's
// data lives under; specs are each node's original spec (RestartNode
// rebuilds against the exact same spec, in particular the same bind
// address -- the rest of the cluster's raft.Configuration and its own
// persisted log still name that address).
type TCPCluster struct {
	Cluster
	dir   string
	o     options
	specs []nodeSpec

	// hub routes forwarded writes between nodes in-process; non-nil only when
	// WithForwarding was passed. Shared by every node (initial, joined, or
	// restarted) so a follower can reach whichever node is leader.
	hub *InmemForwardHub
}

type nodeSpec struct {
	id, addr, dataDir, dbPath string
	bootstrap                 []raft.Server // non-nil only for a cluster's initial members
}

// FreeTCPAddr grabs an OS-assigned loopback port by briefly binding and
// releasing it, then returns "127.0.0.1:<port>".
func FreeTCPAddr(t TB) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("testutils: FreeTCPAddr: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("testutils: FreeTCPAddr: %v", err)
	}
	return addr
}

// NewTCPCluster builds n literaft nodes wired exactly like a real node
// process, minus one deliberate default: real TCP transport and file
// snapshot store on disk under dir, a real driver.Driver + *sql.DB
// registered under a unique sql.Register alias, but an in-memory raft
// log/stable store (raftsqlite.New(":memory:")) rather than an on-disk one,
// for test speed -- see WithOnDiskRaftStore for tests that need the latter.
// Supports RestartNode for crash/restart-recovery tests, which
// NewInmemCluster's in-memory stores can't (there'd still be a snapshot
// store and FSM db durable enough to restart from, even with the raft log
// itself gone -- hraft resyncs the rest from the cluster).
func NewTCPCluster(t TB, dir string, n int, opts ...Option) *TCPCluster {
	t.Helper()
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}

	specs := make([]nodeSpec, n)
	for i := range specs {
		id := fmt.Sprintf("n%d", i)
		specs[i] = nodeSpec{
			id:      id,
			addr:    FreeTCPAddr(t),
			dataDir: filepath.Join(dir, id),
			dbPath:  filepath.Join(dir, id+".db"),
		}
	}
	servers := make([]raft.Server, n)
	for i, s := range specs {
		servers[i] = raft.Server{Suffrage: raft.Voter, ID: raft.ServerID(s.id), Address: raft.ServerAddress(s.addr)}
	}
	for i := range specs {
		specs[i].bootstrap = servers
	}

	var hub *InmemForwardHub
	if o.forwarding {
		hub = NewInmemForwardHub()
	}

	nodes := make([]*Node, n)
	for i, s := range specs {
		nodes[i] = startTCPNode(t, s, o, hub)
	}

	return &TCPCluster{Cluster{t: t, nodes: nodes}, dir, o, specs, hub}
}

// ReadyLeader waits for a node that's both the raft leader and has
// finished its Log's gaining-leadership drain -- unlike Cluster.Leader,
// safe to immediately issue a write against.
func (c *TCPCluster) ReadyLeader() *Node {
	c.t.Helper()
	var leader *Node
	Eventually(c.t, 10*time.Second, 20*time.Millisecond, func() bool {
		for _, n := range c.nodes {
			if n.Raft.State() == raft.Leader && n.Gate.Ready() {
				leader = n
				return true
			}
		}
		return false
	}, "a ready leader")
	return leader
}

// Join starts a brand new node under this cluster's directory, not yet
// part of any raft configuration (Bootstrap left unset) -- callers add it
// via some existing member's Raft.AddVoter.
func (c *TCPCluster) Join(t TB, id string) *Node {
	t.Helper()
	s := nodeSpec{
		id:      id,
		addr:    FreeTCPAddr(t),
		dataDir: filepath.Join(c.dir, id),
		dbPath:  filepath.Join(c.dir, id+".db"),
	}
	n := startTCPNode(t, s, c.o, c.hub)
	c.nodes = append(c.nodes, n)
	c.specs = append(c.specs, s)
	return n
}

// RestartNode simulates a process restart of node i: shuts it down and
// starts a fresh Node against its exact original spec (same data dir, db
// path, and bind address).
func (c *TCPCluster) RestartNode(t TB, i int) *Node {
	t.Helper()
	if err := c.nodes[i].Shutdown(); err != nil {
		t.Fatalf("testutils: RestartNode: shutting down old node: %v", err)
	}
	n := startTCPNode(t, c.specs[i], c.o, c.hub)
	c.nodes[i] = n
	return n
}

// IndexOf returns n's position among Nodes(), for passing to RestartNode.
func (c *TCPCluster) IndexOf(n *Node) int {
	for i, cn := range c.nodes {
		if cn == n {
			return i
		}
	}
	c.t.Fatalf("testutils: IndexOf: node not found in cluster")
	return -1
}

// startTCPNode brings up one node much as a real node process does: a real
// TCP transport and file snapshot store under s.dataDir, an in-memory
// raftsqlite log/stable store by default (o.onDiskRaftStore switches it to
// a real file under s.dataDir instead), a real fsm.FSM over s.dbPath, a
// real hraft.Raft (which
// bootstraps if s.bootstrap is set -- tolerating raft.ErrCantBootstrap so
// restarting an already-bootstrapped node is a harmless no-op), a write
// gate wrapping it (leadergate.Gate, or forwardinggate.Gate wrapping one
// when hub is non-nil), and a driver.Driver registered under a fresh
// database/sql alias.
func startTCPNode(t TB, s nodeSpec, o options, hub *InmemForwardHub) *Node {
	t.Helper()

	transport, err := raft.NewTCPTransport(s.addr, nil, 3, 10*time.Second, o.logOutput)
	if err != nil {
		t.Fatalf("testutils: raft.NewTCPTransport(%s): %v", s.addr, err)
	}

	if err := os.MkdirAll(s.dataDir, 0o755); err != nil {
		t.Fatalf("testutils: MkdirAll(%s): %v", s.dataDir, err)
	}

	raftStorePath := ":memory:"
	if o.onDiskRaftStore {
		raftStorePath = filepath.Join(s.dataDir, "raft.db")
	}
	raftStore, err := raftsqlite.New(raftStorePath)
	if err != nil {
		t.Fatalf("testutils: raftsqlite.New: %v", err)
	}

	snapStore, err := raft.NewFileSnapshotStore(s.dataDir, 2, o.logOutput)
	if err != nil {
		t.Fatalf("testutils: raft.NewFileSnapshotStore: %v", err)
	}

	f, err := fsm.New(s.dbPath, o.fsmOpts...)
	if err != nil {
		t.Fatalf("testutils: fsm.New(%s): %v", s.id, err)
	}

	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(s.id)
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

	r, err := raft.NewRaft(cfg, f, raftStore, raftStore, snapStore, transport)
	if err != nil {
		t.Fatalf("testutils: raft.NewRaft(%s): %v", s.id, err)
	}

	if s.bootstrap != nil {
		err := r.BootstrapCluster(raft.Configuration{Servers: s.bootstrap}).Error()
		if err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
			t.Fatalf("testutils: BootstrapCluster(%s): %v", s.id, err)
		}
	}

	// With forwarding enabled, the write gate is a forwardinggate.Gate wrapping
	// a leadergate.Gate; on a follower it forwards writes to the leader
	// through the shared in-process hub instead of rejecting them.
	var g Gate
	if hub != nil {
		g = forwardinggate.New(r, f, hub.Transport(raft.ServerAddress(s.addr)),
			forwardinggate.WithForwardTimeout(o.applyTimeout), forwardinggate.WithHandlerLockTimeout(o.applyTimeout))
	} else {
		g = leadergate.New(r, f, leadergate.WithApplyTimeout(o.applyTimeout))
	}

	drv := driver.New(f, g)
	alias := "literaft-testutils-" + uuid.NewString()
	sql.Register(alias, drv)
	db, err := sql.Open(alias, "")
	if err != nil {
		t.Fatalf("testutils: sql.Open(%s): %v", s.id, err)
	}

	return &Node{
		ID:     s.id,
		Addr:   raft.ServerAddress(s.addr),
		Raft:   r,
		FSM:    f,
		DBPath: s.dbPath,
		Driver: drv,
		Gate:   g,
		DB:     db,
		shutdown: func() error {
			var errs []error
			if err := db.Close(); err != nil {
				errs = append(errs, err)
			}
			drv.Close()
			g.Close()
			if err := r.Shutdown().Error(); err != nil {
				errs = append(errs, fmt.Errorf("raft shutdown: %w", err))
			}
			if err := f.Close(); err != nil {
				errs = append(errs, fmt.Errorf("fsm close: %w", err))
			}
			if err := raftStore.Close(); err != nil {
				errs = append(errs, fmt.Errorf("closing raft log store: %w", err))
			}
			if err := transport.Close(); err != nil {
				errs = append(errs, fmt.Errorf("closing raft transport: %w", err))
			}
			return errors.Join(errs...)
		},
	}
}
