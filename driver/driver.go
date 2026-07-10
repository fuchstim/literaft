package driver

import (
	"github.com/google/uuid"
	sqlite3vfs "github.com/ncruces/go-sqlite3/vfs"

	"github.com/hashicorp/raft"

	"github.com/fuchstim/literaft/fsm"
	raftgate "github.com/fuchstim/literaft/internal/raft/gate"
	"github.com/fuchstim/literaft/internal/vfs"
)

type Driver struct {
	gate            *raftgate.Gate
	dbPath, vfsName string
}

func New(r *raft.Raft, fsm *fsm.FSM, opts ...Option) *Driver {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}

	vfsName := uuid.NewString()

	gate := raftgate.New(r, fsm.NodeID(), o.applyTimeout)
	vfs.Register(vfsName, sqlite3vfs.Find("os"), gate, fsm.PageSize())

	return &Driver{gate, fsm.DBPath(), vfsName}
}

func (d *Driver) Close() {
	d.gate.Close()
	sqlite3vfs.Unregister(d.vfsName)
}

// Ready reports whether this Driver's raft is currently the leader and has
// finished draining its apply backlog for the current term. A client write
// attempted before Ready returns true will fail its gate with a
// raftgate.CatchingUpError.
func (d *Driver) Ready() bool { return d.gate.Ready() }

// LastRejection returns the concrete error from this Driver's most recently
// completed write proposal (nil if it succeeded, or if none has been made
// yet). This is the reliable way to recover whether a rejected write was a
// *raftgate.NotLeaderError or a raftgate.CatchingUpError: that distinction
// doesn't reliably survive the round trip back through database/sql, which
// by then may report only a generic error.
func (d *Driver) LastRejection() error { return d.gate.LastRejection() }

// VFSName returns the name this Driver registered its gated VFS under.
func (d *Driver) VFSName() string { return d.vfsName }
