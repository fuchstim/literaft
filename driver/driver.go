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
