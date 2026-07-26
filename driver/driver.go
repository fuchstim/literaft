package driver

import (
	"github.com/google/uuid"
	sqlite3vfs "github.com/ncruces/go-sqlite3/vfs"

	"github.com/fuchstim/literaft/fsm"
	raftgate "github.com/fuchstim/literaft/internal/raft/gate"
	"github.com/fuchstim/literaft/internal/vfs"
)

type Driver struct {
	gate            *raftgate.Gate
	dbPath, vfsName string
}

func New(fsm *fsm.FSM, log raftgate.LogAdapter, opts ...Option) *Driver {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}

	vfsName := uuid.NewString()

	gate := raftgate.New(fsm, log, o.logger.Named("gate"))
	vfs.Register(vfsName, sqlite3vfs.Find("os"), gate, o.logger.Named("vfs"))

	return &Driver{gate, fsm.DBPath(), vfsName}
}

func (d *Driver) Close() {
	sqlite3vfs.Unregister(d.vfsName)
}

// LastRejection returns the concrete error from this Driver's most recently
// completed write proposal (nil if it succeeded, or if none has been made
// yet). This is the reliable way to recover whether a rejected write was a
// *log.NotLeaderError or a log.CatchingUpError (for a LogAdapter backed by
// a real cluster, e.g. *log.SingleWriterLog): that distinction doesn't
// reliably survive the round trip back through database/sql, which by
// then may report only a generic error.
func (d *Driver) LastRejection() error { return d.gate.LastRejection() }

// VFSName returns the name this Driver registered its gated VFS under.
func (d *Driver) VFSName() string { return d.vfsName }
