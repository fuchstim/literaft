package driver

import (
	"sync"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	sqlite3vfs "github.com/ncruces/go-sqlite3/vfs"

	"github.com/fuchstim/literaft/fsm"
	"github.com/fuchstim/literaft/internal/gate"
	"github.com/fuchstim/literaft/internal/vfs"
	"github.com/fuchstim/literaft/internal/wal"
)

type Driver struct {
	gate            *recordinggate
	dbPath, vfsName string
}

func New(r *raft.Raft, f *fsm.FSM, opts ...Option) *Driver {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}

	vfsName := uuid.NewString()

	gate := gate.New(r, f, o.logger, o.leaderTransport, o.applyTimeout, o.forwardTimeout, o.handlerLockTimeout)
	rg := &recordinggate{Gate: gate}
	vfs.Register(vfsName, sqlite3vfs.Find("os"), rg, o.logger.Named("vfs"))

	return &Driver{rg, f.DBPath(), vfsName}
}

func (d *Driver) Close() {
	sqlite3vfs.Unregister(d.vfsName)
	d.gate.Close()
}

func (d *Driver) Ready() bool          { return d.gate.Ready() }
func (d *Driver) LastRejection() error { return d.gate.LastRejection() }
func (d *Driver) VFSName() string      { return d.vfsName }

type recordinggate struct {
	*gate.Gate

	lastRejectionMu sync.Mutex
	lastRejection   error
}

func (g *recordinggate) ProposeTransaction(frames []*wal.Frame) error {
	err := g.Gate.ProposeTransaction(frames)
	if err != nil {
		g.lastRejectionMu.Lock()
		defer g.lastRejectionMu.Unlock()

		g.lastRejection = err
	}

	return err
}

func (g *recordinggate) LastRejection() error {
	g.lastRejectionMu.Lock()
	defer g.lastRejectionMu.Unlock()

	return g.lastRejection
}
