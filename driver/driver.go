package driver

import (
	"sync"

	"github.com/google/uuid"
	sqlite3vfs "github.com/ncruces/go-sqlite3/vfs"

	"github.com/fuchstim/literaft/internal/vfs"
	"github.com/fuchstim/literaft/internal/wal"
	"github.com/fuchstim/literaft/raft/fsm"
)

type Driver struct {
	gate            *recordingGate
	dbPath, vfsName string
}

func New(f *fsm.FSM, gate vfs.Gate, opts ...Option) *Driver {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}

	vfsName := uuid.NewString()

	rg := &recordingGate{gate: gate}
	vfs.Register(vfsName, sqlite3vfs.Find("os"), rg, o.logger.Named("vfs"))

	return &Driver{rg, f.DBPath(), vfsName}
}

func (d *Driver) Close() {
	sqlite3vfs.Unregister(d.vfsName)
}

// LastRejection returns the concrete error from this Driver's most recently
// completed write proposal (nil if it succeeded, or if none has been made
// yet). This is the reliable way to recover whether a rejected write was a
// *rafterrors.NotLeaderError or a *rafterrors.CatchingUpError: that
// distinction doesn't reliably survive the round trip back through
// database/sql, which by then may report only a generic error.
func (d *Driver) LastRejection() error { return d.gate.LastRejection() }

// VFSName returns the name this Driver registered its gated VFS under.
func (d *Driver) VFSName() string { return d.vfsName }

// recordingGate wraps a vfs.Gate to record the outcome of the most recently
// completed ProposeTransaction call, so LastRejection can recover it after
// the error itself no longer reliably survives the round trip through
// database/sql.
type recordingGate struct {
	gate vfs.Gate

	lastErrMu sync.Mutex
	lastErr   error
}

var _ vfs.Gate = (*recordingGate)(nil)

func (g *recordingGate) ProposeTransaction(frames []*wal.Frame) error {
	err := g.gate.ProposeTransaction(frames)
	g.lastErrMu.Lock()
	defer g.lastErrMu.Unlock()

	g.lastErr = err
	return err
}

func (g *recordingGate) LastRejection() error {
	g.lastErrMu.Lock()
	defer g.lastErrMu.Unlock()
	return g.lastErr
}
