package raftgate

import (
	"fmt"
	"sync"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/fuchstim/literaft/fsm"
	raftproto "github.com/fuchstim/literaft/internal/raft/proto"
	"github.com/fuchstim/literaft/internal/vfs"
)

var _ vfs.Gate = (*Gate)(nil)

// Gate implements vfs.Gate: it encodes a captured write transaction as a
// RAFT log entry and proposes it through log, self-skip-marking the entry
// on fsm first so this node's own FSM.Apply doesn't redundantly
// materialize a transaction it's about to publish some other way (see
// CLAUDE.md's "self-apply skip must stay transient" gotcha). Gate itself
// has no opinion on what log or its leader/readiness semantics are --
// that's the LogAdapter's concern (log.SingleWriterLog, for a real
// cluster).
type Gate struct {
	fsm *fsm.FSM
	log LogAdapter

	lastErrMu sync.Mutex
	lastErr   error
}

func New(fsm *fsm.FSM, log LogAdapter) *Gate {
	return &Gate{fsm: fsm, log: log}
}

// ProposeTransaction implements vfs.Gate. Any error the LogAdapter's Apply
// returns -- including an ambiguous "proposed, outcome unknown" case --
// surfaces as an error here too.
func (g *Gate) ProposeTransaction(frames []*vfs.Frame, nTruncate uint32) error {
	err := g.proposeTransaction(frames, nTruncate)
	g.lastErrMu.Lock()
	g.lastErr = err
	g.lastErrMu.Unlock()
	return err
}

func (g *Gate) proposeTransaction(frames []*vfs.Frame, nTruncate uint32) error {
	txn := &raftproto.Transaction{
		Pages:     make([]*raftproto.Page, len(frames)),
		NTruncate: nTruncate,
	}
	for i, f := range frames {
		txn.Pages[i] = &raftproto.Page{Pgno: f.Pgno, Data: f.Page}
	}

	e := &raftproto.Entry{
		Header:  &raftproto.Header{Id: uuid.NewString()},
		Payload: &raftproto.Entry_Transaction{Transaction: txn},
	}
	g.fsm.SkipEntry(e.Header.Id)
	defer g.fsm.UnskipEntry(e.Header.Id)

	b, err := proto.Marshal(e)
	if err != nil {
		return fmt.Errorf("failed to marshal entry: %w", err)
	}

	// Return the LogAdapter's rejection unwrapped: it already carries its own
	// category and result code, and this is the value LastRejection surfaces,
	// so callers recover it directly.
	if err := g.log.Apply(b); err != nil {
		return err
	}

	return nil
}

// LastRejection returns the error from the most recently completed
// ProposeTransaction call (nil if that call succeeded, or if
// ProposeTransaction has never been called).
func (g *Gate) LastRejection() error {
	g.lastErrMu.Lock()
	defer g.lastErrMu.Unlock()
	return g.lastErr
}
