package vfs

// Frame is one captured (page number, page image) pair from a write
// transaction's WAL frames, in the order SQLite wrote them
// (docs/DESIGN.md §RAFT log entry format).
type Frame struct {
	Pgno uint32
	Page []byte
}

// Entry is a whole write transaction's captured frames, proposed to a Gate
// as a unit when its commit frame is withheld.
type Entry struct {
	Frames    []Frame
	NTruncate uint32 // post-commit database size in pages
}

// Gate decides whether a captured write transaction may publish. Propose
// blocks until the decision is known: a nil error releases the withheld
// commit frame to disk; any other error aborts the transaction, and the
// commit frame never reaches disk (docs/DESIGN.md §write path steps 3-5).
//
// AlwaysCommit is a single-node stub that always commits; raft/ supplies a
// real one backed by a real RAFT proposal.
type Gate interface {
	Propose(Entry) error
}

// GateFunc adapts a function to a Gate.
type GateFunc func(Entry) error

func (f GateFunc) Propose(e Entry) error { return f(e) }

// AlwaysCommit is a stub gate: every proposal commits immediately, as
// if replication were a single-node no-op.
var AlwaysCommit Gate = GateFunc(func(Entry) error { return nil })
