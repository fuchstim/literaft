package vfs

import raftproto "github.com/fuchstim/literaft/internal/raft/proto"

// Gate decides whether a captured write transaction may publish. Propose
// blocks until the decision is known: a nil error releases the withheld
// commit frame to disk; any other error aborts the transaction, and the
// commit frame never reaches disk.
type Gate interface {
	Propose(*raftproto.Transaction) error
}
