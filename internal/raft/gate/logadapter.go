package raftgate

// LogAdapter commits entry (an already-marshaled raftproto.Entry) through
// whatever consensus mechanism backs it, blocking until the outcome is
// known: a nil error means entry has committed and, on this node, has
// already run through fsm.FSM.Apply if it was ever going to (Gate relies
// on this to bound its own self-skip marker -- see Gate's doc).
// log.SingleWriterLog is the real hraft-backed implementation; Gate itself
// is agnostic to how Apply gets there.
type LogAdapter interface {
	Apply(entry []byte) error
}
