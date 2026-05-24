package raftnode

import (
	"io"

	"github.com/hashicorp/raft"
)

var _ raft.FSM = (*emptyFSM)(nil)

type emptyFSM struct{}

func newEmptyFSM() *emptyFSM { return &emptyFSM{} }

func (*emptyFSM) Apply(*raft.Log) interface{}         { return nil }
func (*emptyFSM) Snapshot() (raft.FSMSnapshot, error) { return &emptyFSMSnapshot{}, nil }
func (*emptyFSM) Restore(rc io.ReadCloser) error      { return rc.Close() }

type emptyFSMSnapshot struct{}

func (*emptyFSMSnapshot) Persist(sink raft.SnapshotSink) error { return sink.Close() }
func (*emptyFSMSnapshot) Release()                             {}
