package raftnode

import (
	"io"

	"github.com/hashicorp/raft"
)

// LogStore, StableStore, and SnapshotStore are the persistence surfaces that
// raft requires. Real implementations will be wired in later; for now the
// emptyXxx stubs satisfy each interface without persisting anything.
type (
	LogStore      = raft.LogStore
	StableStore   = raft.StableStore
	SnapshotStore = raft.SnapshotStore
)

var (
	_ StableStore   = (*emptyStableStore)(nil)
	_ SnapshotStore = (*emptySnapshotStore)(nil)
)

type emptyStableStore struct{}

func newEmptyStableStore() *emptyStableStore { return &emptyStableStore{} }

func (*emptyStableStore) Set([]byte, []byte) error         { return nil }
func (*emptyStableStore) Get([]byte) ([]byte, error)       { return nil, nil }
func (*emptyStableStore) SetUint64([]byte, uint64) error   { return nil }
func (*emptyStableStore) GetUint64([]byte) (uint64, error) { return 0, nil }

type emptySnapshotStore struct{}

func newEmptySnapshotStore() *emptySnapshotStore { return &emptySnapshotStore{} }

func (*emptySnapshotStore) Create(raft.SnapshotVersion, uint64, uint64, raft.Configuration, uint64, raft.Transport) (raft.SnapshotSink, error) {
	return nil, nil
}
func (*emptySnapshotStore) List() ([]*raft.SnapshotMeta, error) { return nil, nil }
func (*emptySnapshotStore) Open(string) (*raft.SnapshotMeta, io.ReadCloser, error) {
	return nil, nil, nil
}
