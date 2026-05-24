package raftnode

import (
	"github.com/fuchstim/sqlite-raft/internal/lifecycle"
	"go.uber.org/fx"
)

type RaftNode struct{}

func New(lc *lifecycle.Lifecycle, params *Params) (*RaftNode, error) {
	n := &RaftNode{}

	lc.Append(fx.StartStopHook(n.Start, n.Stop))

	return n, nil
}

func (n *RaftNode) Start() error {
	return nil
}

func (n *RaftNode) Stop() error {
	return nil
}
