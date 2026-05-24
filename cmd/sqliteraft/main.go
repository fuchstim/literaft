package main

import (
	"github.com/fuchstim/sqlite-raft/internal/config"
	"github.com/fuchstim/sqlite-raft/internal/lifecycle"
	"github.com/fuchstim/sqlite-raft/internal/logger"
	"github.com/fuchstim/sqlite-raft/internal/raftnode"
	"go.uber.org/fx"
)

var server = fx.Module("sqliteraft",
	config.Module,
	logger.Module,
	lifecycle.Module,

	fx.Invoke(func(*raftnode.RaftNode) {}), // Ensure the node is started
)

func main() {
	fx.New(server).Run()
}
