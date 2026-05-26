package main

import (
	"github.com/fuchstim/literaft/internal/config"
	"github.com/fuchstim/literaft/internal/lifecycle"
	"github.com/fuchstim/literaft/internal/logger"
	"github.com/fuchstim/literaft/internal/raftnode"
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
