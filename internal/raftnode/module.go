package raftnode

import (
	"github.com/fuchstim/sqlite-raft/internal/config"
)

var Module = config.ProvideWithParams[*Params](New)
