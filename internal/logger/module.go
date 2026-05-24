package logger

import (
	"github.com/fuchstim/sqlite-raft/internal/config"
	"go.uber.org/fx"
)

var Module = fx.Module("logger",
	config.ProvideWithParams[*Params](newLogger),
)
