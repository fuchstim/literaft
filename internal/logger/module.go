package logger

import (
	"github.com/fuchstim/literaft/internal/config"
	"go.uber.org/fx"
)

var Module = fx.Module("logger",
	config.ProvideWithParams[*Params](newLogger),
)
