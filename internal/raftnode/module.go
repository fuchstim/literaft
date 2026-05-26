package raftnode

import (
	"github.com/fuchstim/literaft/internal/config"
)

var Module = config.ProvideWithParams[*Params](New)
