package config

import (
	"go.uber.org/fx"
)

var Module = fx.Module("config",
	fx.Invoke(fx.Annotate(readConfig, fx.ParamTags(`group:"params"`))),
)
