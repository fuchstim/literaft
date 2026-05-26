package lifecycle

import (
	"context"

	"github.com/fuchstim/literaft/internal/ctxlog"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var _ fx.Lifecycle = &Lifecycle{}

type Lifecycle struct {
	lc     fx.Lifecycle
	logger *zap.Logger
}

func New(lc fx.Lifecycle, logger *zap.Logger) *Lifecycle {
	return &Lifecycle{
		lc:     lc,
		logger: logger,
	}
}

func (l *Lifecycle) Append(hook fx.Hook) {
	hookWithLogger := fx.Hook{}

	if hook.OnStart != nil {
		hookWithLogger.OnStart = func(ctx context.Context) error {
			return hook.OnStart(ctxlog.NewContext(ctx, l.logger))
		}
	}

	if hook.OnStop != nil {
		hookWithLogger.OnStop = func(ctx context.Context) error {
			return hook.OnStop(ctxlog.NewContext(ctx, l.logger))
		}
	}

	l.lc.Append(hookWithLogger)
}
