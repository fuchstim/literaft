package ctxlog

import (
	"context"

	"go.uber.org/fx"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const ContextKey = "ctxlog"

func NewContext(ctx context.Context, logger *zap.Logger) context.Context {
	return context.WithValue(ctx, ContextKey, logger)
}

func NewLifecycleHook(logger *zap.Logger, hook fx.Hook) fx.Hook {
	hookWithLogger := fx.Hook{}

	if hook.OnStart != nil {
		hookWithLogger.OnStart = func(ctx context.Context) error {
			return hook.OnStart(NewContext(ctx, logger))
		}
	}

	if hook.OnStop != nil {
		hookWithLogger.OnStop = func(ctx context.Context) error {
			return hook.OnStop(NewContext(ctx, logger))
		}
	}

	return hookWithLogger
}

func FromContext(ctx context.Context) *zap.Logger {
	l, ok := ctx.Value(ContextKey).(*zap.Logger)
	if !ok {
		return nil
	}

	return l
}

func With(ctx context.Context, fields ...zap.Field) context.Context {
	return NewContext(ctx, fromContextOrNop(ctx).With(fields...))
}

func Log(ctx context.Context, lvl zapcore.Level, msg string, fields ...zap.Field) {
	fromContextOrNop(ctx).Log(lvl, msg, fields...)
}

func Debug(ctx context.Context, msg string, fields ...zap.Field) {
	fromContextOrNop(ctx).Debug(msg, fields...)
}

func Info(ctx context.Context, msg string, fields ...zap.Field) {
	fromContextOrNop(ctx).Info(msg, fields...)
}

func Warn(ctx context.Context, msg string, fields ...zap.Field) {
	fromContextOrNop(ctx).Warn(msg, fields...)
}

func Error(ctx context.Context, msg string, fields ...zap.Field) {
	fromContextOrNop(ctx).Error(msg, fields...)
}

func DPanic(ctx context.Context, msg string, fields ...zap.Field) {
	fromContextOrNop(ctx).DPanic(msg, fields...)
}

func Panic(ctx context.Context, msg string, fields ...zap.Field) {
	fromContextOrNop(ctx).Panic(msg, fields...)
}

func Fatal(ctx context.Context, msg string, fields ...zap.Field) {
	fromContextOrNop(ctx).Fatal(msg, fields...)
}

func fromContextOrNop(ctx context.Context) *zap.Logger {
	l := FromContext(ctx)
	if l == nil {
		return zap.NewNop()
	}
	return l
}
