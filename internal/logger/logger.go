package logger

import (
	"go.uber.org/zap"
)

func newLogger(params *Params) (*zap.Logger, error) {
	var l *zap.Logger
	var err error

	if params.Development {
		l, err = zap.NewDevelopment()
	} else {
		l, err = zap.NewProduction()
	}

	if err != nil {
		return nil, err
	}

	lvl, err := zap.ParseAtomicLevel(params.Level)
	if err != nil {
		return nil, err
	}

	return l.WithOptions(zap.IncreaseLevel(lvl)), nil
}
