package fsm

import (
	"time"

	"github.com/hashicorp/go-hclog"
)

type Option func(*options)

type options struct {
	checkpointInterval       time.Duration
	checkpointThresholdPages int
	logger                   hclog.Logger
}

func defaultOptions() options {
	return options{
		checkpointInterval:       5 * time.Second,
		checkpointThresholdPages: 1000,
		logger:                   hclog.NewNullLogger(),
	}
}

func WithLogger(l hclog.Logger) Option {
	return func(o *options) {
		if l != nil {
			o.logger = l
		}
	}
}

func WithCheckpointInterval(d time.Duration) Option {
	return func(o *options) { o.checkpointInterval = d }
}

func WithCheckpointThresholdPages(n int) Option {
	return func(o *options) { o.checkpointThresholdPages = n }
}
