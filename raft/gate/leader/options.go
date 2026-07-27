package leadergate

import (
	"time"

	"github.com/hashicorp/go-hclog"
)

type Option func(*options)

type options struct {
	applyTimeout time.Duration
	logger       hclog.Logger
}

func defaultOptions() options {
	return options{
		applyTimeout: 5 * time.Second,
		logger:       hclog.NewNullLogger(),
	}
}

func WithLogger(l hclog.Logger) Option {
	return func(o *options) {
		if l != nil {
			o.logger = l
		}
	}
}

func WithApplyTimeout(d time.Duration) Option {
	return func(o *options) { o.applyTimeout = d }
}
