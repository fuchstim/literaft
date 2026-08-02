package driver

import (
	"time"

	"github.com/hashicorp/go-hclog"

	"github.com/fuchstim/literaft/proto"
)

type Option func(*options)

type options struct {
	logger             hclog.Logger
	leaderTransport    proto.LeaderTransport
	applyTimeout       time.Duration
	forwardTimeout     time.Duration
	handlerLockTimeout time.Duration
}

func defaultOptions() options {
	return options{
		logger:             hclog.NewNullLogger(),
		leaderTransport:    nil,
		applyTimeout:       5 * time.Second,
		forwardTimeout:     2 * time.Second,
		handlerLockTimeout: 1 * time.Second,
	}
}

func WithLogger(l hclog.Logger) Option {
	return func(o *options) {
		if l != nil {
			o.logger = l
		}
	}
}

func WithLeaderTransport(lt proto.LeaderTransport) Option {
	return func(o *options) {
		o.leaderTransport = lt
	}
}

func WithApplyTimeout(d time.Duration) Option {
	return func(o *options) {
		o.applyTimeout = d
	}
}

func WithForwardTimeout(d time.Duration) Option {
	return func(o *options) {
		o.forwardTimeout = d
	}
}

func WithHandlerLockTimeout(d time.Duration) Option {
	return func(o *options) {
		o.handlerLockTimeout = d
	}
}
