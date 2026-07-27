package forwardinggate

import (
	"time"

	leadergate "github.com/fuchstim/literaft/raft/gate/leader"
	"github.com/hashicorp/go-hclog"
)

type Option func(*options)

type options struct {
	forwardTimeout     time.Duration
	handlerLockTimeout time.Duration
	logger             hclog.Logger
	baseGateOptions    []leadergate.Option
}

func defaultOptions() options {
	return options{
		forwardTimeout:     2 * time.Second,
		handlerLockTimeout: 1 * time.Second,
		logger:             hclog.NewNullLogger(),
		baseGateOptions:    nil,
	}
}

func WithLogger(l hclog.Logger) Option {
	return func(o *options) {
		if l != nil {
			o.logger = l
		}
	}
}

func WithForwardTimeout(d time.Duration) Option {
	return func(o *options) { o.forwardTimeout = d }
}

func WithHandlerLockTimeout(d time.Duration) Option {
	return func(o *options) { o.handlerLockTimeout = d }
}

func WithBaseGateOptions(opts ...leadergate.Option) Option {
	return func(o *options) { o.baseGateOptions = opts }
}
