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
	baseGate           *leadergate.Gate
}

func defaultOptions() options {
	return options{
		forwardTimeout:     2 * time.Second,
		handlerLockTimeout: 1 * time.Second,
		logger:             hclog.NewNullLogger(),
		baseGate:           nil,
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

func WithBaseGate(g *leadergate.Gate) Option {
	return func(o *options) { o.baseGate = g }
}
