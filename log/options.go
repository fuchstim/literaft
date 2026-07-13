package log

import "time"

type Option func(*options)

type options struct {
	applyTimeout       time.Duration
	forwardTimeout     time.Duration
	handlerLockTimeout time.Duration
}

func defaultOptions() options {
	return options{
		applyTimeout:       5 * time.Second,
		forwardTimeout:     2 * time.Second,
		handlerLockTimeout: 1 * time.Second,
	}
}

// WithApplyTimeout bounds each hraft.Apply call the SingleWriterLog makes.
// Defaults to 5s.
func WithApplyTimeout(d time.Duration) Option {
	return func(o *options) { o.applyTimeout = d }
}

// WithForwardTimeout bounds a follower's whole forwarding budget: the
// round-trip to the leader plus the wait for the entry to replicate back and
// be consumed. Keep it small (order seconds): the proposer holds the WAL
// write lock throughout, so a large value can stall this node's own apply and
// cluster commit progress. Defaults to 2s.
func WithForwardTimeout(d time.Duration) Option {
	return func(o *options) { o.forwardTimeout = d }
}

// WithHandlerLockTimeout bounds the leader-side write-lock acquisition; on
// timeout the handler answers BUSY. Keep it below the forward timeout so the
// leader gives up on the lock before the follower gives up waiting. Defaults
// to 1s.
func WithHandlerLockTimeout(d time.Duration) Option {
	return func(o *options) { o.handlerLockTimeout = d }
}
