package raftsqlite

import (
	"time"

	"github.com/hashicorp/go-hclog"
)

type Option func(*options)

type options struct {
	busyTimeout time.Duration
	logger      hclog.Logger
}

func defaultOptions() options {
	return options{
		busyTimeout: 5 * time.Second,
		logger:      hclog.NewNullLogger(),
	}
}

// WithLogger threads an hclog.Logger through the store. Defaults to a no-op
// logger, so an embedded store stays silent unless the caller opts in.
func WithLogger(l hclog.Logger) Option {
	return func(o *options) {
		if l != nil {
			o.logger = l
		}
	}
}

// WithBusyTimeout sets how long a call waits for a locked database to free
// up (PRAGMA busy_timeout) before giving up. Defaults to 5s.
func WithBusyTimeout(d time.Duration) Option {
	return func(o *options) { o.busyTimeout = d }
}
