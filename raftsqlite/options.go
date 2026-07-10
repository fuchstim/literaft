package raftsqlite

import "time"

type Option func(*options)

type options struct {
	busyTimeout time.Duration
}

func defaultOptions() options {
	return options{
		busyTimeout: 5 * time.Second,
	}
}

// WithBusyTimeout sets how long a call waits for a locked database to free
// up (PRAGMA busy_timeout) before giving up. Defaults to 5s.
func WithBusyTimeout(d time.Duration) Option {
	return func(o *options) { o.busyTimeout = d }
}
