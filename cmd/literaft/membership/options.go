package membership

import "time"

type Option func(*options)

type options struct {
	timeout time.Duration
}

func defaultOptions() options {
	return options{timeout: 10 * time.Second}
}

// WithTimeout bounds each raft configuration-change future (AddVoter /
// RemoveServer) the leader applies. Defaults to 10s.
func WithTimeout(d time.Duration) Option {
	return func(o *options) { o.timeout = d }
}
