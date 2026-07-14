package membership

import (
	"time"

	"github.com/hashicorp/go-hclog"
)

type Option func(*options)

type options struct {
	timeout time.Duration
	logger  hclog.Logger
}

func defaultOptions() options {
	return options{timeout: 10 * time.Second, logger: hclog.NewNullLogger()}
}

// WithLogger threads an hclog.Logger through the membership server. Defaults
// to a no-op logger.
func WithLogger(l hclog.Logger) Option {
	return func(o *options) {
		if l != nil {
			o.logger = l
		}
	}
}

// WithTimeout bounds each raft configuration-change future (AddVoter /
// RemoveServer) the leader applies. Defaults to 10s.
func WithTimeout(d time.Duration) Option {
	return func(o *options) { o.timeout = d }
}
