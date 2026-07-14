package driver

import "github.com/hashicorp/go-hclog"

type Option func(*options)

type options struct {
	logger hclog.Logger
}

func defaultOptions() options {
	return options{
		logger: hclog.NewNullLogger(),
	}
}

// WithLogger threads an hclog.Logger through the gate and registered VFS this
// Driver owns, each under a named child. Defaults to a no-op logger, so an
// embedded Driver stays silent unless the caller opts in.
func WithLogger(l hclog.Logger) Option {
	return func(o *options) {
		if l != nil {
			o.logger = l
		}
	}
}
