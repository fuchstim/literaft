package log

import "time"

type Option func(*options)

type options struct {
	applyTimeout time.Duration
}

func defaultOptions() options {
	return options{
		applyTimeout: 5 * time.Second,
	}
}

// WithApplyTimeout bounds each hraft.Apply call the SingleWriterLog makes.
// Defaults to 5s.
func WithApplyTimeout(d time.Duration) Option {
	return func(o *options) { o.applyTimeout = d }
}
