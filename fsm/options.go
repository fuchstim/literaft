package fsm

import "time"

type Option func(*options)

type options struct {
	checkpointInterval       time.Duration
	checkpointThresholdPages int
}

func defaultOptions() options {
	return options{
		checkpointInterval:       5 * time.Second,
		checkpointThresholdPages: 1000,
	}
}

func WithCheckpointInterval(d time.Duration) Option {
	return func(o *options) { o.checkpointInterval = d }
}

func WithCheckpointThresholdPages(n int) Option {
	return func(o *options) { o.checkpointThresholdPages = n }
}
