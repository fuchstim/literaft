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

// WithCheckpointInterval sets how often the background checkpointer runs; a
// non-positive value disables it, leaving only the dirty-page threshold. A
// node accepting forwarded writes must keep this positive: those writes skip
// the threshold checkpoint, so without the periodic one the WAL grows
// unbounded.
func WithCheckpointInterval(d time.Duration) Option {
	return func(o *options) { o.checkpointInterval = d }
}

func WithCheckpointThresholdPages(n int) Option {
	return func(o *options) { o.checkpointThresholdPages = n }
}
