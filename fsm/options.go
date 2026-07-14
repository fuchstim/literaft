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
// non-positive value disables it, leaving only the dirty-page threshold. Keep
// it positive in production: the threshold only fires once enough dirty pages
// accumulate (on release, for both self-locked and forwarded writes), so a
// steady sub-threshold trickle still needs the periodic checkpoint to keep the
// WAL from growing.
func WithCheckpointInterval(d time.Duration) Option {
	return func(o *options) { o.checkpointInterval = d }
}

func WithCheckpointThresholdPages(n int) Option {
	return func(o *options) { o.checkpointThresholdPages = n }
}
