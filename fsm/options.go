package fsm

import (
	"time"

	"github.com/hashicorp/go-hclog"
)

type Option func(*options)

type options struct {
	checkpointInterval       time.Duration
	checkpointThresholdPages int
	logger                   hclog.Logger
}

func defaultOptions() options {
	return options{
		checkpointInterval:       5 * time.Second,
		checkpointThresholdPages: 1000,
		logger:                   hclog.NewNullLogger(),
	}
}

// WithLogger threads an hclog.Logger through the FSM and the subsystems it
// owns, each under a named child. Defaults to a no-op logger, so an embedded
// FSM stays silent unless the caller opts in.
func WithLogger(l hclog.Logger) Option {
	return func(o *options) {
		if l != nil {
			o.logger = l
		}
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
