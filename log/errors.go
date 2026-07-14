package log

import (
	"fmt"

	"github.com/hashicorp/raft"
)

// NotLeaderError is returned by SingleWriterLog.Apply when this node isn't
// the raft leader. A follower rejects the write outright rather than
// forwarding it; Leader carries the current leader's address (empty if
// unknown) so a caller can redirect.
type NotLeaderError struct {
	Leader raft.ServerAddress
}

func (e *NotLeaderError) Error() string {
	if e.Leader == "" {
		return "not the leader (leader unknown)"
	}
	return fmt.Sprintf("not the leader (leader hint: %s)", e.Leader)
}

// CatchingUpError is returned by SingleWriterLog.Apply when this node has
// just won an election but hasn't yet finished draining its apply backlog.
// The node is the raft leader but its local SQLite state may not yet
// reflect every entry the cluster has already committed, so serving a
// write now could silently drop or reorder causally-prior data. Callers
// should retry shortly rather than redirect.
type CatchingUpError struct{}

func (CatchingUpError) Error() string {
	return "elected leader but still draining the apply backlog"
}

// EnqueueTimeoutError means a proposal timed out before it entered the raft
// log: it definitively did NOT enter the log, so a caller may report a clean
// rejection.
type EnqueueTimeoutError struct{ Err error }

func (e *EnqueueTimeoutError) Error() string {
	return "proposal not enqueued before timeout: " + e.Err.Error()
}
func (e *EnqueueTimeoutError) Unwrap() error { return e.Err }

// AmbiguousError means a proposal's outcome is unknown -- it may or may not
// have committed (e.g. leadership lost mid-flight). Must be treated as
// possibly-committed; a blind retry could double-apply.
type AmbiguousError struct{ Err error }

func (e *AmbiguousError) Error() string {
	return "proposal outcome ambiguous: " + e.Err.Error()
}
func (e *AmbiguousError) Unwrap() error { return e.Err }

// StaleBaseError means a forwarded write lost to a concurrent write: its base
// index no longer equals the leader's applied index. Retryable -- re-run the
// transaction to recompute on fresher state. LeaderLastApplied is a
// diagnostic (the leader's applied index at rejection time).
type StaleBaseError struct{ LeaderLastApplied uint64 }

func (e *StaleBaseError) Error() string {
	return "forwarded write rejected: base index is stale (a concurrent write won)"
}

// ForwardBusyError means the leader could not admit a forwarded write in time
// (write lock or enqueue). Retryable.
type ForwardBusyError struct{ Reason string }

func (e *ForwardBusyError) Error() string {
	if e.Reason == "" {
		return "forwarded write rejected: leader busy"
	}
	return "forwarded write rejected: leader busy (" + e.Reason + ")"
}

// AmbiguousForwardError means a forwarded write was (or may have been)
// proposed but its outcome wasn't confirmed before the timeout. NOT retryable
// as-is: the write may still commit and materialize later, so treat it as
// at-least-once (re-run only logic safe under that, or check state first).
type AmbiguousForwardError struct{ Err error }

func (e *AmbiguousForwardError) Error() string {
	if e.Err == nil {
		return "forwarded write outcome ambiguous"
	}
	return "forwarded write outcome ambiguous: " + e.Err.Error()
}
func (e *AmbiguousForwardError) Unwrap() error { return e.Err }

// NotDeliveredError means a transport proved the forward request never
// reached the leader, so nothing was proposed. Retryable, and cleanly so: it
// skips the ambiguous-outcome wait. Return it only when non-delivery is
// certain -- an in-flight request whose outcome is unknown (e.g. a connection
// dropped mid-call) must stay a plain, possibly-committed error.
type NotDeliveredError struct{ Err error }

func (e *NotDeliveredError) Error() string {
	if e.Err == nil {
		return "forward request not delivered to the leader"
	}
	return "forward request not delivered to the leader: " + e.Err.Error()
}
func (e *NotDeliveredError) Unwrap() error { return e.Err }
