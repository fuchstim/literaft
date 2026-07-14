// Package rafterrors is the single taxonomy for a rejected write proposal.
//
// Every rejection a raftgate.LogAdapter (or the write-forwarding path) produces
// is one of four concrete types, each pinned to a Category that decides both
// what a caller should do and which sqlite3 result code the VFS surfaces:
//
//	Redirect   NotLeaderError   sqlite3.READONLY     redirect to the leader
//	Retryable  CatchingUpError  sqlite3.BUSY         retry; nothing was applied
//	Retryable  NotAppliedError  sqlite3.BUSY         retry; nothing was applied
//	Ambiguous  AmbiguousError   sqlite3.IOERR_WRITE  possibly committed; do not blindly retry
//
// The Category is the classification; the result code is derived from it
// (Category.ResultCode), so one place decides how a rejection maps to SQLite's
// retry semantics -- there is no per-call-site result-code tagging. Callers
// that hold the error directly should classify via Classify / SafeToRetry
// rather than matching concrete types; the concrete types stay exported only so
// a caller can recover extra detail (e.g. NotLeaderError.Leader) via errors.As.
package rafterrors

import (
	"errors"
	"fmt"

	"github.com/ncruces/go-sqlite3"
)

// Category classifies a rejected write proposal by the action a caller should
// take, which in turn fixes the sqlite3 result code the VFS surfaces.
type Category int

const (
	// Redirect: this node is not the leader and nothing was proposed. A client
	// should redirect to the leader rather than retry the same connection.
	Redirect Category = iota + 1
	// Retryable: the write provably never entered the raft log, so re-running
	// the same statement is safe and is expected to eventually make progress.
	Retryable
	// Ambiguous: the write may or may not have committed. A blind retry could
	// double-apply, so it must be treated as possibly-committed.
	Ambiguous
)

// ResultCode is the sqlite3 result code the VFS surfaces for a rejection in
// this category, and the single source of truth for how a category maps to
// SQLite's retry semantics: Retryable is BUSY (a client retries), Redirect is
// READONLY (distinct, so a client redirects instead of spinning on the same
// connection), and Ambiguous is IOERR_WRITE (a hard failure a client must not
// blindly retry).
func (c Category) ResultCode() sqlite3.ExtendedErrorCode {
	switch c {
	case Redirect:
		return sqlite3.ExtendedErrorCode(sqlite3.READONLY)
	case Retryable:
		return sqlite3.ExtendedErrorCode(sqlite3.BUSY)
	default:
		return sqlite3.IOERR_WRITE
	}
}

// coded is the common core of every taxonomy error: it fixes the Category and
// derives the result code from it. Embedding it makes each concrete type
// satisfy vfs.CodedError without repeating the mapping.
type coded struct{ cat Category }

func (c coded) Category() Category                    { return c.cat }
func (c coded) ResultCode() sqlite3.ExtendedErrorCode { return c.cat.ResultCode() }

// NotLeaderError is a Redirect: this node isn't the raft leader, so the write
// was rejected before anything was proposed.
type NotLeaderError struct {
	coded
	// Leader is the address the rejecting node believed was the leader (empty
	// if none was known), so a caller can redirect.
	Leader string
}

// NewNotLeaderError returns a NotLeaderError hinting at leader (which may be
// empty when no leader is known).
func NewNotLeaderError(leader string) *NotLeaderError {
	return &NotLeaderError{coded: coded{Redirect}, Leader: leader}
}

func (e *NotLeaderError) Error() string {
	if e.Leader == "" {
		return "not the leader (leader unknown)"
	}
	return fmt.Sprintf("not the leader (leader hint: %s)", e.Leader)
}

// CatchingUpError is a Retryable: this node has won an election but hasn't
// finished draining its apply backlog, so its local state may not yet reflect
// every committed entry. A caller should retry shortly rather than redirect.
type CatchingUpError struct{ coded }

// NewCatchingUpError returns a CatchingUpError.
func NewCatchingUpError() *CatchingUpError {
	return &CatchingUpError{coded{Retryable}}
}

func (e *CatchingUpError) Error() string {
	return "elected leader but still draining the apply backlog"
}

// NotAppliedError is a Retryable: the write provably never entered the raft log
// (enqueue timeout, a stale forwarding base, a busy leader, or a proven
// non-delivery), so re-running is safe and cleanly recomputes on fresher state.
type NotAppliedError struct {
	coded
	// Reason is a short description of why the write never entered the log.
	Reason string
	err    error
}

// NewNotAppliedError returns a NotAppliedError describing reason, optionally
// wrapping the underlying cause (which may be nil).
func NewNotAppliedError(reason string, cause error) *NotAppliedError {
	return &NotAppliedError{coded: coded{Retryable}, Reason: reason, err: cause}
}

func (e *NotAppliedError) Error() string {
	if e.err == nil {
		return "write not applied: " + e.Reason
	}
	return "write not applied: " + e.Reason + ": " + e.err.Error()
}

func (e *NotAppliedError) Unwrap() error { return e.err }

// AmbiguousError is Ambiguous: the write may or may not have committed (e.g.
// leadership was lost mid-flight, or a forwarded request's outcome went
// unconfirmed). It must be treated as possibly-committed; a blind retry could
// double-apply.
type AmbiguousError struct {
	coded
	err error
}

// NewAmbiguousError returns an AmbiguousError wrapping cause (which may be nil).
func NewAmbiguousError(cause error) *AmbiguousError {
	return &AmbiguousError{coded: coded{Ambiguous}, err: cause}
}

func (e *AmbiguousError) Error() string {
	if e.err == nil {
		return "write outcome ambiguous (possibly committed)"
	}
	return "write outcome ambiguous (possibly committed): " + e.err.Error()
}

func (e *AmbiguousError) Unwrap() error { return e.err }

// Classify reports the Category of the first taxonomy error in err's chain, and
// whether one was found.
func Classify(err error) (Category, bool) {
	var c interface{ Category() Category }
	if errors.As(err, &c) {
		return c.Category(), true
	}
	return 0, false
}

// SafeToRetry reports whether re-running the statement that produced err is
// safe: true for Redirect and Retryable rejections (both provably never
// committed), false for an Ambiguous outcome or any non-taxonomy error.
func SafeToRetry(err error) bool {
	cat, ok := Classify(err)
	return ok && (cat == Redirect || cat == Retryable)
}
