package vfs

import (
	"errors"

	"github.com/ncruces/go-sqlite3"
)

// Gate decides whether a captured write transaction may publish.
// ProposeTransaction blocks until the decision is known: a nil error
// releases the withheld commit frame to disk; any other error aborts the
// transaction, and the commit frame never reaches disk.
type Gate interface {
	ProposeTransaction(frames []*Frame, nTruncate uint32) error
}

type gateError struct {
	error
	code sqlite3.ExtendedErrorCode
}

// Unwrap exposes the wrapped error to errors.As/errors.Is: callers like
// Driver.LastRejection recover concrete types (e.g. a CatchingUpError)
// through the chain, and embedding alone doesn't provide this -- the
// promoted Error method comes from the embedded field's static (interface)
// type, which declares no Unwrap.
func (e *gateError) Unwrap() error { return e.error }

// GateError tags a ProposeTransaction rejection with the sqlite3 result
// code File should surface for it, instead of the IOERR_WRITE default.
func GateError(err error, code sqlite3.ExtendedErrorCode) *gateError {
	return &gateError{error: err, code: code}
}

// ErrCode reports the sqlite3 result code a GateError anywhere in err's chain
// was tagged with, and whether such a tag was found. It lets a caller tell a
// retryable rejection (sqlite3.BUSY) from a hard I/O failure without matching
// on the concrete error type behind the tag.
func ErrCode(err error) (sqlite3.ExtendedErrorCode, bool) {
	ge := &gateError{}
	if errors.As(err, &ge) {
		return ge.code, true
	}
	return 0, false
}
