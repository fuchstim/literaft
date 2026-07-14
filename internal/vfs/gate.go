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

// CodedError is an error that names the sqlite3 result code the write path
// should surface for it instead of the IOERR_WRITE default. A Gate returns one
// to say how a rejection must be reported -- e.g. a retryable rejection as
// sqlite3.BUSY so a client retries rather than treating it as a hard I/O
// failure. The rejection taxonomy itself lives outside this package (the VFS
// stays agnostic of raft); vfs only needs the carried code.
type CodedError interface {
	error
	ResultCode() sqlite3.ExtendedErrorCode
}

// ErrCode reports the result code carried by a CodedError anywhere in err's
// chain, and whether one was found. It lets the write path tell a retryable
// rejection from a hard I/O failure without matching on any concrete type.
func ErrCode(err error) (sqlite3.ExtendedErrorCode, bool) {
	var c CodedError
	if errors.As(err, &c) {
		return c.ResultCode(), true
	}
	return 0, false
}
