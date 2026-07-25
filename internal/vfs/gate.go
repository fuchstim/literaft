package vfs

import (
	"errors"

	"github.com/fuchstim/literaft/internal/wal"
	"github.com/ncruces/go-sqlite3"
)

// Gate decides whether a captured write transaction may publish.
// ProposeTransaction blocks until the decision is known: a nil error
// releases the withheld commit frame to disk; any other error aborts the
// transaction, and the commit frame never reaches disk.
type Gate interface {
	ProposeTransaction(frames []*wal.Frame, nTruncate uint32) error
}

type CodedError interface {
	error
	ResultCode() sqlite3.ExtendedErrorCode
}

func ErrCode(err error) (sqlite3.ExtendedErrorCode, bool) {
	var c CodedError
	if errors.As(err, &c) {
		return c.ResultCode(), true
	}
	return 0, false
}
