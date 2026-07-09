package snapshotter

import (
	"errors"
	"fmt"
	"os"
)

// tempFileReader wraps a temp file so Close both closes and removes it
type tempFileReader struct {
	*os.File
}

func (t *tempFileReader) Close() error {
	closeErr := t.File.Close()
	if err := os.Remove(t.File.Name()); err != nil && !os.IsNotExist(err) {
		closeErr = errors.Join(closeErr, fmt.Errorf("failed to remove temp file %s: %w", t.File.Name(), err))
	}

	return closeErr
}
