package snapshotter

import (
	"errors"
	"fmt"
	"io"
	"os"
)

// headeredReader yields the fixed snapshot header followed by the backup
// bytes (via an io.MultiReader), delegating Close to the underlying temp
// file (which both closes and removes it).
type headeredReader struct {
	io.Reader
	closer io.Closer
}

func (h *headeredReader) Close() error { return h.closer.Close() }

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
