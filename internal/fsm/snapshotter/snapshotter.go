package snapshotter

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/fuchstim/literaft/internal/fsm/walappender"
	"github.com/ncruces/go-sqlite3"
)

type Snapshotter struct {
	dbPath   string
	pageSize uint32
}

func New(dbPath string, pageSize uint32) *Snapshotter {
	return &Snapshotter{
		dbPath:   dbPath,
		pageSize: pageSize,
	}
}

// Snapshot uses SQLite's online backup API (https://sqlite.org/backup.html) to back
// the "main" database up into a private temp file, then hands
// back a reader over that file.
func (s *Snapshotter) Snapshot() (io.ReadCloser, error) {
	db, err := sqlite3.Open("file:" + s.dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database at path `%s`: %w", s.dbPath, err)
	}

	tmp, err := os.CreateTemp("", "literaft-snapshot-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}

	if err := db.Backup("main", tmp.Name()); err != nil {
		return nil, errors.Join(
			fmt.Errorf("failed to backup up database at path `%s`: %w", s.dbPath, err),
			os.Remove(tmp.Name()),
		)
	}

	return &tempFileReader{tmp}, nil
}

// Restore copies all of r's bytes to a temp file, then appends all of
// its frames to the local WAL. Finally it performs a TRUNCATE checkpoint
// to commit all WAL frames to the local .db file.
func (b *Snapshotter) Restore(r io.Reader) error {
	tmp, err := os.CreateTemp("", "literaft-restore-*")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()

	size, err := io.Copy(tmp, r)
	if err != nil {
		return fmt.Errorf("failed to copy snapshot to temp file: %w", err)
	}
	if size == 0 {
		return errors.New("snapshot is empty")
	}
	if size%int64(b.pageSize) != 0 {
		return fmt.Errorf("snapshot is %d bytes, not a whole multiple of the page size %d", size, b.pageSize)
	}

	w, err := walappender.Open(b.dbPath, b.pageSize, -1, 0)
	if err != nil {
		return fmt.Errorf("failed to open WAL at path `%s`: %w", b.dbPath, err)
	}
	defer w.Close()

	nPages := uint32(size / int64(b.pageSize))
	frames := make([]*walappender.Frame, nPages)
	for i := range nPages {
		page := make([]byte, b.pageSize)
		if _, err := tmp.ReadAt(page, int64(i)*int64(b.pageSize)); err != nil {
			return fmt.Errorf("failed to read page %d from snapshot: %w", i+1, err)
		}

		if i == 0 {
			pageSize := uint32(binary.BigEndian.Uint16(page[16:18]))
			if pageSize == 1 {
				pageSize = 65536 // SQLite's szPage field encodes 64K pages as 1.
			}
			if pageSize != b.pageSize {
				return fmt.Errorf("snapshot page size %d does not match cluster page size %d", pageSize, b.pageSize)
			}
		}

		var nTruncate uint32
		if i == nPages-1 {
			nTruncate = nPages
		}
		frames[i] = walappender.NewFrame(i+1, nTruncate, page)
	}

	// TODO: This loads the entire DB into memory which is not ideal. We should stream the frames to the WAL instead of loading them all into memory.
	if err := w.AppendFrames(frames); err != nil {
		return fmt.Errorf("failed to append snapshot frames to WAL: %w", err)
	}

	db, err := sqlite3.Open("file:" + b.dbPath)
	if err != nil {
		return fmt.Errorf("failed to open database at path `%s`: %w", b.dbPath, err)
	}
	defer db.Close()

	if _, _, err := db.WALCheckpoint("main", sqlite3.CHECKPOINT_TRUNCATE); err != nil {
		return fmt.Errorf("failed to checkpoint database at path `%s`: %w", b.dbPath, err)
	}

	return nil
}
