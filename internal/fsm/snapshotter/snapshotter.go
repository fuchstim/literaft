package snapshotter

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/fuchstim/literaft/internal/fsm/walappender"
	"github.com/hashicorp/go-hclog"
	"github.com/ncruces/go-sqlite3"
)

type Snapshotter struct {
	dbPath   string
	pageSize uint32
	logger   hclog.Logger
}

func New(dbPath string, pageSize uint32, logger hclog.Logger) *Snapshotter {
	return &Snapshotter{
		dbPath:   dbPath,
		pageSize: pageSize,
		logger:   logger,
	}
}

// Snapshot uses SQLite's online backup API to back the "main" database up
// into a private temp file, then hands back a reader over the fixed header
// (carrying index -- the raft index this snapshot was taken at) followed by
// that file's bytes.
func (s *Snapshotter) Snapshot(index uint64) (io.ReadCloser, error) {
	s.logger.Info("capturing snapshot", "index", index)

	db, err := sqlite3.Open("file:" + s.dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database at path `%s`: %w", s.dbPath, err)
	}
	defer db.Close()

	tmp, err := os.CreateTemp("", "literaft-snapshot-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}

	if err := db.Backup("main", tmp.Name()); err != nil {
		return nil, errors.Join(
			fmt.Errorf("failed to backup up database at path `%s`: %w", s.dbPath, err),
			tmp.Close(),
			os.Remove(tmp.Name()),
		)
	}

	// Rewind the temp file to the start so the reader yields the header
	// followed by the whole backup.
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return nil, errors.Join(
			fmt.Errorf("failed to rewind snapshot temp file: %w", err),
			tmp.Close(),
			os.Remove(tmp.Name()),
		)
	}

	header := encodeHeader(index)
	tf := &tempFileReader{tmp}
	s.logger.Info("captured snapshot", "index", index)
	return &headeredReader{
		Reader: io.MultiReader(bytes.NewReader(header[:]), tmp),
		closer: tf,
	}, nil
}

// Restore reads the fixed header (returning its raft index), copies the
// remaining bytes to a temp file, then appends all of its frames to the local
// WAL. Finally it performs a TRUNCATE checkpoint to commit all WAL frames to
// the local .db file.
func (b *Snapshotter) Restore(r io.Reader) (uint64, error) {
	index, err := decodeHeader(r)
	if err != nil {
		return 0, err
	}
	b.logger.Info("restoring snapshot", "index", index)

	tmp, err := os.CreateTemp("", "literaft-restore-*")
	if err != nil {
		return 0, fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()

	size, err := io.Copy(tmp, r)
	if err != nil {
		return 0, fmt.Errorf("failed to copy snapshot to temp file: %w", err)
	}
	if size == 0 {
		return 0, errors.New("snapshot is empty")
	}
	if size%int64(b.pageSize) != 0 {
		return 0, fmt.Errorf("snapshot is %d bytes, not a whole multiple of the page size %d", size, b.pageSize)
	}

	w, err := walappender.Open(b.dbPath, b.pageSize, -1, 0, b.logger.Named("walappender"))
	if err != nil {
		return 0, fmt.Errorf("failed to open WAL at path `%s`: %w", b.dbPath, err)
	}
	defer w.Close()

	nPages := uint32(size / int64(b.pageSize))
	frames := make([]*walappender.Frame, nPages)
	for i := range nPages {
		page := make([]byte, b.pageSize)
		if _, err := tmp.ReadAt(page, int64(i)*int64(b.pageSize)); err != nil {
			return 0, fmt.Errorf("failed to read page %d from snapshot: %w", i+1, err)
		}

		if i == 0 {
			pageSize := uint32(binary.BigEndian.Uint16(page[16:18]))
			if pageSize == 1 {
				pageSize = 65536 // SQLite's szPage field encodes 64K pages as 1.
			}
			if pageSize != b.pageSize {
				return 0, fmt.Errorf("snapshot page size %d does not match cluster page size %d", pageSize, b.pageSize)
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
		return 0, fmt.Errorf("failed to append snapshot frames to WAL: %w", err)
	}

	db, err := sqlite3.Open("file:" + b.dbPath)
	if err != nil {
		return 0, fmt.Errorf("failed to open database at path `%s`: %w", b.dbPath, err)
	}
	defer db.Close()

	if _, _, err := db.WALCheckpoint("main", sqlite3.CHECKPOINT_TRUNCATE); err != nil {
		return 0, fmt.Errorf("failed to checkpoint database at path `%s`: %w", b.dbPath, err)
	}

	b.logger.Info("restored snapshot", "index", index, "pages", nPages)
	return index, nil
}
