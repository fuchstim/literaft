package snapshotter

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/fuchstim/literaft/internal/fsm/walappender"
	"github.com/fuchstim/literaft/internal/wal"
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

	s.logger.Info("captured snapshot", "index", index)

	header := NewSnapshotHeader(index)
	tf := &tempFileReader{tmp}
	return &headeredReader{
		Reader: io.MultiReader(bytes.NewReader(header[:]), tmp),
		closer: tf,
	}, nil
}

func (b *Snapshotter) Restore(r io.Reader) (SnapshotHeader, error) {
	headerBytes := make([]byte, SnapshotHeaderSize)
	if _, err := io.ReadFull(r, headerBytes); err != nil {
		return SnapshotHeader{}, fmt.Errorf("failed to read snapshot header: %w", err)
	}

	header := SnapshotHeader(headerBytes)
	if err := header.Validate(); err != nil {
		return SnapshotHeader{}, fmt.Errorf("failed to validate snapshot header: %w", err)
	}
	b.logger.Info("restoring snapshot", "index", header.LastAppliedIndex)

	w, err := walappender.Open(b.dbPath, b.pageSize, -1, 0, b.logger.Named("walappender"))
	if err != nil {
		return SnapshotHeader{}, fmt.Errorf("failed to open WAL at path `%s`: %w", b.dbPath, err)
	}
	defer w.Close()

	nPages, curPage, nextPage := uint32(1), make([]byte, b.pageSize), make([]byte, b.pageSize)
	if _, err := io.ReadFull(r, curPage); err != nil {
		return SnapshotHeader{}, fmt.Errorf("failed to first page from snapshot: %w", err)
	}

	pageSize := uint32(binary.BigEndian.Uint16(curPage[16:18]))
	if pageSize == 1 {
		pageSize = 65536 // SQLite's szPage field encodes 64K pages as 1.
	}
	if pageSize != b.pageSize {
		return SnapshotHeader{}, fmt.Errorf("snapshot page size %d does not match cluster page size %d", pageSize, b.pageSize)
	}

	var frames []*wal.Frame
	for {
		nTruncate := uint32(0)
		if _, err := io.ReadFull(r, nextPage); err != nil {
			if !errors.Is(err, io.EOF) {
				return SnapshotHeader{}, fmt.Errorf("failed to read page %d from snapshot: %w", nPages+1, err)
			}

			// If we hit EOF, `curPage` was the last page in the snapshot.
			nTruncate = nPages
		}

		frame := &wal.Frame{Header: &wal.FrameHeader{}}
		frame.Header.SetPgNo(nPages)
		frame.Header.SetNTruncate(nTruncate)
		frame.Data = curPage
		frames = append(frames, frame)

		if nTruncate > 0 {
			break
		}

		nPages++
		curPage = nextPage
		nextPage = make([]byte, b.pageSize)
	}

	// TODO: Stream frames to WAL appender instead of buffering them all in memory first.
	// Requires WAL appender to support streaming frames
	if err := w.AppendFrames(frames, nil); err != nil {
		return SnapshotHeader{}, fmt.Errorf("failed to append frames to WAL at path `%s`: %w", b.dbPath, err)
	}

	db, err := sqlite3.Open("file:" + b.dbPath)
	if err != nil {
		return SnapshotHeader{}, fmt.Errorf("failed to open database at path `%s`: %w", b.dbPath, err)
	}
	defer db.Close()

	if _, _, err := db.WALCheckpoint("main", sqlite3.CHECKPOINT_TRUNCATE); err != nil {
		return SnapshotHeader{}, fmt.Errorf("failed to checkpoint database at path `%s`: %w", b.dbPath, err)
	}

	b.logger.Info("restored snapshot", "index", header.LastAppliedIndex, "pages", nPages)
	return header, nil
}
