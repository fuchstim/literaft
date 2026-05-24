package raftnode

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/hashicorp/raft"
)

// File layout for each snapshot:
//
//   [4 bytes, big-endian uint32: length of meta JSON]
//   [meta JSON]
//   [payload bytes]
//
// The filename is the snapshot ID. In-progress snapshots are written to a
// sibling file with a `.partial` suffix and atomically renamed on Close.

const (
	snapshotPartialSuffix = ".partial"
	snapshotFilePerm      = 0o644
	snapshotDirPerm       = 0o755
)

var _ raft.SnapshotStore = (*fileSnapshotStore)(nil)

type fileSnapshotStore struct {
	dir string
}

func newFileSnapshotStore(dir string) (*fileSnapshotStore, error) {
	if err := os.MkdirAll(dir, snapshotDirPerm); err != nil {
		return nil, fmt.Errorf("create snapshot dir %s: %w", dir, err)
	}
	return &fileSnapshotStore{dir: dir}, nil
}

func (s *fileSnapshotStore) Create(
	version raft.SnapshotVersion,
	index, term uint64,
	configuration raft.Configuration,
	configurationIndex uint64,
	_ raft.Transport,
) (raft.SnapshotSink, error) {
	id := fmt.Sprintf("%d-%d-%d", term, index, time.Now().UnixNano())
	tmpPath := filepath.Join(s.dir, id+snapshotPartialSuffix)

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, snapshotFilePerm)
	if err != nil {
		return nil, fmt.Errorf("create partial snapshot %s: %w", tmpPath, err)
	}

	return &fileSnapshotSink{
		tmpFile:   f,
		tmpPath:   tmpPath,
		finalPath: filepath.Join(s.dir, id),
		meta: raft.SnapshotMeta{
			Version:            version,
			ID:                 id,
			Index:              index,
			Term:               term,
			Configuration:      configuration,
			ConfigurationIndex: configurationIndex,
		},
	}, nil
}

func (s *fileSnapshotStore) List() ([]*raft.SnapshotMeta, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("read snapshot dir %s: %w", s.dir, err)
	}

	var metas []*raft.SnapshotMeta
	for _, entry := range entries {
		if entry.IsDir() || strings.HasSuffix(entry.Name(), snapshotPartialSuffix) {
			continue
		}
		meta, err := s.readMeta(entry.Name())
		if err != nil {
			continue
		}
		metas = append(metas, meta)
	}

	sort.Slice(metas, func(i, j int) bool {
		if metas[i].Index != metas[j].Index {
			return metas[i].Index > metas[j].Index
		}
		return metas[i].Term > metas[j].Term
	})
	return metas, nil
}

func (s *fileSnapshotStore) Open(id string) (*raft.SnapshotMeta, io.ReadCloser, error) {
	f, err := os.Open(filepath.Join(s.dir, id))
	if err != nil {
		return nil, nil, fmt.Errorf("open snapshot %s: %w", id, err)
	}
	meta, err := decodeSnapshotMeta(f)
	if err != nil {
		f.Close()
		return nil, nil, fmt.Errorf("decode snapshot %s: %w", id, err)
	}
	return meta, f, nil
}

func (s *fileSnapshotStore) readMeta(name string) (*raft.SnapshotMeta, error) {
	f, err := os.Open(filepath.Join(s.dir, name))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return decodeSnapshotMeta(f)
}

func decodeSnapshotMeta(r io.Reader) (*raft.SnapshotMeta, error) {
	var length uint32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return nil, fmt.Errorf("read meta length: %w", err)
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("read meta body: %w", err)
	}
	var meta raft.SnapshotMeta
	if err := json.Unmarshal(buf, &meta); err != nil {
		return nil, fmt.Errorf("unmarshal meta: %w", err)
	}
	return &meta, nil
}

var _ raft.SnapshotSink = (*fileSnapshotSink)(nil)

type fileSnapshotSink struct {
	meta      raft.SnapshotMeta
	tmpFile   *os.File
	tmpPath   string
	finalPath string
	closed    bool
}

func (s *fileSnapshotSink) ID() string { return s.meta.ID }

func (s *fileSnapshotSink) Write(p []byte) (int, error) {
	n, err := s.tmpFile.Write(p)
	s.meta.Size += int64(n)
	return n, err
}

func (s *fileSnapshotSink) Close() error {
	if s.closed {
		return errors.New("snapshot sink already closed")
	}
	s.closed = true

	if err := s.tmpFile.Sync(); err != nil {
		s.tmpFile.Close()
		os.Remove(s.tmpPath)
		return fmt.Errorf("sync partial snapshot: %w", err)
	}
	if err := s.tmpFile.Close(); err != nil {
		os.Remove(s.tmpPath)
		return fmt.Errorf("close partial snapshot: %w", err)
	}

	if err := s.finalize(); err != nil {
		os.Remove(s.tmpPath)
		os.Remove(s.finalPath)
		return err
	}
	return os.Remove(s.tmpPath)
}

func (s *fileSnapshotSink) Cancel() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.tmpFile.Close()
	return os.Remove(s.tmpPath)
}

func (s *fileSnapshotSink) finalize() error {
	metaBytes, err := json.Marshal(&s.meta)
	if err != nil {
		return fmt.Errorf("marshal meta: %w", err)
	}

	final, err := os.OpenFile(s.finalPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, snapshotFilePerm)
	if err != nil {
		return fmt.Errorf("create snapshot %s: %w", s.finalPath, err)
	}
	defer final.Close()

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(metaBytes)))
	if _, err := final.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("write meta length: %w", err)
	}
	if _, err := final.Write(metaBytes); err != nil {
		return fmt.Errorf("write meta body: %w", err)
	}

	payload, err := os.Open(s.tmpPath)
	if err != nil {
		return fmt.Errorf("reopen partial snapshot: %w", err)
	}
	defer payload.Close()

	if _, err := io.Copy(final, payload); err != nil {
		return fmt.Errorf("copy payload: %w", err)
	}
	return final.Sync()
}
