// Package raftsqlite implements hraft's raft.LogStore and raft.StableStore
// on top of github.com/ncruces/go-sqlite3 in WAL mode, as a drop-in
// replacement for github.com/hashicorp/raft-boltdb/v2 that doesn't fsync on
// every write.
package raftsqlite

import (
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/ncruces/go-sqlite3"
	ncrdriver "github.com/ncruces/go-sqlite3/driver"
)

var (
	_ raft.LogStore    = (*Store)(nil)
	_ raft.StableStore = (*Store)(nil)
)

// ErrKeyNotFound is returned by Get/GetUint64 for a key that was never Set.
// hraft distinguishes "no error" from "key not found" by checking this
// error's message against the literal string "not found" rather than
// comparing sentinel values, so this message must not change.
var ErrKeyNotFound = errors.New("not found")

// Store is a raft.LogStore and raft.StableStore backed by a single SQLite
// database. Durability comes from RAFT quorum rather than local fsync, the
// same philosophy applied to the replicated database itself (see
// CLAUDE.md): journal_mode=WAL, synchronous=NORMAL.
type Store struct {
	db     *sql.DB
	logger hclog.Logger
}

// New opens (creating if necessary) a SQLite database at path as a Store.
// path is passed through to SQLite as-is after a "file:" prefix, so the
// special name ":memory:" opens a private, in-memory-only database.
//
// The returned Store holds at most one physical SQLite connection open at a
// time: every call serializes through it rather than through a pool. This
// keeps an in-memory Store isolated to itself with no need for SQLite's
// shared-cache mode, at the cost of read/write concurrency a busier store
// might want -- reasonable for a raft log/stable store, whose writes are
// already serialized to one RAFT round-trip at a time.
func New(path string, opts ...Option) (*Store, error) {
	if path == "" {
		return nil, fmt.Errorf("path is required")
	}

	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}

	db, err := ncrdriver.Open("file:"+path, func(c *sqlite3.Conn) error {
		if err := c.Exec(fmt.Sprintf("PRAGMA busy_timeout=%d", o.busyTimeout.Milliseconds())); err != nil {
			return err
		}
		if err := c.Exec("PRAGMA journal_mode=WAL"); err != nil {
			return err
		}
		return c.Exec("PRAGMA synchronous=NORMAL")
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open database at path `%s`: %w", path, err)
	}
	db.SetMaxOpenConns(1)

	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS logs (
			idx         INTEGER PRIMARY KEY,
			term        INTEGER NOT NULL,
			type        INTEGER NOT NULL,
			data        BLOB,
			extensions  BLOB,
			appended_at INTEGER NOT NULL
		)
	`); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create logs table: %w", err)
	}

	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS stable (
			key   BLOB PRIMARY KEY,
			value BLOB NOT NULL
		)
	`); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create stable table: %w", err)
	}

	logger := o.logger.Named("raftsqlite")
	logger.Info("opened raft store", "path", path)
	return &Store{db: db, logger: logger}, nil
}

// Close closes the underlying SQLite connection.
func (s *Store) Close() error {
	return s.db.Close()
}

// FirstIndex implements raft.LogStore.
func (s *Store) FirstIndex() (uint64, error) {
	var idx sql.NullInt64
	if err := s.db.QueryRow(`SELECT MIN(idx) FROM logs`).Scan(&idx); err != nil {
		return 0, err
	}
	return uint64(idx.Int64), nil
}

// LastIndex implements raft.LogStore.
func (s *Store) LastIndex() (uint64, error) {
	var idx sql.NullInt64
	if err := s.db.QueryRow(`SELECT MAX(idx) FROM logs`).Scan(&idx); err != nil {
		return 0, err
	}
	return uint64(idx.Int64), nil
}

// GetLog implements raft.LogStore.
func (s *Store) GetLog(index uint64, log *raft.Log) error {
	var term uint64
	var typ int64
	var data, extensions []byte
	var appendedAtNanos int64

	err := s.db.QueryRow(
		`SELECT term, type, data, extensions, appended_at FROM logs WHERE idx = ?`,
		int64(index),
	).Scan(&term, &typ, &data, &extensions, &appendedAtNanos)
	if errors.Is(err, sql.ErrNoRows) {
		return raft.ErrLogNotFound
	}
	if err != nil {
		return err
	}

	log.Index = index
	log.Term = term
	log.Type = raft.LogType(typ)
	log.Data = data
	log.Extensions = extensions
	log.AppendedAt = time.Unix(0, appendedAtNanos).UTC()
	return nil
}

// StoreLog implements raft.LogStore.
func (s *Store) StoreLog(log *raft.Log) error {
	return s.StoreLogs([]*raft.Log{log})
}

// StoreLogs implements raft.LogStore.
func (s *Store) StoreLogs(logs []*raft.Log) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO logs (idx, term, type, data, extensions, appended_at) VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(idx) DO UPDATE SET
			term = excluded.term, type = excluded.type, data = excluded.data,
			extensions = excluded.extensions, appended_at = excluded.appended_at
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, log := range logs {
		if _, err := stmt.Exec(
			int64(log.Index), int64(log.Term), int64(log.Type),
			log.Data, log.Extensions, log.AppendedAt.UnixNano(),
		); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// DeleteRange implements raft.LogStore, deleting logs in [min, max]
// inclusive.
func (s *Store) DeleteRange(min, max uint64) error {
	s.logger.Debug("deleting log range", "min", min, "max", max)
	_, err := s.db.Exec(`DELETE FROM logs WHERE idx BETWEEN ? AND ?`, int64(min), int64(max))
	return err
}

// Set implements raft.StableStore.
func (s *Store) Set(key, val []byte) error {
	_, err := s.db.Exec(
		`INSERT INTO stable (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		key, val,
	)
	return err
}

// Get implements raft.StableStore.
func (s *Store) Get(key []byte) ([]byte, error) {
	var val []byte
	err := s.db.QueryRow(`SELECT value FROM stable WHERE key = ?`, key).Scan(&val)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrKeyNotFound
	}
	if err != nil {
		return nil, err
	}
	return val, nil
}

// SetUint64 implements raft.StableStore.
func (s *Store) SetUint64(key []byte, val uint64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, val)
	return s.Set(key, buf)
}

// GetUint64 implements raft.StableStore.
func (s *Store) GetUint64(key []byte) (uint64, error) {
	val, err := s.Get(key)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(val), nil
}
