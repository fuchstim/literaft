package fsm

import (
	"fmt"
	"os"

	"github.com/fuchstim/literaft/internal/lock"
)

const (
	sqlitePendingByte = 0x40000000
	sqliteSharedFirst = sqlitePendingByte + 2
	sqliteSharedSize  = 510
)

// fsm.FSM holds the SQLite shared lock on the database file for as long as it's open, so that
// closing another reader's connection won't delete the -wal and -shm files out from under it.
// This lock is separate from the WAL-index locks, which only coordinate WAL readers and writers.
func acquireSharedDBLock(dbPath string) (*os.File, error) {
	f, err := os.OpenFile(dbPath, os.O_RDWR, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open `%s`: %w", dbPath, err)
	}
	if err := lock.ReadLock(f, sqliteSharedFirst, sqliteSharedSize, true); err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to acquire a shared lock on `%s`: %w", dbPath, err)
	}
	return f, nil
}
