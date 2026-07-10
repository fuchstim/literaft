package fsm

import (
	"fmt"
	"os"
)

// SQLite's own unix VFS file-locking byte offsets. Every connection that has
// a database file open, in any journal mode including WAL, holds a plain
// OS-level SHARED (read) lock on this byte range for as long as it's open --
// not just during a transaction. This is a separate lock from the
// wal-index's write-lock/read-mark locks, which only coordinate WAL readers
// and writers during normal operation; this one exists purely for
// close-time bookkeeping. On close, a connection tries to upgrade this same
// shared lock to EXCLUSIVE and, if that succeeds -- meaning no other shared
// holder exists anywhere -- checkpoints and deletes -wal/-shm. Because it's
// a plain OS file lock, that check is visible across processes, not just
// within one.
//
// fsm.FSM holds this lock explicitly for as long as it's open, so a
// transient external reader can never observe zero other holders and
// conclude it's safe to checkpoint-and-delete, which would silently orphan
// every not-yet-checkpointed frame. Confirmed empirically: without this, a
// single external reader briefly opening and closing against a follower
// reliably deletes its -wal out from under it.
const (
	sqlitePendingByte = 0x40000000
	sqliteSharedFirst = sqlitePendingByte + 2
	sqliteSharedSize  = 510
)

// acquireSharedDBLock opens dbPath (creating it if it doesn't exist yet --
// mirrors the -wal/-shm files' own O_CREATE) and takes the OS-level SHARED
// lock described above, blocking until available. The returned file must
// be closed to release it.
func acquireSharedDBLock(dbPath string) (*os.File, error) {
	f, err := os.OpenFile(dbPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open `%s`: %w", dbPath, err)
	}
	if err := readLock(f, sqliteSharedFirst, sqliteSharedSize, true); err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to acquire a shared lock on `%s`: %w", dbPath, err)
	}
	return f, nil
}
