package fsm

import (
	"fmt"
	"os"
)

// SQLite's own unix VFS locking byte offsets (os_unix.c's PENDING_BYTE /
// SHARED_FIRST / SHARED_SIZE, https://sqlite.org/src/doc/trunk/src/os_unix.c
// "File Locking Notes"). Every connection that has a database file open, in
// any journal mode including WAL, holds a plain OS-level SHARED (read) lock
// on this byte range for as long as it's open -- not just during a
// transaction, the way rollback-journal mode's locking otherwise reads.
// This is a completely different lock from -shm's own WAL_WRITE_LOCK/
// read-mark locks (which only ever coordinate WAL readers and writers
// during normal operation, see internal/fsm/walappender/shm); this one's
// entire purpose is SQLite's own close-time bookkeeping. On sqlite3_close,
// a connection tries to upgrade this same shared lock to EXCLUSIVE
// (sqlite3WalClose's own "am I the last one?" check) and, if that
// succeeds -- meaning no other shared holder exists anywhere -- checkpoints
// and deletes -wal/-shm. Because it's a plain OS file lock, that check is
// visible across processes, not just within one.
//
// fsm.FSM holds this lock explicitly and directly for as long as it's open
// (see FSM.New/Close), so a transient external reader -- exactly
// requirement #3's "read-only connections from other processes" -- can
// never observe zero other holders and conclude it's safe to
// checkpoint-and-delete, which would silently orphan every
// not-yet-checkpointed frame walAppender has written. Confirmed empirically:
// without this, a single external reader briefly opening and closing
// against a follower reliably deletes its -wal out from under it.
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
