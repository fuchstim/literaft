# shm

Gives the follower-apply path (`apply/`) direct control over the `-shm`
wal-index: mmap the file, take the same byte-range locks SQLite itself takes,
and expose the mapped bytes for `apply/` to read and write. Needed because
`sqlite3vfs.SharedMemory` is an exported interface with unexported methods --
you can only hand a value back to the library, never drive it yourself (see
`docs/NCRUCES_NOTES.md` §"Why the SHM must be vendored").

## Why this isn't a literal copy of upstream

The original plan (`docs/NCRUCES_NOTES.md`) was to copy ncruces'
`vfs/shm_ofd.go` wholesale. That turned out not to be possible as a literal
copy-paste:

- `vfsShm.shmMap` maps the `-shm` file into the **wazero WASM linear memory**
  (`wrp.MapRegion`, where `wrp` is a specific SQLite-wasm module instance) so
  the SQLite-in-wasm engine can address it as a wasm pointer. Follower-apply
  runs no wasm SQLite engine at all -- it's plain Go code -- so it has no
  `*sqlite3_wrap.Wrapper` to map into, and doesn't want one: it just needs an
  ordinary `[]byte` over the file, which is what a plain OS `mmap` gives you
  directly.
- The locking and error-handling helpers (`osReadLock`, `osWriteLock`,
  `sysError`, `_ErrorCode`, ...) live in `internal/` packages or are
  unexported package-private identifiers, so they aren't importable from
  outside `github.com/ncruces/go-sqlite3/vfs` regardless.

What *is* load-bearing and copied for reference is the **locking protocol**:
the exact byte offsets (`_SHM_BASE`, `_SHM_DMS`, `_SHM_NLOCK`), the OFD lock
mechanics (`F_OFD_SETLK`/`F_OFD_SETLKW`, including the undocumented Darwin
fcntl command numbers 90/91/93 that aren't in `golang.org/x/sys/unix`), and
the "dead man's switch" shm-open handshake. Getting these wrong doesn't cause
a compile error, it causes silent, non-interoperable locking between our
follower-apply handle and SQLite's own connections -- readers and writers
would stop actually excluding each other. So `shm/upstream/*.upstream` holds
pinned, diffable reference copies of the exact upstream source (see
`UPSTREAM.md` for the commit hash; regenerate with `make vendor-shm`), and
`lock_linux.go` / `lock_darwin.go` / `shm.go` in this package are a clean-room
adaptation of that same protocol onto a plain `mmap`, with comments pointing
back at the upstream file each piece corresponds to.

## Layout

- `lock_linux.go`, `lock_darwin.go` -- per-OS OFD byte-range lock primitives
  (`readLock`/`writeLock`/`unlock`/`testLock`), adapted from
  `upstream/os_linux.go.upstream` / `upstream/os_darwin.go.upstream`.
- `shm.go` -- `Open`/`Close` and the mmap + dead-man's-switch handshake,
  adapted from `upstream/shm_ofd.go.upstream`; the lock byte-offset constants
  (`writeLockOffset` etc.) are copied from `upstream/const.go.upstream`.

`apply/` is the actual wal-index reader/writer (header format, hash table,
checksums) built on top of the raw mmap this package provides. No upstream Go
source exists for that format at all, since it's implemented entirely inside
the compiled SQLite-wasm blob -- so it's derived directly from
https://sqlite.org/walformat.html and cross-checked against this build's
actual on-disk output in tests. See `apply/README.md`.
