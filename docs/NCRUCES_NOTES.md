# NCRUCES_NOTES

Everything specific to building this on `github.com/ncruces/go-sqlite3`.
Verified against **v0.35.2** (the version currently pinned in `go.mod`;
re-verify against whatever's pinned there — the internals below are
unexported and can shift on any bump).

---

## The engine is a pure-Go reimplementation

ncruces replaces SQLite's OS interface with a pure-Go VFS running SQLite itself
as wasm via wazero. No cgo. Consequences:

- There is **no C `unix` VFS to delegate to.** "Base VFS" = ncruces' own default
  Go VFS. Our wrapper (`internal/vfs`) wraps *that*.
- On Linux/macOS the default VFS uses **`mmap`** for the wal-index shared memory
  and **OFD locks** for file locking — the same mechanisms SQLite uses, which is
  why the default build is **compatible with the standard Unix and Windows
  SQLite VFSes**. That compatibility is the basis for requirement #3
  (external read-only processes).

---

## On-disk / external-reader compatibility — verified, not assumed

Our requirement #3 (an *unmodified* stock SQLite process opens the db
read-only, concurrently) rests on lock interoperability:

- ncruces default uses **OFD locks** (`F_OFD_SETLK`).
- Stock SQLite's unix VFS uses classic **POSIX advisory locks** (`F_SETLK`).

On Linux and macOS these two lock types **do contend with each other** in the
kernel, so coordination works — verified directly by
`internal/vfs/external_reader_test.go`, which drives the real system `sqlite3`
CLI as an external reader against a live wrapper-VFS writer.

**Do NOT ship `sqlite3_dotlk`.** That build tag switches shm to a
cross-platform, **in-process** sharing implementation — WAL dbs then can only
be accessed by a single process, and other processes fail with
`SQLITE_PROTOCOL`/`SQLITE_IOERR`/`SQLITE_CANTOPEN`. That directly breaks
requirement #3. Same caution for `sqlite3_flock` (BSD locks): reduced
concurrency (`BEGIN IMMEDIATE` behaves like `BEGIN EXCLUSIVE`) and a *different*
compatibility class (`unix-flock`). Use the **default** build only.

Use `vfs.SupportsFileLocking` and `vfs.SupportsSharedMemory` at startup to fail
fast on an unsupported platform (`internal/vfs/external_reader_test.go`'s
`requireExternalSQLite` does exactly this before running).

---

## The exported VFS surface we build on

From `github.com/ncruces/go-sqlite3/vfs` (names as of v0.35.2):

- `Register(name, vfs)` / `Unregister(name)` / `Find(name)` — register our
  wrapper under a name we pass via `?vfs=` in the DSN (`internal/vfs.Register`
  does this).
- `VFS` interface — `Open(name Filename, flags OpenFlag) (File, error)` plus
  access/delete/full-pathname. `internal/vfs.VFS` implements this and calls the
  wrapped VFS inside `Open`.
- `File` interface — `ReadAt`/`WriteAt`/`Truncate`/`Sync`/`Size`/`Lock`/`Unlock`/
  `CheckReservedLock`/etc. `internal/vfs.File` delegates all except the `-wal`
  write path.
- **Optional File capability interfaces** — a `File` may also implement
  `FileSharedMemory` (exposes `SharedMemory()`), `FileLockState`,
  `FileCheckpoint`, `FileCommitPhaseTwo`, `FilePersistWAL`, `FilePragma`,
  `FileSizeHint`, `FileControl`-style ones, `FileUnwrap`, etc. **When wrapping,
  forward these through** (type-assert the underlying file and re-expose), or
  you silently disable WAL/shm and other features. `FileUnwrap` in particular
  exists so shims can expose the wrapped file — mirror it.
  `internal/vfs/file.go`'s long list of `var _ sqlite3vfs.FileXxx = (*File)(nil)`
  assertions plus `vfsutil.WrapXxx` forwarding calls is this requirement, made
  exhaustive.
- `Filename` — has `Database()`, `Journal()`, `WAL()`, `URIParameter(...)`. Used
  in `internal/vfs.VFS.Open`/`OpenFilename` to tag file type (main-db vs wal vs
  journal) so we only intercept writes on the WAL.
- `SharedMemory` interface + `NewSharedMemory(path, flags)` constructor — see
  next section; the interface is **opaque**.

The default VFS instance is what SQLite uses when no `?vfs=` is given; obtain
the one to wrap via `vfs.Find("")` (or `vfs.Find("os")`, also reserved for the
default) at registration.

---

## Why we implement our own shm layer

`SharedMemory` is an **exported interface with unexported methods** (the
shm map / lock / unmap operations are package-private). `NewSharedMemory`
returns a value satisfying that interface, but you can only hand it *back* to
the library — you cannot call its methods or implement your own from outside
the package.

The follower-apply path (`internal/fsm/walappender`) needs to **drive** the
wal-index directly: take `WAL_WRITE_LOCK`, append frames, update the
pgno→frame page-map slots, and advance `mxFrame` with a tear-safe header
write. None of that is reachable through the opaque interface.

`internal/fsm/walappender/shm/` is therefore an **original implementation**
of the same wal-index mmap/locking protocol (`Open`, `Region`,
`Lock`/`RLock`/`TryLock`/`Unlock`, `Close`) — not a copy of ncruces' concrete
shm code. It has to stay in lockstep with the on-disk wal-index layout and
lock offsets SQLite-in-wasm actually uses, or readers corrupt; re-verify
against those when bumping the pinned `go-sqlite3` version.

This is unrelated to the top-level `vendor/` directory (`go mod vendor`'s own
output, gitignored, a full mirror of every dependency) — that's refreshed
with a plain `go mod vendor` when needed and never hand-edited.

**Coordination within one process.** Our shm handle and SQLite's own shm
handle both map the same `-shm` file (`MAP_SHARED`) and use OFD locks. Two
FDs in the same process *do* contend on OFD locks (per open-file-description,
not per-process), and two mmaps of the same file are coherent. So the
follower-apply handle coordinates with SQLite's handle exactly as two
processes would — which is the behavior we want.

---

## The *other* lock: the main `.db` file's own SHARED lock

Distinct from everything above, and easy to miss: every connection with a
database open — WAL mode included — also holds a plain OS-level **SHARED**
lock on the *main* `.db` file itself (not `-shm`), for as long as it's open,
using the exact same classic byte-range locking scheme non-WAL SQLite always
used (`os_unix.c`'s `SHARED_FIRST`/`SHARED_SIZE`, see `WAL_FORMAT.md §main
.db file locking`). This lock does nothing to coordinate readers and writers
during normal WAL operation — that's what the wal-index's own read-mark
slots and `WAL_WRITE_LOCK` (previous section) are for. Its only real purpose
is `sqlite3_close`'s "am I the last connection with this database open
anywhere?" check: closing tries to upgrade this shared lock to exclusive,
which only succeeds if no other connection, in this or any other process,
still holds it — and if it succeeds, that connection is entitled to
checkpoint-and-delete `-wal`/`-shm` on its way out.

A node's own long-lived connections don't reliably hold this lock just by
being open and otherwise idle, which meant an ordinary, transient external
reader (requirement #3) could legitimately win that upgrade race and delete
a node's `-wal` out from under it (`DECISIONS.md` ADR-012). `fsm/dblock.go`
takes this lock explicitly and holds it for the node's whole lifetime — see
that ADR and `WAL_FORMAT.md` for the mechanics.

---

## wal-index page-map ≠ our future OCC pagemap

Stated in CLAUDE.md but worth repeating here because
`internal/fsm/walappender/shm/` touches the first one:

- **wal-index page-map** — the pgno→WAL-frame hash slots that live *inside*
  the `-shm` in SQLite's own format. Updated during follower apply so readers
  can locate frames. `internal/fsm/walappender/shm/` manipulates this.
- **OCC pagemap** — a *separate*, deferred (`DECISIONS.md` ADR-008) structure
  of pgno→version for validating forwarded writes. Not built yet. Not in
  `shm/`.

---

## PRAGMAs / DSN

- `?vfs=<our-registered-name>` in the DSN to select the wrapper
  (`driver.Driver`'s `dsn()` builds this automatically; a caller driving
  `internal/vfs`/`internal/raft/gate` directly needs to do it by hand).
- `journal_mode=WAL`, `synchronous=NORMAL` (see `DECISIONS.md` ADR-006).
  `fsm.New` sets the former once per database; `driver.Driver`'s connector
  sets the latter on every pooled connection it opens (not just the first —
  it's a per-connection setting).
- Keep at least one RW connection open per process so the wal-index stays
  live for external readers — and hold the main-`.db`-file shared lock
  explicitly too (`DECISIONS.md` ADR-012); a kept-alive connection alone is
  not sufficient for the latter.
- If you ever must use `nolock=1`/`EXCLUSIVE` locking (unsupported-platform
  fallbacks), note they are **incompatible with requirement #3** and with the
  `database/sql` pool (`db.SetMaxOpenConns(1)` required) — avoid on the main
  path.

---

## Related upstream VFSes worth reading for patterns

- `vfs/adiantum` and `vfs/xts` — wrapping VFSes (encryption at rest). Good
  reference for how to wrap `VFS`/`File` and forward the optional capability
  interfaces correctly.
- `vfs/memdb`, `vfs/mvcc` — custom VFSes from scratch; useful for the
  shm/locking shape, though both are in-memory (not file-compatible).
- `ncruces/litestream` — read-replica streaming; adjacent problem (shipping
  WAL changes) with a different topology.
