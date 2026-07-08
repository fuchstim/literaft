# NCRUCES_NOTES

Everything specific to building this on `github.com/ncruces/go-sqlite3`.
Verified against **v0.35.1** (published 2026-06-16). Re-verify against the
version pinned in `go.mod`; the internals below are unexported and can shift.

---

## The engine is a pure-Go reimplementation

ncruces replaces SQLite's OS interface with a pure-Go VFS running SQLite itself
as wasm via wazero. No cgo. Consequences:

- There is **no C `unix` VFS to delegate to.** "Base VFS" = ncruces' own default
  Go VFS. Our wrapper wraps *that*.
- On Linux/macOS the default VFS uses **`mmap` for the wal-index shared memory**
  and **OFD locks** for file locking — the same mechanisms SQLite uses, which is
  why the docs state the default build is **compatible with the standard Unix
  and Windows SQLite VFSes**. That compatibility is the basis for requirement #3
  (external read-only processes).

---

## On-disk / external-reader compatibility — a claim to TEST

The package documents default-build compatibility with the standard Unix SQLite
VFS. But our requirement #3 (an *unmodified* stock SQLite process opens the db
read-only, concurrently) rests on lock interoperability:

- ncruces default uses **OFD locks** (`F_OFD_SETLK`).
- Stock SQLite's unix VFS uses classic **POSIX advisory locks** (`F_SETLK`).

On Linux these two lock types **do contend with each other** in the kernel, so
coordination should work — but this is exactly the kind of thing to verify
empirically rather than assume. Make it an early milestone (ROADMAP M1): write a
db with our node process, then open it read-only with the system `sqlite3` CLI
(or a C program) *while a write is in flight*, and confirm the reader never sees
a torn / un-replicated state and that checkpointing respects the external
reader's read-mark.

**Do NOT ship `sqlite3_dotlk`.** That build tag switches shm to a
cross-platform, **in-process** sharing implementation — WAL dbs then can only be
accessed by a single process, and other processes fail with
`SQLITE_PROTOCOL`/`SQLITE_IOERR`/`SQLITE_CANTOPEN`. That directly breaks
requirement #3. Same caution for `sqlite3_flock` (BSD locks): reduced
concurrency (`BEGIN IMMEDIATE` behaves like `BEGIN EXCLUSIVE`) and a *different*
compatibility class (`unix-flock`). Use the **default** build.

Use `vfs.SupportsFileLocking` and `vfs.SupportsSharedMemory` at startup to fail
fast on an unsupported platform.

---

## The exported VFS surface we build on

From `github.com/ncruces/go-sqlite3/vfs` (names as of v0.35.1):

- `Register(name, vfs)` / `Unregister(name)` / `Find(name)` — register our
  wrapper under a name we pass via `?vfs=` in the DSN.
- `VFS` interface — `Open(name Filename, flags OpenFlag) (File, error)` plus
  access/delete/full-pathname. Our wrapper implements this and calls the wrapped
  VFS inside `Open`.
- `File` interface — `ReadAt`/`WriteAt`/`Truncate`/`Sync`/`Size`/`Lock`/`Unlock`/
  `CheckReservedLock`/etc. Our wrapping `File` delegates all except the `-wal`
  write path.
- **Optional File capability interfaces** — a `File` may also implement
  `FileSharedMemory` (exposes `SharedMemory()`), `FileLockState`,
  `FileCheckpoint`, `FileCommitPhaseTwo`, `FilePersistWAL`, `FilePragma`,
  `FileSizeHint`, `FileControl`-style ones, `FileUnwrap`, etc. **When wrapping,
  forward these through** (type-assert the underlying file and re-expose), or you
  silently disable WAL/shm and other features. `FileUnwrap` in particular exists
  so shims can expose the wrapped file — mirror it.
- `Filename` — has `Database()`, `Journal()`, `WAL()`, `URIParameter(...)`. Use
  it in `Open` to tag file type (main-db vs wal vs journal) so we only intercept
  writes on the WAL.
- `SharedMemory` interface + `NewSharedMemory(path, flags)` constructor — see
  next section; the interface is **opaque**.

The default VFS instance is what SQLite uses when no `?vfs=` is given; obtain the
one to wrap via `vfs.Find("")` (or the documented default-name) at registration.

---

## Why the SHM must be VENDORED (the crux)

`SharedMemory` is an **exported interface with unexported methods** (the
shm map / lock / unmap operations are package-private). `NewSharedMemory` returns
a value satisfying that interface, but you can only hand it *back* to the
library — you cannot call its methods or implement your own from outside the
package.

The follower-apply path needs to **drive** the wal-index directly: take
`WAL_WRITE_LOCK`, append frames, update the pgno→frame page-map slots, and
advance `mxFrame` with a tear-safe header write. None of that is reachable
through the opaque interface. Hence:

**Plan:** copy the concrete shm implementation (the mmap-backed struct and its
platform lock files — on Linux that's the OFD-lock path) into our own `shm/`
package, exporting the operations we need. Record the **exact upstream commit
hash** it was copied from in `shm/UPSTREAM.md`, and add a CI check or a note to
re-diff on dependency bumps (the wal-index layout and lock offsets must stay in
lockstep with the version SQLite-in-wasm actually uses, or readers corrupt).

Files to look for upstream (names circa v0.35.1, verify): `vfs/shm.go`
(interface + `NewSharedMemory`), and the platform impls
(`vfs/shm_*.go` — the mmap/OFD variants). Copy the concrete struct + the
constants for the locking byte offsets and reader-slot layout.

**Coordination within one process.** Our vendored shm handle and SQLite's own
shm handle both map the same `-shm` file (`MAP_SHARED`) and use OFD locks. Two
FDs in the same process *do* contend on OFD locks (that's per open-file-
description, not per-process), and two mmaps of the same file are coherent. So
the follower-apply handle coordinates with SQLite's handle exactly as two
processes would — which is the behavior we want.

---

## wal-index page-map ≠ our future OCC pagemap

Stated in CLAUDE.md but worth repeating here because the vendored code touches
the first one:

- **wal-index page-map** — the pgno→WAL-frame hash slots that live *inside* the
  `-shm` in SQLite's own format. Updated during follower apply so readers can
  locate frames. Vendored `shm/` manipulates this.
- **OCC pagemap** — a *separate*, deferred (`DECISIONS.md` ADR-008) structure of
  pgno→version for validating forwarded writes. Not built yet. Not in `shm/`.

---

## PRAGMAs / DSN

- `?vfs=<our-registered-name>` in the DSN to select the wrapper.
- `journal_mode=WAL`, `synchronous=NORMAL` (see ADR-006).
- Keep at least one RW connection open per process so the wal-index stays live
  for external readers.
- If you ever must use `nolock=1`/`EXCLUSIVE` locking (unsupported-platform
  fallbacks), note they are **incompatible with requirement #3** and with the
  `database/sql` pool (`db.SetMaxOpenConns(1)` required) — avoid on the main
  path.

---

## Related upstream VFSes worth reading for patterns

- `vfs/adiantum` and `vfs/xts` — wrapping VFSes (encryption at rest). Good
  reference for how to wrap `VFS`/`File` and forward the optional capability
  interfaces correctly.
- `vfs/memdb`, `vfs/mvcc` — custom VFSes from scratch; useful for the shm/locking
  shape, though both are in-memory (not file-compatible).
- `ncruces/litestream` — read-replica streaming; adjacent problem (shipping WAL
  changes) with a different topology.
