# WAL_FORMAT

On-disk byte layout reference for the format-sensitive tasks in this repo:
**commit-frame interception** (read-only parse in `internal/vfs.File.WriteAt`),
**follower apply** (writing frames + driving the wal-index,
`internal/fsm/walappender`), and **external-reader-safe WAL/-shm lifetime**
(`fsm/dblock.go`). This is a working reference: confirm against the
authoritative spec at https://sqlite.org/walformat.html and
https://sqlite.org/fileformat2.html before relying on any offset.

All multi-byte integers in the WAL and wal-index headers are **big-endian**.

---

## `-wal` file

```
[ 32-byte WAL header ][ frame ][ frame ] ...
```

### WAL header (32 bytes, offset 0)

| off | size | field                                                     |
|-----|------|-----------------------------------------------------------|
| 0   | 4    | magic (0x377f0682 or 0x377f0683; low bit = checksum endian)|
| 4   | 4    | file format version (3007000)                             |
| 8   | 4    | page size                                                 |
| 12  | 4    | checkpoint sequence number                                |
| 16  | 4    | salt-1 (random, changes each WAL reset/checkpoint-restart)|
| 20  | 4    | salt-2 (random)                                           |
| 24  | 4    | checksum-1 (over first 24 bytes)                          |
| 28  | 4    | checksum-2                                                |

The salts are **per-node / per-WAL-epoch**; this is why page images are
shipped and re-encoded per node (`DECISIONS.md` ADR-003), rather than raw WAL
bytes.

### Frame = 24-byte frame header + `page_size` bytes of page data

Frame header:

| off | size | field                                                                 |
|-----|------|-----------------------------------------------------------------------|
| 0   | 4    | **page number** (pgno)                                                |
| 4   | 4    | **db size in pages after commit** (**non-zero ⇔ commit frame**, else 0)|
| 8   | 4    | salt-1 (must equal the WAL header's salt-1 for the frame to be valid) |
| 12  | 4    | salt-2                                                                 |
| 16  | 4    | checksum-1 (cumulative over all frames so far + this frame's data)    |
| 20  | 4    | checksum-2                                                             |

**Commit-frame detection (leader `WriteAt`, `internal/vfs/walframe.go`'s
`parseFrameHeader`/`frameHeader.isCommit`):** parse bytes 4–7 of each frame
header. Zero → non-commit frame (pass through). Non-zero → commit frame (this
is the value of `nTruncate`; withhold and gate).

**Frame validity / recovery:** on recovery SQLite replays frames whose salts
match the header and whose cumulative checksum is valid, up to the last frame
that is a valid **commit** frame. A withheld commit frame therefore leaves the
preceding data frames inert, which is the basis for the gate's crash-safety
(`DESIGN.md §write path` step 5).

**Follower apply** (`internal/fsm/walappender/frame.go`'s
`Frame.encodeHeader`, `checksum.go`'s `checksum`) must, per frame, recompute
checksum-1/2 as the running cumulative checksum using *this node's* salts and
write the header accordingly, reproducing SQLite's WAL checksum algorithm
(`wal.c`'s `walChecksumBytes`: two Fibonacci-weighted running sums, 8 bytes at
a time) exactly.

---

## `-shm` file (wal-index)

Shared-memory wal-index. SQLite-format; both SQLite's own handle and the
`internal/fsm/walappender/shm/` handle map it. Layout (per
https://sqlite.org/walformat.html#the_wal_index, confirm offsets against the
version in use):

- **Two copies of the wal-index header** followed by a checksum. The two-copy +
  barrier write is the **tear-safe publish protocol**: writers write copy 1,
  barrier, write copy 2; readers read copy 1, barrier, read copy 2, and accept
  only if the two agree and the checksum matches (retrying otherwise).
  `mxFrame` lives in this header. Follower apply MUST reproduce this exact
  protocol when advancing `mxFrame` (`internal/fsm/walappender/walindex.go`'s
  `readWALIndexHeader`/`writeWALIndexHeader`), or concurrent readers
  (including external processes) observe torn state.
- **Checkpoint-info block**: `nBackfill` and the array of reader **read-marks**
  (`aReadMark[WAL_NREADER]`, `WAL_NREADER = 5`). Checkpoint backfills only up to
  the minimum active read-mark (`DESIGN.md §checkpoint`).
- **Page-map / hash table**: maps pgno → the frame index in the WAL holding the
  most recent copy of that page. This is the "shm page map" the follower-apply
  path updates (`internal/fsm/walappender/walindex.go`'s
  `addFrameToWALIndex`/`framePage`/`frameZero`; distinct from the deferred OCC
  pagemap, see `NCRUCES_NOTES.md`). Readers use it to find the newest frame
  `<= mxFrame` for each page.

### Locking region (byte-range locks on the shm)

SQLite reserves a set of locking slots in the wal-index
(`internal/fsm/walappender/shm/shm.go`'s `WriteLock`/`CheckpointLock`/
`RecoverLock`/`ReadLock(i)` constants). The relevant ones:

- `WAL_WRITE_LOCK` (index 0): the single-writer lock. SQLite takes it for a
  local write txn (the signal that starts the capture buffer). Follower apply
  takes it directly via `internal/fsm/walappender/shm/`.
- `WAL_CKPT_LOCK` (index 1): held by the checkpointer.
- `WAL_RECOVER_LOCK` (index 2): held during wal-index recovery/rebuild.
- `WAL_READ_LOCK(0..WAL_NREADER-1)` (indices 3–7): reader slots, one per
  read-mark. A read txn takes a SHARED lock on the slot matching its chosen
  read-mark. **Acquisition of a read lock is the read-snapshot-established
  signal** the (deferred) read-set accumulator would key its reset on
  (`DECISIONS.md` ADR-008 step 4).

These are all `-shm`-file locks, byte offsets `baseOffset + index` within
that file (`shm.go`'s `Lock`/`RLock`/`Unlock`). They are not the same lock
as the next section's: that one lives on the *main* `.db` file, is a
completely different byte range in a completely different file, and exists
for a different reason.

---

## Main `.db` file locking

Distinct from everything in `-shm` above: SQLite's classic (pre-WAL,
`os_unix.c`-derived) locking byte range on the **main database file itself**,
which every connection (WAL mode included) still participates in, not just
legacy rollback-journal-mode ones:

| constant        | value                    | purpose                                                          |
|-----------------|--------------------------|-------------------------------------------------------------------|
| `PENDING_BYTE`   | `0x40000000`             | base offset (`1GiB` into the file; never actually written)        |
| `RESERVED_BYTE`  | `PENDING_BYTE + 1`       | reserved-lock byte (rollback-mode `BEGIN IMMEDIATE`/write intent) |
| `SHARED_FIRST`   | `PENDING_BYTE + 2`       | start of the shared-lock byte range                                |
| `SHARED_SIZE`    | `510`                    | length of the shared-lock byte range                               |

A connection's **SHARED** lock (an `F_RDLCK` OFD lock over
`[SHARED_FIRST, SHARED_FIRST+SHARED_SIZE)`) is what every open connection
(WAL or not) holds for as long as it has the database open at all. This lock
does **not** coordinate readers/writers during normal WAL operation (that's
entirely the previous section's job); its only real purpose is
`sqlite3_close`'s own "am I the last connection with this database open,
anywhere?" check, which tries to upgrade this same shared lock to
**EXCLUSIVE** and, on success, checkpoints and deletes `-wal`/`-shm`.

`fsm/dblock.go`'s `acquireSharedDBLock` takes this exact lock (a raw
`F_RDLCK` OFD lock via the same `readLock` helper pattern
`internal/fsm/walappender/shm/lock_linux.go`/`lock_darwin.go` already use for
the `-shm` locks above) on the main `.db` file directly, held for the
`fsm.FSM`'s whole lifetime, so an external reader's own close can never
observe zero other holders and delete the WAL out from under a node. See
`DESIGN.md §external-reader safety` and `DECISIONS.md` ADR-012 for why this
exists and why it must be acquired only *after* `PRAGMA journal_mode=WAL`
has already succeeded on that connection (acquiring it earlier collides with
the one-time rollback-journal→WAL conversion, which needs more exclusive
access than steady-state WAL operation ever does again).

---

## `.db` file (main database): header fields touched by writes

Only the bits relevant to remembering *why single-row writes still touch global
state* (`DECISIONS.md` ADR-008 note: a follower's images depend on global db
state, not just the edited table). The 100-byte db header at offset 0 includes:
page size (16–17), file change counter (24–27), db size in pages (28–31),
freelist trunk page & count (32–39), schema cookie (40–43), and the
version-valid-for number (92–95). Freelist churn and pointer-map pages (in
auto-vacuum dbs) also move on writes that don't touch user rows directly.
Hence physical redo ships whatever pages actually changed, and OCC read-sets
legitimately include page 1.

---

## Quick checklist for the three tasks

**Commit-frame interception (leader, read-only in `internal/vfs.File.WriteAt`):**
1. Is this a `-wal` write at a frame boundary? (track offset from WAL header +
   n·(24+page_size)).
2. Parse pgno (0–3) and commit marker (4–7).
3. commit marker == 0 → append `(pgno, data)` to capture, pass write through.
4. commit marker != 0 → append, withhold write, propose to the gate.

**Follower apply (`internal/fsm/walappender`; writes `-wal` + drives
wal-index):**
1. Take `WAL_WRITE_LOCK`.
2. For each `(pgno, data)`: build a 24-byte frame header with this node's salts,
   compute running checksums, write header+data at the next WAL offset; set the
   commit marker (bytes 4–7 = `nTruncate`) on the final frame only.
3. Update the page-map hash slots for each pgno.
4. Advance `mxFrame` via the tear-safe two-copy header write + barrier.
5. Release `WAL_WRITE_LOCK`.

**External-reader-safe WAL lifetime (`fsm/dblock.go`):**
1. After `PRAGMA journal_mode=WAL` has already succeeded on this path, open a
   raw file descriptor on the main `.db` file.
2. Take a blocking `F_RDLCK` OFD lock on `[SHARED_FIRST, SHARED_FIRST+SHARED_SIZE)`.
3. Hold it for as long as the node's `fsm.FSM` is open; release only on
   `Close` (closing the descriptor releases the OFD lock).
