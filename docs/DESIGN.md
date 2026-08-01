# DESIGN

The complete design for the RAFT-backed SQLite VFS. `CLAUDE.md` has the summary;
this is the reference. `DECISIONS.md` records why alternatives were rejected.

---

## Architecture

```
  application
      │  (N read-write connections, same process)
      ▼
  SQLite core  ──►  internal/vfs  ──delegates most ops──►  ncruces default VFS
                       │                                       │
                       │                                       └─► .db / -wal / -shm
                       │                                            (SQLite-compatible)
                       └── commit-frame interception ──► internal/raft/gate.Gate ──► log.SingleWriterLog ──► RAFT (hashicorp/raft)

  follower-apply path (separate from SQLite's own I/O):
      RAFT committed entry ──► fsm.FSM.Apply ──► internal/fsm/walappender ──► shm/ ──► -wal + wal-index
                                                                              (mxFrame, page-map, write lock)

  one running node (cmd/literaft/main.go's run(), or driver.New for an embedder):
      hraft.Raft  ──►  log.NewSingleWriterLog  ──►  raftgate.LogAdapter
      fsm.FSM                                              │
        │                                                  │
        └──────────────────►  driver.New(fsm, log)  ◄──────┘
                                     │
                                     ├─► internal/raft/gate.Gate + internal/vfs.Register
                                     └─► database/sql driver.Driver (ncruces/go-sqlite3/driver underneath)
```

`internal/vfs` is a **wrapper** around ncruces' default (pure-Go) VFS. `Open`
tags each file by type and returns a wrapping `File`. Nearly everything
delegates unchanged; the wrapper adds logic in exactly one place on the normal
path (`xWrite` on the `-wal` file) plus file tagging in `Open`.

Because the wrapper delegates `SharedMemory()` (the `FileSharedMemory` interface)
to the underlying `File`, the wal-index lives in the real file-backed mmap and
stays standard. That lets external unmodified SQLite processes mmap the same
`-shm`, honor the same read-marks, and never observe an un-published (= not
RAFT-committed) frame.

The **follower-apply** path does not go through SQLite. `internal/fsm/walappender`
opens its own handle to the `-shm`/`-wal` using its own shm implementation
(`internal/fsm/walappender/shm/`) and drives `mxFrame`, the page-map hash
slots, and `WAL_WRITE_LOCK` directly, coordinating with SQLite's own handle
via OFD locks and the shared mmap. See `§follower-apply`.

`fsm.FSM` (package `fsm`) is the single object gluing these together: it owns
the node's own SQLite connection (for schema/page-size probing, and to hold
the main-db-file shared lock described in `§external-reader safety` below),
a `walappender.WALAppender`, and a `snapshotter.Snapshotter`, and implements
`hraft.FSM` directly (`Apply`/`Snapshot`/`Restore`).

`internal/raft/gate.Gate` (the concrete implementation of `vfs.Gate`) is
thin and hraft-agnostic: it encodes a captured write transaction
into an `internal/raft/proto.Entry`, self-skip-marks it on the `fsm.FSM`
(`§RAFT log entry format` below), marshals it, and hands the bytes to a
`raftgate.LogAdapter` (a one-method interface, `Apply([]byte) error`).
`log.SingleWriterLog` (package `log`, a sibling of `driver`/`fsm`, not the
standard library) is the real, hraft-backed `LogAdapter`: it owns the
`*hraft.Raft` handle, the leader/ready/drain state machine (`§Role
transitions` below), and translates hraft's own failure modes into the shared
rejection taxonomy in `internal/raft/gate/errors` (`rafterrors`): a
`*rafterrors.NotLeaderError`, a `*rafterrors.CatchingUpError`, etc. Gate knows
nothing of any of that; it would work unchanged against a `LogAdapter` backed
by some other consensus mechanism entirely.

A second `LogAdapter` is designed but not yet built (`FOLLOWER_WRITES.md`,
milestone M9/issue #32): `log.ForwardingLog` wraps a `*log.SingleWriterLog`
plus a caller-supplied `log.LeaderTransport` and, on a follower, forwards
the proposal to the leader under a base-index check instead of failing it.
`driver.New`, the gate, and this section's write path are untouched by it,
which is what the `LogAdapter` seam is for.

`driver.New(fsm, log)` takes a `*fsm.FSM` and a `raftgate.LogAdapter` the
caller already constructed, builds the `internal/raft/gate.Gate` around the
LogAdapter, registers a process-unique gated VFS (`internal/vfs.Register`),
and returns a `database/sql`-compatible `driver.Driver`. `cmd/literaft/main.go`'s
`run()` is the reference caller: it wires the `hraft.Raft`
transport/log-store/snapshot-store (log-store and stable-store backed by
`raftsqlite.Store`, a SQLite-backed `raft.LogStore`/`raft.StableStore` that
replaces `raft-boltdb`), builds the `fsm.FSM`, wraps the `hraft.Raft` in a
`log.NewSingleWriterLog`, and hands the `fsm.FSM` plus that `LogAdapter` to
`driver.New`. There is no intermediate "node" package doing this wiring on
`driver`'s behalf; `driver.New` and the direct-caller pattern is that
wiring layer.

---

## RAFT log entry format

A write transaction becomes one RAFT log entry (`internal/raft/proto.Entry`,
a protobuf message generated from `internal/raft/proto/entry.proto`): a
`Header` carrying a per-proposal ID, and a `Transaction` payload with an
ordered list of `(pgno, page_image)` pairs (`Page`) and `nTruncate` (the
post-commit database size in pages, which doubles as the commit marker).
`Entry.payload` is a oneof so future entry kinds besides `Transaction` (e.g.
config changes) can share the same `Header`.

Rationale for physical redo over the page list is in `DECISIONS.md` ADR-003.
Full page images under a total order are self-consistent by
construction: entry E2's writer read the post-E1 snapshot (local
serialization guarantees it), so E2's image already incorporates E1, and
"latest full image per page under total order" is serial-execution order.
Nothing to reconcile even when consecutive entries touch the same page. The
price is the strict in-order, gapless apply requirement.

`Header.Id` is not a node identifier; it is a per-proposal token (a UUID,
freshly generated by `Gate.proposeTransaction` for every call). Its only job
is letting `fsm.FSM.Apply` recognize "this is the one proposal I'm still in
the middle of," so it doesn't re-materialize via
`walappender.AppendTransaction` something the leader already published
itself via its own SQLite write path (ADR-005, `§follower-apply` below).
`Gate.proposeTransaction` marks the ID in `fsm.FSM.skipEntries` immediately
before its own `LogAdapter.Apply` call (which, for `log.SingleWriterLog`,
invokes `hraft.Raft.Apply`) and clears it (deferred)
immediately after that call returns. See ADR-011 for why that transience,
not a permanent per-entry property, is what makes this safe.

There is no client-request-ID field for ambiguous-commit dedup; that
remains fully deferred (ADR-003, ADR-008/ADR-009). `Header.Id` looks similar
but serves the self-apply skip specifically; don't conflate the two if a
reqID field is ever added.

---

## Write path (leader): step by step

1. A RW connection begins a write txn. SQLite takes `WAL_WRITE_LOCK`
   (exclusive shm lock on the write slot). `internal/vfs.File` observes this
   and starts a **capture buffer** for this writer, recording the WAL offset
   (= start of frames beyond current `mxFrame`).

2. SQLite writes frames via `xWrite` to `-wal`. For each frame the wrapper
   parses `pgno` (header bytes 0–3) and the page image, and appends
   `(pgno, data)` to the capture buffer.
   - **Non-commit frames** (header bytes 4–7 == 0, e.g. cache spill on a large
     txn) are **passed straight through to disk**. They sit beyond `mxFrame`
     (invisible) and, without a trailing valid commit frame, are inert on
     recovery, so writing them through is safe and preserves the memory
     relief that spilling exists to provide.
   - **The commit frame** (header bytes 4–7 != 0) is **withheld**: buffered, not
     yet written to the real `-wal`.

3. On the commit frame, `internal/vfs.File` calls
   `Gate.ProposeTransaction(frames, nTruncate)` (`internal/raft/gate.Gate`,
   the concrete implementation of the `vfs.Gate` interface) with the whole
   captured batch of `(pgno, page)` frames plus the post-commit database
   size, and **blocks**. `Gate.proposeTransaction` builds an
   `internal/raft/proto.Transaction` from them, wraps it in an
   `internal/raft/proto.Entry` with a fresh per-proposal `Header.Id`,
   marshals it, and hands the bytes to its `raftgate.LogAdapter`'s `Apply`
   (which, for a real cluster, is `log.SingleWriterLog.Apply` →
   `hraft.Raft.Apply`; see `§RAFT log entry format` above). RAFT needs no
   SQLite lock, so readers keep going; only other writers wait (they're
   behind the write lock anyway).

4. **RAFT commits (quorum):** `Gate.ProposeTransaction` returns `nil`;
   `internal/vfs.File` flushes the withheld commit frame to the real `-wal`,
   then returns `SQLITE_OK` from `xWrite`. SQLite proceeds normally: optional
   `xSync`, then `walIndexAppend` + `walIndexWriteHdr`, the standard
   two-copy, barriered header publish that advances `mxFrame`. The txn is now
   visible. The shm is never touched here; SQLite's own publish provides the
   exact tear-safe protocol. The proposing
   node's own `fsm.FSM.Apply` for this same committed entry sees its
   `Header.Id` still marked in `skipEntries` (set just before this
   `LogAdapter.Apply` call, cleared right after it returns) and skips
   re-materializing via `walappender`; everyone else applies it normally.

5. **RAFT fails** (lost leadership / timeout / no quorum / not-leader):
   `Gate.ProposeTransaction` returns whatever its `LogAdapter.Apply` returned.
   For `log.SingleWriterLog`, that is one of the shared `rafterrors` taxonomy
   errors: a `*rafterrors.NotLeaderError` (redirect, surfaced as
   `sqlite3.READONLY`), a `*rafterrors.CatchingUpError` (retryable, surfaced
   as `sqlite3.BUSY` so a caller retries rather than treats it as a hard I/O
   failure), a `*rafterrors.NotAppliedError` (retryable), or a
   `*rafterrors.AmbiguousError` (possibly-committed, surfaced as the default
   `IOERR_WRITE`). Each error's category fixes its `sqlite3` result code
   (`rafterrors.Category.ResultCode`), and `internal/vfs.File` reads it via
   `vfs.ErrCode`, discards the buffered commit frame, and returns the
   corresponding I/O error from `xWrite`. The failure occurs before the
   wal-index append, so `mxFrame` never moves and the shm is untouched. The
   data frames already on disk are inert and get overwritten by the next txn
   at the same offset. `COMMIT` fails; the app rolls back and redirects to the
   leader (the `NotLeaderError` carries a leader hint; see
   `Gate.LastRejection`, `driver.Driver.LastRejection`).

Because the gate is the commit-frame write, not a sync, this works under
`synchronous=NORMAL` (see `§durability`).

---

## Read path

Entirely stock: `internal/vfs` passes `xRead` and all shm methods through.

1. Read the wal-index header (two-copy + checksum), learn `mxFrame`.
2. Pick a read-mark slot `<= mxFrame`, take its reader lock.
3. Per page: consult the wal-index hash for the newest frame `<= mxFrame`; if
   found `xRead` from `-wal`, else `xRead` from `.db`.

Holds identically for local RW connections acting as readers, local readers, and
external read-only processes: all share the same file-backed wal-index and none
can see beyond the published `mxFrame`, which the gate guarantees equals
"RAFT-committed on this node."

**Consistency note:** a local read returns this node's committed state.
On the leader that's current; on a follower it may lag. For linearizable reads,
route to the leader under a lease or use a RAFT read-index barrier (RAFT-side,
not VFS). Still deferred (issue #35).

---

## Checkpoint path

Checkpointing is **local to each node, no consensus**: it relocates
already-committed frames from `-wal` into `.db`, changing no logical state.
`fsm.FSM`'s `walappender.WALAppender` drives this itself (`runCheckpointer`):
a `PRAGMA wal_checkpoint(PASSIVE)` on a dedicated connection, on a timer
(`WithCheckpointInterval`, default 5s) and after `WithCheckpointThresholdPages`
dirty pages have accumulated (default 1000). This runs unconditionally, on
every node regardless of leader/follower role, since a passive checkpoint is
harmless (and a no-op if it can't make progress) either way. `internal/vfs`
does not intercept `wal_checkpoint` itself.

1. Take `CHECKPOINT` lock.
2. Read `nBackfill` and reader read-marks from the wal-index checkpoint-info
   block; backfill frames `-wal` → `.db` **only up to the minimum read-mark held
   by any reader**, including external read-only processes, since they
   registered their locks in the shared `-shm`. Standard mechanism; works
   because the shm is delegated.
3. Sync `.db`, advance `nBackfill`.
4. For `RESTART`/`TRUNCATE`: once fully backfilled and no reader needs old WAL
   content, reset the WAL (`mxFrame → 0`, new salt, header change-counter
   bumped). External readers detect the change and re-read. Standard.

Only step 4 ever bounds `-wal` size, and PASSIVE never reaches it, which is
true in stock SQLite too. Sqlite.org's own "Checkpointing" docs explain how
a stock db still stays bounded under nothing but PASSIVE auto-checkpoints:
"whenever a write operation occurs, the writer checks how much progress the
checkpointer has made, and if the entire WAL has been transferred into the
database and synced and if no readers are making use of the WAL, then the
writer will rewind the WAL back to the beginning." That is the writer's own
job, separate from whatever checkpoint mode last ran. `walappender` does not
get this automatically the way a real SQLite writer does, since `AppendFrames`
(`internal/fsm/walappender/walappender.go`) is a hand-rolled writer path;
`rewindLogIfBackfilled` adds the equivalent check:

- Before appending, if `nBackfill == mxFrame` (everything already
  backfilled), make one non-blocking attempt at `WAL_READ_LOCK(1)` through
  the last reader slot (the same range real SQLite's own `walRestartHdr`
  takes). Lock 0 is excluded: per SQLite's own wal.c, that is the
  mark new readers already fall back to exactly when `nBackfill == mxFrame`
  (ignoring the WAL entirely, reading straight from `.db`), so it never needs
  protecting from this reset.
- On success, reset the header (`mxFrame → 0`, fresh salt) and write a fresh
  on-disk WAL file header (the same machinery `maybeBootstrap` uses to start
  a brand-new WAL), then continue appending from the beginning.
- If the locks are unavailable, append after the existing `mxFrame`,
  same as a checkpoint that can't make progress.

Without this, a follower that stays caught up purely through ordinary log
replay (never needing `InstallSnapshot`, the only other place a
RESTART/TRUNCATE-equivalent reset happens) has no bound on its `-wal` size at
all.

One correctness note specific to `checkpoint()`'s own `*sqlite3.Conn`: opened
fresh, it silently declines every `WALCheckpoint` call (`nLog`/`nCkpt` come
back `-1`/`-1`, no error) until it has executed at least one prior read
(confirmed empirically, not documented upstream). `walappender.Open` primes it
with one throwaway `SELECT count(*) FROM sqlite_master` right after bootstrap,
which is enough for every later call on that same connection.

One integration note: checkpoints never race the RAFT gate (different locks,
and the gate only fires on commit-frame writes). A `TRUNCATE`-checkpointed
`.db` is also the natural precursor to a **RAFT snapshot** (see "Follower-apply
path" below), not because the snapshot mechanism (SQLite's online backup API)
requires it for correctness, but because it shrinks the amount of WAL the
backup has to walk and the window in which a concurrent writer can force it
to restart. RAFT then compacts its log below the snapshot's applied index.
`internal/fsm/snapshotter.Snapshotter.Snapshot` does not force this checkpoint
itself; it calls SQLite's backup API directly against whatever's on disk,
which is correct either way (Backup resolves pages still only in the `-wal`),
just potentially slower under a busy concurrent writer.

---

## Follower-apply path: step by step

Followers have no local writer producing frames, so `fsm.FSM.Apply` must
materialize the batch itself via `internal/fsm/walappender`. This is the most
format-sensitive code in the repo; it must reproduce what `walFrames` +
`walIndexWriteHdr` do, exactly, or external readers break.

1. `fsm.FSM.Apply` decodes the committed `internal/raft/proto.Entry` and
   checks whether its `Header.Id` is currently marked in `skipEntries`. If so
   (this node proposed it and is still inside the one
   `Gate.proposeTransaction` call that published it), **return without
   touching the WAL at all**: this node's own SQLite write path already
   published it (`§write path` step 4). Otherwise, continue.

   The marker is transient (set/cleared around one `LogAdapter.Apply` call,
   not a permanent property of the entry) precisely so it does not fire for a
   retroactive commit that lands after that call already returned. See
   `DECISIONS.md` ADR-011 for the two scenarios (hraft's Figure-8 retroactive
   commit; an ordinary restart after this node has taken its own RAFT
   snapshot) a permanent, `NodeID`-keyed version of this check used to get
   wrong.

2. `walappender.WALAppender.AppendTransaction` takes `WAL_WRITE_LOCK` on the shm (via
   `internal/fsm/walappender/shm/`). On a follower there are no local
   writers, so contention is only with the local checkpointer.
3. Append each `(pgno, data)` frame to the local `-wal`, computing **this
   node's** salt-based cumulative checksums (each node has its own WAL
   epoch/salt, which is why page images are shipped, not raw WAL bytes).
4. Update the **wal-index page-map** (pgno→frame hash slots) so readers can find
   the new frames.
5. Publish: write both wal-index header copies with a barrier, advancing
   `mxFrame`. Tear-safe protocol mandatory.
6. Release the write lock.

Local read connections continue throughout via read-marks, exactly as on the
leader.

**InstallSnapshot (very-behind follower):** if the node has fallen past the
leader's retained log, it catches up via snapshot, not replay.
`fsm.FSM.Snapshot`/`Restore` delegate to `internal/fsm/snapshotter.Snapshotter`.
`Snapshot` uses SQLite's online backup API (https://sqlite.org/backup.html)
against whichever node hraft asks (leader or follower) to copy its live "main"
database into a private temp file (the sanctioned way to get a
point-in-time, self-consistent copy), and hands hraft a reader over it, so the
(possibly slow, over-the-network) `Persist` is decoupled from further local
`Apply` calls, which must keep proceeding against live state in the meantime.

`Restore`, on the installing follower, treats the incoming snapshot bytes as
a sequence of whole database pages (page 1, page 2, ..., page N, read
directly off the byte stream at fixed `pageSize` strides) and appends them
as one `internal/fsm/walappender` entry (frame `i` is page `i+1`, `nTruncate`
set to the page count on the last frame), then runs a `TRUNCATE` checkpoint.
This reuses the same append/publish machinery `§follower-apply` steps 2–6
already need, rather than a separate whole-file-swap-and-reopen-every-
connection code path (see `DECISIONS.md` ADR-013 for the earlier design this
replaced). It validates the incoming bytes are a whole multiple of the page
size and that the snapshot's own page-size field (read from its own page 1)
matches this node's configured page size, rejecting otherwise. No connection
ever needs to close and reopen for this to work; `fsm.FSM`'s own long-lived
connection and its `dbLock` (`§external-reader safety` below) stay untouched
throughout.

---

## External-reader safety (WAL/-shm lifecycle)

Requirement #3 needs more than "the on-disk format is standard": it needs a
node's `-wal`/`-shm` to survive a transient external reader connecting and
disconnecting. Real SQLite's own `sqlite3_close` path is willing
to checkpoint-and-delete `-wal`/`-shm` if it can prove (via a plain OS file
lock on the main `.db` file, not anything in `-shm`) that it's the last
connection with the database open. A node process that never establishes its
own persistent claim on that same lock leaves every one of its own
connections (including the long-lived ones `fsm.FSM`/`walappender` keep
open) invisible to that proof, so an ordinary, transient external reader
can legitimately conclude it is the last connection and delete the WAL out
from under the node, orphaning every not-yet-checkpointed
`walappender`-written frame.

`fsm.FSM` closes this gap directly: on construction (`fsm.New`, after
`PRAGMA journal_mode=WAL` is established; see `DECISIONS.md` ADR-012 for why
that ordering matters) it opens its own raw file descriptor on the `.db` path
and takes a plain OS-level **SHARED** lock on SQLite's own reserved
`SHARED_FIRST`/`SHARED_SIZE` byte range (`fsm/dblock.go`; see `WAL_FORMAT.md`
`§main .db file locking` for the exact offsets), held for as long as the
`FSM` is open. This is a completely different lock from anything in
`-shm`: it exists purely for this close-time bookkeeping, is a plain OS
file lock so it's visible across processes (not just within one), and never
pins any SQLite-level snapshot, so it never caps how much of the WAL a
checkpoint can reclaim.

---

## Conflict handling

There is **no page-level conflict resolution**, and there cannot be a state
where two nodes commit conflicting page modifications. Proposing into the
RAFT log is leader-only and RAFT totally-orders every write before any of it
becomes visible. Conflicts are **prevented by serialization**, never merged.
Follower-originated writes are currently rejected outright (ADR-007); the
accepted-but-unbuilt M9 design (`FOLLOWER_WRITES.md`, ADR-015) lets a
follower submit a write to the leader instead, admitted only if it was
computed on exactly the leader's current applied state, still
prevention-by-rejection, zero merge logic.

**Intra-node (two RW conns on the leader):** pure stock SQLite. They serialize
on `WAL_WRITE_LOCK`. Lost-update case: the second writer's read→write upgrade
finds `mxFrame` moved past its snapshot and gets `SQLITE_BUSY_SNAPSHOT`; it must
roll back and retry (or use `BEGIN IMMEDIATE`). The gate does not perturb this:
W1 holds the write lock across its RAFT round-trip; W2 waits; W2's stale-snapshot
check fires as usual. Consequence: **≤1 in-flight local proposal at a time**, so
leader write throughput is capped at one txn per RAFT round-trip. To increase
throughput, batch multiple SQLite txns into one RAFT entry; do not try to
overlap proposals (that breaks the single-write-lock invariant).

**Cross-node (stale leader / split brain):** old leader L1 (minority) and new
leader L2 (majority) both write page P and both gate. Only L2 reaches quorum.
L1's proposal never gets majority (and it learns L2's higher term and steps
down), so L1's gate fails → discard commit frame, I/O error, `mxFrame` unmoved.
At no instant are two conflicting txns both committed. This is RAFT safety
(one-leader-per-term + Leader Completeness); the gate translates "no quorum" into
"clean SQLite txn failure."

**Cross-node (forwarded follower write, designed, not built):** a
follower's captured page images travel to the leader with the base index
they were computed on; the leader accepts iff that equals its own last
applied index, checked under its `WAL_WRITE_LOCK` on a `Ready` leader,
which is what makes the check equivalent to the log-head comparison ADR-008
demanded, closing its lost-update trap. Any concurrent write anywhere stales
an in-flight forward → rejection → the app re-runs the transaction on
fresher state. Full protocol, locking rules, and failure matrix:
`FOLLOWER_WRITES.md`.

**Role transitions:** this whole area of state (leader/ready/
drain) lives in `log.SingleWriterLog`, not `internal/raft/gate.Gate`; see
`§Architecture` above. `Gate` itself has no opinion on any of it; it
forwards whatever its `LogAdapter.Apply` returns.

- *Losing leadership* with an in-flight writer W1 blocked on a proposal: hraft
  itself resolves every in-flight local `Apply`/`Barrier` future with
  `ErrLeadershipLost` before it flips `LeaderCh` to `false`
  (`runLeader`'s step-down path), and the local SQLite writer's own
  `WAL_WRITE_LOCK` (an OFD lock shared with
  `internal/fsm/walappender/shm/`) already serializes any follower-apply
  against a still-in-flight local write. No additional draining needed beyond
  this (`log/singlewriter_test.go`'s "surfaces a lost-leadership proposal"
  case).
- *Gaining leadership:* a node can be **apply-behind** (has every committed log
  entry but hasn't materialized them into local SQLite). Its log is complete so
  it can win an election, but its SQLite state is stale. `log.SingleWriterLog`
  (the real hraft-backed `raftgate.LogAdapter`) tracks a `ready` flag, closed
  (`false`) the instant a leadership term begins and opened only once a
  current-term `hraft.Barrier` call returns, which by construction blocks
  until every already-committed entry, including any backlog, has been sent
  through `fsm.FSM.Apply` on this node (`SingleWriterLog.drain`).
  `SingleWriterLog.Apply` rejects with a `*rafterrors.CatchingUpError` while
  closed (a retryable-category error, surfaced as `sqlite3.BUSY`), which
  `Gate.ProposeTransaction` passes straight through.

  This drain is also what closes the Figure-8 self-apply race: the stale
  entry from an earlier, unfinished leadership stint is always applied
  during the drain, before any new self-proposal can even start (`Ready`
  stays closed until the drain finishes, so any proposal through
  `Gate.ProposeTransaction` during that window is rejected before ever
  reaching `hraft.Apply`), so there's no live per-proposal `skipEntries`
  marker for it to be mistaken against. **See `DECISIONS.md` ADR-011.**

**Ambiguous commit:** RAFT "proposed, outcome unknown" is treated as failure by
the gate, but the entry may have committed cluster-wide. The local app sees a
failed txn that took effect; a blind retry would double-apply.
Client-request-ID dedup stays deferred (issue #34) and, per the M9 forwarding
design, is only ever needed if blind
re-propose is added: a forwarded write never re-sends the same page images
after a possibly-proposed outcome (every retry is a fresh SQL execution on
fresher state), and `FOLLOWER_WRITES.md`'s failure matrix enumerates how
each ambiguous path resolves, including the one case that upgrades this
from anomaly to bug, a local publish failure after commit (issue #60,
fatal by design).

---

## Durability

Durability comes from the **RAFT log quorum**, not local fsync. So:

- `synchronous=NORMAL` is sufficient; `FULL` is not required. The gate is the
  commit-frame write, not a sync.
- The local WAL fsync can be skipped or batched. A crashed node rebuilds its
  WAL tail from the RAFT log on restart, via hraft's own snapshot-restore +
  log-replay (`vendor/github.com/hashicorp/raft`'s
  `restoreSnapshot`/`processLogs`), idempotent by construction since RAFT
  entries are full page images, not deltas: replaying an already-applied entry
  converges to the same state rather than corrupting it. (See ADR-011 for the
  self-apply skip's own, separate correctness history, a distinct concern from
  ordinary crash/restart recovery, now fixed by making the skip transient.)
- Within one machine, the OS page cache keeps external readers coherent
  regardless of fsync.

---

## Log-behind vs apply-behind

- **Log-behind**: missing committed entries. Handled by RAFT's election
  restriction (a voter denies its vote to a less-up-to-date candidate;
  Leader Completeness). Such a node stays a follower; its writes keep failing.
- **Apply-behind**: has the entries, hasn't materialized them. Can win an
  election with stale SQLite state → the trap addressed by the "gaining
  leadership" drain above. This split is specific to this design because the
  write path reads through the state machine's own storage.

---

## Prior art

- **LiteFS** (Fly.io): closest model (single primary, read-only replicas,
  per-txn WAL frame shipping, real on-disk files), but intercepts at the
  filesystem via FUSE, not a VFS.
- **dqlite** (Canonical): the canonical RAFT + custom-VFS design, but in-memory
  and historically relies on a patched SQLite replication hook, so it does not
  provide standard on-disk files for external readers.
- **rqlite**: puts RAFT above unmodified SQLite and replicates SQL statements;
  sidesteps the VFS entirely but gives neither in-process multi-writer semantics
  nor shared files.

This project's requirement set (VFS-level, standard files, external read-only
processes) is closest to LiteFS's model but implemented in-process as a wrapper
VFS, which the commit-frame interception makes tractable without patching SQLite.
