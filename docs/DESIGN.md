# DESIGN

The complete design for the RAFT-backed SQLite VFS. `CLAUDE.md` has the summary;
this is the reference. `DECISIONS.md` records *why* alternatives were rejected.

---

## Architecture

```
  application
      │  (N read-write connections, same process)
      ▼
  SQLite core  ──►  RAFT VFS  ──delegates most ops──►  ncruces default VFS
                       │                                   │
                       │                                   └─► .db / -wal / -shm
                       │                                        (SQLite-compatible)
                       └── commit-frame interception ──► RAFT (existing lib)

  follower-apply path (separate from SQLite's own I/O):
      RAFT committed entry ──► apply/ ──► vendored shm/ ──► -wal + wal-index
                                          (mxFrame, page-map, write lock)
```

The VFS is a **wrapper** around ncruces' default (pure-Go) VFS. `Open` tags each
file by type and returns a wrapping `File`. Nearly everything delegates
unchanged; the wrapper adds logic in exactly one place on the normal path —
`xWrite` on the `-wal` file — plus file tagging in `Open`.

Because we delegate `SharedMemory()` (the `FileSharedMemory` interface) to the
underlying `File`, the wal-index lives in the real file-backed mmap and stays
standard. That is what lets external unmodified SQLite processes mmap the same
`-shm`, honor the same read-marks, and never observe an un-published (= not
RAFT-committed) frame.

The **follower-apply** path does *not* go through SQLite. It opens its own
handle to the `-shm`/`-wal` using the **vendored** shm code (`shm/`) and drives
`mxFrame`, the page-map hash slots, and `WAL_WRITE_LOCK` directly — coordinating
with SQLite's own handle via OFD locks and the shared mmap. See
`§follower-apply`.

---

## RAFT log entry format

A write transaction becomes one RAFT log entry: an ordered list of
`(pgno, page_image)` plus `nTruncate` (the post-commit database size in pages,
which doubles as the commit marker), plus a client request ID (for future
dedup).

Rationale is in `DECISIONS.md` ADR-003. Short version: **physical redo**.
Full page images under a total order are self-consistent by construction —
entry E2's writer read the post-E1 snapshot (local serialization guarantees
it), so E2's image already incorporates E1, and "latest full image per page
under total order" *is* serial-execution order. Nothing to reconcile even when
consecutive entries touch the same page. The price is the strict in-order,
gapless apply requirement.

---

## Write path (leader) — step by step

1. A RW connection begins a write txn. SQLite takes `WAL_WRITE_LOCK` (exclusive
   shm lock on the write slot). The VFS observes this and starts a **capture
   buffer** for this writer, recording the WAL offset (= start of frames beyond
   current `mxFrame`).

2. SQLite writes frames via `xWrite` to `-wal`. For each frame the VFS parses
   `pgno` (header bytes 0–3) and the page image, and appends `(pgno, data)` to
   the capture buffer.
   - **Non-commit frames** (header bytes 4–7 == 0, e.g. cache spill on a large
     txn) are **passed straight through to disk**. They sit beyond `mxFrame`
     (invisible) and, without a trailing valid commit frame, are inert on
     recovery — so writing them through is safe *and* preserves the memory
     relief that spilling exists to provide.
   - **The commit frame** (header bytes 4–7 != 0) is **withheld**: buffered, not
     yet written to the real `-wal`.

3. On the commit frame, the VFS proposes the whole captured batch
   (`[(pgno,data)...]`, `nTruncate`, reqID) to RAFT and **blocks**. RAFT needs no
   SQLite lock, so readers keep going; only other *writers* wait (they're behind
   the write lock anyway).

4. **RAFT commits (quorum):** flush the withheld commit frame to the real
   `-wal`, then return `SQLITE_OK` from `xWrite`. SQLite proceeds normally:
   optional `xSync`, then `walIndexAppend` + `walIndexWriteHdr` — the standard
   two-copy, barriered header publish that advances `mxFrame`. The txn is now
   visible, and correctly so. **We never touch the shm here** — SQLite's own
   publish gives us the exact tear-safe protocol for free.

5. **RAFT fails** (lost leadership / timeout / no quorum / not-leader): discard
   the buffered commit frame, return an I/O error from `xWrite`. The failure
   occurs *before* the wal-index append, so `mxFrame` never moves and the shm is
   untouched. The data frames already on disk are inert and get overwritten by
   the next txn at the same offset. `COMMIT` fails; the app rolls back and
   redirects to the leader. (Defensively you may force a wal-index rebuild on
   abort to be robust across SQLite versions, but the ordering above already
   leaves the shm clean.)

Because the gate is the commit-frame *write*, not a sync, this works under
`synchronous=NORMAL` (see `§durability`).

---

## Read path

Entirely stock — the VFS passes `xRead` and all shm methods through.

1. Read the wal-index header (two-copy + checksum), learn `mxFrame`.
2. Pick a read-mark slot `<= mxFrame`, take its reader lock.
3. Per page: consult the wal-index hash for the newest frame `<= mxFrame`; if
   found `xRead` from `-wal`, else `xRead` from `.db`.

Holds identically for local RW connections acting as readers, local readers, and
external read-only processes — all share the same file-backed wal-index and none
can see beyond the published `mxFrame`, which the gate guarantees equals
"RAFT-committed on this node."

**Consistency note:** a local read returns *this node's* committed state.
On the leader that's current; on a follower it may lag. For linearizable reads,
route to the leader under a lease or use a RAFT read-index barrier (RAFT-side,
not VFS).

---

## Checkpoint path

Checkpointing is **local to each node, no consensus** — it relocates
already-committed frames from `-wal` into `.db`, changing no logical state.
Driven by `wal_autocheckpoint` / explicit `wal_checkpoint` as normal; the VFS
does not intercept it.

1. Take `CHECKPOINT` lock.
2. Read `nBackfill` and reader read-marks from the wal-index checkpoint-info
   block; backfill frames `-wal` → `.db` **only up to the minimum read-mark held
   by any reader** — including external read-only processes, since they
   registered their locks in the shared `-shm`. Standard mechanism; works
   because we delegated the shm.
3. Sync `.db`, advance `nBackfill`.
4. For `RESTART`/`TRUNCATE`: once fully backfilled and no reader needs old WAL
   content, reset the WAL (`mxFrame → 0`, new salt, header change-counter
   bumped). External readers detect the change and re-read. Standard.

Two integration notes: checkpoints never race the RAFT gate (different locks,
and the gate only fires on commit-frame writes); and a freshly
`TRUNCATE`-checkpointed `.db` is the natural cut point for a **RAFT snapshot** —
snapshot the db file, then RAFT compacts its log below that applied index. This
is the only real coupling between checkpointing and RAFT, and it's optional.

Followers need their own checkpoint driver (a maintenance connection or direct
checkpoint call) to bound WAL growth, since they have no local writer triggering
autocheckpoint.

---

## Follower-apply path — step by step

Followers have no local writer producing frames, so the RAFT state machine's
`apply(entry)` must materialize the batch itself. This is the most
format-sensitive code in the repo; it must reproduce what `walFrames` +
`walIndexWriteHdr` do, exactly, or external readers break.

1. Take `WAL_WRITE_LOCK` on the shm (via vendored `shm/`). On a follower there
   are no local writers, so contention is only with the local checkpointer.
2. Append each `(pgno, data)` frame to the local `-wal`, computing **this
   node's** salt-based cumulative checksums (each node has its own WAL
   epoch/salt — that's why we ship page images, not raw WAL bytes).
3. Update the **wal-index page-map** (pgno→frame hash slots) so readers can find
   the new frames.
4. Publish: write both wal-index header copies with a barrier, advancing
   `mxFrame`. Tear-safe protocol mandatory.
5. Release the write lock. Persist `lastApplied` (atomically with any derived
   state) so restart is idempotent.

Local read connections continue throughout via read-marks, exactly as on the
leader.

**InstallSnapshot (very-behind follower):** if the node has fallen past the
leader's retained log, it catches up via snapshot, not replay — swap in the
`TRUNCATE`-checkpointed `.db`, reset the local WAL, resume applying from the
snapshot's index.

---

## Conflict handling

There is **no page-level conflict resolution**, and there cannot be a state
where two nodes commit conflicting page modifications. Writes are leader-only and
RAFT totally-orders every write before any of it becomes visible. Conflicts are
**prevented by serialization**.

**Intra-node (two RW conns on the leader):** pure stock SQLite. They serialize
on `WAL_WRITE_LOCK`. Lost-update case: the second writer's read→write upgrade
finds `mxFrame` moved past its snapshot and gets `SQLITE_BUSY_SNAPSHOT`; it must
roll back and retry (or use `BEGIN IMMEDIATE`). The gate doesn't perturb this —
W1 holds the write lock across its RAFT round-trip; W2 waits; W2's stale-snapshot
check fires as usual. Consequence: **≤1 in-flight local proposal at a time**, so
leader write throughput is capped at one txn per RAFT round-trip. Want more?
Batch multiple SQLite txns into one RAFT entry — don't try to overlap proposals
(that breaks the single-write-lock invariant).

**Cross-node (stale leader / split brain):** old leader L1 (minority) and new
leader L2 (majority) both write page P and both gate. Only L2 reaches quorum.
L1's proposal never gets majority (and it learns L2's higher term and steps
down), so L1's gate fails → discard commit frame, I/O error, `mxFrame` unmoved.
At no instant are two conflicting txns both committed. This is just RAFT safety
(one-leader-per-term + Leader Completeness); the gate translates "no quorum" into
"clean SQLite txn failure."

**Role transitions (the subtle part):**
- *Losing leadership* with an in-flight writer W1 blocked on a proposal: the
  transition must **drain/abort all in-flight local writes first** (fail their
  gates, force rollback, release the write lock) *before* starting follower
  apply. The follower-apply path and a stuck local writer must never both touch
  the WAL. Hard ordering requirement.
- *Gaining leadership:* a node can be **apply-behind** (has every committed log
  entry but hasn't materialized them into local SQLite). Its log is complete so
  it *can* win an election, but its SQLite state is stale. Because our write path
  **reads through** the state machine's storage, a fresh leader must, before
  accepting its first write: (1) commit a no-op entry for the current term to
  establish its commit index, (2) **drain apply** until
  `lastApplied == commitIndex`, (3) only then open the gate to local proposals.
  Apply-catch-up is a hard precondition for serving writes here, not a
  background nicety.

**Ambiguous commit:** RAFT "proposed, outcome unknown" is treated as failure by
the gate — but the entry may have committed cluster-wide. The local app sees a
failed txn that in fact took effect; on retry it would double-apply. Resolved
the standard way: client-request-ID dedup in the apply path (a re-issued write is
a no-op if its ID already applied). This is the one place needing app-level
idempotency; it's inherent to synchronous replication, not SQLite-specific.

---

## Durability

Durability comes from the **RAFT log quorum**, not local fsync. So:

- `synchronous=NORMAL` is fine — you don't need `FULL`. The gate is the
  commit-frame write, not a sync.
- You can skip/batch the local WAL fsync. A crashed node rebuilds its WAL tail
  from the RAFT log on restart (idempotent via persisted `lastApplied`).
- Within one machine, the OS page cache keeps external readers coherent
  regardless of fsync.

---

## Log-behind vs apply-behind (why "behind" is two problems)

- **Log-behind** — missing committed entries. Handled free by RAFT's election
  restriction (a voter denies its vote to a less-up-to-date candidate;
  Leader Completeness). Such a node stays a follower; its writes keep failing.
- **Apply-behind** — has the entries, hasn't materialized them. Can win an
  election with stale SQLite state → the trap addressed by the "gaining
  leadership" drain above. This split is specific to our design because the
  write path reads through the state machine's own storage.

---

## Prior art (orientation)

- **LiteFS** (Fly.io): closest model — single primary, read-only replicas,
  per-txn WAL frame shipping, real on-disk files — but intercepts at the
  filesystem via FUSE, not a VFS.
- **dqlite** (Canonical): the canonical RAFT + custom-VFS design, but in-memory
  and historically relies on a patched SQLite replication hook, so it does not
  hand you standard on-disk files for external readers.
- **rqlite**: puts RAFT *above* unmodified SQLite and replicates SQL statements;
  sidesteps the VFS entirely but gives neither in-process multi-writer semantics
  nor shared files.

Our requirement set (VFS-level, standard files, external read-only processes)
is closest to LiteFS's model but implemented in-process as a wrapper VFS, which
the commit-frame interception makes tractable without patching SQLite.
