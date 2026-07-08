# ROADMAP

Milestones for building the RAFT-backed SQLite VFS. Current scope: **leader
writes + follower apply, reject all follower-originated writes** (ADR-007).
Forwarding/OCC (ADR-008) and client transaction models (ADR-009) are deferred.

Each milestone should be independently testable. Don't move to conflict/RAFT
integration before the plumbing milestones pass, and don't build any ADR-008
machinery yet.

---

## M0 — Wrapper VFS skeleton  *(start here)*

Wrap ncruces' default VFS with a pass-through that changes nothing observable.

- Implement `VFS.Open` that tags file type via `Filename` and returns a wrapping
  `File`.
- Delegate every `File` method to the wrapped file, including forwarding the
  optional capability interfaces (`FileSharedMemory`, `FileLockState`,
  `FileCheckpoint`, `FileUnwrap`, …). See `NCRUCES_NOTES.md`.
- Register under a name; open a WAL db through `?vfs=`.
- **Done when:** the full ncruces test workload runs through the wrapper with no
  behavior change, multiple in-process RW connections work with normal WAL
  concurrency (requirement #2), and files are bit-identical to no-wrapper runs.

## M1 — External read-only compatibility (requirement #3)

Prove the load-bearing claim before building on it.

- With a node process holding a live RW connection (and mid-write), open the same
  db read-only from an **unmodified** stock SQLite (`sqlite3` CLI or a small C
  program).
- Verify: reader sees only committed state, never torn/partial; checkpoint
  respects the external reader's read-mark; OFD (ours) vs POSIX `F_SETLK`
  (stock) locking interoperates on Linux.
- **Done when:** an external reader stays correct across writes and checkpoints.
  If this fails, stop and resolve locking/format compatibility before anything
  else — the whole premise depends on it.

## M2 — Commit-frame interception with a stub gate

Add the write-path capture + gate, but with a trivial single-node "RAFT" that
always commits immediately.

- In `xWrite` on `-wal`: track frame boundaries, parse pgno + commit marker
  (`WAL_FORMAT.md`), build the capture buffer, withhold the commit frame, call
  the (stub) gate, release on success.
- Verify capture buffer matches what SQLite actually wrote (compare against a
  non-intercepted run).
- Exercise the **abort branch** deliberately (stub returns failure): confirm
  `mxFrame` never advances, on-disk data frames are inert, next txn overwrites
  cleanly, and `COMMIT` surfaces an error.
- **Done when:** single-node writes commit through the gate and forced aborts
  leave a clean, recoverable db.

## M3 — Vendored shm + follower apply

Copy the shm implementation and materialize an entry into a local db.

- Vendor the concrete shm impl into `shm/`; record upstream commit hash in
  `shm/UPSTREAM.md` (`NCRUCES_NOTES.md §vendoring`).
- Implement `apply/`: take `WAL_WRITE_LOCK`, write frames with this node's salts +
  running checksums, update page-map slots, advance `mxFrame` with the tear-safe
  two-copy header write, release, persist `lastApplied`.
- Cross-check: a db built purely by follower-apply of captured entries is
  readable by stock SQLite and equal (logically) to the leader's db.
- **Done when:** an entry captured on one instance applies on another and both
  serve identical reads, including to an external reader (re-run M1 against an
  apply-built db).

## M4 — Real RAFT integration

Swap the stub for the chosen RAFT library via the `raft/` adapter.

- Leader: gate proposes to RAFT, releases commit frame on quorum.
- Followers: RAFT state machine `apply` calls `apply/`.
- Reject follower-originated client writes with a leader hint (ADR-007).
- Node process keeps ≥1 RW connection alive; followers run a checkpoint driver.
- **Done when:** a multi-node cluster replicates writes, followers serve
  (possibly stale) reads, and killing/adding nodes converges.

## M5 — Role transitions & the hard orderings *(done)*

The subtle correctness work from `DESIGN.md §conflicts`, scoped to leadership
churn itself; snapshot-based catch-up is split out into M6 below.

- **Losing leadership:** hraft already resolves every in-flight local
  `Apply`/`Barrier` future with `ErrLeadershipLost` *before* it flips
  `LeaderCh` to `false` (`runLeader`'s step-down path), and the local SQLite
  writer's own `WAL_WRITE_LOCK` (an OFD lock shared with the vendored `shm/`
  apply path) already serializes any follower-apply against a still-in-flight
  local write. No additional code needed beyond verifying this via tests
  (`raft/gate_test.go`'s "surfaces a lost-leadership proposal" case).
- **Gaining leadership:** `raft.Gate` now tracks a `ready` flag, closed
  (`false`) the instant a leadership term begins and opened only once a
  current-term `raft.Barrier` call returns -- which by construction blocks
  until every already-committed entry, including any backlog, has been sent
  through `FSM.Apply` on this node (`raft/gate.go`'s `drain`). `Gate.Propose`
  rejects with `CatchingUpError` while closed. This also closes the Figure-8
  self-apply race documented in `raft/fsm.go`: a stale entry from an earlier,
  unfinished leadership stint is always applied *during* the drain, while no
  new self-proposal can be racing for the self-apply marker.
- **Done when:** leadership churn under load never produces a torn WAL, a lost
  update, or a stale-state leader serving writes. Verified by
  `raft/leadership_test.go`: a node with a real, durably-replicated-but-
  unapplied backlog is handed leadership (`LeadershipTransferToServer`), stays
  un-`Ready` and rejects writes until the backlog drains, then applies it
  exactly once and correctly resumes ADR-005's self-skip for new writes.

## M6 — Snapshots & very-behind followers *(done)*

Split out of the original M5 scope (see `DECISIONS.md` ADR-010): comparable in
size to M3's vendored-shm work.

- `raft.FSM.Snapshot`/`Restore` are real: `Snapshot` delegates to a
  `raftadapter.Snapshotter` (implemented by `internal/node`'s `dbBackend`),
  which drives a `TRUNCATE` checkpoint -- the natural cut point
  (`DESIGN.md` §checkpoint) -- so the `.db` file alone becomes a complete,
  self-contained copy, then hands hraft a private temp-file copy of it so
  `FSMSnapshot.Persist` can stream it later without blocking further `Apply`.
- `Restore` closes the applier and both kept-alive SQLite connections,
  swaps the incoming bytes in as the new `.db` (atomic rename), deletes the
  now-stale `-wal`/`-shm` (a superseded generation apply's bootstrap would
  otherwise refuse to touch), and reopens everything fresh -- `apply.Open`'s
  existing "uninitialized wal-index" bootstrap path covers the rest, no
  `apply/` changes needed.
- `internal/node.Config` exposes `SnapshotThreshold`/`SnapshotInterval`/
  `TrailingLogs` (defaulting to hraft's own defaults), letting tests force
  fast, real snapshotting instead of waiting on production-sized thresholds.
- **Done when:** a follower too far behind for normal log replication catches
  up via a snapshot instead, and ends up logically-equivalent to the leader,
  including to an external reader. Verified by
  `internal/node/backend_test.go` (a direct `Snapshot`/`Restore` round trip,
  checked against a plain unmodified-VFS reader) and
  `internal/node/snapshot_test.go` (a real cluster with `TrailingLogs` low
  enough that a brand-new joiner's needed log entries are provably compacted
  away, so only `InstallSnapshot` -- not `AppendEntries` replay -- can
  converge it).

## M7 — Hardening

- **Crash/restart recovery: rebuild WAL tail from the log via `lastApplied`;
  idempotence.** *(done)* `internal/node.Start` now unconditionally discards
  any leftover `-wal`/`-shm` before anything else touches them (the shm
  dead-man's-switch wipes `-shm` fresh on every restart anyway, which used to
  leave `apply.Applier.bootstrap` refusing to touch a nonzero-size `-wal`) --
  see `apply/README.md`'s former "what's out of scope" entry for the gap this
  closes. Recovery is then just hraft's own already-existing snapshot-restore
  + log-replay (`vendor/github.com/hashicorp/raft`'s `restoreSnapshot`/
  `processLogs`), which already resumes from its last locally-stored
  snapshot's index (persisted `lastApplied`) or from index 1 if none, and is
  idempotent by construction here since RAFT entries are full page images,
  not deltas (CLAUDE.md) -- replaying an already-applied entry converges to
  the same state rather than corrupting it. This also surfaced and fixed a
  latent, previously-untested bug: `raft.FSM`'s `Snapshotter` was wired
  *after* `hraft.NewRaft`, which synchronously restores this node's latest
  local snapshot (if any) on startup and would otherwise fail outright on
  any restart of a node that had ever snapshotted. Verified by
  `internal/node/restart_test.go`: follower and leader restarts, both before
  and after a local snapshot exists, each checked against an external
  unmodified-VFS reader (M1's bar).
- Fault injection around the gate (crash between withhold and publish; between
  frame write and header advance).
- Fuzz the frame parser and the apply encoder against real SQLite output.
- Bench: leader write throughput ≈ 1 txn / RAFT round-trip; confirm batching
  multiple SQLite txns per entry raises it; read concurrency unaffected.

---

## Deferred (do NOT build under current scope)

- **Forwarding follower-computed writes + OCC** — `DECISIONS.md` ADR-008. The
  read-set-at-`xFetch` capture, per-page version pagemap, in-flight overlay, and
  validation engine. Revisit only if leader SQL-exec CPU is proven to be the
  bottleneck *and* local reads inside interactive txns are required.
- **Client transaction models** — `DECISIONS.md` ADR-009. Single-shot SQL
  forwarding first (no OCC); Model A (whole-txn-on-leader) for interactive;
  Model C (packaged txns) as the robust option; Model B == the OCC design.
- **Client-request-ID dedup** for ambiguous-commit — needed once forwarding
  exists; keep the reqID field in the entry format now so the hook is ready.
- **Linearizable reads** (leader lease / RAFT read-index) — RAFT-side, add when
  a use case needs it.
