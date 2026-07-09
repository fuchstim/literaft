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

## M8 — Library polish & packaging

Cleanups and API-surface work identified while M7 hardening is underway.
These don't block M7, but should land before literaft is embedded by
anything outside this repo.

- **`dbBackend.Snapshot` should use SQLite's online backup API, not a raw
  file copy.** (`internal/node/backend.go`'s `Snapshot`) Copying
  `b.cfg.DBPath` byte-for-byte after a `TRUNCATE` checkpoint works today
  because the checkpoint lock is held across the whole call, but it leans on
  a pre-checkpoint plus filesystem-level copy semantics rather than the
  sanctioned way to get a point-in-time, self-consistent copy of a SQLite
  database.
- **`vfs/File` should translate `gate.Propose` errors into the right SQLite
  result code**, not always `sqlite3.IOERR_WRITE` (`vfs/file.go`'s `xWrite`
  commit branch, currently around line 292). A `CatchingUpError` (RAFT not
  yet ready -- `raft/gate.go`'s `Ready`/drain) should surface as
  `sqlite3.BUSY` so a client retries instead of treating it as a hard I/O
  failure; `NotLeaderError` needs its own mapping so a client-side redirect
  can tell it apart from both. Note the existing comment at
  `vfs/file.go:277-291`: SQLite's automatic post-failure rollback already
  destroys the one channel (`wrp.SysError`) this detail could otherwise ride
  through, so `Gate.LastRejection` -- not the returned result code -- is the
  reliable mechanism callers need; the result-code translation is still
  worth doing for BUSY's retry semantics, but can't be the only fix.
- **Collapse `raft.NewFSM` + `FSM.SetSnapshotter` into one constructor
  call.** Currently `internal/node.Start` calls
  `raft.NewFSM(materializer)` and then immediately
  `fsm.SetSnapshotter(backend)` (`internal/node/node.go:137`).
  `SetSnapshotter` exists as a separate step only because, in today's wiring
  order, the snapshotter (the backend) doesn't exist yet at the point the
  FSM needs constructing -- if that ordering constraint isn't real, fold it
  into `NewFSM(materializer, snapshotter)` and delete the two-step API.
- **Write a top-level `README.md`** covering how to actually use the
  package: minimal example wiring a VFS + gate + FSM + backend into a
  running node, the required PRAGMAs (CLAUDE.md's `journal_mode=WAL`,
  `synchronous=NORMAL`), the leader-only write restriction and how a caller
  sees/handles a not-leader rejection, and a pointer to `docs/` for the
  design rationale. There's currently no repo-root README, only subpackage
  ones (`apply/README.md`, `shm/README.md`, `cmd/literaft/README.md`).
- **Export a `database/sql`-compatible driver** (registered via
  `sql.Register`) that accepts an `hraft.Raft`, an `FSM`, page size, and
  whatever other config `internal/node` currently wires up by hand, so
  embedding literaft doesn't require reimplementing `internal/node`'s
  plumbing. This is the natural point to decide which of `internal/node`'s
  responsibilities (keep-alive RW connection, checkpoint driver, config
  defaults) belong in the driver itself vs. stay caller-supplied. *(done)*
  Landed as a new standalone `driver/` package (not a refactor of
  `internal/node` -- see "Deferred" below): `driver.New(r, fsm, dbPath,
  opts...)` -- required args direct, optional ones via functional options
  (`WithPageSize`, `WithApplyTimeout`, `WithCheckpointInterval`, `WithName`;
  CLAUDE.md "Public API style") -- builds the Gate, registers a
  process-unique gated+page-size-enforcing VFS, keeps one dedicated
  connection alive to drive the follower checkpoint loop, and the resulting
  `*driver.Driver` implements `database/sql/driver.Driver`/
  `driver.DriverContext` by delegating to `ncruces/go-sqlite3/driver`'s
  `SQLite` type for all connection/statement/row machinery, injecting
  `PRAGMA synchronous=NORMAL` on every pooled connection. `sql.Register` is
  caller-driven, not an `init()`: the caller builds its own
  `*hraft.Raft`/`*raftadapter.FSM` exactly as `internal/node.Start` does
  (`internal/node/node.go`), passes them to `driver.New`, then registers the
  result under its own chosen alias -- `sql.Open`'s second argument is
  reserved and unused for now (see "Deferred" below). `LastRejection`/
  `Ready` give parity with `internal/node.Node`'s equivalents for the same
  reason the `vfs/File` result-code bullet above exists. Verified by
  `driver/driver_test.go` against a real (non-stub) `apply.Applier`-backed
  raft cluster, including an M1-style plain-VFS-external-reader check.
- **Speed up the node test suite.** `internal/node`'s integration tests
  (`cluster_test.go`, `restart_test.go`, `snapshot_test.go`) spin up real
  `hraft` clusters and wait out actual leader-election/heartbeat/snapshot
  timers via `Eventually`/`time.Sleep` -- the whole suite takes ~57s for
  just 7 specs today, and grows with every M7 fault-injection/fuzzing/
  benchmark spec added on top. Investigate tightening hraft's election/
  heartbeat timeouts for tests, trimming redundant `time.Sleep` calls (e.g.
  `restart_test.go:130`, `snapshot_test.go:67`), and/or running independent
  specs in parallel.

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
- **Refactor `internal/node` to consume `driver/`** — `internal/node.Start`
  wires raft transport/store/FSM/VFS/keep-alive/checkpoint plumbing by hand,
  predating `driver/` (#31), which now does the VFS/Gate/keep-alive/
  checkpoint half of that same job as a reusable package. Collapsing
  `internal/node`'s equivalent code onto `driver.Driver` would remove the
  duplication, but was out of scope for #31 itself (which added `driver/`
  standalone, without touching `internal/node`/`cmd/literaft`). Revisit once
  `driver/` has real external users to validate its API against.
- **Multiple databases on one RAFT cluster, keyed by `sql.Open`'s name
  argument** — `driver.Driver.Open`/`OpenConnector` currently ignore the
  `name` argument `database/sql` passes them; one `driver.Driver` always
  serves the single `dbPath` it was built with. Revisit if a use case
  needs one `hraft.Raft` (one RAFT log) fronting more than one logical
  SQLite database, dispatched by that name.
