# ROADMAP

Milestones for building the RAFT-backed SQLite VFS. Current scope: **leader
writes + follower apply, reject all follower-originated writes** (ADR-007).
Forwarding/OCC (ADR-008) and client transaction models (ADR-009) are deferred.

Each milestone should be independently testable. Don't move to conflict/RAFT
integration before the plumbing milestones pass, and don't build any ADR-008
machinery yet.

> **Restored/reconciled note (2026-07):** this file was deleted by the
> "Refactor" commit (`97c63a7`) along with the rest of `docs/`, and
> reconstructed afterward. Per CLAUDE.md, GitHub issues/milestones are the
> real source of truth and this file is a derived mirror — the bullets below
> were cross-checked against the actual project board
> (https://github.com/users/fuchstim/projects/3) and current code, not
> reconstructed from memory alone, but two items below (`#29`, `#37`) look
> resolved by code that now exists without their GitHub issues having been
> closed to match — flagged inline rather than closed unilaterally.

---

## M0 — Wrapper VFS skeleton  *(done)*

Wrap ncruces' default VFS with a pass-through that changes nothing observable.

- `internal/vfs.VFS.Open`/`OpenFilename` tag file type via `Filename` and
  return a wrapping `File`.
- Every `File` method delegates to the wrapped file, including forwarding the
  optional capability interfaces (`FileSharedMemory`, `FileLockState`,
  `FileCheckpoint`, `FileUnwrap`, …). See `NCRUCES_NOTES.md`.
- Register under a name; open a WAL db through `?vfs=`.
- **Done when:** the full ncruces test workload runs through the wrapper with no
  behavior change, multiple in-process RW connections work with normal WAL
  concurrency (requirement #2), and files are bit-identical to no-wrapper runs.
  Verified by `internal/vfs/vfs_test.go`.

## M1 — External read-only compatibility (requirement #3)  *(done)*

Prove the load-bearing claim before building on it.

- With a node process holding a live RW connection (and mid-write), open the same
  db read-only from an **unmodified** stock SQLite (`sqlite3` CLI).
- Verify: reader sees only committed state, never torn/partial; checkpoint
  respects the external reader's read-mark; OFD (ours) vs POSIX `F_SETLK`
  (stock) locking interoperates on Linux/macOS.
- **Done when:** an external reader stays correct across writes and checkpoints.
  Verified by `internal/vfs/external_reader_test.go`. (A related, later-discovered
  gap — an external reader's own *close* deleting a node's `-wal`/`-shm` — is
  M7's `#41`-adjacent `ADR-012` fix, not part of this milestone's original
  scope, which only covered reads.)

## M2 — Commit-frame interception with a stub gate  *(done)*

Add the write-path capture + gate, but with a trivial single-node "RAFT" that
always commits immediately.

- In `internal/vfs.File.WriteAt` on `-wal`: track frame boundaries, parse pgno +
  commit marker (`WAL_FORMAT.md`), build the capture buffer, withhold the
  commit frame, call the gate, release on success.
- Verify capture buffer matches what SQLite actually wrote (compare against a
  non-intercepted run).
- Exercise the **abort branch** deliberately (a gate that rejects): confirm
  `mxFrame` never advances, on-disk data frames are inert, next txn overwrites
  cleanly, and `COMMIT` surfaces an error.
- **Done when:** single-node writes commit through the gate and forced aborts
  leave a clean, recoverable db. Verified by `internal/vfs/gate_test.go`,
  `rollback_test.go`, `spill_test.go`.

## M3 — shm + follower apply  *(done)*

Implement the shm layer and materialize an entry into a local db.

- Implement the concrete shm layer directly in `internal/fsm/walappender/shm/`,
  since ncruces' `SharedMemory` interface is opaque and can't be driven from
  outside the package.
- `internal/fsm/walappender.WALAppender`: take `WAL_WRITE_LOCK`, write frames with
  this node's salts + running checksums, update page-map slots, advance
  `mxFrame` with the tear-safe two-copy header write, release.
- Cross-check: a db built purely by follower-apply of captured entries is
  readable by stock SQLite and equal (logically) to the leader's db.
- **Done when:** an entry captured on one instance applies on another and both
  serve identical reads, including to an external reader (re-run M1 against a
  walappender-built db). Verified by
  `internal/fsm/walappender/walappender_test.go`.

## M4 — Real RAFT integration  *(done)*

Wire `hashicorp/raft` in via `internal/raft/gate` (the gate) and
`internal/raft/proto` (the entry wire format).

- Leader: `Gate.Propose` proposes to RAFT, releases commit frame on quorum.
- Followers: `fsm.FSM.Apply` (RAFT's state machine callback) calls
  `internal/fsm/walappender`.
- Reject follower-originated client writes with a leader hint (ADR-007).
- Node process keeps ≥1 RW connection alive; every node runs a checkpoint
  driver (`internal/fsm/walappender`'s own, not role-conditional — see
  `DESIGN.md §checkpoint`).
- **Done when:** a multi-node cluster replicates writes, followers serve
  (possibly stale) reads, and killing/adding nodes converges. Verified by
  `internal/testutils/cluster_test.go`.

## M5 — Role transitions & the hard orderings  *(done)*

The subtle correctness work from `DESIGN.md §conflicts`, scoped to leadership
churn itself; snapshot-based catch-up is split out into M6 below.

- **Losing leadership:** hraft already resolves every in-flight local
  `Apply`/`Barrier` future with `ErrLeadershipLost` *before* it flips
  `LeaderCh` to `false` (`runLeader`'s step-down path), and the local SQLite
  writer's own `WAL_WRITE_LOCK` (an OFD lock shared with
  `internal/fsm/walappender/shm/`'s apply path) already serializes any
  follower-apply against a still-in-flight local write. No additional code
  needed beyond verifying this via tests
  (`internal/raft/gate/gate_test.go`'s "surfaces a lost-leadership proposal"
  case).
- **Gaining leadership:** `internal/raft/gate.Gate` tracks a `ready` flag,
  closed (`false`) the instant a leadership term begins and opened only once
  a current-term `hraft.Barrier` call returns — which by construction blocks
  until every already-committed entry, including any backlog, has been sent
  through `fsm.FSM.Apply` on this node (`Gate.drain`). `Gate.Propose` rejects
  with `CatchingUpError` while closed.
- **Done when:** leadership churn under load never produces a torn WAL, a lost
  update, or a stale-state leader serving writes. Verified by
  `internal/raft/gate/leadership_test.go`: a node with a real,
  durably-replicated-but-unapplied backlog is handed leadership
  (`LeadershipTransferToServer`), stays un-`Ready` and rejects writes until
  the backlog drains, then applies it exactly once and correctly resumes
  ADR-005's self-skip for new writes.
- **No longer true as originally written:** this milestone's own "done when"
  bar, at the time it was written, included "closes the Figure-8 self-apply
  race" as a side effect of the drain. A later refactor changed the
  self-apply mechanism in a way that reopened exactly that race — see
  `DECISIONS.md` ADR-011 and M7's `#41` below. `leadership_test.go` above
  still passes because it never exercises a node materializing *its own*
  stale entry (only a different node's); `internal/raft/gate/figure8_test.go`
  is the (currently `PIt`/pending) spec that does.

## M6 — Snapshots & very-behind followers  *(done)*

Split out of the original M5 scope (see `DECISIONS.md` ADR-010): comparable in
size to M3's shm work.

- `fsm.FSM.Snapshot`/`Restore` are real: `Snapshot` delegates to
  `internal/fsm/snapshotter.Snapshotter`, which uses SQLite's online backup API
  to hand hraft a private temp-file copy of the live "main" database, so
  `FSMSnapshot.Persist` can stream it later without blocking further `Apply`.
- `Snapshotter.Restore` treats the incoming snapshot bytes as a sequence of
  whole database pages and appends them as ordinary
  `internal/fsm/walappender` frames (reusing the same append/publish
  machinery follower-apply already needs), then runs a `TRUNCATE` checkpoint
  — see `DESIGN.md §follower-apply` for why this replaced the earlier
  whole-file-swap-and-reopen-every-connection design.
- `internal/testutils`'s cluster options (`WithSnapshotThreshold`,
  `WithSnapshotInterval`, `WithTrailingLogs`, defaulting to hraft's own
  defaults) let tests force fast, real snapshotting instead of waiting on
  production-sized thresholds.
- **Done when:** a follower too far behind for normal log replication catches
  up via a snapshot instead, and ends up logically-equivalent to the leader,
  including to an external reader. Verified by
  `internal/fsm/snapshotter/snapshotter_test.go` (a direct `Snapshot`/`Restore`
  round trip, checked against a plain unmodified-VFS reader) and
  `internal/testutils/snapshot_test.go` (a real cluster with `TrailingLogs`
  low enough that a brand-new joiner's needed log entries are provably
  compacted away, so only `InstallSnapshot` — not `AppendEntries` replay —
  can converge it).

## M7 — Hardening *(current milestone)*

- **Crash/restart recovery: rebuild WAL tail from the log; idempotence.**
  *(done)* Recovery is hraft's own already-existing snapshot-restore +
  log-replay (`vendor/github.com/hashicorp/raft`'s `restoreSnapshot`/
  `processLogs`), idempotent by construction since RAFT entries are full page
  images, not deltas — replaying an already-applied entry converges to the
  same state rather than corrupting it. Verified by
  `internal/testutils/restart_test.go`: follower and leader restarts, both
  before and after a local snapshot exists, each checked against an external
  unmodified-VFS reader (M1's bar) — except the "leader restarted after a
  local snapshot" case, which is `PIt`/pending; see `#41` below, it's the
  same bug as the Figure-8 case, hitting via an ordinary restart instead of a
  partition.
- **Reinstate the full test suite the "Refactor" commit deleted, and add new
  coverage.** *(done)* `97c63a7` deleted every test file in the repo
  (~3,150 lines across 19 files) alongside the package restructuring
  described throughout these docs. Coverage was reinstated against the new
  layout (`internal/fsm/walappender`, `internal/fsm/snapshotter`,
  `internal/raft/proto`, `internal/vfs`, `internal/raft/gate`, `driver`,
  `cmd/literaft`), plus a new `internal/testutils` harness package (real
  in-memory and real TCP/BoltDB cluster builders) since the deleted
  `internal/node` used to provide this and no longer exists (`ADR-013`). New
  coverage added where the new structure created fresh testable surface
  (wal-index header/hash-table edge cases, `Snapshotter.Restore`'s
  page-parsing validation branches, `Register`'s page-size validation).
  Surfaced three bugs along the way, tracked separately below and in
  `DECISIONS.md`.
- **Fix: external reader closing a connection could delete a node's
  `-wal`/`-shm`.** *(done, `DECISIONS.md` ADR-012, `fsm/dblock.go`)* Found
  while reinstating the tests above. Not filed as its own GitHub issue since
  it was found and fixed within the same pass.
- **Fix: `internal/vfs.Register`'s `pageSize` silently corrupted frame
  parsing instead of just disabling enforcement when passed `0`.** *(done)*
  Also found while reinstating tests; `Register` now panics on `pageSize ==
  0` rather than accept a value that desyncs frame-offset math.
- **Found, tracked, *not* fixed: `fsm.FSM`'s self-apply skip regressed from
  transient to permanent.** ([issue #41](https://github.com/fuchstim/literaft/issues/41),
  `DECISIONS.md` ADR-011) A node's own entry, retroactively committed via
  hraft's Figure-8 rule or replayed after this node's own `FSM.Restore`
  rewinds it to an older snapshot, is silently and permanently dropped
  instead of materialized. Two `PIt`/pending regression tests demonstrate it
  (`internal/raft/gate/figure8_test.go`,
  `internal/testutils/restart_test.go`). Fix direction: restore a transient,
  per-proposal self-skip instead of the current permanent per-entry one.
- [**Fault injection around the commit-frame gate**](https://github.com/fuchstim/literaft/issues/24)
  — crash between withhold and publish; between frame write and header
  advance. Not started.
- [**Fuzz the frame parser and apply encoder**](https://github.com/fuchstim/literaft/issues/25)
  against real SQLite output (`internal/vfs`'s frame parsing,
  `internal/fsm/walappender`'s frame encoding). Not started.
- [**Benchmark leader write throughput and read concurrency**](https://github.com/fuchstim/literaft/issues/26)
  — confirm ≈1 txn/RAFT-round-trip, that batching multiple SQLite txns per
  entry raises it, and that read concurrency is unaffected. Not started.
- [**Switch RAFT entry wire format to protobuf**](https://github.com/fuchstim/literaft/issues/42)
  — replace `internal/raft/proto/entry.go`'s hand-rolled binary encoding
  (`Entry.Encode`/`DecodeEntry`) with a protobuf-generated message, for
  schema evolution and to shrink the hand-written bounds-checking surface
  the fuzzing ticket above (#25) has to cover. Call sites: `gate.go`'s
  `Gate.Propose`, `fsm.go`'s `FSM.Apply`. Not started.
- [**Consolidate duplicated WAL frame-format constants/layout between vfs and
  walappender**](https://github.com/fuchstim/literaft/issues/44) —
  `walHeaderSize`/`frameHeaderSize`, the pgno/nTruncate byte-offset layout,
  the frame-offset stride formula, and the commit-frame predicate
  (`nTruncate != 0`) are each independently declared in both
  `internal/vfs/walframe.go` (decode side) and
  `internal/fsm/walappender`'s `walappender.go`/`frame.go` (encode side) —
  same domain fact expressed two different ways, riskiest at the
  frame-offset math (modulus test vs. direct multiplication), which could
  silently drift. Pull the shared layout knowledge into one package (e.g.
  `internal/walformat`); checksum computation and wal-index/shm handling
  stay walappender-only (real architectural split, not duplication). Not
  started.

## M8 — Library polish & packaging

Cleanups and API-surface work identified while M7 hardening is underway.
These don't block M7, but should land before literaft is embedded by
anything outside this repo.

- **`Snapshotter.Snapshot` uses SQLite's online backup API, not a raw file
  copy.** *(done)* See M6 above; this was already true pre-refactor
  (`internal/node/backend.go`'s `Snapshot`) and remains true in
  `internal/fsm/snapshotter.Snapshotter`.
- [**`internal/vfs.File` should translate `Gate.Propose` errors into the
  right SQLite result code**](https://github.com/fuchstim/literaft/issues/28),
  not always `sqlite3.IOERR_WRITE` (`internal/vfs/file.go`'s `writeFrameData`
  commit branch — still has a `// TODO: Return sqlite3.BUSY for retriable
  errors` as of this writing). A `CatchingUpError` should surface as
  `sqlite3.BUSY` so a client retries instead of treating it as a hard I/O
  failure; `NotLeaderError` needs its own mapping so a client-side redirect
  can tell it apart from both. `Gate.LastRejection`/`Driver.LastRejection` —
  not the returned result code — remain the reliable mechanism for now
  (SQLite's own post-failure rollback destroys the one channel a result code
  could otherwise ride through). Not started.
- [**Collapse the FSM constructor + snapshotter-wiring into one call**](https://github.com/fuchstim/literaft/issues/29)
  — **looks done, GitHub issue not yet closed to match.** The pre-refactor
  API (`raft.NewFSM(materializer)` then a separate
  `fsm.SetSnapshotter(backend)` call, because the snapshotter didn't exist
  yet at FSM-construction time in that wiring order) is exactly what this
  issue asked to collapse. The current `fsm.New(nodeID, dbPath, opts...)`
  constructs the walappender and snapshotter itself, in one call, with no
  two-step wiring at all — this looks like it satisfies the issue, but
  wasn't done *as* this issue (it fell out of the broader "Refactor"
  commit), so the issue itself is still open on GitHub as of this writing.
- **Write a top-level `README.md`** covering how to actually use the
  package: minimal example wiring an `hraft.Raft` + `fsm.FSM` into
  `driver.New`, the required PRAGMAs (CLAUDE.md's `journal_mode=WAL`,
  `synchronous=NORMAL` — `driver.Driver` already applies the latter to every
  pooled connection), the leader-only write restriction and how a caller
  sees/handles a not-leader rejection (`Driver.LastRejection`), and a
  pointer to `docs/` for the design rationale. There is still no repo-root
  `README.md`. Not started.
- **`database/sql`-compatible driver.** *(done)* `driver.New(r, fsm,
  opts...)` — required args direct, optional ones via functional options
  (`WithApplyTimeout`; CLAUDE.md "Public API style") — builds the gate,
  registers a process-unique gated VFS, and the resulting `*driver.Driver`
  implements `database/sql/driver.Driver`/`driver.DriverContext` by
  delegating to `ncruces/go-sqlite3/driver`'s `SQLite` type, injecting
  `PRAGMA synchronous=NORMAL` on every pooled connection. Narrower than the
  original version this replaced: no `WithPageSize`/`WithName`/
  `WithCheckpointInterval` (page size comes from `fsm.PageSize()`,
  checkpointing lives inside `internal/fsm/walappender` now, the VFS name is
  always a random UUID) and, until reinstated alongside this repo's test
  coverage, no `Ready()`/`LastRejection()`/`VFSName()` either — see
  `DECISIONS.md` ADR-013.
- [**Speed up the node test suite**](https://github.com/fuchstim/literaft/issues/39)
  — **substantially addressed, differently than originally scoped.** The
  original ask (tighten hraft election/heartbeat timeouts, trim `Eventually`/
  `time.Sleep` waits, run independent specs in parallel) targeted
  `internal/node`'s own integration suite, which no longer exists —
  superseded by `internal/testutils`. `ginkgo -r -p 20 ./...` (now CLAUDE.md's
  documented default) cut the full repo's test time from ~39s to ~16s via
  real process parallelism, which addresses the "grows with every spec
  added" complaint directly. Some of the original, narrower asks are still
  open: `internal/testutils/restart_test.go`'s `writeRowsAndForceSnapshot`
  still has an explicit `time.Sleep(1 * time.Second)` waiting out the
  snapshot goroutine's own timer rather than polling for it.

---

## Deferred (do NOT build under current scope)

- **Forwarding follower-computed writes + OCC** — [issue #32](https://github.com/fuchstim/literaft/issues/32),
  `DECISIONS.md` ADR-008. The read-set-at-`xFetch` capture, per-page version
  pagemap, in-flight overlay, and validation engine. Revisit only if leader
  SQL-exec CPU is proven to be the bottleneck *and* local reads inside
  interactive txns are required.
- **Client transaction models** — [issue #33](https://github.com/fuchstim/literaft/issues/33),
  `DECISIONS.md` ADR-009. Single-shot SQL forwarding first (no OCC); Model A
  (whole-txn-on-leader) for interactive; Model C (packaged txns) as the
  robust option; Model B == the OCC design.
- **Client-request-ID dedup** for ambiguous-commit — [issue #34](https://github.com/fuchstim/literaft/issues/34).
  Needed once forwarding exists. Unlike an earlier draft of `DESIGN.md`/
  `DECISIONS.md`, there is currently no reqID field anywhere in the entry
  format to build on — this needs to be added from scratch when forwarding
  is built, not merely wired up.
- **Linearizable reads** (leader lease / RAFT read-index) — [issue #35](https://github.com/fuchstim/literaft/issues/35).
  RAFT-side, add when a use case needs it.
- ~~**Refactor `internal/node` to consume `driver/`**~~ — [issue #37](https://github.com/fuchstim/literaft/issues/37)
  **looks resolved, GitHub issue not yet closed to match.** `internal/node`
  no longer exists at all — its responsibilities were absorbed into
  `driver/` and each caller's own direct wiring (`cmd/literaft/main.go`'s
  `run()`), a more thorough version of what this issue asked for (which
  proposed `internal/node` delegating to `driver/` internally, keeping its
  own type as the public surface). See `DECISIONS.md` ADR-013.
- **Multiple databases on one RAFT cluster, keyed by `sql.Open`'s name
  argument** — [issue #38](https://github.com/fuchstim/literaft/issues/38).
  `driver.Driver.Open`/`OpenConnector` (`driver/conn.go`) still ignore the
  `name` argument `database/sql` passes them; one `driver.Driver` always
  serves the single database it was built with. Revisit if a use case
  needs one `hraft.Raft` (one RAFT log) fronting more than one logical
  SQLite database, dispatched by that name.
