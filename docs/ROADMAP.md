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

## M5 — Role transitions & the hard orderings

The subtle correctness work from `DESIGN.md §conflicts`.

- **Losing leadership:** drain/abort in-flight local writes (fail gates, release
  write lock) *before* starting follower apply.
- **Gaining leadership:** commit a current-term no-op, drain apply until
  `lastApplied == commitIndex`, only then open the gate. (Handles apply-behind.)
- **Very-behind:** InstallSnapshot from a `TRUNCATE`-checkpointed `.db`, reset
  local WAL, resume applying.
- Wire the RAFT snapshot cut to a truncate-checkpointed db (optional coupling).
- **Done when:** leadership churn under load never produces a torn WAL, a lost
  update, or a stale-state leader serving writes.

## M6 — Hardening

- Crash/restart recovery: rebuild WAL tail from the log via `lastApplied`;
  idempotence.
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
