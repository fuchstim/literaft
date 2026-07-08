# DECISIONS

ADR-style log of the choices behind this design and, importantly, the
alternatives that were rejected and why. When something here feels like
unnecessary constraint, the rejected-alternatives section is usually the answer.

---

## ADR-001 — Gate replication at the WAL commit frame

**Decision.** Intercept the transaction's commit frame in `xWrite` on `-wal`,
hold it during the RAFT round-trip, release only on quorum.

**Why.** Visibility in WAL mode is the `mxFrame` bump in shm, which has no VFS
callback. The commit frame is the last controllable point before publish and is
identifiable from the on-disk format alone (header bytes 4–7 non-zero). Gating
here gives both properties we need: readers can't see an un-replicated txn, and a
rejected txn isn't crash-recoverable (no valid commit frame on disk).

**Rejected:** gating at `xSync` (doesn't exist under `synchronous=NORMAL`, and
sync ≠ visibility); gating at shm write (no hook; and we'd have to reimplement
the tear-safe header protocol on the leader for no benefit).

---

## ADR-002 — Wrap ncruces' default VFS; don't patch SQLite

**Decision.** Implement a wrapper `VFS`/`File` over `github.com/ncruces/go-sqlite3`'s
default VFS. Add logic only in `Open` (tagging) and `xWrite` on `-wal`; delegate
everything else, including `SharedMemory()`.

**Why.** Keeps `.db`/`-wal`/`-shm` standard (requirement #3) and keeps in-process
multi-writer WAL concurrency (requirement #2) for free. No SQLite fork to
maintain.

**Rejected:** patching SQLite with a replication hook (dqlite's historical
approach) — loses external file compatibility and is a maintenance burden;
FUSE interception (LiteFS) — out of process, not what was asked; statement
replication above SQLite (rqlite) — see ADR-003.

---

## ADR-003 — Replicate physical redo (page images), not SQL, not raw WAL bytes

**Decision.** Each RAFT entry is `[(pgno, page_image)...]` + `nTruncate` + reqID.

**Why not SQL statements.** Re-execution non-determinism: `RANDOM()`,
`CURRENT_TIMESTAMP`, ROWID allocation, collation differences would diverge pages
across nodes. Physical redo freezes the leader's choices into the images —
divergence is structurally impossible.

**Why not raw WAL bytes.** WAL frame headers carry per-WAL salts and a cumulative
checksum chain tied to each node's own WAL epoch. Each node must re-encode into
its own WAL with its own salt/checksums, so files are semantically identical but
byte-different — which is exactly what we want (each node needs only its *own*
WAL valid for its *own* external readers).

**Consequence / accepted cost.** Full images under a total order are
self-consistent, so there is nothing to reconcile — but apply must be strictly
in-order and gapless (an image is valid only against the base it was computed
on). Total ordering becomes a correctness precondition, and it's the same reason
leader write throughput is one txn per round-trip.

---

## ADR-004 — Writes are leader-only; no conflict resolution exists

**Decision.** Only the leader accepts writes. There is no page-merge,
last-writer-wins, or cross-node MVCC anywhere.

**Why.** Two layers of serialization make conflicts impossible rather than
resolved: WAL's single-writer lock (intra-node) and RAFT's single-leader total
order (cross-node). Building conflict resolution would be solving a problem the
architecture already prevents.

---

## ADR-005 — Leader publishes via SQLite; only follower-apply drives shm

**Decision.** On the leader commit path we release the withheld commit frame and
let SQLite run its own `walIndexWriteHdr`. We manipulate the wal-index directly
*only* in the follower-apply path.

**Why.** SQLite's own publish is the exact tear-safe two-copy + barrier protocol.
Reimplementing it on the leader would add risk for zero benefit. The follower has
no SQLite writer to do the publish, so it must — and that's where the vendored
shm code and the format-sensitive tear-safe reimplementation live.

---

## ADR-006 — `synchronous=NORMAL`; durability from quorum

**Decision.** Run `NORMAL`, not `FULL`; local WAL fsync may be skipped/batched.

**Why.** The gate is the commit-frame *write*, not a sync. Durability is the RAFT
log quorum. A crashed node rebuilds its WAL tail from the log via persisted
`lastApplied`. See `DESIGN.md §durability`.

---

## ADR-007 — For now, reject all follower-originated writes  *(current scope)*

**Decision.** A client write on a follower returns an error + leader hint; the
client redirects. No forwarding, no OCC. This is the current milestone's scope.

**Why.** Correct forwarding of *follower-computed* writes requires the full OCC
apparatus (ADR-008), which is substantial and only pays off in a narrow regime.
Rejection is trivially correct and unblocks everything else (leader writes,
follower apply, external-reader compatibility).

**Redirection is the safe path.** Two viable shapes when we do add client
support (see ADR-009): forward the *SQL* (recompute on leader — safe by
construction), or run the whole transaction on the leader. Neither needs OCC.

---

## ADR-008 — Deferred: forwarding follower-computed writes + OCC design

**Status: DEFERRED.** Captured so we don't re-derive it. Revisit only if leader
SQL-execution CPU is measurably the bottleneck *and* local reads inside
interactive transactions are needed — a narrow intersection. In most
RAFT-SQLite systems fsync + replication dominate and this never pays off.

The problem: a follower computes page images against its **local (possibly
stale) snapshot**. Those images are bound to that base — splicing them into the
authoritative log would corrupt it. So you cannot transparently forward image
batches. Statement forwarding is fine; image forwarding is not.

The evolution we worked through (each step fixing the previous):

1. **Attach a base index token; leader does compare-and-swap.** Correct idea. But
   the index to compare against is the leader's **last log index (log head)**,
   NOT `commitIndex` or `lastApplied` — the head can be ahead when entries are
   in flight, and checking commit/applied admits a lost-update bug. With the
   correct check the acceptance window is essentially "leader quiescent," so
   under any concurrency the abort rate → ~100%. Determinism (ADR-003) is what
   lets the leader trust matching-base images without re-executing.

2. **Per-page version tokens (hash or counter) instead of one db-wide counter.**
   A pagemap `pgno → hash/version`, updated **only on apply** (must track
   *committed* state). Follower forwards touched pages + their prev tokens;
   leader accepts iff unchanged. Widens the window from "any concurrent write"
   to "only pages you touched." Prefer a monotonic per-page **version counter**
   over a content hash (no collision risk; a truncated-hash false match would
   accept an invalid write → corruption). Both rely on ADR-003 determinism to be
   comparable across nodes.

3. **Validate the READ set, not the write set.** The load-bearing correction.
   Write-set-only validation is merely write-write conflict detection
   (first-committer-wins) — it permits stale-read / write-skew anomalies. You
   must forward every page the txn *read* with its pre-image token, and the
   leader validates all of them. Page granularity makes this **sound but
   conservative** (hot structural pages — page 1, upper b-tree interior nodes —
   over-abort; allocation phantoms may slip through, so call it "mostly
   serializable with caveats").

4. **Capture the read set at the page cache, not the VFS.** `xRead` sees only
   cache *misses* — a subset — so a VFS-derived read set is unsound (silent
   lost update on warm-cache pages). Instrument `xFetch` via a
   `SQLITE_CONFIG_PCACHE2` wrapper (grab defaults with `GETPCACHE2`, delegate,
   record `key` before calling through). This *over*-captures (safe direction).
   - **Scope to the transaction** by resetting the accumulator at read-snapshot
     establishment — the SHARED lock on a `WAL_READ_LOCK(i)` slot, observable in
     `xShmLock` — and harvesting at `commit_hook`. Autocommit → per-statement
     (correct; each is its own txn). Explicit `BEGIN…COMMIT` → one reset, union
     of all pages until `COMMIT`.
   - **Read→write upgrade falls out free** because we key on the *read* lock, not
     the write lock: the accumulator was already filling during the read phase,
     and the upgrade takes a *different* lock slot (no reset), so pre-upgrade
     reads stay in the set.
   - **Pre-image tokens:** don't hash during the txn (buffer not reliably
     populated at `xFetch` on a miss). Collect pgnos, then join against the
     pagemap at forward time — reading the pagemap *as of* the applied index
     recorded at read-lock acquisition (versioned/COW pagemap), or constrain the
     txn to read the latest applied snapshot.

**In-flight overlay (needed if pipelining):** the pagemap updates on *apply* but
the leader appends at the *head*; validate against pagemap ∪ {pages dirtied by
appended-not-yet-applied entries}, folding in on apply and rolling back on
leadership change. Otherwise forbid pipelining (`lastApplied == head`), losing
throughput.

**Crash-consistency:** the pagemap is derived state — persist it atomically with
`lastApplied`, or rebuild from the checkpointed db on recovery (hash every page;
expensive for large dbs).

**Honest bottom line.** This rebuilds a read/write-set OCC engine — exactly the
machinery the leader-only design was structured to avoid. It saves only leader
SQL-execution CPU, adds a follower→leader RTT, and over-aborts under structural
churn. And it widens the blast radius of a *buggy* follower (RAFT is
non-Byzantine): re-execution on the leader is self-checking; trusting forwarded
images is not.

---

## ADR-009 — Deferred: how client redirection supports transactions

**Status: DEFERRED** (pairs with ADR-007). Captured for when we add client
transaction support.

- **Single-shot writes (autocommit / shippable batch):** trivial. Forward the
  *SQL* to the leader; it executes against its own current state, so images are
  computed on the authoritative base by construction. No OCC. This is where most
  systems stop (the rqlite model).
- **Interactive `BEGIN…COMMIT`:** the transaction holds server-side state (pinned
  snapshot + write lock) across client think-time. Three models:
  - **Model A — whole txn on the leader, follower is a dumb proxy.** Only model
    giving true interactive isolation (it *is* a normal SQLite txn, client on the
    far end of a socket). Fits our invariants perfectly: the leader's own
    connection produces frames and hits the existing gate; **needs nothing from
    ADR-008**. Cost: per-statement RTT; leader holds the single WAL write lock for
    the txn's whole wall-clock duration incl. think-time → one slow interactive
    txn stalls *every* writer cluster-wide. Requires aggressive idle-txn timeouts;
    client must handle its txn being killed.
  - **Model B — read local, write by redirect, reconcile at commit.** This *is*
    the ADR-008 OCC design wearing a "redirection" label; not a simpler
    alternative. Inherits all OCC caveats.
  - **Model C — submit the transaction as a unit** (stored-proc-like package /
    registered function run start-to-finish on the leader). Recovers single-shot
    simplicity (one RTT, lock held only during execution, no think-time
    exposure) but gives up mid-transaction dependent queries unless that logic
    travels inside the package.

**Recommendation when we get here.** Model A for genuine interactive txns
(accept latency, add hard idle timeouts); Model C if the app can submit txns as
units (most robust — no think-time lock exposure). Reach for B/OCC only in the
narrow ADR-008 regime.

---

## ADR-010 — Split InstallSnapshot out of M5 into its own milestone

**Decision.** M5 ships only the leadership-churn ordering work (`ROADMAP.md`'s
"losing leadership" / "gaining leadership" bullets): `raft.Gate` gates local
proposals on a current-term `raft.Barrier` drain, closing the apply-behind
window and the Figure-8 self-apply race (`raft/fsm.go`'s doc comment).
InstallSnapshot for very-behind followers and wiring the RAFT snapshot cut to a
`TRUNCATE` checkpoint move to a new M6; the former "M6 — Hardening" becomes M7.

**Why.** The two pieces don't share much implementation surface: leadership
churn is about `raft.Gate`/`raft.FSM` ordering against the existing WAL/apply
machinery, while InstallSnapshot needs a real `raft.FSM.Snapshot`/`Restore`
(currently stubbed to error), a new on-disk snapshot format, integration with
hraft's `SnapshotStore`, and WAL-reset-and-resume-apply logic — comparable in
size to M3's vendored-shm work. `ROADMAP.md`'s own text already flagged the
snapshot-cut wiring as "(optional coupling)," and M4 had already deferred
snapshot mechanics entirely (`SnapshotThreshold` set high specifically to avoid
triggering it). The M5 "done when" bar — leadership churn never produces a torn
WAL, a lost update, or a stale-state leader serving writes — doesn't need
InstallSnapshot to be met; a node that's merely apply-behind (not log-behind)
catches up via normal replication, which small/moderate-log clusters never
exceed.

**Consequence.** `raft.FSM.Snapshot`/`Restore` stay stubbed and
`internal/node`'s `SnapshotThreshold` stays high through M5; a follower that
falls far enough behind to need a real snapshot isn't handled until M6.
