# DECISIONS

Log of design decisions and rejected alternatives, in ADR style.

> ADR-001 through ADR-010 predate a package restructuring (`vfs/`→
> `internal/vfs/`, `apply/`+`shm/`→`internal/fsm/walappender/(+shm/)`,
> `raft/`→`fsm/`+`internal/raft/gate/`+`internal/raft/proto/`, and later the
> dissolution of `internal/node/`, ADR-013); package names below have been
> updated to match, but the decisions themselves are unchanged. ADR-011
> through ADR-013 document what that restructuring changed, including a
> regression it introduced and its fix. ADR-014 documents a later, separate
> split of `raftgate.Gate` away from hraft itself. ADR-015 (2026-07)
> accepts a follower-write forwarding design, partially updating the
> status of ADR-007/ADR-008; see the status notes on those entries.

---

## ADR-001: Gate replication at the WAL commit frame

**Decision.** Intercept the transaction's commit frame in `xWrite` on `-wal`,
hold it during the RAFT round-trip, release only on quorum.

**Why.** Visibility in WAL mode is the `mxFrame` bump in shm, which has no VFS
callback. The commit frame is the last controllable point before publish and is
identifiable from the on-disk format alone (header bytes 4–7 non-zero). Gating
here gives both required properties: readers can't see an un-replicated txn, and
a rejected txn isn't crash-recoverable (no valid commit frame on disk).

**Rejected:** gating at `xSync` (doesn't exist under `synchronous=NORMAL`, and
sync ≠ visibility); gating at shm write (no hook, and it would require
reimplementing the tear-safe header protocol on the leader for no benefit).

---

## ADR-002: Wrap ncruces' default VFS; don't patch SQLite

**Decision.** Implement a wrapper `VFS`/`File` (`internal/vfs`) over
`github.com/ncruces/go-sqlite3`'s default VFS. Add logic only in `Open`
(tagging) and `xWrite` on `-wal`; delegate everything else, including
`SharedMemory()`.

**Why.** Keeps `.db`/`-wal`/`-shm` standard (requirement #3) and keeps in-process
multi-writer WAL concurrency (requirement #2) for free. No SQLite fork to
maintain.

**Rejected:** patching SQLite with a replication hook (dqlite's historical
approach) loses external file compatibility and is a maintenance burden;
FUSE interception (LiteFS) is out of process, not what was asked; statement
replication above SQLite (rqlite), see ADR-003.

---

## ADR-003: Replicate physical redo (page images), not SQL, not raw WAL bytes

**Decision.** Each RAFT entry (`internal/raft/proto.Entry`) is the proposing
node's ID, `[(pgno, page_image)...]`, and `nTruncate`.

**Why not SQL statements.** Re-execution non-determinism: `RANDOM()`,
`CURRENT_TIMESTAMP`, ROWID allocation, and collation differences would diverge
pages across nodes. Physical redo freezes the leader's choices into the images,
so divergence is structurally impossible.

**Why not raw WAL bytes.** WAL frame headers carry per-WAL salts and a cumulative
checksum chain tied to each node's own WAL epoch. Each node must re-encode into
its own WAL with its own salt/checksums, so files are semantically identical but
byte-different, which is the desired result (each node needs only its *own*
WAL valid for its *own* external readers).

**Consequence / accepted cost.** Full images under a total order are
self-consistent, so there is nothing to reconcile, but apply must be strictly
in-order and gapless (an image is valid only against the base it was computed
on). Total ordering becomes a correctness precondition, and it's the same reason
leader write throughput is one txn per round-trip.

**Not a client-request-ID field.** An earlier draft of this ADR (and of
`DESIGN.md`) described the entry format as carrying "a client request ID (for
future dedup)". No such field was ever added, before or after the
refactor; client-request-ID dedup remains unbuilt and deferred (tracked on
GitHub). `Header.Id`, the per-proposal token the
current format does carry, serves a different, narrower purpose (the
self-apply skip) and must not be conflated with this; see ADR-011.

---

## ADR-004: Writes are leader-only; no conflict resolution exists

**Decision.** Only the leader accepts writes. There is no page-merge,
last-writer-wins, or cross-node MVCC anywhere.

**Why.** Two layers of serialization make conflicts impossible rather than
resolved: WAL's single-writer lock (intra-node) and RAFT's single-leader total
order (cross-node). Building conflict resolution would be solving a problem the
architecture already prevents.

---

## ADR-005: Leader publishes via SQLite; only follower-apply drives shm

**Decision.** On the leader commit path we release the withheld commit frame and
let SQLite run its own `walIndexWriteHdr`. We manipulate the wal-index directly
*only* in the follower-apply path (`internal/fsm/walappender`).

**Why.** SQLite's own publish is the tear-safe two-copy + barrier protocol.
Reimplementing it on the leader would add risk for zero benefit. The follower has
no SQLite writer to do the publish, so it must, and that's where our own
shm code and the format-sensitive tear-safe reimplementation live.

**How "is this my own entry" is decided has changed twice since this ADR was
written:** originally a transient marker set only around the specific
`hraft.Apply` call publishing it, then (a regression, not a refinement of
this ADR) a permanent `entry.NodeID == f.NodeID()` check, now fixed back to
a transient, per-proposal marker (`Header.Id` in `fsm.FSM.skipEntries`). See
ADR-011.

---

## ADR-006: `synchronous=NORMAL`; durability from quorum

**Decision.** Run `NORMAL`, not `FULL`; local WAL fsync may be skipped/batched.

**Why.** The gate is the commit-frame *write*, not a sync. Durability is the RAFT
log quorum. A crashed node rebuilds its WAL tail from the log via hraft's own
snapshot-restore + replay. See `DESIGN.md §durability`.

---

## ADR-007: For now, reject all follower-originated writes  *(current scope)*

> **Status update (2026-07):** rejection remains the shipped behavior and
> the default, but "no forwarding, ever" is no longer the whole story: a
> design for forwarding follower-computed writes *without* OCC was accepted
> as ADR-015 (`FOLLOWER_WRITES.md`, milestone M9, issue #32). It is opt-in
> via an alternative `LogAdapter` and not yet built; until M9 ships, this
> ADR describes reality.

**Decision.** A client write on a follower returns an error + leader hint; the
client redirects. No forwarding, no OCC. This is the current milestone's scope.

**Why.** Correct forwarding of *follower-computed* writes requires the full OCC
apparatus (ADR-008), which is substantial and only pays off in a narrow regime.
Rejection is trivially correct and unblocks everything else (leader writes,
follower apply, external-reader compatibility).

**Redirection is the safe path.** Two viable shapes for adding client
support (see ADR-009): forward the *SQL* (recompute on leader, safe by
construction), or run the whole transaction on the leader. Neither needs OCC.

---

## ADR-008: Deferred: forwarding follower-computed writes + OCC design

**Status: DEFERRED for steps 2–4 (the OCC apparatus); step 1 adopted.**
The base-index compare-and-swap sketched as step 1 below was accepted, with
a serialization discipline that answers this ADR's own objection to it, as
ADR-015 / `FOLLOWER_WRITES.md` (milestone M9). Everything from step 2 on
(per-page versions, read-set validation, `xFetch` capture, the in-flight
overlay) stays deferred with the original trigger: revisit only if leader
SQL-execution CPU is measurably the bottleneck *and* local reads inside
interactive transactions are needed, a narrow intersection. In most
RAFT-SQLite systems fsync + replication dominate and this never pays off.

The problem: a follower computes page images against its **local (possibly
stale) snapshot**. Those images are bound to that base; splicing them into the
authoritative log would corrupt it. So you cannot transparently forward image
batches. Statement forwarding is fine; image forwarding is not.

Steps considered, each addressing a problem in the previous:

1. **Attach a base index token; leader does compare-and-swap.** Correct idea. But
   the index to compare against is the leader's **last log index (log head)**,
   NOT `commitIndex` or `lastApplied`: the head can be ahead when entries are
   in flight, and checking commit/applied admits a lost-update bug. With the
   correct check the acceptance window is "leader quiescent," so
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

3. **Validate the READ set, not the write set.** Read-set validation, not
   write-set validation.
   Write-set-only validation is write-write conflict detection
   (first-committer-wins); it permits stale-read / write-skew anomalies. You
   must forward every page the txn *read* with its pre-image token, and the
   leader validates all of them. Page granularity makes this **sound but
   conservative** (hot structural pages, page 1 and upper b-tree interior nodes,
   over-abort; allocation phantoms may slip through, so call it "mostly
   serializable with caveats").

4. **Capture the read set at the page cache, not the VFS.** `xRead` sees only
   cache *misses*, a subset, so a VFS-derived read set is unsound (silent
   lost update on warm-cache pages). Instrument `xFetch` via a
   `SQLITE_CONFIG_PCACHE2` wrapper (grab defaults with `GETPCACHE2`, delegate,
   record `key` before calling through). This *over*-captures (safe direction).
   - **Scope to the transaction** by resetting the accumulator at read-snapshot
     establishment (the SHARED lock on a `WAL_READ_LOCK(i)` slot, observable in
     `xShmLock`) and harvesting at `commit_hook`. Autocommit → per-statement
     (correct; each is its own txn). Explicit `BEGIN…COMMIT` → one reset, union
     of all pages until `COMMIT`.
   - **Read→write upgrade falls out free** because we key on the *read* lock, not
     the write lock: the accumulator was already filling during the read phase,
     and the upgrade takes a *different* lock slot (no reset), so pre-upgrade
     reads stay in the set.
   - **Pre-image tokens:** don't hash during the txn (buffer not reliably
     populated at `xFetch` on a miss). Collect pgnos, then join against the
     pagemap at forward time, reading the pagemap *as of* the applied index
     recorded at read-lock acquisition (versioned/COW pagemap), or constrain the
     txn to read the latest applied snapshot.

**In-flight overlay (needed if pipelining):** the pagemap updates on *apply* but
the leader appends at the *head*; validate against pagemap ∪ {pages dirtied by
appended-not-yet-applied entries}, folding in on apply and rolling back on
leadership change. Otherwise forbid pipelining (`lastApplied == head`), losing
throughput.

**Crash-consistency:** the pagemap is derived state; persist it atomically with
`lastApplied`, or rebuild from the checkpointed db on recovery (hash every page;
expensive for large dbs).

**Summary.** This rebuilds a read/write-set OCC engine, the
machinery the leader-only design was structured to avoid. It saves only leader
SQL-execution CPU, adds a follower→leader RTT, and over-aborts under structural
churn. It also widens the blast radius of a *buggy* follower (RAFT is
non-Byzantine): re-execution on the leader is self-checking; trusting forwarded
images is not.

---

## ADR-009: Deferred: how client redirection supports transactions

**Status: DEFERRED** (pairs with ADR-007). Captured for adding client
transaction support.

- **Single-shot writes (autocommit / shippable batch):** trivial. Forward the
  *SQL* to the leader; it executes against its own current state, so images are
  computed on the authoritative base by construction. No OCC. This is where most
  systems stop (the rqlite model).
- **Interactive `BEGIN…COMMIT`:** the transaction holds server-side state (pinned
  snapshot + write lock) across client think-time. Three models:
  - **Model A: whole txn on the leader, follower is a dumb proxy.** Only model
    giving true interactive isolation (it *is* a normal SQLite txn, client on the
    far end of a socket). Fits the invariants: the leader's own
    connection produces frames and hits the existing gate; **needs nothing from
    ADR-008**. Cost: per-statement RTT; leader holds the single WAL write lock for
    the txn's whole wall-clock duration incl. think-time → one slow interactive
    txn stalls *every* writer cluster-wide. Requires aggressive idle-txn timeouts;
    client must handle its txn being killed.
  - **Model B: read local, write by redirect, reconcile at commit.** This *is*
    the ADR-008 OCC design wearing a "redirection" label; not a simpler
    alternative. Inherits all OCC caveats.
  - **Model C: submit the transaction as a unit** (stored-proc-like package /
    registered function run start-to-finish on the leader). Recovers single-shot
    simplicity (one RTT, lock held only during execution, no think-time
    exposure) but gives up mid-transaction dependent queries unless that logic
    travels inside the package.

**Recommendation.** Model A for genuine interactive txns
(accept latency, add hard idle timeouts); Model C if the app can submit txns as
units (most robust, no think-time lock exposure). Reach for B/OCC only in the
narrow ADR-008 regime.

---

## ADR-010: Split InstallSnapshot out of M5 into its own milestone

**Decision.** M5 shipped only the leadership-churn ordering work
(the "losing leadership" / "gaining leadership" work items): the gate
gates local proposals on a current-term `hraft.Barrier` drain, closing the
apply-behind window (and, at the time this ADR was written, the Figure-8
self-apply race; see ADR-011 for how that guarantee was later lost).
InstallSnapshot for very-behind followers and wiring the RAFT snapshot cut to a
`TRUNCATE` checkpoint moved to a new M6.

**Why.** The two pieces don't share much implementation surface: leadership
churn is about gate/FSM ordering against the existing WAL/apply machinery,
while InstallSnapshot needed a real `FSM.Snapshot`/`Restore` (at the time
stubbed to error), a new on-disk snapshot format, integration with hraft's
`SnapshotStore`, and WAL-reset-and-resume-apply logic, comparable in size to
the shm work. The M5 "done when" bar (leadership churn never
produces a torn WAL, a lost update, or a stale-state leader serving writes)
didn't need InstallSnapshot to be met; a node that's merely apply-behind (not
log-behind) catches up via normal replication, which small/moderate-log
clusters never exceed.

**Consequence.** `FSM.Snapshot`/`Restore` stayed stubbed and the snapshot
threshold stayed high through M5; a follower that fell far enough behind to
need a real snapshot wasn't handled until M6 (both M5 and M6 are done now).

---

## ADR-011: Self-apply skip: transient marker, not a permanent per-node check

**Decision (of the "Refactor" commit, `97c63a7`; not deliberated in this ADR
log at the time, reconstructed here after the fact).** The pre-refactor
design (`raft/fsm.go`, `raft/gate.go`) skipped re-materializing a leader's own
committed entry via a **transient** marker: `Gate.Propose` called
`fsm.beginSelfApply()` immediately before the specific `hraft.Apply` call
publishing that one proposal, and `fsm.endSelfApply()` (deferred) cleared it
the instant that call returned, success or failure, synchronously, before
`Propose` itself returned to its caller. The refactor replaced this with a
**permanent**, static check in `fsm.FSM.Apply`: `entry.NodeID == f.NodeID()`.

**Why the transient scope is required.** ADR-005's whole premise is "the leader
publishes via its own SQLite write path; `fsm.FSM.Apply` must not also
materialize that same entry via `walappender`, or the write happens twice."
That's only true for the split second the write is happening, not "forever, no
matter when a retroactive commit surfaces this same entry again," including if
that exact entry is retroactively committed much later,
long after the original `Propose` call resolved one way or the other. The
transient marker encoded precisely that scope: "skip *this one*, *right now*";
once cleared, a later `FSM.Apply` for the same entry (via hraft's Figure-8
retroactive-commit rule, or via replay after `FSM.Restore` rewinds local state
to an older snapshot) runs through the ordinary follower-apply path like any
other node's entry, which is correct, since by the time
that later `Apply` runs, this node's own on-disk state may no longer
contain what it published the first time (see below). The permanent
`NodeID`-based check has no such transient window: it skips the entry
*every* time `FSM.Apply` ever sees it, on this node, forever, including in
both scenarios where that skip is wrong.

**Two concrete, reproducible failure modes** (both demonstrated by regression
tests, `PIt`/pending at the time this bug was live; see "Status" below):

1. **hraft's Figure-8 rule.** A node's own proposal can be left uncommitted
   when it loses leadership mid-proposal (ADR-007's "ambiguous commit"). If
   that node later regains leadership and a subsequent entry in its new term
   commits, the stale entry is retroactively committed too. The gate's own
   drain (`Gate.drain`, a current-term `hraft.Barrier`) is supposed to make
   this safe by materializing it before serving new writes, but the entry's
   `NodeID` still equals this node's own ID, so it's skipped forever instead.
   `internal/raft/gate/figure8_test.go` (moved to `log/figure8_test.go` by
   ADR-014) isolates a 3-node cluster, proposes on the eventual leader while
   fully partitioned (so the entry commits nowhere), reconnects only that
   node with one peer (deterministically regains leadership), lets the drain
   run, and confirms the other two nodes materialize the entry while the
   original proposer never does.

2. **An ordinary leader restart after taking its own RAFT snapshot**, no
   partition or leadership drama needed. `hraft.NewRaft` synchronously calls
   `FSM.Restore` on startup whenever a local snapshot exists, which resets
   local state back to that snapshot via `walappender.AppendEntry` (an
   *older* point in time than the node's on-disk state had before restart).
   Every log entry after that snapshot which this same node authored still
   carries its own `NodeID`, so replay skips all of them, even though the
   restore just wound local disk back to a point where they're genuinely
   missing again. The node permanently loses every row it wrote between its
   last local snapshot and its restart.
   `internal/testutils/restart_test.go`'s "recovers a leader restarted after
   it has taken a local snapshot" reproduces this directly: after restart,
   `raft.AppliedIndex()` reports fully caught up, but the row count is
   permanently stuck at whatever the last local snapshot had.

**Status.** Tracked as
[issue #41](https://github.com/fuchstim/literaft/issues/41) (milestone M7).
Fixed alongside issue #42's protobuf wire-format rework. Both scenarios were
committed as `PIt` (Ginkgo "pending") specs while the regression stayed
visible and reproducible; both now pass as ordinary `It`s.

**Fix.** `raftgate.Gate` holds a `*fsm.FSM` reference again (`New(r, fsm,
timeout)`), and each `raft.Log`'s `Entry.Header.Id` is now a per-proposal
UUID rather than a static node ID. `Gate.propose` calls `fsm.FSM.SkipEntry`
immediately before its own `raft.Apply` and the deferred `UnskipEntry`
immediately after that call returns, so the marker only ever covers the one
in-flight proposal it belongs to, restoring the transient scope the
old `beginSelfApply`/`endSelfApply` had.

(`New(r, fsm, timeout)` and `Gate.propose`'s direct `raft.Apply` call are as
of this ADR; ADR-014 later split `raft.Apply` itself out from under `Gate`
into a separate `LogAdapter`. The transient skip-marker scope this ADR fixes
is unaffected; it just moved to `Gate.proposeTransaction` wrapping
`LogAdapter.Apply` instead of `raft.Apply` directly.)

---

## ADR-012: Prevent external-reader-triggered WAL deletion with an explicit main-db-file shared lock

**Decision.** `fsm.FSM` opens a dedicated raw file handle on the node's `.db`
path and takes a plain OS-level **SHARED** (read) lock on SQLite's own
reserved `SHARED_FIRST`/`SHARED_SIZE` byte range (`fsm/dblock.go`), held for
the `FSM`'s entire lifetime, acquired only *after* `PRAGMA journal_mode=WAL`
has already succeeded on this path.

**Why this exists.** Real SQLite's `sqlite3_close` path is, correctly,
willing to checkpoint-and-delete a database's `-wal`/`-shm` if it can prove
(by trying to upgrade a plain OS shared lock on the *main* `.db` file,
`os_unix.c`'s `SHARED_FIRST`/`SHARED_SIZE`, not anything in `-shm`'s own
`WAL_WRITE_LOCK`/read-mark locks, to exclusive) that it's the last
connection with the database open anywhere, in any process. A node's own
long-lived connections (`fsm.FSM`'s own SQLite connection, and
`walappender.WALAppender`'s) don't reliably hold this particular lock just by
being open and idle; an ordinary, transient external reader (exactly
requirement #3's "read-only connections from other processes") briefly
opening and closing against a follower can therefore correctly conclude *it*
is the last connection and perform this same checkpoint-and-delete,
orphaning every `walappender`-written frame that hadn't yet been
checkpointed into `.db` (the next `Open` of that path starts from a fresh,
empty `-wal`). Confirmed empirically before this fix existed: 200 iterations
of `fsm.New` + one external open/query/close reliably deleted the WAL on the
very first iteration.

**Rejected: a dedicated connection with a permanently-open (and periodically
refreshed) SQL-level read transaction**, instead of a raw OS lock. Tried
first; it does work (a connection with a genuinely open read transaction
does hold the relevant lock), but needs periodic refreshing to avoid pinning
an ever-more-stale MVCC snapshot that would cap how much of the WAL a
checkpoint could ever reclaim, real complexity for a problem that
is a plain file-locking fact rather than anything to do with SQLite's
transaction/snapshot machinery. The raw-lock approach pins nothing and needs
no refresh cycle.

**Why the lock must be acquired *after* `PRAGMA journal_mode=WAL`, not
before.** Enabling WAL mode for the first time on a fresh/rollback-journal
database is a one-time conversion that needs more exclusive access to
the file than steady-state WAL operation ever does again; holding this
node's own shared lock across that call produces `SQLITE_BUSY` ("database is
locked"), confirmed empirically. Steady-state WAL operation, by contrast, is
designed around many connections holding this same shared lock concurrently
without contention, the purpose of it existing as a *shared*
lock.

See `WAL_FORMAT.md §main .db file locking` for the exact byte offsets, and
`DESIGN.md §external-reader safety` for where this fits in the bigger
picture.

---

## ADR-013: `internal/node` dissolved into `driver/` + direct caller wiring

**Decision (of the "Refactor" commit, `97c63a7`; no rationale was recorded in
that commit's message or elsewhere; this entry documents the resulting shape
after the fact rather than reconstructing a deliberation that
wasn't captured).** `internal/node.Start`/`internal/node.Node`, which used
to own raft transport/log-store/snapshot-store construction, the
`raftadapter.FSM`/`Gate`, VFS registration, the kept-alive RW connection, and
the follower checkpoint driver, all behind one `Config` struct, no longer
exists. Its responsibilities are now split between `fsm.FSM` (owns the
SQLite connection, `walappender`, `snapshotter`, and now the ADR-012 db
lock), `driver.New` (builds the gate, registers the VFS, wraps a
`database/sql`-compatible driver), and each caller's own direct construction
of the `hraft.Raft` transport/stores; `cmd/literaft/main.go`'s `run()` is
the reference example, and is now the *only* place that wiring happens,
rather than being one layer inside a reusable `internal/node`.

This is a more thorough version of what the deferred "Refactor `internal/node`
to consume `driver/`" item originally proposed (having
`internal/node` call into `driver/` internally, keeping its own `Node` type
as the public surface); the actual change removed the intermediate type
entirely rather than having it delegate.

**What this cost.** `internal/node.Node.WithDB` (a safe accessor for the
kept-alive connection, guarding against a concurrent snapshot `Restore`
closing/reopening it out from under a caller) has no direct replacement;
`driver.Driver`'s pooled `database/sql` connections don't have the same
concurrent-restore hazard in the first place, since a `Restore` no longer
closes and reopens connections at all (see `DESIGN.md`'s follower-apply
section on the current, connection-preserving `Restore`). `driver.Driver`
also dropped several of `internal/node`/the original `driver` package's
options (`WithPageSize`, `WithName`, `WithCheckpointInterval` are gone;
`fsm.FSM.PageSize()` and its own internal checkpointer replace what those
configured); `Ready()`/`LastRejection()`/`VFSName()` were reinstated as
thin forwarders, though `Ready()` didn't last: ADR-014 removed it a second
time, for an unrelated reason.

---

## ADR-014: `raftgate.Gate` split from hraft; `log.SingleWriterLog` owns the cluster

**Decision (no rationale recorded in the commit that made this change; this
entry documents the resulting shape after the fact, same as ADR-013).**
`internal/raft/gate.Gate` no longer holds a `*raft.Raft` or knows anything
hraft-specific. It now depends on a new one-method interface it defines
itself, `raftgate.LogAdapter` (`Apply(entry []byte) error`), and does nothing
but: build an `internal/raft/proto.Entry` from a captured transaction,
self-skip-mark it on `fsm.FSM` (ADR-011's mechanism, unchanged), marshal it,
and hand the bytes to the `LogAdapter`. Everything that used to live on
`Gate` and touched hraft (the `*raft.Raft` handle, `Ready`, the
leadership watcher, the gaining-leadership drain (`Gate.drain`), the apply
timeout, and the `NotLeaderError`/`CatchingUpError` types themselves) moved
to a new top-level package, `log`, as `log.SingleWriterLog` (implementing
`LogAdapter`). `raftgate.New`'s signature changed from `New(r *raft.Raft, fsm
*fsm.FSM, timeout time.Duration)` to `New(fsm *fsm.FSM, log LogAdapter)`;
`driver.New` followed the same shape, from `New(r *raft.Raft, fsm *fsm.FSM,
opts ...Option)` to `New(fsm *fsm.FSM, log raftgate.LogAdapter)`; `driver`'s
own `Option`/`WithApplyTimeout` are gone, replaced by `log.Option`/
`log.WithApplyTimeout` passed to `log.NewSingleWriterLog` instead.
`vfs.Gate`'s method also changed shape, from `Propose(*raftproto.Transaction)`
to `ProposeTransaction(frames []*vfs.Frame, nTruncate uint32)`, so
`internal/vfs` no longer needs to import `internal/raft/proto` at all: Gate
builds the `Transaction` itself, from the frames it's handed.

**What this cost.** `Driver.Ready()` (restored in ADR-013) is gone again, this
time for a good reason rather than an oversight: `Driver` only ever holds a
`raftgate.LogAdapter`, a narrow interface with no `Ready` method, since not
every `LogAdapter` need have a concept of "elected leader, draining a
backlog" (that's specific to a real raft cluster). A caller that needs
readiness, to decide whether to redirect a client away from a not-yet-drained
leader, now has to hang onto its own concrete `*log.SingleWriterLog`
alongside the `driver.Driver` it built from it, rather than asking `Driver`
for it. `internal/testutils`'s TCP-tier `Node` gained a `Log
*log.SingleWriterLog` field for this: `Node.Driver.Ready()` stopped
compiling, and `TCPCluster.ReadyLeader` now checks `Node.Log.Ready()`
instead.

**A related, easy-to-miss error-plumbing detail.** Since `CatchingUpError` is
retriable (see `DESIGN.md §Role transitions`) while most gate rejections
aren't, `log.SingleWriterLog.Apply` now tags it with a new
`vfs.GateError(err, code)` wrapper carrying an explicit `sqlite3.ExtendedErrorCode`
(`BUSY`), which `internal/vfs.File`'s write path checks for via `errors.As`
and uses instead of its `IOERR_WRITE` default, the first time any rejection
reason has been distinguished at the sqlite result-code level, closing a gap
that had been tracked as a TODO. The first version of `GateError` didn't
implement `Unwrap`, which silently broke `errors.As`/`errors.Is` discovery of
the error it wrapped (e.g. recovering a `log.CatchingUpError` from
`Driver.LastRejection()`) the moment it passed through `Gate.proposeTransaction`'s
own `fmt.Errorf("...: %w", ...)` wrap: anonymously embedding an `error`
field only promotes `Error() string`, not `Unwrap() error`. Fixed by adding
`(*gateError).Unwrap`, caught by a test that fabricates exactly that
double-wrapped shape (`log/leadership_test.go`'s premature-write case) and
confirmed (by temporarily removing the fix) to fail without it.

**Test relocation.** Everything under `internal/raft/gate/*_test.go` that
exercised hraft-specific behavior through a real cluster (`gate_test.go`'s
`NotLeaderError`/lost-leadership/materialization cases, `figure8_test.go`,
`leadership_test.go`'s gaining-leadership drain) moved to `log/` (as
`log/singlewriter_test.go`, `log/figure8_test.go`, `log/leadership_test.go`),
built on a `raftgate.Gate` + `log.SingleWriterLog` pair per node rather than
a `Gate` alone, since that pairing is what a real cluster wires
together now. `internal/raft/gate`'s own tests shrank to what's left to
verify without any raft cluster at all: proto-encoding of a captured
transaction, the self-skip marker's transience (proven against a real
`fsm.FSM`, no raft involved), `LastRejection` bookkeeping, and that
`Gate`'s own error-wrapping preserves `errors.As`/`errors.Is` discoverability.

---

## ADR-015: Follower-computed writes via a base-index check; OCC still rejected

**Status: ACCEPTED and BUILT (milestone M9, issue #32).** The opt-in
forwarding machinery (`log.ForwardingLog` + `log.LeaderTransport`, the
`fsm.FSM` lastApplied counter and three-state skip-marker CAS, the
walappender loaned-lock API, the versioned snapshot header, and a reference
gRPC transport) shipped; `cmd/literaft` enables it by default. One heavier
correctness test is still pending a root-cause and the remaining
failure-matrix cluster tests are still to be added, so `#32` stays open. The
full protocol reference is `FOLLOWER_WRITES.md`; this entry records the
decision and what was rejected.

**Decision.** Adopt ADR-008's **step 1** (forward a follower's captured
page images to the leader together with the base index they were computed
on; the leader proposes them to RAFT iff that base equals its own last
applied index) and *nothing past step 1*. The acceptance check is made
sound (equivalent to the log-head comparison ADR-008 required) by three
existing structures rather than new machinery: the `Ready` drain (a `Ready`
leader has no uncommitted or committed-unapplied data entries), the leader's
own `WAL_WRITE_LOCK` serializing *all* proposals (local writers hold it
natively, the forward handler holds it explicitly **across the raft
round-trip until the leader's own FSM applied the entry**: releasing earlier
re-opens ADR-008's lost-update window, because a local writer's
`BUSY_SNAPSHOT` check only sees locally *published* state), and an
FSM-owned `lastApplied` counter that never trails published state except
under the publisher's held lock. Opt-in surface: an alternative
`raftgate.LogAdapter` (`log.ForwardingLog` wrapping `log.SingleWriterLog` +
a caller-supplied `log.LeaderTransport`); gate, vfs write path, entry wire
format, and `driver.New` unchanged.

**Why accept the ~100% abort rate under concurrency that ADR-008 used as
the argument against this shape.** ADR-008 was weighing step 1 as a
*throughput* feature and correctly found it useless for that: under
contention every forward stales. This ADR accepts it as a *capability*
feature: occasional or low-contention writes no longer need client-side
redirect plumbing, while the performance model stays "write on the leader."
The price is confined to the proposals that lose.

**Rejected (again): steps 2–4 of ADR-008** (per-page version pagemap,
read-set validation, `xFetch` capture, in-flight overlay). Unchanged
reasoning, unchanged revisit trigger; see ADR-008's status note.

**Rejected: SQL/statement forwarding as the mechanism here.** It remains
the *right* answer for interactive transactions (ADR-009, Models A/C) and
needs none of this, but it re-executes on the leader, which is the
cost this design exists to avoid for already-computed single-shot writes.
The two are complementary, not competing.

**Rejected: comparing against hraft's `AppliedIndex()`.** It advances when
a batch is dispatched to the FSM goroutine, before `FSM.Apply` runs
(vendored `api.go` says so outright), so it can lead the local database.
The FSM must own its own counter, advanced under the write lock.

**Consequences.**
- The snapshot stream gains a small versioned header carrying the
  snapshot's raft index: hraft never passes `meta.Index` to `FSM.Restore`,
  and without it a restored-then-quiet follower would stamp stale bases
  forever. Old headerless snapshots are rejected (clean-slate precedent,
  same stance as issue #42's wire-format change).
- A local publish failure *after* RAFT commit becomes **fatal by design**
  (issue #60): the review of this design found that path silently diverges
  a node today, and forwarding would amplify it into a cluster-wide lost
  update via the base stamp.
- The skip marker (ADR-011) grows into a three-state CAS
  (pending/consumed/abandoned) with a consumed-obligates-publish rule; the
  gate's transient set/clear call sites are untouched.
- No client-request-ID dedup is needed (narrowing issue #34's premise):
  the same page images are never re-proposed after a possibly-proposed
  outcome; every application retry re-executes SQL on fresher state.
- The shm write lock's OFD semantics (same-OFD acquisitions convert instead
  of excluding; same-OFD unlock releases the whole OFD's claim) become
  significant for correctness: the leader-side handler is the first
  second-in-process acquirer, so the new lock-loan API fronts the OS lock
  with an in-process mutex and suppresses the appender's own lock/unlock
  under a loan.

---

## ADR-016: One rejection taxonomy in `rafterrors`, superseding scattered typed errors + `vfs.GateError`

**Status: ACCEPTED and BUILT (milestone M7, issue #74).** Supersedes the
error-plumbing detail recorded under ADR-014 (`vfs.GateError`) and the
per-status typed errors ADR-015's forwarding work grew.

**Problem.** By the end of M9, a rejected write proposal could be one of eight
concrete error types split across `log/` (`NotLeaderError`, `CatchingUpError`,
`EnqueueTimeoutError`, `AmbiguousError`, `StaleBaseError`, `ForwardBusyError`,
`AmbiguousForwardError`, `NotDeliveredError`), with retryability encoded
*separately* by whether a call site wrapped the error in
`vfs.GateError(err, sqlite3.BUSY)`. Knowledge was split three ways: the error
types in `log`, the BUSY tagging applied ad-hoc in `log`, the code
interpretation in `internal/vfs`; and the same type surfaced with different
codes on different paths (`NotLeaderError` was untagged on the direct path but
BUSY-tagged on the forwarding post-retry path). There was no single place that
answered "is this retryable."

**Decision.** A single taxonomy package, `internal/raft/gate/errors`
(`rafterrors`), with three categories (`Redirect`, `Retryable`, `Ambiguous`)
and four concrete types constructed via `NewNotLeaderError` /
`NewCatchingUpError` / `NewNotAppliedError` / `NewAmbiguousError`. The category
is the single source of truth: it fixes the surfaced `sqlite3` result code
(`Category.ResultCode`: Redirect→`READONLY`, Retryable→`BUSY`,
Ambiguous→`IOERR_WRITE`), so there is no per-call-site tagging. `log`'s
adapters (and the reference/test transports) now *translate* their conditions
into these errors instead of defining their own; the eight old types collapse
into the four (enqueue-timeout / stale-base / forward-busy / proven
non-delivery all become `NotAppliedError`; both ambiguous variants become
`AmbiguousError`).

`internal/vfs` stays raft-agnostic: it defines a generic `vfs.CodedError`
interface (`ResultCode() sqlite3.ExtendedErrorCode`) that the taxonomy errors
satisfy, and `vfs.ErrCode` reads it; the old concrete `vfs.gateError` wrapper
and `vfs.GateError` constructor are gone.

**Why `NotLeaderError` → `READONLY` (issue #28).** A redirect is its own
category, distinct from a retryable BUSY: a client on a bare
`SingleWriterLog` follower should redirect to the leader, not spin retrying the
same connection. Giving it a distinct code (a follower is effectively
read-only for writes) lets a client tell redirect from retry from the result
code alone, not only from the concrete type recovered via `LastRejection`.
Both redirect and retryable are `SafeToRetry` (both provably pre-commit), which
is what the forwarding correctness test keys on; only `Ambiguous` is not.

**Consequences.**
- `SingleWriterLog.Apply`'s dead FSM-rejection branch (`future.Response()`)
  is removed: `fsm.FSM.Apply` panics on failure and otherwise returns nil, so
  that response was always nil.
- `Gate.proposeTransaction` no longer wraps the adapter error in an opaque
  `fmt.Errorf("failed to apply entry: %w", …)`; it returns the taxonomy error
  directly, so `LastRejection` surfaces it cleanly.
- The returned-code vs `LastRejection` duality (also flagged by #28) stays,
  but is no longer a smell: the code carries coarse retryability (and often
  doesn't survive `database/sql`), while `LastRejection` remains the reliable
  channel for recovering the concrete type and its detail (e.g. a leader
  hint).
