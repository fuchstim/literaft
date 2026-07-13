# FOLLOWER_WRITES — forwarding follower-computed writes under a base-index check

Design for accepting write transactions on follower nodes. Status: **built**
(milestone M9, [issue #32](https://github.com/fuchstim/literaft/issues/32)),
as opt-in machinery — a node wired with `log.ForwardingLog` accepts
follower-originated writes; without it, ADR-007's "reject with a leader hint"
still holds. `DECISIONS.md` ADR-015 records the decision; this file is the
reference for the protocol itself. The protocol was adversarially reviewed
against the current code and the vendored `hashicorp/raft` before
acceptance; the review's corrections are folded in throughout and called out
where they changed the obvious-looking design. (One heavier correctness test
and the remaining failure-matrix cluster tests are still outstanding — see
`ROADMAP.md` M9.)

---

## Summary & scope

A write transaction executed on a follower computes physical page images
against that follower's **local** (possibly stale) snapshot. Those images are
valid only against that exact base state (ADR-003), so they cannot be spliced
into the RAFT log unconditionally. This design forwards them to the leader
together with the **base index** they were computed on — the raft log index
of the last transaction applied to the follower's local database — and the
leader proposes them to RAFT **iff** that base equals the leader's own last
applied index, under a serialization discipline that makes the comparison
exact (see §soundness). Any mismatch is a rejection; the application retries
by re-running the transaction, which recomputes against fresher state.

This is deliberately **whole-database-granularity optimism**: any concurrent
write anywhere in the cluster — even to disjoint pages — invalidates an
in-flight follower proposal. What it buys for that price:

- **No page-level conflict machinery.** No read-set capture, no pagemap, no
  OCC validation engine — ADR-008's steps 2–4 stay deferred, and the "no
  conflict resolution anywhere" invariant survives: conflicts are still
  *prevented* (by rejection), never merged.
- **Strict correctness.** An accepted forwarded entry is, provably, computed
  on exactly the state it will apply against — the same guarantee a
  leader-local write gets from SQLite's own snapshot check.
- **Read-your-writes on the originating follower**: its `COMMIT` returns only
  after the leader accepted the entry *and* the follower's own FSM applied it
  locally.

Out of scope (unchanged deferrals): page-level OCC (ADR-008), SQL/statement
forwarding and interactive-transaction models (ADR-009 — still the right
answer for interactive txns), client-request-ID dedup (#34 — not needed here,
see §failure-handling), transport authentication/encryption (caller-owned),
linearizable reads (#35).

---

## Relation to ADR-007 / ADR-008 / ADR-009

ADR-008 already worked through a "base index token + leader compare-and-swap"
design as its **step 1** and rejected the naive version for one precise
reason: the index the leader must compare against is its **last log index
(log head)**, not `lastApplied` — the head runs ahead of applied state while
proposals are in flight, and checking applied state admits a lost update.
This design adopts step 1 and answers that objection not by tracking the log
head (which only hraft knows) but by **forcing `lastApplied == log head` at
the moment of the check** (§soundness). ADR-008's other conclusion — that
widening the acceptance window requires the full OCC apparatus — is
unchallenged; this design simply accepts the narrow window.

ADR-007's rejection-with-hint remains the *default* behavior and the fallback
whenever forwarding can't proceed (no transport configured, leader unknown,
rejection). ADR-009's statement-forwarding recommendation is unaffected:
forwarding *SQL* re-executes on the leader's authoritative state and never
needs any of this; forward *page images* only where re-execution on the
leader is what you're trying to avoid.

---

## Protocol

Notation: F = the follower originating the write, L = the current leader,
E = the proposed entry (`internal/raft/proto.Entry`), `base` = F's
`lastApplied` at proposal time, M = the raft log index E is assigned if
accepted.

### Follower side (originating node)

1. A local RW connection on F runs a write txn. Stock behavior throughout:
   SQLite takes `WAL_WRITE_LOCK`, writes non-commit frames through to F's own
   `-wal` (invisible, beyond `mxFrame`), and the wrapper VFS captures
   `(pgno, page)` pairs — exactly `DESIGN.md §write path` steps 1–2, which
   are role-agnostic.
2. On the commit frame, `Gate.proposeTransaction` runs **unchanged**: build
   E with a fresh per-proposal `Header.Id`, mark it in `fsm.FSM`'s skip
   markers, marshal, call `LogAdapter.Apply(bytes)`, deferred-unmark.
3. The configured adapter is a `log.ForwardingLog` wrapping the node's
   `*log.SingleWriterLog`. `ForwardingLog.Apply` first calls the inner
   adapter. If that returns nil or any error other than
   `*log.NotLeaderError`, it returns the result as-is — on a leader this
   adapter is byte-for-byte today's behavior. A `NotLeaderError` is the
   forward trigger: it is returned strictly *before* anything was proposed,
   so forwarding is always safe at that point.
4. Forward path: unmarshal E (for `Header.Id`), read
   `base := fsm.LastApplied()`, build a `ForwardRequest{entry, base_index}`,
   and send it via the caller-supplied transport to the leader address the
   inner adapter reports. The base is exact, not racy: this goroutine is
   inside `xWrite` of the commit frame, so the txn **holds F's
   `WAL_WRITE_LOCK`**; `lastApplied` only advances under that lock
   (§lastapplied), and SQLite's own snapshot-upgrade check
   (`SQLITE_BUSY_SNAPSHOT`) already guaranteed the txn's read snapshot
   equals the published state at lock acquisition. So
   `base == the state E was computed on`, always.
5. Dual-wait: on an `OK` response, wait (bounded — §liveness) until F's own
   `fsm.FSM.Apply` has **consumed E's skip marker**, i.e. E replicated back
   to F through the ordinary raft stream and was recognized as this
   in-flight proposal. Only then return nil. The gate then releases the
   withheld commit frame and SQLite performs its normal publish
   (`walIndexAppend` + `walIndexWriteHdr`) — the originating follower
   **self-applies via SQLite**, exactly like a leader does (ADR-005);
   `walappender` is not involved on F for its own accepted proposal.
6. Every non-OK outcome resolves through the marker CAS (§marker) — this is
   load-bearing, not cleanup; see §failure-handling.

### Leader side (handler)

The `ForwardingLog` on every node registers a handler with the transport;
only the node that is currently leader will accept work through it.

1. Decode the `ForwardRequest` and E; sanity-check shape: every page image
   is exactly `fsm.PageSize()` bytes (the fixed cluster-wide page size),
   pgnos are non-zero, `nTruncate != 0` (a forwarded entry is always a
   whole committed txn). This is shape validation, not content validation —
   RAFT is non-Byzantine and forwarded images are trusted, same as ADR-008
   noted; a malicious or buggy follower can corrupt the cluster and the
   transport's authentication (caller-owned) is the boundary.
2. Acquire L's **`WAL_WRITE_LOCK`** through the walappender's shm handle,
   with a deadline (→ `BUSY` response on timeout), and register a **held-lock
   loan** under E's `Header.Id` (§locking — the loan is how L's own
   `FSM.Apply` will materialize E without re-acquiring the lock the handler
   already holds).
3. Under the held lock: if `base != fsm.LastApplied()` → release, respond
   `STALE_BASE` (with L's `lastApplied` as a diagnostic). Nothing was
   proposed.
4. Call the **inner** `SingleWriterLog.Apply(entry_bytes)` — reusing its
   existing checks and semantics verbatim: not leader → `NOT_LEADER` (+
   hint), not `Ready` (drain in progress) → `CATCHING_UP`, enqueue timeout →
   definitively-not-proposed failure, and on nil the entry is **committed
   and applied on L** (hraft's `ApplyFuture` responds only after L's own
   `FSM.Apply` ran — verified in the vendored source, `fsm.go`'s
   `applySingle` responds after `fsm.Apply` returns).
5. Respond `OK` (with M as a diagnostic), release the loan and the lock
   (also on every error path, per the loan lifecycle rules in §locking).

On L, E is *not* marked in `skipEntries` (only F marked it), so L's
`FSM.Apply` materializes E via `walappender` — under the handler's loaned
lock — and advances L's `lastApplied` before the handler responds. Every
other follower applies E through the ordinary `DESIGN.md §follower-apply`
path, indistinguishable from a leader-originated entry.

---

## Soundness: why `base == lastApplied` is the log-head check here

The apply invariant (ADR-003) demands: E, computed on base B, may only be
appended where the set of data entries preceding it is exactly the set at B.
Equivalently, at the instant of `raft.Apply`, there must be **no data entry —
committed, in-flight, or lurking uncommitted in the log — beyond B**. Three
mechanisms together make the simple `base == lastApplied` comparison
equivalent to that:

1. **`Ready` (the existing drain).** `SingleWriterLog` only accepts proposals
   after a current-term `hraft.Barrier` completed (`DESIGN.md §role
   transitions`). hraft commits prior-term entries only by committing a
   current-term entry (the Figure-8 rule; `commitment.go`'s
   `quorumMatchIndex >= startIndex` check in the vendored source), and the
   Barrier is such an entry — so a `Ready` leader has **no uncommitted data
   entries in its log and no committed-unapplied backlog**. This kills the
   "stale entry from an earlier term commits retroactively after we checked"
   case, and it means a resent proposal after an ambiguous outcome can never
   double-apply: if the earlier attempt's entry is still in the new leader's
   log, either it commits during the drain (then `lastApplied` includes it →
   `STALE_BASE`) or the leader isn't `Ready` yet (→ `CATCHING_UP`).
2. **All proposals on a node serialize on that node's `WAL_WRITE_LOCK`, held
   until the entry is applied on that node.** Leader-local writers hold it
   natively across the gate (that's `DESIGN.md §conflicts`' "≤1 in-flight
   local proposal"); the forward handler holds it explicitly across steps
   2–5. So under the lock, nothing else is in flight *from this node* — and
   since the leader is the only node that proposes, nothing is in flight at
   all.
3. **`lastApplied` catches up to the log head before the lock is ever
   released** (§lastapplied): a local proposal's future resolves only after
   the skip-apply ran; a forwarded proposal's handler waits for L's
   materializing apply. Combined with (1) and (2): under the held lock on a
   `Ready` leader, `lastApplied == last data-entry log index`. The
   comparison the user-visible protocol makes (`base == lastApplied`) *is*
   ADR-008's required log-head comparison — using the one index the FSM can
   know without asking hraft.

Why the handler must hold the lock **across the round-trip**, not just the
check: suppose it released after `raft.Apply`. E (accepted, committing)
materializes on L *asynchronously* via the FSM. A leader-local writer W could
take the write lock in that window; SQLite's `BUSY_SNAPSHOT` check validates
W's snapshot only against **locally published** state — which doesn't include
E yet — so W sails through and computes pages on a base missing E. W's entry
lands after E in the log. Lost update, exactly the anomaly ADR-008 step 1
warned about. Holding the lock until E is locally applied closes it: W blocks
on the lock, and when it finally acquires, E is published, so W's snapshot
check sees it.

One structural constraint falls out of the verification: **`fsm.FSM` must
never implement hraft's `BatchingFSM`**. The argument above leans on hraft
responding to each entry's future after *that entry's* `FSM.Apply` (per-entry
ordering inside batches); `ApplyBatch` changes when futures resolve relative
to individual applies. hraft's group commit / batching at the log layer is
fine (verified — `dispatchLogs` assigns indexes in dispatch order and the
per-entry future semantics survive it); only the FSM-side batching interface
is off-limits.

---

## `lastApplied` — the counter everything keys off

**Definition.** Per node: the raft log index of the last `LogCommand` entry
whose effects are durably in this node's **local** database (`-wal` +
wal-index). In-memory, owned by `fsm.FSM`.

**Why not `raft.AppliedIndex()`.** hraft advances its `lastApplied` in
`processLogs`, when a batch is *dispatched to the FSM goroutine's channel* —
before `FSM.Apply` has run. Its own doc comment says so explicitly ("does
NOT mean that the application's FSM has yet consumed it"; vendored
`api.go`). Using it would stamp bases for state the local db doesn't have
yet.

**Advance rules** (both paths run with the node's `WAL_WRITE_LOCK` held by
*someone*, which is what makes reads under the lock exact):

- **Materialize path** (any non-self entry): inside
  `walappender`'s append, within the locked section — *not* after the lock
  is released. Advancing after unlock opens a window where a local txn
  acquires the lock, passes SQLite's snapshot check against the
  just-published state, but stamps the *previous* index → spurious
  `STALE_BASE` on every write that races an apply. Safe direction, but
  pointless aborts; keep the invariant tight instead:
  **`lastApplied` never leads the locally published state, and never trails
  it except under the publisher's own held write lock.**
- **Skip path** (the node's own in-flight proposal): at marker consumption,
  on the FSM goroutine. The proposer holds the write lock at that moment and
  publishes via SQLite before releasing it, so the "leads published state"
  window exists only under the proposer's own hold — unobservable to any
  other base-stamper (they'd need the lock). The one way to observably break
  this invariant is a *failed publish after consumption* — which is why that
  failure is fatal by design (§prerequisites).

**Initialization & Restore.** Starts at 0 on a fresh db; ordinary startup
replay (hraft snapshot-restore + log replay) rebuilds it through the two
rules above. But `FSM.Restore` needs the snapshot's raft index, and **hraft
does not pass it** — `raft.FSM.Restore(io.ReadCloser)` gets only the stream;
`meta.Index` stays inside hraft. Without it, a restored node's `lastApplied`
undershoots until the next live entry applies; on a quiet cluster it would
stamp stale bases and get `STALE_BASE` **forever** (retrying doesn't help —
the base never changes). So this design requires a small **versioned header
on the snapshot stream**: magic (chosen to be impossible as a page-1 prefix,
which is always `"SQLite format 3\0"`), a format version, and the snapshot's
raft index. `Snapshot()` runs on hraft's FSM goroutine, serialized with
`Apply`, so the backup copy is taken at exactly `lastApplied` and that's the
index the header carries. `Restore` sets `lastApplied` to the header's index
**unconditionally** — including *downward* on the startup-rewind case
(ADR-011's scenario 2: restore to an older snapshot, then replay forward).
Old headerless snapshots are rejected with a clear error; same clean-slate
upgrade stance as the #42 wire-format change.

---

## The skip-marker state machine

`fsm.FSM.skipEntries` grows from a set to a per-proposal state machine,
guarded by the existing mutex. The gate's call sites are **unchanged**
(`SkipEntry` before `LogAdapter.Apply`, deferred `UnskipEntry` after) —
ADR-011's hard-won transience is preserved exactly: a marker exists only
inside the one `Gate.proposeTransaction` call that owns it.

States: **pending** → **consumed** (terminal) or **pending** → **abandoned**
(terminal).

- `SkipEntry(id)`: create in **pending**, with a signal channel.
- `FSM.Apply` finds the id: if **pending** → transition to **consumed**,
  *skip materialization*, close the signal channel, set
  `lastApplied = log.Index`. If **abandoned** (or absent) → materialize
  normally via `walappender`. The check-and-transition is atomic under the
  mutex; the signal must be a non-blocking close, since this runs on hraft's
  FSM goroutine.
- The proposer's failure path (`ForwardingLog`, on timeout or any non-OK
  response): atomically, if **pending** → **abandoned**, return the error;
  if already **consumed** → **return nil instead** — the proposal
  succeeded, publish.
- `UnskipEntry(id)`: delete the entry whatever its state (by then it's
  terminal); pure bookkeeping.

The two invariants this encodes — both directions were verified to be
corruption or divergence if violated:

1. **Consumed obligates publish.** Once the FSM has skipped materialization,
   this node's *only* copy of E's effects is the SQLite write path's
   withheld commit frame; roll that back and E never exists locally
   (silently, forever — hraft won't deliver the index again). So the
   proposal's outcome is decided by *whichever proves commitment first*: the
   transport's `OK`, or local consumption. A lost response race — leader
   accepted, E replicated back and got consumed, then the transport errored
   — **must** resolve as success. This also degrades gracefully: if the
   response channel is flaky but replication works, forwards still succeed.
2. **Never publish without consumption.** If the gate released the commit
   frame while the marker went **abandoned**, the FSM would *also*
   materialize E on arrival. Re-appending the same images is harmless only
   if nothing interleaved; if any later entry applied in between,
   re-materializing E **reorders page images** — corruption. (This is the
   same argument as ADR-003's gapless-order requirement, applied locally.)

Rejections that never proposed (`STALE_BASE`, `NOT_LEADER`, `CATCHING_UP`,
`BUSY`) can't race consumption at all — no entry exists to be consumed —
but they take the same CAS path for uniformity.

---

## Locking & serialization

Three serializers, one per layer, same as the rest of the design
(`CLAUDE.md`: "WAL is single-writer, RAFT is single-leader"):

- **F's `WAL_WRITE_LOCK`** — serializes F's local writers, pins the base,
  and (held across the forward RTT) delays F's own follower-apply of
  *competing* entries; see §liveness for why that stall is bounded and
  acceptable.
- **L's `WAL_WRITE_LOCK`** — serializes *all* proposals cluster-wide
  (local writers natively, forwarded ones via the handler).
- **RAFT's total order** — as ever.

### The OFD-lock trap and the loan lifecycle

The shm write lock is an **OFD (open-file-description) lock**, and
`walappender` takes it through its *single* shm file handle
(`internal/fsm/walappender/shm/`). Two facts about OFD locks shape
everything here (verified against `shm/lock_darwin.go`/`lock_linux.go` and
ncruces' conn-side equivalents):

- Lock requests on the **same OFD never conflict** — they convert. Cross-OFD
  requests (e.g. against SQLite connections' own handles) conflict normally.
- An unlock on the same OFD **releases the byte range for the whole OFD**,
  no matter which goroutine "acquired" it.

Today that's harmless: hraft's single FSM goroutine is the only acquirer on
walappender's handle. The forward handler is the *second* in-process
acquirer, so the naive implementation is silently wrong twice over (it
neither excludes the FSM goroutine nor survives `AppendFrames`'
unconditional deferred unlock). The rules, in full — these came out of the
adversarial review and are the difference between "design" and "footgun":

1. **An in-process mutex fronts the OS lock** for all acquisitions through
   the walappender handle. Lock order: mutex, then OS lock (the OS lock
   still does the real cross-OFD exclusion against SQLite's connections and
   external processes).
2. **The loan registry (id → held lock) has its own short-critical-section
   lock** — *never* the mutex the handler holds across the round-trip.
   Otherwise: handler waits on the `ApplyFuture`, which resolves only after
   `FSM.Apply(E)`, which must consult the registry, which blocks on the
   handler's mutex — deadlock.
3. **A loaned append suppresses both the lock *and* the unlock.**
   `AppendFrames` today unconditionally defers its unlock; run under a loan,
   that would release the handler's OS lock mid-protocol (same-OFD unlock —
   see above) and let a local writer interleave before the handler finished.
4. **Loan reclamation is CAS'd against use.** The handler must not release
   the OS lock while the FSM goroutine could still pick the loan up: an
   apply that believes it holds a loaned lock while nobody holds the OS lock
   appends concurrently with a local writer — torn WAL, torn wal-index.
   Simplest sufficient rule, adopted here: **the handler never abandons the
   wait before its `ApplyFuture` resolves.** That wait is bounded — hraft
   resolves every in-flight future with `ErrLeadershipLost` during
   step-down, so the future can't hang past a leadership change.
5. **A loaned append skips the deferred threshold checkpoint.** That
   checkpoint runs on the FSM goroutine at the end of `AppendFrames`; under
   a loan it would extend the handler's lock hold (which local writers are
   queued on) by a full passive checkpoint. The ticker-driven checkpointer
   covers the debt.

### Deadlock audit (result: none, given the rules above)

Cycles examined across {F's write lock, L's write lock, handler mutex, loan
registry lock, `skipEntriesMu`, `checkpointMu`, hraft's FSM goroutine}:

- *Handler ↔ FSM goroutine on L*: broken by the loan (rules 2–4). Without a
  loan this is a hard deadlock — the handler holds the lock waiting for the
  future; the future needs `FSM.Apply(E)`; the apply needs the lock.
- *Proposer ↔ FSM goroutine on F*: the proposer holds F's write lock waiting
  for consumption; consumption takes **no** lock (skip = no walappender) and
  nothing can precede E in the apply queue *if the leader accepted* (accept
  ⟺ no data entries in (base, M); non-data entries never reach the FSM).
  If the leader instead rejects, F's FSM may be blocked on the lock behind
  the proposer, but the rejection response doesn't depend on F's FSM — the
  proposer errors out, SQLite rolls back, the lock releases. Bounded stall,
  no cycle (§liveness).
- *Restore vs proposer on F*: `Restore` (on the FSM goroutine) appends via a
  walappender and blocks on the write lock behind an in-flight forward whose
  marker may never be consumed (its entry got compacted *into* the incoming
  snapshot). The dual-wait timeout is what breaks this — it is therefore
  **mandatory, not optional**. Timeout → abandoned → gate error → SQLite
  rollback (the commit frame never reached disk, so the rolled-back frames
  are recovery-inert) → lock free → Restore appends over the abandoned
  frames (appends are positioned by `mxFrame`, not file end, so junk beyond
  `mxFrame` is overwritten — same argument as `DESIGN.md §write path` step
  5) and TRUNCATE-checkpoints. The app saw an error for a transaction whose
  effects then arrive via the snapshot: the pre-existing ambiguous-commit
  contract, not a new anomaly.
- *`checkpointMu`, `skipEntriesMu`*: never held together with the write lock
  in a conflicting order (the threshold checkpoint runs after the unlock;
  `skipEntriesMu` sections are map-ops + a channel close).

---

## Prerequisites

Two pieces of groundwork this design depends on, both independently
worthwhile:

1. **Fatal publish-after-commit failures —
   [issue #60](https://github.com/fuchstim/literaft/issues/60).** If the
   commit-frame flush (or SQLite's subsequent wal-index publish) fails
   *after* the gate returned nil, the entry is committed cluster-wide but
   rolled back locally, and the skip marker was already consumed — the node
   permanently lacks its own committed entry. **This is a live bug today on
   the leader path, with no forwarding involved**; it was found by this
   design's review. Forwarding amplifies it from local divergence to
   cluster-wide lost update: the diverged node's next proposal stamps a
   `base` that *includes* the missing entry, and the leader — whose check
   trusts `lastApplied` — accepts images computed on state the proposer
   doesn't have. The rule this design assumes: **any local-publish failure
   after commitment is fatal** (panic, like `FSM.Apply`'s existing
   contract); recovery is restart + hraft's snapshot-restore + replay, which
   converges because entries are full page images. That in turn requires
   restart to actually work from a non-empty `-wal` with an uninitialized
   wal-index — `walappender.Open` currently refuses ("recovery from an
   existing WAL isn't implemented yet"); see #60 for both halves.
2. **The snapshot-stream index header** (§lastapplied). Without it,
   forwarding from any restored-then-quiet follower livelocks on
   `STALE_BASE`.

---

## API surface

Everything below is design-level shape; names may be bikeshed at
implementation time. The load-bearing properties: the gate, the `vfs`
protocol path, `raftgate.LogAdapter` (`Apply(entry []byte) error`), and
`driver.New` are **unchanged** (an embedder opts in by constructing a
`ForwardingLog` and handing it to `driver.New` — which is the whole
"alternative LogAdapter" contract); the replicated entry format is
**unchanged** (the base index travels in the transport envelope, never in
the log).

```go
// package log

// LeaderTransport ships opaque byte blobs between nodes; blobs are
// proto-encoded ForwardRequest/ForwardResponse messages. Implementations own
// listeners, dialing, retries-at-the-connection-level, auth, and lifecycle;
// literaft owns the payloads and the handler logic. Propose returns the
// leader's response bytes, or an error if it can't be reached / didn't
// answer. Handle registers the single callback invoked for each inbound
// request on every node (only the current leader will accept work).
type LeaderTransport interface {
	Propose(ctx context.Context, leader raft.ServerAddress, request []byte) ([]byte, error)
	Handle(handler func(ctx context.Context, request []byte) ([]byte, error))
}

// ForwardTarget is what ForwardingLog needs from the local FSM; *fsm.FSM
// implements it. Defined here, by the consumer, like raftgate.LogAdapter.
type ForwardTarget interface {
	LastApplied() uint64
	PageSize() uint32
	// AwaitEntryApplied blocks until the identified in-flight proposal's
	// skip marker is consumed, then returns nil. On ctx expiry it resolves
	// the marker CAS: consumed → nil, pending → abandoned + error.
	AwaitEntryApplied(ctx context.Context, id string) error
	// BeginHeldApply acquires this node's WAL write lock (in-process mutex +
	// OFD lock) and registers a loan so FSM.Apply materializes the
	// identified entry under it. release is mandatory on all paths.
	BeginHeldApply(ctx context.Context, id string) (release func(), err error)
}

func NewForwardingLog(inner *SingleWriterLog, transport LeaderTransport,
	target ForwardTarget, opts ...Option) *ForwardingLog
```

Required arguments positional, optional ones as functional options
(`CLAUDE.md §public API style`): `WithForwardTimeout(d)` (the whole
propose+dual-wait budget on the follower — see §liveness for why its default
must be small), `WithHandlerLockTimeout(d)` (the leader-side write-lock
acquisition deadline behind `BUSY`).

`SingleWriterLog` grows two small things: a leader-address accessor (for
`transport.Propose` targeting; it already knows `LeaderWithID`), and typed
classification of its non-`NotLeaderError` failures — today an
`ErrEnqueueTimeout` (definitively **not** proposed) and an
`ErrLeadershipLost` (ambiguous) both come back as anonymous wrapped errors;
the handler and the forward path need to tell "safe to report
definitively-rejected" from "ambiguous," and callers today would benefit from
the same distinction.

Wire format, `internal/raft/proto/forward.proto`:

```protobuf
message ForwardRequest {
  bytes  entry      = 1;  // marshaled Entry, proposed verbatim on acceptance
  uint64 base_index = 2;
}

message ForwardResponse {
  enum Status {
    OK           = 0;
    NOT_LEADER   = 1;  // + leader_addr hint if known
    STALE_BASE   = 2;  // + last_applied diagnostic
    CATCHING_UP  = 3;  // leader elected but still draining (not Ready)
    BUSY         = 4;  // couldn't get the write lock in time
    AMBIGUOUS    = 5;  // proposed, outcome unknown (e.g. leadership lost mid-flight)
  }
  Status status       = 1;
  string leader_addr  = 2;
  uint64 last_applied = 3;
  string detail       = 4;
}
```

`entry` is bytes, not an embedded `Entry`, so the leader proposes the exact
bytes it validated (one decode for validation, zero re-encodes). Statuses
`NOT_LEADER`/`STALE_BASE`/`CATCHING_UP`/`BUSY` are all **pre-propose**: the
entry definitively did not enter the log.

Error mapping on the follower (all through the existing `vfs.GateError`
mechanism, mirroring how `CatchingUpError` already surfaces):
`STALE_BASE`/`CATCHING_UP`/`BUSY` → retryable, tagged `sqlite3.BUSY` — the
app re-runs the transaction and recomputes on fresher state. `NOT_LEADER`
with a usable hint → `ForwardingLog` may re-resolve and re-send **the same
request once** (safe: nothing was proposed, and the base check re-validates
staleness at the new leader) before surfacing a retryable error.
`AMBIGUOUS`/transport-error-after-send → resolve via the marker CAS
(§marker); if it lands on the error side, surface a **non-retryable-as-is**
error: the write may still commit and then materializes locally via
`walappender` — the application must treat it exactly like today's
ambiguous-commit on the leader (re-run only logic that is safe under
at-least-once, or check state first).

The rule underneath that mapping: **the same page blob is never re-proposed
after a possibly-proposed outcome.** Every application-level retry is a fresh
SQL execution → fresh images → fresh base. That is why this design needs no
client-request-ID dedup (narrowing #34's premise: dedup becomes necessary
only if someone later adds blind re-propose of ambiguous outcomes).

Reference transport: an implementation ticket under M9 will add a minimal
TCP/HTTP transport for `cmd/literaft` and `internal/testutils` (the raft
transport itself isn't reusable for custom RPCs); the design deliberately
keeps it out of the core packages.

---

## Read-your-writes

The user-facing guarantee: **when `COMMIT` returns on a follower connection,
every subsequent read on any of that node's connections (and any external
reader of its files) sees the write.** Mechanism: the dual-wait means the
gate doesn't release the commit frame until E is committed cluster-wide
*and* locally recognized; the commit frame then flushes and SQLite publishes
`mxFrame` before `COMMIT` returns — and the publish happens under the same
write lock the txn has held all along, so no other local writer observed any
intermediate state. Other followers see E when their own apply reaches it
(ordinary follower staleness, unchanged); linearizable cross-node reads
remain #35.

---

## Failure matrix

Every path below was walked against the code; "consistent" means: no node
diverges, `mxFrame` never exposes an uncommitted txn, and a later replay
converges.

| # | Failure | Outcome at F | Cluster state | Why it's consistent |
|---|---------|--------------|---------------|---------------------|
| 1 | `STALE_BASE` (concurrent write won) | Retryable error (`BUSY`); SQLite rolls back | E never in log | Nothing proposed; rolled-back frames are inert (no commit frame on disk) |
| 2 | `NOT_LEADER` (stale hint / mid-election) | One safe re-resolve+re-send, then retryable error | E never in log | Response is strictly pre-propose |
| 3 | `CATCHING_UP` / `BUSY` | Retryable error | E never in log | Same |
| 4 | Transport dies **before** send / leader unreachable | Retryable error | E never in log | Nothing left the node |
| 5 | Transport dies **after** send, E did **not** commit | Marker still pending at timeout → abandoned → error | E never in log (or truncated) | Ambiguous surfaced as failure; nothing to materialize |
| 6 | Transport dies after send, E **did** commit; E replicates to F in time | Marker consumed → `Apply` returns nil → **txn succeeds** | E at M | Consumption proves commitment; the lost response is irrelevant |
| 7 | Leader `OK` but E's replication to F outruns the timeout (partition, F snapshotting) | Timeout → abandoned → error; E materializes later via `walappender` (or arrives inside a snapshot) | E at M | Ambiguous-commit contract: error surfaced, write appears later; abandoned frames get overwritten at `mxFrame` |
| 8 | L loses leadership after `raft.Apply` | hraft fails the future (`ErrLeadershipLost`) → `AMBIGUOUS` to F → case 5/6/7 by whether E ultimately commits | Either | Figure-8 retroactive commit lands on F with **no live marker** only if F abandoned — then it materializes normally (ADR-011's exact requirement) |
| 9 | L crashes mid-handler | Transport error → case 5/6/7 | Either | Held OS lock dies with the process; L restarts via replay |
| 10 | F crashes after consumption, before commit-frame flush | Local txn gone; `lastApplied` was in-memory | E at M | Restart replay re-delivers E; no marker → materializes via `walappender` |
| 11 | Commit-frame flush **fails** (no crash) after consumption / after `OK` | **Fatal by design** (#60) | E at M | Running on would mean `lastApplied` > published state, poisoning every future base; restart + replay converges instead |
| 12 | `InstallSnapshot` arrives at F during an in-flight forward | Restore blocks on F's write lock ≤ forward timeout; then case 7 | E inside the snapshot | See §locking's Restore bullet |
| 13 | Two followers forward concurrently | Loser gets `STALE_BASE` | Winner at M | L's write lock serializes; second check sees the advanced `lastApplied` |
| 14 | Forward races a leader-local writer | Whichever takes L's write lock first wins; the other waits, then (forward) fails the base check / (local writer) proceeds on the newly published state | Serial | The lock *is* the tiebreak; local writers never get gate-rejected on behalf of forwards |

---

## Liveness & performance

- **Latency.** A forwarded commit costs one follower→leader RTT plus the
  normal RAFT round-trip plus replication back to F (usually concurrent with
  the response). Leader-local writes are untouched.
- **Throughput.** Unchanged at the cluster level: proposals were already
  serialized (≤1 in flight); forwards enter the same single-file queue at
  L's write lock. Under write contention, follower proposals lose
  systematically (any interleaved write stales them) — **leader-locality is
  still the performance model**; forwarding is for occasional or
  low-contention writes, not for spreading a hot write load. This is the
  accepted cost of skipping OCC (ADR-008's "abort rate → ~100% under
  concurrency" applies to exactly this design, on purpose).
- **The forward timeout is mandatory and must default small** (order
  seconds, not tens). Two independent reasons, both verified: (a) it is the
  only thing breaking the Restore wait-cycle (§locking); (b) while the
  proposer holds F's write lock, F's FSM goroutine can block materializing a
  *competing* entry, hraft's bounded FSM dispatch channel then fills, and
  F's **main raft goroutine** blocks — F stops acking `AppendEntries`.
  If F is quorum-critical, cluster commit progress stalls until the timeout
  fires. A rejected forward therefore delays F's own catch-up (and possibly
  the cluster) by up to the timeout; keep it well under raft's
  election/stall tolerances and make it configurable
  (`WithForwardTimeout`).
- **Leader-side lock holds.** The handler holds L's write lock for the raft
  round-trip — the same duration a leader-local writer holds it — plus E's
  local materialization. Local writers experience forwards as ordinary
  write-lock contention (honoring `busy_timeout`), never as gate rejections.
- **Deployment.** The transport must be wired on every node (any follower
  can originate; any node can become leader). A node without a transport
  simply degrades to today's ADR-007 rejection. Heterogeneous clusters
  (some nodes forwarding-enabled) are safe but confusing — the capability is
  effectively cluster-wide config.

---

## Testing plan

- **Unit, `fsm/`:** the marker CAS — concurrent consume vs abandon from two
  goroutines lands in exactly one terminal state, consumed always publishes,
  abandoned always materializes; `lastApplied` advance points (inside the
  locked section; skip path; `Restore` sets it downward).
- **Unit, `log/` with a fake transport + fake target:** the handler matrix
  (every `ForwardResponse.Status`, lock-timeout → `BUSY`, base mismatch,
  sanity-check rejects); the forward path's response/consumption race (both
  orders), the single `NOT_LEADER` re-resolve, and the
  never-re-propose-after-ambiguous rule.
- **Cluster, `log/` (the existing figure8/leadership harness style):**
  forward accepted while a leader-local write is in flight (loser staled);
  leadership lost between `raft.Apply` and response (case 8, both
  ultimate outcomes — the Figure-8 spec extended to a *forwarded* entry);
  drain-window forwards get `CATCHING_UP`.
- **`internal/testutils` / `integration/`:** extend
  `integration/correctness_test.go`'s trigger-heavy workload so a share of
  writes goes through follower connections via a real (test) transport —
  the byte-identical-to-plain-SQLite bar plus `PRAGMA integrity_check` and
  the external unmodified-VFS reader must hold unchanged; restart and
  InstallSnapshot cases from `restart_test.go`/`snapshot_test.go` re-run
  with an in-flight forward (cases 7, 10, 12); read-your-writes asserted on
  the originating node immediately after `COMMIT`.
- **Fault injection (#24 ties in):** kill the leader mid-handler (case 9);
  fail the commit-frame flush after `OK` and assert the fatal path (#60's
  regression test); drop transport responses while letting replication
  through (case 6).

---

## Deferred follow-ups

- **Page-level OCC** (ADR-008 steps 2–4): unchanged trigger — leader
  SQL-exec CPU proven to be the bottleneck *and* local reads inside
  interactive txns required. This design neither needs it nor precludes it;
  the `ForwardRequest` envelope is where per-page tokens would ride.
- **Client-request-ID dedup** (#34): only if blind re-propose of ambiguous
  outcomes is ever added; the no-blind-retry rule makes it unnecessary here.
- **Auto-retry with recompute** (driver-level re-run of the SQL on
  `STALE_BASE`): impossible below `database/sql` — the driver sees pages,
  not statements. An app-level helper is the most that fits.
- **Linearizable reads** (#35): unrelated mechanism (read-index/lease), but
  note forwarding does *not* provide it — a follower's reads outside its own
  just-committed writes remain stale-able.
