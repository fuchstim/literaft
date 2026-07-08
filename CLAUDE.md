# CLAUDE.md

Context for a RAFT-backed SQLite VFS, implemented in Go on top of
`github.com/ncruces/go-sqlite3`. This file is the entry point; deeper material
lives in `docs/`. Read `docs/DESIGN.md` before touching the write or
follower-apply paths.

---

## What we're building

A custom SQLite VFS that turns a single-node SQLite database into a
RAFT-replicated one, **without patching SQLite and without giving up
on-disk file compatibility**. The three hard requirements:

1. **WAL mode.** The database runs in WAL mode; the gate for replication is the
   WAL commit frame (see below).
2. **Multiple read-write connections from the same process** with the same
   concurrency as stock SQLite in WAL mode (single writer, concurrent readers).
   The only added cost is that each commit blocks for one RAFT round-trip.
3. **Read-only connections from other processes.** The `.db`, `-wal`, and
   `-shm` files stay byte-for-byte SQLite-compatible so an *unmodified* SQLite
   process can open the database read-only.

RAFT itself is an existing library (see `docs/DECISIONS.md` for the interface we
assume). This project is about the VFS, not the consensus algorithm.

---

## The one idea everything hangs on

In WAL mode a transaction becomes **visible** when the wal-index header's
`mxFrame` is advanced — not when frames land in the `-wal` file. SQLite always
writes frames past `mxFrame` (invisible) and only then publishes by bumping
`mxFrame` in shared memory. There is no VFS callback at that exact publish
instant.

So we gate **one step earlier**: intercept the write of the transaction's
**commit frame** in the WAL, hold it while RAFT reaches quorum, and only let
SQLite proceed to publish if RAFT commits. The commit frame is identifiable from
the on-disk format alone (24-byte frame header, bytes 4–7 = "db size after
commit" — non-zero **only** on a commit frame). No SQLite internals required.

Two consequences make this safe:

- Readers (local and external) never see an un-replicated txn, because
  `mxFrame` doesn't move until after quorum.
- A rejected txn is not crash-recoverable, because its **commit frame never
  reaches disk** unless RAFT committed; WAL recovery replays only up to the last
  valid commit frame.

Structural fit worth remembering: WAL is single-writer, RAFT is single-leader.
The WAL write lock is the per-node serializer; RAFT is the cross-node one.

---

## Invariants (do not break these)

- **Writes are leader-only.** A write attempted on a follower fails the gate.
  There is *no* page-level conflict resolution anywhere in the system —
  conflicts are *prevented* by RAFT's total order, not reconciled. If you find
  yourself writing merge logic, stop and re-read `docs/DESIGN.md §conflicts`.
- **RAFT log entries are physical redo:** an ordered list of `(pgno, page_image)`
  plus `nTruncate` (post-commit db size / commit marker). Not raw WAL bytes
  (salts + checksums are per-node), not SQL statements (non-determinism).
- **Apply is strictly in-order and gapless.** A full page image is only valid
  against the exact base state it was computed on. Never reorder, skip, or
  parallelize apply — even across entries touching disjoint pages. Total
  ordering is a *correctness precondition* here, not just a consistency nicety.
- **Fixed cluster-wide page size.** Enforce it at open.
- **The leader publishes via SQLite itself.** On the leader we never poke the
  shm on the commit path — we just release the withheld commit frame and let
  SQLite run its normal `walIndexWriteHdr`. We only manipulate the shm directly
  on the **follower-apply** path.
- **Keep ≥1 RW connection open per node process** so the `-shm` wal-index stays
  live; otherwise a lone external reader hits the read-only-shm fallback.

---

## Current status & milestone

**M0–M6 done** (see `docs/ROADMAP.md`): wrapper VFS, external-reader
compatibility, commit-frame gate, vendored shm + follower apply, real
`hashicorp/raft` integration (`raft/`, `internal/node/`, `cmd/literaft/`), the
leadership-churn ordering work, and real snapshot take/install — a multi-node
cluster replicates writes, followers serve reads, killing/adding nodes
converges, a node that regains leadership with an apply backlog drains it
(`raft.Gate`'s `Ready`/`Barrier` drain) before serving local writes again, and
a follower too far behind for normal log replication catches up via
`raft.FSM.Snapshot`/`Restore` (a `TRUNCATE`-checkpointed `.db` swapped in as a
unit by `internal/node`'s `dbBackend`) instead. Current work is **Milestone 7**
(hardening: crash/restart recovery, fault injection, fuzzing, throughput
benchmarks — see `docs/ROADMAP.md`).

**Scope decision for now:** *reject all follower-originated writes.* A client
write that lands on a follower returns an error with a leader hint; the client
redirects. Forwarding follower-computed writes to the leader (and the OCC
machinery that would make it safe) is **explicitly deferred** — see
`docs/DECISIONS.md` ADR-007/008 and `docs/ROADMAP.md`. Do not build read-set
capture or the OCC pagemap yet.

Note this still leaves **follower apply** in scope: followers must materialize
committed RAFT entries into their local `-wal` + wal-index to stay current and
electable. That is the path that needs the copied SHM code (below).

---

## Repo layout (proposed)

```
/CLAUDE.md                 – this file
/docs/                     – design, decisions, roadmap, format & library notes
/go.mod
/vfs/                      – the RAFT VFS: wraps ncruces' default VFS + File
    vfs.go                 – VFS wrapper (Open tags files, delegates)
    file.go                – File wrapper; xWrite commit-frame interception
    walframe.go            – WAL frame header parse/build (on-disk format)
/shm/                      – VENDORED copy of ncruces' shm implementation
    (copied so follower-apply can drive mxFrame / page-map / write lock)
/apply/                    – follower-apply: RAFT entry -> local -wal + wal-index
/raft/                     – thin adapter over the chosen RAFT library
/cmd/ or /internal/        – node process wiring, keeps a RW conn alive
```

Adjust as the code takes shape; keep `shm/` clearly marked as vendored + its
upstream commit hash recorded (see `docs/NCRUCES_NOTES.md`).

---

## Build / test

Standard Go. The engine is pure Go (wazero), no cgo.

```
go build ./...
go test ./...
```

Platform: develop on Linux (amd64/arm64) or macOS — those have full file-lock +
shm support in ncruces. Linux uses **OFD locks**; macOS too. Do **not** rely on
the `sqlite3_dotlk` build tag for anything user-facing: it makes shm in-process
only, which **breaks requirement #3** (external readers). Default build tags
only.

Recommended PRAGMAs on every connection: `journal_mode=WAL`,
`synchronous=NORMAL` (durability comes from the RAFT quorum, not local fsync —
see `docs/DESIGN.md §durability`).

---

## Top gotchas (bite-marks from the design discussion)

- **`SharedMemory` is opaque.** The exported interface has unexported methods;
  you cannot drive it. Follower-apply needs a **vendored copy** of the concrete
  shm impl. See `docs/NCRUCES_NOTES.md`.
- **wal-index page-map ≠ our future OCC pagemap.** "Update the shm page map"
  during apply means the *wal-index's* pgno→frame hash slots (SQLite format,
  required so readers find frames). The OCC pgno→hash/version map from the
  forwarding discussion is a *different*, deferred structure. Don't conflate.
- **Header publish must be tear-safe.** When apply pokes the wal-index, replicate
  SQLite's two-copy header + barrier protocol exactly, or concurrent readers
  (incl. external processes) will see torn state. Highest-risk code in the repo.
- **External-reader compatibility is a claim to verify, not assume.** ncruces
  (OFD locks) vs stock SQLite (POSIX `F_SETLK`) interop needs an actual test
  early — it's requirement #3. See `docs/ROADMAP.md` M1.
- **Ambiguous commit.** RAFT "proposed, outcome unknown" must be treated as
  failure by the gate, which means a txn can fail locally yet commit
  cluster-wide. Client-request-ID dedup in the apply path is how you avoid
  double-apply on retry. (Deferred with forwarding, but keep the hook in mind.)
