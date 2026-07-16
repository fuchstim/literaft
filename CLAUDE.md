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
compatibility, commit-frame gate, shm + follower apply, real
`hashicorp/raft` integration (`internal/raft/gate/`, `internal/raft/proto/`,
`fsm/`, `cmd/literaft/`), the leadership-churn ordering work, and real
snapshot take/install — a multi-node cluster replicates writes, followers
serve reads, killing/adding nodes converges, a node that regains leadership
with an apply backlog drains it (`log.SingleWriterLog`'s `Ready`/`Barrier`
drain) before serving local writes again, and a follower too far behind for
normal log replication catches up via `fsm.FSM.Snapshot`/`Restore` (delegating
to `internal/fsm/snapshotter.Snapshotter`, which appends the incoming snapshot
as ordinary WAL frames rather than swapping the whole file) instead. Current
work is **Milestone 7** (hardening: fault injection, fuzzing, and the
remaining flakes — see `docs/ROADMAP.md`).

**Scope decision:** follower-originated writes are **rejected by default**
(ADR-007) — a client write on a follower returns an error with a leader hint
and the client redirects — *unless* the node is wired with the **opt-in M9
forwarding** machinery (`log.ForwardingLog` + a `log.LeaderTransport`), now
**built** (ADR-015, `docs/FOLLOWER_WRITES.md`, issue #32): a follower's
captured page images are forwarded to the leader under a whole-db base-index
check (no OCC), accepted iff computed on exactly the leader's applied state.
`cmd/literaft` enables it by default (`-forward-writes`). The heavier
byte-identical correctness check through follower connections and the
failure-matrix cluster tests (leadership churn mid-forward, `InstallSnapshot`
during forwarding) are now in place; the earlier intermittent divergence
(issue #64) was root-caused to a follower read-modify-write silently
no-opping against a stale local snapshot — a frame-less statement never
reaches the gate, so the base-index check never runs (a manifestation of
stale-able follower reads, not a forwarding/apply fault; see
`docs/FOLLOWER_WRITES.md §read-your-writes`). The OCC apparatus (read-set
capture, per-page version pagemap) remains fully deferred — see
`docs/DECISIONS.md` ADR-007/008. Do not build it.

Note this still leaves **follower apply** in scope: followers must materialize
committed RAFT entries into their local `-wal` + wal-index to stay current and
electable. That is the path that needs the custom shm implementation (below).

---

## GitHub workflow

GitHub issues and milestones are the source of truth for outstanding work.
`docs/ROADMAP.md` is a derived mirror kept only for easier AI consumption in
this file's context — update it *from* GitHub, not the other way around.

**GitHub ↔ roadmap mapping**

- Each GH milestone maps to a roadmap section (`## M# — ...`) of the same
  name; each GH issue under it maps to a bullet in that section (bullet text
  = issue body). Every roadmap-tracked issue lives in project 3
  (https://github.com/users/fuchstim/projects/3, repo `fuchstim/literaft`).
  The "Deferred (out of current scope)" milestone maps to the roadmap's
  "Deferred" section.
- Status: the project's `Status` field is `Done` for completed items (issue
  also closed) or `Backlog` otherwise — roadmap-derived issues only ever use
  those two values. A milestone is closed once every issue under it is done.
- **When GitHub issues/milestones change, update `docs/ROADMAP.md` in the
  same pass:** a new issue becomes a new bullet, an edited issue body becomes
  an edited bullet, an issue closed/marked `Done` gets reflected in the
  bullet, a new milestone becomes a new `## M# — ...` section. Don't let the
  two drift.
- IDs for scripting this via `gh api`/`gh project`: project id
  `PVT_kwHOATlZK84Bc32q`, `Status` field id `PVTSSF_lAHOATlZK84Bc32qzhXdftE`,
  options `Backlog`=`f75ad846`, `Done`=`98236657`. `gh project item-edit`
  passes single-select option IDs via `-f` (not `-F`) — `-F` auto-coerces a
  numeric-looking ID into a JSON number and the mutation rejects it.

**Implementing a ticket**

- Check out a new branch before starting work on an issue — never commit
  straight to `master`. Branch names are prefixed with the local user's handle or GitHub username
  (e.g. `tfuchs/m7-fault-injection`).
- Open a PR against `master` that links back to the issue (e.g. `Closes #<n>`
  in the PR body) so merging it auto-closes the issue.
- Exception: if the user explicitly asks in the current turn to commit/push
  straight to `master` (not just approves a normal git action in passing),
  that instruction overrides the branch+PR rule above.

---

## Repo layout

```
/CLAUDE.md                        – this file
/docs/                            – design, decisions, roadmap, format & library notes
/go.mod
/fsm/                             – owns a node's SQLite connection, walappender,
    fsm.go                          snapshotter, and the external-reader-safety
    options.go                      db lock; implements hraft.FSM directly
    dblock.go, lock_{linux,darwin}.go
/internal/
    vfs/                          – the RAFT VFS: wraps ncruces' default VFS + File
        vfs.go, file.go             (Open tags files, xWrite intercepts the WAL
        frame.go, gate.go            commit frame; walframe.go parses/builds
        walframe.go                 WAL frame headers, on-disk format)
    fsm/
        walappender/              – follower-apply: RAFT entry -> local -wal + wal-index
            walappender.go, walindex.go, checksum.go, frame.go
            shm/                  – custom mmap+lock implementation of the SQLite
                                     wal-index shared memory (so follower-apply
                                     can drive mxFrame / page-map / write lock)
        snapshotter/              – RAFT snapshot capture/restore via SQLite's
                                     online backup API
    raft/
        gate/                     – thin, hraft-agnostic commit-frame gate: builds a
                                     RAFT entry and proposes it through a
                                     raftgate.LogAdapter
            errors/               – rafterrors: the single rejected-proposal
                                     taxonomy (Redirect/Retryable/Ambiguous +
                                     the sqlite result code each maps to);
                                     LogAdapters translate into it
        proto/                    – RAFT log entry wire format (encode/decode)
    membership/                   – cluster join/leave control plane: a gRPC
                                     service (membership.go + proto/) sharing the
                                     node's gRPC server with the raft transport;
                                     AddVoter/RemoveVoter forward to the leader,
                                     so a joining node needs only any member's
                                     address. Used by cmd/literaft's -join and
                                     -leave. Membership is durable: shutdown
                                     never removes a node (restart rejoins on its
                                     own, re-announcing via a peer if its address
                                     changed); -leave is the explicit decommission.
    testutils/                    – test-only cluster harnesses (in-memory and
                                     real TCP+raftsqlite), used by _test.go files
                                     across the module
/log/                             – log.SingleWriterLog: the real hraft-backed
                                     raftgate.LogAdapter, owning *hraft.Raft and
                                     leader/ready/drain state; translates
                                     rejections into the rafterrors taxonomy
/driver/                          – database/sql-compatible driver: wires a
                                     caller-supplied fsm.FSM + raftgate.LogAdapter
                                     into a gate + registered VFS + database/sql.Driver
/raftsqlite/                      – raft.LogStore/raft.StableStore backed by
                                     SQLite (replaces raft-boltdb, which fsyncs
                                     on every write)
/cmd/literaft/                    – node process entrypoint (flag parsing,
                                     lifecycle) + an interactive SQL REPL. On a
                                     TTY it defaults to a bubbletea split-pane
                                     TUI (tui.go: REPL pane + live log-stream
                                     pane fed by logsink.go); -tui=false or a
                                     non-TTY falls back to the plain line REPL
                                     (repl.go)
/integration/                     – whole-system tests too slow for the unit
                                     suites they exercise: a throughput
                                     benchmark and a replication-fidelity
                                     correctness test, both driven against a
                                     real testutils.TCPCluster
```

---

## Build / test

Standard Go. The engine is pure Go (wazero), no cgo.

```
go build ./...
```

**Protobuf / gRPC codegen lives in the `Makefile`.** The `.pb.go` and
`_grpc.pb.go` files under `internal/**/proto/` are generated from the sibling
`.proto` with `buf` (each proto package carries a `//go:generate buf generate`
and its own `buf.gen.yaml`) — don't hand-edit them. Regenerate everything with
`make generate`, the single codegen entry point: it installs the pinned
toolchain (`buf`, `protoc-gen-go`, and `protoc-gen-go-grpc` — the last is
required for the gRPC *service* stubs, e.g. `internal/membership`) and then
runs `go generate ./...` with that toolchain on `PATH`. Bump a tool by editing
its version variable in the `Makefile`. A `buf.gen.yaml` that emits gRPC stubs
must list the `protoc-gen-go-grpc` plugin in addition to `protoc-gen-go`.

Run tests via the `ginkgo` CLI, not `go test ./...` directly — this repo's
suites are Ginkgo/Gomega. `-p` is a boolean ("run in parallel with an
auto-detected number of nodes"), not a process-count flag — a number after
it (e.g. `-p 20`) is silently ignored as a harmless nonexistent extra
package pattern, not consumed as a count. Use `--procs=N` (alias `--nodes`)
for an explicit count, or bare `-p` for auto-detected parallelism (much
faster); omit both (or pass `--procs=1`) when actively debugging a specific
failure, since parallel output interleaves and timing-sensitive tests
behave differently under concurrent load.

```
ginkgo -r --procs=20 ./...
```

To check whether a test is flaky, use `--repeat N` (runs the suite N+1
times, must pass every time) rather than `--until-it-fails` wrapped in a
shell timeout — `--repeat` has a real, deterministic stopping point instead
of needing an external time bound:

```
ginkgo --repeat 10 ./path/to/package/...
```

Platform: develop on Linux (amd64/arm64) or macOS — those have full file-lock +
shm support in ncruces. Linux uses **OFD locks**; macOS too. Do **not** rely on
the `sqlite3_dotlk` build tag for anything user-facing: it makes shm in-process
only, which **breaks requirement #3** (external readers). Default build tags
only.

Recommended PRAGMAs on every connection: `journal_mode=WAL`,
`synchronous=NORMAL` (durability comes from the RAFT quorum, not local fsync —
see `docs/DESIGN.md §durability`).

**Reading Go package source.** When you need to read the source of a
dependency (e.g. `github.com/ncruces/go-sqlite3`), read it from the local
`./vendor/` directory, not the system-wide Go module cache
(`$GOPATH/pkg/mod`). `./vendor/` is a plain `go mod vendor` mirror of the full
dependency tree kept around locally for exactly this purpose — refresh it
with `go mod vendor` if it looks stale rather than falling back to the module
cache.

---

## Public API style

Every public constructor (`New`, `Open`, `Start`, ...) on a public interface
takes its required arguments directly as positional parameters — never
bundled into a config/options struct alongside optional ones. Optional
arguments use the **functional options pattern** instead: an unexported
`options` struct carrying the defaults, `type Option func(*options)`,
exported `WithXxx(...) Option` constructors, and a trailing `...Option`
parameter on the real constructor. Rationale: a struct field can't
distinguish "caller explicitly set the zero value" from "caller didn't set
it at all," and a variadic `...Option` list can grow new options without
breaking existing call sites, unlike adding a field to a struct that already
has callers relying on its zero value. `log/` (`log/options.go`,
`log.NewSingleWriterLog`) is the reference example. Older config-struct-based
constructors (e.g. `internal/node.Config`) predate this convention and
haven't been migrated.

---

## Code comments

Keep comments brief, factual, and technical: state what's non-obvious, not a
mini-essay. No fluff, no filler, no em-dashes, no narrative framing. Don't
reference other functions, files, issues, or PRs by name; such a pointer
goes stale the moment the referenced thing is renamed or moved, and the
comment becomes wrong. Say what's true right here.

---

## Documentation style

Everything in `docs/` is a neutral technical reference: factual, technical,
and informational. No fluff, no filler, no em-dashes, no narrative framing
(no dramatized section titles, rhetorical questions, storytelling asides, or
emphasis-for-drama). Use plain declarative statements, and prefer commas,
colons, semicolons, or parentheses over dashes. Preserve technical precision:
keep exact file paths, package and symbol names, cross-references, tables,
and code blocks intact. Numeric-range en-dashes (e.g. "bytes 4–7") are
acceptable.

---

## Top gotchas (bite-marks from the design discussion)

- **`SharedMemory` is opaque.** The exported interface has unexported methods;
  you cannot drive it. Follower-apply needs its own implementation of the
  concrete shm impl. See `docs/NCRUCES_NOTES.md`.
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
- **The self-apply skip must stay transient, never permanent.** `fsm.FSM.Apply`
  (`fsm/fsm.go`) skips materializing an entry only while its `Header.Id` is
  present in `f.skipEntries` — a marker `Gate.proposeTransaction` sets before
  its own `LogAdapter.Apply` call (`raft.Apply`, for a real cluster) and
  clears (deferred) right after, scoped to that one in-flight proposal. A
  static, permanent check (e.g. keying off a node ID instead of a
  per-proposal token, as an earlier version of this code did) breaks replay
  in at least two ways: (1) hraft's Figure-8 rule retroactively committing a
  self-authored entry from an earlier, unfinished leadership stint
  (`log/figure8_test.go`), and (2) `FSM.Restore`
  resetting local state back to an older snapshot on startup, after which
  every self-authored entry past that snapshot needs to replay normally,
  since the restore just made it genuinely missing again
  (`internal/testutils/restart_test.go`'s "recovers a leader restarted after
  it has taken a local snapshot"). Both would silently and permanently
  diverge that node's local disk from the cluster. Previously tracked as
  [issue #41](https://github.com/fuchstim/literaft/issues/41).
