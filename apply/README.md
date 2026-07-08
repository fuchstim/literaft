# apply

Materializes RAFT log entries (see `vfs.Entry`, captured by the M2
commit-frame gate) into a follower's local `-wal` file and wal-index,
without a SQLite connection in the loop at all (`docs/DESIGN.md`
§follower-apply). This is the highest-risk code in the repo: every field
offset and algorithm here has to match what the SQLite-wasm engine itself
would have written, or a reader -- including an external, unmodified SQLite
process -- sees a corrupt or torn database.

## Where the format came from

No upstream Go source exists for the wal-index or WAL frame format: it's
implemented entirely inside the compiled SQLite-wasm blob
(`github.com/ncruces/go-sqlite3`), not in any Go code we could copy. This
package is derived from two sources, in order of trust:

1. **SQLite's actual C source** (`wal.c`, fetched from
   `https://github.com/sqlite/sqlite` at the time of writing) for the
   struct layouts (`WalIndexHdr`, `WalCkptInfo`), the hash-table algorithm
   (`walHash`/`walIndexAppend`/`walFramePage`), and the checksum algorithm
   (`walChecksumBytes`, `walEncodeFrame`, `walIndexWriteHdr`).
2. **This exact build's actual on-disk output**, empirically decoded byte
   by byte and cross-checked against (1) rather than trusted on its own.

That empirical check mattered for one specific, easy-to-get-wrong question:
byte order. The wal-index is native-byte-order, not the WAL file's
mandated big-endian, and the WAL frame checksum's word order depends on a
per-WAL flag (`bigEndCksum`) that's set from whatever the writer's "native"
order was. Since SQLite here runs compiled to wasm, and wasm's linear
memory is little-endian *by spec* regardless of the host CPU, this engine's
native order is always little-endian, deterministically, on every platform
this library runs on -- but "should always be little-endian by this
reasoning" is exactly the kind of claim CLAUDE.md says to verify, not
assume. So before writing any of the checksum/header code, a throwaway test
created a real WAL through this VFS, dumped the raw `-wal`/`-shm` bytes,
and manually recomputed both the WAL header's checksum and the wal-index
header's self-checksum by hand to confirm they matched -- byte for byte --
before relying on that assumption anywhere. `checksum.go`'s doc comment
records the conclusion; the throwaway probe itself was deleted once it had
done its job.

## A gap the tests found: apply/ alone isn't enough to bootstrap a follower

`Applier.bootstrap` writes a fresh WAL header the moment the wal-index looks
uninitialized, but that turned out not to be sufficient by itself: a brand
new, 0-byte main `.db` file with only a sibling `-wal`/`-shm` next to it is
**not** recognized as a WAL-mode database by a plain `sqlite3.Open`. SQLite
determines journal mode from bytes 18-19 of page 1 *in the main database
file*; if that file is empty, there's nothing to read the mode from, and
SQLite never even looks for a `-wal` sibling. Confirmed empirically (a
throwaway repro that applied entries into a fully fresh file and then
queried it got "no such table", `PRAGMA journal_mode` reported `delete`, and
`PRAGMA page_count` reported 0 -- despite the `-wal` file containing
perfectly valid, correctly-checksummed frames).

Real leaders don't hit this because `PRAGMA journal_mode=WAL` on a schema-less
database writes page 1 directly into the main file as part of turning WAL
mode on (verified in `vfs`'s own format-derivation probe) -- there's no WAL
yet to write it into. So the fix isn't in `apply/` at all: it's that the
CLAUDE.md invariant "keep >=1 RW connection open per node process" is doing
more work than "keep the shm live for external readers" suggests. That
connection running `PRAGMA journal_mode=WAL` once, before `apply.Apply` is
ever called, is what gives the main file its WAL-mode marker page 1 -- after
that, `apply/`'s WAL-only frames correctly take precedence for any reader,
including a fresh connection that never ran the pragma itself. `apply_test.go`
does this with a `keeper` connection before applying anything, matching what
a real follower node's startup path needs to do.

## What's out of scope here

- **Recovering a wal-index from an existing, non-empty `-wal` file.**
  `Applier.bootstrap` only handles the fully-fresh case (no `-wal` content
  at all) and refuses otherwise. Rebuilding the wal-index by replaying an
  existing WAL is real SQLite recovery logic and is deferred to hardening
  (`docs/ROADMAP.md` M6, "rebuild WAL tail from the log via lastApplied").
- **Checkpointing.** Follower checkpoint driving is separate
  (`docs/DESIGN.md` §checkpoint) and untouched by this package.
- **The `WalCkptInfo` block** (`nBackfill`, read-marks, byte offset 96-135
  of the wal-index header page): apply/ never reads or writes it. Those
  fields are only ever established by a real SQLite connection (the "keep
  >=1 RW connection open" invariant), and apply/ only ever advances
  `mxFrame` in the `WalIndexHdr` copies -- it never needs to touch
  checkpoint state to append frames.
