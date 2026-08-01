// Command raftlogdump decodes a raftsqlite log store and dumps every stored
// raft.Log entry to the terminal. LogCommand entries are additionally
// unmarshaled as raftproto.LogEntry, the physical-redo transaction format,
// and printed page by page. It's a debugging aid for inspecting a node's
// persisted raft log offline.
package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"

	raftproto "github.com/fuchstim/literaft/raft/proto"
	"github.com/fuchstim/literaft/raftsqlite"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "raftlogdump:", err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	verbose := false
	var path string
	for _, arg := range args {
		switch arg {
		case "-v", "--verbose":
			verbose = true
		case "-h", "--help":
			fmt.Fprintln(out, "usage: raftlogdump [-v] <raftsqlite-db-path>")
			return nil
		default:
			if path != "" {
				return fmt.Errorf("usage: raftlogdump [-v] <raftsqlite-db-path>")
			}
			path = arg
		}
	}
	if path == "" {
		return fmt.Errorf("usage: raftlogdump [-v] <raftsqlite-db-path>")
	}

	store, err := raftsqlite.New(path)
	if err != nil {
		return fmt.Errorf("failed to open raft store at %s: %w", path, err)
	}
	defer store.Close()

	first, err := store.FirstIndex()
	if err != nil {
		return fmt.Errorf("failed to read first index: %w", err)
	}
	last, err := store.LastIndex()
	if err != nil {
		return fmt.Errorf("failed to read last index: %w", err)
	}
	if last == 0 {
		fmt.Fprintln(out, "log store is empty")
		return nil
	}

	for idx := first; idx <= last; idx++ {
		var log raft.Log
		if err := store.GetLog(idx, &log); err != nil {
			if errors.Is(err, raft.ErrLogNotFound) {
				fmt.Fprintf(out, "index %d: not found (compacted)\n", idx)
				continue
			}
			return fmt.Errorf("failed to read log at index %d: %w", idx, err)
		}
		printLog(out, &log, verbose)
	}
	return nil
}

// printLog prints one raft.Log's envelope fields, then, for a LogCommand
// entry, its decoded raftproto.LogEntry payload. Other log types (noop,
// barrier, configuration) carry no application payload, so nothing further
// is decoded for them.
func printLog(out io.Writer, log *raft.Log, verbose bool) {
	fmt.Fprintf(out, "index %d: term=%d type=%s appended_at=%s\n",
		log.Index, log.Term, log.Type, log.AppendedAt.Format(time.RFC3339Nano))

	if log.Type != raft.LogCommand {
		return
	}

	entry := &raftproto.LogEntry{}
	if err := proto.Unmarshal(log.Data, entry); err != nil {
		fmt.Fprintf(out, "  error: failed to unmarshal LogEntry: %v\n", err)
		return
	}

	fmt.Fprintf(out, "  id=%s\n", entry.GetHeader().GetId())

	txn := entry.GetTransaction()
	if txn == nil {
		fmt.Fprintln(out, "  (no transaction payload)")
		return
	}

	fmt.Fprintf(out, "  transaction: %d page(s), n_truncate=%d\n", len(txn.GetPages()), txn.GetNTruncate())
	for _, pg := range txn.GetPages() {
		fmt.Fprintf(out, "    page %d: %d bytes\n", pg.GetPgNo(), len(pg.GetData()))
		if verbose {
			fmt.Fprintf(out, "      %s\n", hexWindow(pg.GetData()))
		}
	}
}

// hexWindow renders up to the first 16 bytes of a page's data for the
// verbose per-page dump.
func hexWindow(data []byte) string {
	end := 16
	if end > len(data) {
		end = len(data)
	}
	return fmt.Sprintf("% x", data[:end])
}
