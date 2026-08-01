// Command pagedump reads a single page from a SQLite database file and
// prints it as a hex string. Like dbdiff, it reads the raw page bytes off
// disk rather than opening the database through SQLite, so it works on any
// .db file without needing the engine or a matching schema.
package main

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strconv"
)

// sqliteHeaderSize is the fixed 100-byte database header at the start of
// page 1. The page-size field lives inside it at offset 16.
const sqliteHeaderSize = 100

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "pagedump:", err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	if len(args) == 1 && (args[0] == "-h" || args[0] == "--help") {
		fmt.Fprintln(out, "usage: pagedump <db-file> <page-number>")
		return nil
	}
	if len(args) != 2 {
		return fmt.Errorf("usage: pagedump <db-file> <page-number>")
	}

	path := args[0]
	// Pages are 1-indexed in SQLite.
	pgno, err := strconv.Atoi(args[1])
	if err != nil || pgno < 1 {
		return fmt.Errorf("invalid page number %q: must be a positive integer", args[1])
	}

	db, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	pageSz, err := pageSize(db)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}

	// A trailing partial page means the file is truncated or corrupt; it
	// isn't a whole page, so it's excluded from the valid page count.
	nPages := len(db) / pageSz
	if pgno > nPages {
		return fmt.Errorf("page %d out of range: %s has %d pages", pgno, path, nPages)
	}

	start := (pgno - 1) * pageSz
	page := db[start : start+pageSz]
	fmt.Fprintln(out, hex.EncodeToString(page))
	return nil
}

// pageSize reads the page-size field at header offset 16 (2 bytes,
// big-endian). SQLite encodes a 64 KiB page as the value 1.
func pageSize(db []byte) (int, error) {
	if len(db) < sqliteHeaderSize {
		return 0, fmt.Errorf("file is %d bytes, shorter than the %d-byte SQLite header", len(db), sqliteHeaderSize)
	}
	sz := int(binary.BigEndian.Uint16(db[16:18]))
	if sz == 1 {
		return 65536, nil
	}
	// A valid page size is a power of two between 512 and 32768.
	if sz < 512 || sz > 32768 || sz&(sz-1) != 0 {
		return 0, fmt.Errorf("invalid page size %d in header (not a power of two in [512,65536])", sz)
	}
	return sz, nil
}
