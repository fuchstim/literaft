// Command dbdiff compares two SQLite database files page by page and prints
// which pages differ. It reads raw page images off disk rather than opening
// the databases through SQLite, so it works on any .db file (leader vs
// follower, pre- vs post-apply) without needing the engine or a matching
// schema. It's a debugging aid for the physical-redo replication path, where
// two nodes' main db files are expected to converge byte-for-byte.
package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// sqliteHeaderSize is the fixed 100-byte database header at the start of
// page 1. The page-size field lives inside it at offset 16.
const sqliteHeaderSize = 100

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "dbdiff:", err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	verbose := false
	var paths []string
	for _, arg := range args {
		switch arg {
		case "-v", "--verbose":
			verbose = true
		case "-h", "--help":
			fmt.Fprintln(out, "usage: dbdiff [-v] <db-a> <db-b>")
			return nil
		default:
			paths = append(paths, arg)
		}
	}
	if len(paths) != 2 {
		return fmt.Errorf("usage: dbdiff [-v] <db-a> <db-b>")
	}

	a, err := os.ReadFile(paths[0])
	if err != nil {
		return err
	}
	b, err := os.ReadFile(paths[1])
	if err != nil {
		return err
	}

	pageSizeA, err := pageSize(a)
	if err != nil {
		return fmt.Errorf("%s: %w", paths[0], err)
	}
	pageSizeB, err := pageSize(b)
	if err != nil {
		return fmt.Errorf("%s: %w", paths[1], err)
	}
	if pageSizeA != pageSizeB {
		return fmt.Errorf("page sizes differ: %s is %d bytes, %s is %d bytes",
			paths[0], pageSizeA, paths[1], pageSizeB)
	}
	pageSz := int(pageSizeA)

	// A well-formed SQLite file is a whole number of pages; a trailing
	// partial page means the file is truncated or corrupt. Report it but
	// still diff the whole pages that are present.
	nPagesA, remA := len(a)/pageSz, len(a)%pageSz
	nPagesB, remB := len(b)/pageSz, len(b)%pageSz
	if remA != 0 {
		fmt.Fprintf(out, "warning: %s is not a whole number of %d-byte pages (%d trailing bytes)\n", paths[0], pageSz, remA)
	}
	if remB != 0 {
		fmt.Fprintf(out, "warning: %s is not a whole number of %d-byte pages (%d trailing bytes)\n", paths[1], pageSz, remB)
	}

	fmt.Fprintf(out, "page size %d; %s has %d pages, %s has %d pages\n",
		pageSz, paths[0], nPagesA, paths[1], nPagesB)

	maxPages := nPagesA
	if nPagesB > maxPages {
		maxPages = nPagesB
	}

	differing := 0
	for i := 0; i < maxPages; i++ {
		// Pages are 1-indexed in SQLite; report them that way.
		pgno := i + 1
		switch {
		case i >= nPagesA:
			differing++
			fmt.Fprintf(out, "page %d: only in %s\n", pgno, paths[1])
		case i >= nPagesB:
			differing++
			fmt.Fprintf(out, "page %d: only in %s\n", pgno, paths[0])
		default:
			pa := a[i*pageSz : (i+1)*pageSz]
			pb := b[i*pageSz : (i+1)*pageSz]
			first, count := diffBytes(pa, pb)
			if count == 0 {
				continue
			}
			differing++
			fmt.Fprintf(out, "page %d: %d/%d bytes differ, first at offset %d\n", pgno, count, pageSz, first)
			if verbose {
				fmt.Fprintf(out, "  a: %s\n", hexWindow(pa, first))
				fmt.Fprintf(out, "  b: %s\n", hexWindow(pb, first))
			}
		}
	}

	if differing == 0 {
		fmt.Fprintln(out, "databases are page-for-page identical")
	} else {
		fmt.Fprintf(out, "%d page(s) differ\n", differing)
	}
	return nil
}

// pageSize reads the page-size field at header offset 16 (2 bytes,
// big-endian). SQLite encodes a 64 KiB page as the value 1.
func pageSize(db []byte) (uint32, error) {
	if len(db) < sqliteHeaderSize {
		return 0, fmt.Errorf("file is %d bytes, shorter than the %d-byte SQLite header", len(db), sqliteHeaderSize)
	}
	sz := uint32(binary.BigEndian.Uint16(db[16:18]))
	if sz == 1 {
		return 65536, nil
	}
	// A valid page size is a power of two between 512 and 32768.
	if sz < 512 || sz > 32768 || sz&(sz-1) != 0 {
		return 0, fmt.Errorf("invalid page size %d in header (not a power of two in [512,65536])", sz)
	}
	return sz, nil
}

// diffBytes returns the offset of the first differing byte and the total
// number of differing bytes between two equal-length pages.
func diffBytes(a, b []byte) (first, count int) {
	first = -1
	for i := range a {
		if a[i] != b[i] {
			if first < 0 {
				first = i
			}
			count++
		}
	}
	return first, count
}

// hexWindow renders up to 16 bytes of a page starting at offset, for the
// verbose per-page dump. It clamps to the page bounds.
func hexWindow(page []byte, offset int) string {
	end := offset + 16
	if end > len(page) {
		end = len(page)
	}
	return fmt.Sprintf("@%d % x", offset, page[offset:end])
}
