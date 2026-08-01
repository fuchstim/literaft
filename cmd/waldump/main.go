// Command waldump reads a SQLite -wal file and dumps its header and every
// frame's header fields to the terminal. It reads the raw bytes off disk
// using the internal/wal header/frame layout, so it works on any -wal file
// without needing the engine or a live connection.
package main

import (
	"fmt"
	"io"
	"os"

	"github.com/fuchstim/literaft/internal/wal"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "waldump:", err)
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
			fmt.Fprintln(out, "usage: waldump [-v] <wal-file>")
			return nil
		default:
			if path != "" {
				return fmt.Errorf("usage: waldump [-v] <wal-file>")
			}
			path = arg
		}
	}
	if path == "" {
		return fmt.Errorf("usage: waldump [-v] <wal-file>")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if len(data) < wal.WALHeaderSize {
		return fmt.Errorf("%s is %d bytes, shorter than the %d-byte WAL header", path, len(data), wal.WALHeaderSize)
	}

	header := (*wal.WALHeader)(data[:wal.WALHeaderSize])
	pageSize := header.PageSize()
	if pageSize == 0 {
		return fmt.Errorf("%s: page size in header is 0", path)
	}

	// The magic number's low bit picks the byte order used only for the
	// checksum algorithm; every header/frame field itself is always
	// big-endian, which is what WALHeader/FrameHeader assume.
	byteOrder := "little-endian"
	switch header.Magic() {
	case wal.WALHeaderMagicBE:
		byteOrder = "big-endian"
	case wal.WALHeaderMagicLE:
	default:
		fmt.Fprintf(out, "warning: unrecognized magic 0x%08x\n", header.Magic())
	}

	fmt.Fprintf(out, "header: magic=0x%08x (%s checksums) version=%d page_size=%d checkpoint_seq=%d salt1=0x%08x salt2=0x%08x checksum1=0x%08x checksum2=0x%08x\n",
		header.Magic(), byteOrder, header.Version(), pageSize, header.CheckpointSeq(),
		header.Salt1(), header.Salt2(), header.Checksum1(), header.Checksum2())

	frameSize := wal.FrameHeaderSize + int(pageSize)
	offset := wal.WALHeaderSize
	frameNo := 0
	for offset < len(data) {
		remaining := len(data) - offset
		if remaining < frameSize {
			fmt.Fprintf(out, "warning: %d trailing byte(s) after frame %d, short of one full frame (%d bytes); stopping\n",
				remaining, frameNo, frameSize)
			break
		}
		frameNo++

		fh := (*wal.FrameHeader)(data[offset : offset+wal.FrameHeaderSize])
		pageData := data[offset+wal.FrameHeaderSize : offset+frameSize]

		commit := ""
		if fh.IsCommit() {
			commit = fmt.Sprintf(" COMMIT(n_truncate=%d)", fh.NTruncate())
		}
		fmt.Fprintf(out, "frame %d: page=%d salt1=0x%08x salt2=0x%08x checksum1=0x%08x checksum2=0x%08x%s\n",
			frameNo, fh.PgNo(), fh.Salt1(), fh.Salt2(), fh.Checksum1(), fh.Checksum2(), commit)
		if verbose {
			fmt.Fprintf(out, "  %s\n", hexWindow(pageData))
		}

		offset += frameSize
	}

	fmt.Fprintf(out, "%d frame(s)\n", frameNo)
	return nil
}

// hexWindow renders up to the first 16 bytes of a page's data for the
// verbose per-frame dump.
func hexWindow(data []byte) string {
	end := 16
	if end > len(data) {
		end = len(data)
	}
	return fmt.Sprintf("% x", data[:end])
}
