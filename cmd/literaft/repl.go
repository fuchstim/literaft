package main

import (
	"bufio"
	"fmt"
	"io"
	"strings"

	"github.com/ncruces/go-sqlite3"

	"github.com/fuchstim/literaft/internal/node"
)

// runREPL runs an interactive SQL loop against n's kept-alive connection,
// reading from in and writing prompts/results/errors to out, until in
// reaches EOF or the user enters .exit/.quit. Every statement runs through
// node.Node.WithDB, so it goes through the exact same commit-frame gate
// (docs/DESIGN.md) as any other client write: on a follower, or a leader
// still draining its apply backlog, that write fails and the error is
// printed rather than crashing the loop.
//
// A line is treated as a complete statement once its trimmed text ends in
// ";" -- a deliberately simple heuristic (it doesn't parse strings or
// comments, so a literal containing ";-- not a comment" would mis-trigger)
// that's good enough for an ops/debug tool; anything fancier belongs in a
// real SQL tokenizer, not here.
func runREPL(n *node.Node, in io.Reader, out io.Writer) {
	scanner := bufio.NewScanner(in)

	var buf strings.Builder
	fmt.Fprint(out, "literaft> ")
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		if buf.Len() == 0 {
			switch trimmed {
			case "":
				fmt.Fprint(out, "literaft> ")
				continue
			case ".exit", ".quit":
				return
			}
		}

		buf.WriteString(line)
		buf.WriteByte('\n')

		if !strings.HasSuffix(trimmed, ";") {
			fmt.Fprint(out, "   ...> ")
			continue
		}

		runStatements(n, buf.String(), out)
		buf.Reset()
		fmt.Fprint(out, "literaft> ")
	}
	if buf.Len() > 0 {
		fmt.Fprintln(out, "error: unexpected EOF mid-statement")
	}
}

// runStatements runs every statement in sql in turn (one input can hold
// more than one, separated by ";") against n's kept-alive connection,
// printing each one's results as it completes and stopping at the first
// error.
func runStatements(n *node.Node, sql string, out io.Writer) {
	err := n.WithDB(func(conn *sqlite3.Conn) error {
		for sql != "" {
			stmt, tail, err := conn.Prepare(sql)
			if err != nil {
				return err
			}
			if stmt == nil {
				return nil // sql was only whitespace/a comment
			}
			sql = tail

			readOnly := stmt.ReadOnly()
			runErr := runStmt(stmt, out)
			closeErr := stmt.Close()
			if runErr != nil {
				if !readOnly {
					// A failed write's real cause (not the leader, or still
					// draining) is more useful than the generic IOERR_WRITE
					// that's all that reliably survives back through *Stmt.
					if writeErr := n.LastWriteError(); writeErr != nil {
						return writeErr
					}
				}
				return runErr
			}
			if closeErr != nil {
				return closeErr
			}
		}
		return nil
	})
	if err != nil {
		fmt.Fprintln(out, "error:", err)
	}
}

// runStmt steps stmt to completion, printing a tab-separated header and one
// line per result row for a query, or "OK" for a statement with no result
// columns.
func runStmt(stmt *sqlite3.Stmt, out io.Writer) error {
	cols := stmt.ColumnCount()
	if cols > 0 {
		names := make([]string, cols)
		for i := range names {
			names[i] = stmt.ColumnName(i)
		}
		fmt.Fprintln(out, strings.Join(names, "\t"))
	}

	for stmt.Step() {
		row := make([]string, cols)
		for i := range row {
			row[i] = columnText(stmt, i)
		}
		fmt.Fprintln(out, strings.Join(row, "\t"))
	}
	if err := stmt.Err(); err != nil {
		return err
	}

	if cols == 0 {
		fmt.Fprintln(out, "OK")
	}
	return nil
}

// columnText renders one column of the statement's current row as text,
// special-casing NULL and BLOB (whose raw bytes ColumnText would otherwise
// mangle as text).
func columnText(stmt *sqlite3.Stmt, col int) string {
	switch stmt.ColumnType(col) {
	case sqlite3.NULL:
		return "NULL"
	case sqlite3.BLOB:
		return fmt.Sprintf("<blob %d bytes>", len(stmt.ColumnRawBlob(col)))
	default:
		return stmt.ColumnText(col)
	}
}
