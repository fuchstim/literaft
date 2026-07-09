package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	"github.com/ncruces/go-sqlite3"
)

// addVoterTimeout bounds how long ".addvoter" waits for hraft's own
// raft.AddVoter to commit the configuration-change log entry -- not for the
// new voter to finish catching up afterward, which happens asynchronously
// via normal replication (or a snapshot install, if it's too far behind).
const addVoterTimeout = 10 * time.Second

// runREPL runs an interactive SQL loop against n's kept-alive connection,
// reading from in and writing prompts/results/errors to out, until in
// reaches EOF or the user enters .exit/.quit. Every statement runs through
// node.Node.WithDB, so it goes through the exact same commit-frame gate
// (docs/DESIGN.md) as any other client write: on a follower, or a leader
// still draining its apply backlog, that write fails and the error is
// printed rather than crashing the loop.
//
// It returns true if the user explicitly typed .exit/.quit, false if it
// stopped because in reached EOF. Callers must treat those two differently:
// a headless/daemonized launch (stdin redirected from /dev/null, as under
// systemd or a detached background process) hits EOF on the very first
// read, and a piped script (docs' `literaft ... < seed.sql`) hits it the
// moment the script ends -- neither means "the operator wants this node
// process to exit", only .exit/.quit does.
//
// A line is treated as a complete statement once its trimmed text ends in
// ";" -- a deliberately simple heuristic (it doesn't parse strings or
// comments, so a literal containing ";-- not a comment" would mis-trigger)
// that's good enough for an ops/debug tool; anything fancier belongs in a
// real SQL tokenizer, not here.
//
// One meta-command, ".addvoter <id> <address>", wraps hraft's own
// raft.AddVoter so an operator can grow the cluster from the same session
// instead of a separate tool. Like ".exit"/".quit" it must stand alone on
// its own line -- it's recognized only when no SQL statement is being
// accumulated.
func runREPL(r *raft.Raft, db *sql.DB, in io.Reader, out io.Writer) bool {
	scanner := bufio.NewScanner(in)

	var buf strings.Builder
	fmt.Fprint(out, "literaft> ")
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		if buf.Len() == 0 {
			switch {
			case trimmed == "":
				fmt.Fprint(out, "literaft> ")
				continue
			case trimmed == ".exit" || trimmed == ".quit":
				return true
			case trimmed == ".addvoter" || strings.HasPrefix(trimmed, ".addvoter "):
				runAddVoter(r, trimmed, out)
				fmt.Fprint(out, "literaft> ")
				continue
			}
		}

		buf.WriteString(line)
		buf.WriteByte('\n')

		if !strings.HasSuffix(trimmed, ";") {
			fmt.Fprint(out, "   ...> ")
			continue
		}

		runStatement(db, buf.String(), out)
		buf.Reset()
		fmt.Fprint(out, "literaft> ")
	}
	if buf.Len() > 0 {
		fmt.Fprintln(out, "error: unexpected EOF mid-statement")
	}
	return false
}

// runAddVoter implements the REPL's ".addvoter <id> <address>" command. It
// hands straight off to raft's own raft.AddVoter, so it fails the same way
// any other write does when this node isn't the leader.
func runAddVoter(r *raft.Raft, line string, out io.Writer) {
	fields := strings.Fields(line)
	if len(fields) != 3 {
		fmt.Fprintln(out, "usage: .addvoter <id> <address>")
		return
	}
	id, addr := fields[1], fields[2]

	err := r.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, addVoterTimeout).Error()
	if err != nil {
		fmt.Fprintln(out, "error:", err)
		return
	}
	fmt.Fprintln(out, "OK")
}

// runStatement runs a single SQL statement (which may be multiple semicolon-separated statements) through db, printing results to out. It
// prints any error and returns immediately on the first statement that fails.
func runStatement(db *sql.DB, sql string, out io.Writer) {
	r, err := db.Query(sql)
	if err != nil {
		fmt.Fprintln(out, "error:", err)
		return
	}
	defer r.Close()

	cols, err := r.Columns()
	if err != nil {
		fmt.Fprintln(out, "error:", err)
		return
	}
	if len(cols) > 0 {
		fmt.Fprintln(out, strings.Join(cols, "\t"))
	}

	for r.Next() {
		row := make([]interface{}, len(cols))
		rowPtrs := make([]interface{}, len(cols))
		for i := range row {
			rowPtrs[i] = &row[i]
		}
		if err := r.Scan(rowPtrs...); err != nil {
			fmt.Fprintln(out, "error:", err)
			return
		}
		strRow := make([]string, len(row))
		for i, val := range row {
			if val == nil {
				strRow[i] = "NULL"
			} else {
				strRow[i] = fmt.Sprintf("%v", val)
			}
		}
		fmt.Fprintln(out, strings.Join(strRow, "\t"))
	}
	if err := r.Err(); err != nil {
		fmt.Fprintln(out, "error:", err)
		return
	}

	if len(cols) == 0 {
		fmt.Fprintln(out, "OK")
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
