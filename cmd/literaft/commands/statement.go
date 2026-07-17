package commands

import (
	"database/sql"
	"fmt"
	"io"

	"github.com/fatih/color"
	"github.com/rodaine/table"
)

func runStatement(db *sql.DB, sql string, printHeader bool, out io.Writer) error {
	// For some reason, h.db.Query panics if the SQL statement is just a semicolon, so we handle that case separately.
	if sql == ";" {
		return nil
	}

	r, err := db.Query(sql)
	if err != nil {
		return fmt.Errorf("failed to execute SQL statement: %w", err)
	}
	defer r.Close()

	cols, err := r.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}

	headerFmt := color.New(color.FgGreen, color.Underline).SprintfFunc()
	columnFmt := color.New(color.FgYellow).SprintfFunc()

	colsAny := make([]any, len(cols))
	for i, col := range cols {
		colsAny[i] = col
	}
	tbl := table.
		New(colsAny...).
		WithPrintHeaders(printHeader).
		WithHeaderFormatter(headerFmt).
		WithFirstColumnFormatter(columnFmt).
		WithWriter(out)

	for r.Next() {
		row := make([]interface{}, len(cols))
		rowPtrs := make([]interface{}, len(cols))
		for i := range row {
			rowPtrs[i] = &row[i]
		}
		if err := r.Scan(rowPtrs...); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
		strRow := make([]any, len(row))
		for i, val := range row {
			switch v := val.(type) {
			case nil:
				strRow[i] = "NULL"
			case []byte:
				// A BLOB column: database/sql's generic any scan target
				// returns its raw bytes, which %v would otherwise render as
				// a numeric slice literal rather than SQLite CLI-style
				// output.
				strRow[i] = fmt.Sprintf("<blob %d bytes>", len(v))
			default:
				strRow[i] = fmt.Sprintf("%v", v)
			}
		}
		tbl.AddRow(strRow...)
	}
	if err := r.Err(); err != nil {
		return fmt.Errorf("error iterating over rows: %w", err)
	}

	if len(cols) == 0 {
		fmt.Fprintln(out, "OK")
	} else {
		tbl.Print()
	}

	return nil
}
