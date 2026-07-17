package commands

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/hashicorp/raft"
)

var _ = registerCommand(NewCommand(
	".load",
	".load <filename>",
	"Load and execute SQL statements from a file",
	func(self Command, raft *raft.Raft, db *sql.DB, params []string, out io.Writer) error {
		if len(params) != 1 {
			return errors.New(self.Usage())
		}
		filename := params[0]

		data, err := os.ReadFile(filename)
		if err != nil {
			return fmt.Errorf("failed to read file: %w", err)
		}

		r, err := db.Exec(string(data))
		if err != nil {
			return fmt.Errorf("failed to execute SQL statements: %w", err)
		}

		rowsAffected, err := r.RowsAffected()
		if err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		}

		fmt.Fprintf(out, "OK (%d rows affected)\n", rowsAffected)
		return nil
	},
))
