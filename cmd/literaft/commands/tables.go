package commands

import (
	"database/sql"
	"errors"
	"io"

	"github.com/hashicorp/raft"
)

var _ = registerCommand(NewCommand(
	".tables",
	".tables",
	"List all tables in the database",
	func(self Command, raft *raft.Raft, db *sql.DB, params []string, out io.Writer) error {
		if len(params) != 0 {
			return errors.New(self.Usage())
		}

		return runStatement(db, "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;", false, out)
	},
))
