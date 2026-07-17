package commands

import (
	"database/sql"
	"errors"
	"io"

	"github.com/hashicorp/raft"
)

var _ = registerCommand(NewCommand(
	".schema",
	".schema",
	"Print database schema",
	func(self Command, raft *raft.Raft, db *sql.DB, params []string, out io.Writer) error {
		if len(params) != 0 {
			return errors.New(self.Usage())
		}

		return runStatement(db, "SELECT sql FROM sqlite_master ORDER BY name;", false, out)
	},
))
