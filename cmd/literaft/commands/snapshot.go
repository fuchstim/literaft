package commands

import (
	"database/sql"
	"errors"
	"fmt"
	"io"

	"github.com/hashicorp/raft"
)

var _ = registerCommand(NewCommand(
	".snapshot",
	".snapshot",
	"Create a snapshot of the database",
	func(self Command, raft *raft.Raft, db *sql.DB, params []string, out io.Writer) error {
		if len(params) != 0 {
			return errors.New(self.Usage())
		}

		if err := raft.Snapshot().Error(); err != nil {
			return fmt.Errorf("failed to create snapshot: %w", err)
		}

		fmt.Fprintln(out, "OK")
		return nil
	},
))
