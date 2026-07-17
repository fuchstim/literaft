package commands

import (
	"database/sql"
	"io"

	"github.com/hashicorp/raft"
)

type CommandDelegate func(self Command, raft *raft.Raft, db *sql.DB, params []string, out io.Writer) error

type Command interface {
	Name() string
	Usage() string
	Description() string
	Run(raft *raft.Raft, db *sql.DB, params []string, out io.Writer) error
}

var _ Command = (*commandImpl)(nil)

type commandImpl struct {
	name, usage, description string
	delegate                 CommandDelegate
}

func NewCommand(name, usage, description string, delegate CommandDelegate) *commandImpl {
	return &commandImpl{name, usage, description, delegate}
}

func (c *commandImpl) Name() string {
	return c.name
}

func (c *commandImpl) Usage() string {
	return c.usage
}

func (c *commandImpl) Description() string {
	return c.description
}

func (c *commandImpl) Run(raft *raft.Raft, db *sql.DB, params []string, out io.Writer) error {
	return c.delegate(c, raft, db, params, out)
}
