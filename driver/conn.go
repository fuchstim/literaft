package driver

import (
	"context"
	"database/sql/driver"
	"fmt"

	ncrdriver "github.com/ncruces/go-sqlite3/driver"
)

var (
	_ driver.Driver        = (*Driver)(nil)
	_ driver.DriverContext = (*Driver)(nil)
	_ driver.Connector     = (*connector)(nil)
)

// dsn builds the DSN this Driver always opens, regardless of whatever name
// database/sql passes to Open/OpenConnector -- see the package doc comment
// on sql.Open's reserved dbName argument.
func (d *Driver) dsn() string {
	return "file:" + d.dbPath + "?vfs=" + d.vfsName
}

// Open implements driver.Driver. name is ignored -- see dsn.
func (d *Driver) Open(name string) (driver.Conn, error) {
	c, err := (&ncrdriver.SQLite{}).Open(d.dsn())
	if err != nil {
		return nil, err
	}
	if err := applyRequiredPragmas(c); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

// OpenConnector implements driver.DriverContext. name is ignored, same as
// Open.
func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	inner, err := (&ncrdriver.SQLite{}).OpenConnector(d.dsn())
	if err != nil {
		return nil, err
	}
	return &connector{owner: d, inner: inner}, nil
}

// connector wraps ncruces/go-sqlite3/driver's own connector so every
// physical connection database/sql's pool opens over this Driver's
// lifetime -- not just the first -- gets applyRequiredPragmas applied.
type connector struct {
	owner *Driver
	inner driver.Connector
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.inner.Connect(ctx)
	if err != nil {
		return nil, err
	}
	if err := applyRequiredPragmas(conn); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

func (c *connector) Driver() driver.Driver { return c.owner }

// applyRequiredPragmas sets synchronous=NORMAL (durability comes from the
// RAFT quorum, not local fsync -- CLAUDE.md) on a freshly opened
// connection. Must run on every new connection, not once: synchronous is a
// per-connection setting (internal/node/node.go:168-171 makes the same
// point about its own keeper/checkpointer connections).
func applyRequiredPragmas(c driver.Conn) error {
	raw, ok := c.(ncrdriver.Conn)
	if !ok {
		return fmt.Errorf("driver: opened connection doesn't implement ncrdriver.Conn (got %T)", c)
	}
	return raw.Raw().Exec("PRAGMA synchronous=NORMAL")
}
