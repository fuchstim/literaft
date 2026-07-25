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

func (d *Driver) dsn() string {
	return "file:" + d.dbPath + "?vfs=" + d.vfsName
}

// Open implements driver.Driver. name is ignored.
func (d *Driver) Open(string) (driver.Conn, error) {
	c, err := (&ncrdriver.SQLite{}).Open(d.dsn())
	if err != nil {
		return nil, err
	}
	if err := applyPragmas(c); err != nil {
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

type connector struct {
	owner *Driver
	inner driver.Connector
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.inner.Connect(ctx)
	if err != nil {
		return nil, err
	}
	if err := applyPragmas(conn); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

func (c *connector) Driver() driver.Driver { return c.owner }

func applyPragmas(c driver.Conn) error {
	raw, ok := c.(ncrdriver.Conn)
	if !ok {
		return fmt.Errorf("driver: opened connection doesn't implement ncrdriver.Conn (got %T)", c)
	}

	return raw.Raw().Exec("PRAGMA synchronous=NORMAL")
}
