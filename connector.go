package exasol

import (
	"context"
	"database/sql/driver"
)

// connector implements the [database/sql/driver.Connector] interface.
type connector struct {
	config *config
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	conn := &connection{
		config:   c.config,
		ctx:      ctx,
		isClosed: true,
	}
	err := conn.connect()
	if err != nil {
		return nil, err
	}

	err = conn.login(ctx)
	if err != nil {
		return nil, err
	}

	return conn, err
}

func (c connector) Driver() driver.Driver {
	return &ExasolDriver{}
}
