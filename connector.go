package exasol

import (
	"context"
	"database/sql/driver"
)

type connector struct {
	config *Config
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	conn := &connection{
		Config:   c.config,
		ctx:      ctx,
		isClosed: true,
	}
	err := conn.connect()
	if err != nil {
		return nil, err
	}

	err = conn.login()
	if err != nil {
		return nil, err
	}

	return conn, err
}

func (c connector) Driver() driver.Driver {
	return &ExasolDriver{}
}
