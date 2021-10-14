package exasol

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"time"
)

type ExasolDriver struct{}

type config struct {
	user                      string
	password                  string
	host                      string
	port                      int
	params                    map[string]string // Connection parameters
	apiVersion                int
	clientName                string
	clientVersion             string
	schema                    string
	autocommit                bool
	fetchSize                 int
	compression               bool
	resultSetMaxRows          int
	timeout                   time.Time
	encryption                bool
	validateServerCertificate bool
}

func init() {
	sql.Register("exasol", &ExasolDriver{})
}

func (e ExasolDriver) Open(dsn string) (driver.Conn, error) {
	config, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	c := &connector{
		config: config,
	}
	return c.Connect(context.Background())
}

func (e ExasolDriver) OpenConnector(dsn string) (driver.Connector, error) {
	config, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return &connector{
		config: config,
	}, nil
}
