package exasol

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"time"
)

type ExasolDriver struct{}

type config struct {
	User             string
	Password         string
	Host             string
	Port             int
	Params           map[string]string // Connection parameters
	ApiVersion       int
	ClientName       string
	ClientVersion    string
	Schema           string
	Autocommit       bool
	FetchSize        int
	Compression      bool
	ResultSetMaxRows int
	Timeout          time.Time
	Encryption       bool
	UseTLS           bool
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
