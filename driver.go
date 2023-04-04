// Copyright 2022 Exasol AG. All rights reserved.
//
// This Source Code Form is subject to the terms of the MIT License,
// see https://github.com/exasol/exasol-driver-go/blob/main/LICENSE

// Package exasol implements a driver for the Exasol database.
package exasol

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/exasol/exasol-driver-go/internal/config"
	"github.com/exasol/exasol-driver-go/pkg/connection"
	"github.com/exasol/exasol-driver-go/pkg/dsn"
)

func init() {
	sql.Register("exasol", &ExasolDriver{})
}

// ExasolDriver is an implementation of the [database/sql/driver.Driver] interface.
type ExasolDriver struct{}

// Open implements the driver.Driver interface.
func (e ExasolDriver) Open(input string) (driver.Conn, error) {
	dsnConfig, err := dsn.ParseDSN(input)
	if err != nil {
		return nil, err
	}
	c := &Connector{
		Config: dsn.ToInternalConfig(dsnConfig),
	}
	return c.Connect(context.Background())
}

// OpenConnector implements the driver.DriverContext interface.
func (e ExasolDriver) OpenConnector(input string) (driver.Connector, error) {
	dsnConfig, err := dsn.ParseDSN(input)
	if err != nil {
		return nil, err
	}
	return &Connector{
		Config: dsn.ToInternalConfig(dsnConfig),
	}, nil
}

// Connector implements the [database/sql/driver.Connector] interface.
type Connector struct {
	Config *config.Config
}

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	conn := &connection.Connection{
		Config:   c.Config,
		Ctx:      ctx,
		IsClosed: true,
	}
	err := conn.Connect()
	if err != nil {
		return nil, err
	}

	err = conn.Login(ctx)
	if err != nil {
		return nil, err
	}

	return conn, err
}

func (c Connector) Driver() driver.Driver {
	return &ExasolDriver{}
}

// NewConfig creates a new builder with username/password authentication.
func NewConfig(user, password string) *dsn.DSNConfigBuilder {
	return &dsn.DSNConfigBuilder{
		Config: &dsn.DSNConfig{
			Host:     "localhost",
			Port:     8563,
			User:     user,
			Password: password,
		},
	}
}

// NewConfigWithAccessToken creates a new builder with access token authentication.
func NewConfigWithAccessToken(token string) *dsn.DSNConfigBuilder {
	return &dsn.DSNConfigBuilder{
		Config: &dsn.DSNConfig{
			Host:        "localhost",
			Port:        8563,
			AccessToken: token,
		},
	}
}

// NewConfigWithRefreshToken creates a new builder with refresh token authentication.
func NewConfigWithRefreshToken(token string) *dsn.DSNConfigBuilder {
	return &dsn.DSNConfigBuilder{
		Config: &dsn.DSNConfig{
			Host:         "localhost",
			Port:         8563,
			RefreshToken: token,
		},
	}
}
