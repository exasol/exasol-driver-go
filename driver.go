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
)

// ExasolDriver is an implementation of the [database/sql/driver.Driver] interface.
type ExasolDriver struct{}

type config struct {
	user                      string
	password                  string
	accessToken               string
	refreshToken              string
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
	encryption                bool
	validateServerCertificate bool
	certificateFingerprint    string
}

func init() {
	sql.Register("exasol", &ExasolDriver{})
}

func toInternalConfig(dsnConfig *DSNConfig) *config {
	apiVersion := 2
	if dsnConfig.AccessToken != "" || dsnConfig.RefreshToken != "" {
		apiVersion = 3
	}
	return &config{
		user:                      dsnConfig.User,
		password:                  dsnConfig.Password,
		accessToken:               dsnConfig.AccessToken,
		refreshToken:              dsnConfig.RefreshToken,
		host:                      dsnConfig.Host,
		port:                      dsnConfig.Port,
		params:                    dsnConfig.params,
		apiVersion:                apiVersion,
		clientName:                dsnConfig.ClientName,
		clientVersion:             dsnConfig.ClientVersion,
		schema:                    dsnConfig.Schema,
		autocommit:                *dsnConfig.Autocommit,
		fetchSize:                 dsnConfig.FetchSize,
		compression:               *dsnConfig.Compression,
		resultSetMaxRows:          dsnConfig.ResultSetMaxRows,
		encryption:                *dsnConfig.Encryption,
		validateServerCertificate: *dsnConfig.ValidateServerCertificate,
		certificateFingerprint:    dsnConfig.CertificateFingerprint,
	}
}

// Open implements the driver.Driver interface.
func (e ExasolDriver) Open(dsn string) (driver.Conn, error) {
	dsnConfig, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	c := &connector{
		config: toInternalConfig(dsnConfig),
	}
	return c.Connect(context.Background())
}

// OpenConnector implements the driver.DriverContext interface.
func (e ExasolDriver) OpenConnector(dsn string) (driver.Connector, error) {
	dsnConfig, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return &connector{
		config: toInternalConfig(dsnConfig),
	}, nil
}
