package exasol_test

import (
	"github.com/exasol/go-exasol"
	"github.com/stretchr/testify/suite"
	"strings"
	"testing"
)

type DriverTestSuite struct {
	suite.Suite
}

func TestDriverSuite(t *testing.T) {
	suite.Run(t, new(DriverTestSuite))
}

func (suite *DriverTestSuite) TestOpenConnector() {
	exasolDriver := exasol.ExasolDriver{}
	_, err := exasolDriver.OpenConnector("exa:localhost:1234")
	suite.NoError(err)
}

func (suite *DriverTestSuite) TestOpenConnectorBadDsn() {
	exasolDriver := exasol.ExasolDriver{}
	_, err := exasolDriver.OpenConnector("")
	suite.Error(err)
}

func (suite *DriverTestSuite) TestOpen() {
	exasolDriver := exasol.ExasolDriver{}
	_, err := exasolDriver.Open("exa:localhost:1234")
	suite.Error(err)
	suite.True(strings.Contains(err.Error(), "connection refused"))
}

func (suite *DriverTestSuite) TestOpenBadDsn() {
	exasolDriver := exasol.ExasolDriver{}
	_, err := exasolDriver.Open("")
	suite.Error(err)
	suite.True(strings.Contains(err.Error(), "invalid connection string"))
}
