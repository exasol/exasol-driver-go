package exasol

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
)

type DriverTestSuite struct {
	suite.Suite
}

func TestDriverSuite(t *testing.T) {
	suite.Run(t, new(DriverTestSuite))
}

func (suite *DriverTestSuite) TestOpenConnector() {
	exasolDriver := ExasolDriver{}
	_, err := exasolDriver.OpenConnector("exa:localhost:1234")
	suite.NoError(err)
}

func (suite *DriverTestSuite) TestOpenConnectorBadDsn() {
	exasolDriver := ExasolDriver{}
	_, err := exasolDriver.OpenConnector("")
	suite.Error(err)
}

func (suite *DriverTestSuite) TestOpen() {
	exasolDriver := ExasolDriver{}
	_, err := exasolDriver.Open("exa:localhost:1234")
	suite.Error(err)
	suite.True(strings.Contains(err.Error(), "connection refused"))
}

func (suite *DriverTestSuite) TestOpenBadDsn() {
	exasolDriver := ExasolDriver{}
	_, err := exasolDriver.Open("")
	suite.Error(err)
	suite.True(strings.Contains(err.Error(), "invalid connection string"))
}
