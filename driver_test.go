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

func (suite *DriverTestSuite) TestConfigToDsnWithBooleanValuesTrue() {
	config := NewConfig("sys", "exasol").
		Compression(true).
		Encryption(true).
		Autocommit(true).
		ValidateServerCertificate(true)
	suite.Equal("exa:localhost:8563;user=sys;password=exasol;autocommit=1;compression=1;encryption=1;validateservercertificate=1", config.String())
}

func (suite *DriverTestSuite) TestConfigToDsnWithBooleanValuesFalse() {
	config := NewConfig("sys", "exasol").
		Compression(false).
		Encryption(false).
		Autocommit(false).
		ValidateServerCertificate(false)
	suite.Equal("exa:localhost:8563;user=sys;password=exasol;autocommit=0;compression=0;encryption=0;validateservercertificate=0", config.String())
}

func (suite *DriverTestSuite) TestConfigToDsnWithClientNameAndVersion() {
	config := NewConfig("sys", "exasol").
		ClientName("clientName").
		ClientVersion("clientVersion")
	suite.Equal("exa:localhost:8563;user=sys;password=exasol;clientname=clientName;clientversion=clientVersion", config.String())
}

func (suite *DriverTestSuite) TestConfigToDsnWithSchema() {
	config := NewConfig("sys", "exasol").
		Schema("schemaName")
	suite.Equal("exa:localhost:8563;user=sys;password=exasol;schema=schemaName", config.String())
}

func (suite *DriverTestSuite) TestConfigToDsnWithDefaultValues() {
	config := NewConfig("sys", "exasol")
	suite.Equal("exa:localhost:8563;user=sys;password=exasol", config.String())
}

func (suite *DriverTestSuite) TestConfigWithAccessToken() {
	config := NewConfigWithAccessToken("TOKEN.JWT.TEST")
	suite.Equal("exa:localhost:8563;accesstoken=TOKEN.JWT.TEST", config.String())
}

func (suite *DriverTestSuite) TestConfigWithrefreshToken() {
	config := NewConfigWithRefreshToken("RefreshToken")
	suite.Equal("exa:localhost:8563;refreshtoken=RefreshToken", config.String())
}
