package exasol

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type DsnTestSuite struct {
	suite.Suite
}

func TestDsnSuite(t *testing.T) {
	suite.Run(t, new(DsnTestSuite))
}

func (suite *DsnTestSuite) TestParseValidDsnWithoutParameters() {
	dsn, err := parseDSN("exa:localhost:1234")
	suite.NoError(err)
	suite.Equal("", dsn.User)
	suite.Equal("", dsn.Password)
	suite.Equal("localhost", dsn.Host)
	suite.Equal(1234, dsn.Port)
	suite.Equal(map[string]string{}, dsn.Params)
	suite.Equal(2, dsn.ApiVersion)
	suite.Equal("Go client", dsn.ClientName)
	suite.Equal("", dsn.ClientVersion)
	suite.Equal("", dsn.Schema)
	suite.Equal(true, dsn.Autocommit)
	suite.Equal(128*1024, dsn.FetchSize)
	suite.Equal(false, dsn.Compression)
	suite.Equal(0, dsn.ResultSetMaxRows)
	suite.Equal(time.Time{}, dsn.Timeout)
	suite.Equal(true, dsn.Encryption)
	suite.Equal(true, dsn.ValidateServerCertificate)
}

func (suite *DsnTestSuite) TestParseValidDsnWithParameters() {
	dsn, err := parseDSN(
		"exa:localhost:1234;user=sys;password=exasol;" +
			"autocommit=0;" +
			"encryption=0;" +
			"fetchsize=1000;" +
			"clientname=Exasol Go client;" +
			"clientversion=1.0.0;" +
			"schema=MY_SCHEMA;" +
			"compression=1;" +
			"resultsetmaxrows=100;" +
			"mycustomparam=value")
	suite.NoError(err)
	suite.Equal("sys", dsn.User)
	suite.Equal("exasol", dsn.Password)
	suite.Equal("localhost", dsn.Host)
	suite.Equal(1234, dsn.Port)
	suite.Equal("Exasol Go client", dsn.ClientName)
	suite.Equal("1.0.0", dsn.ClientVersion)
	suite.Equal("MY_SCHEMA", dsn.Schema)
	suite.Equal(false, dsn.Autocommit)
	suite.Equal(1000, dsn.FetchSize)
	suite.Equal(true, dsn.Compression)
	suite.Equal(100, dsn.ResultSetMaxRows)
	suite.Equal(time.Time{}, dsn.Timeout)
	suite.Equal(false, dsn.Encryption)
	suite.Equal(map[string]string{"mycustomparam": "value"}, dsn.Params)
}

func (suite *DsnTestSuite) TestParseValidDsnWithParameters2() {
	dsn, err := parseDSN(
		"exa:localhost:1234;user=sys;password=exasol;autocommit=1;encryption=1;compression=0")
	suite.NoError(err)
	suite.Equal("sys", dsn.User)
	suite.Equal("exasol", dsn.Password)
	suite.Equal("localhost", dsn.Host)
	suite.Equal(1234, dsn.Port)
	suite.Equal(true, dsn.Autocommit)
	suite.Equal(true, dsn.Encryption)
	suite.Equal(false, dsn.Compression)
}

func (suite *DsnTestSuite) TestParseValidDsnWithSpecialChars() {
	dsn, err := parseDSN(
		`exa:localhost:1234;user=sys;password=exasol!,@#$%^&*\;;autocommit=1;encryption=1;compression=0`)
	suite.NoError(err)
	suite.Equal("sys", dsn.User)
	suite.Equal("exasol!,@#$%^&*;", dsn.Password)
	suite.Equal("localhost", dsn.Host)
	suite.Equal(1234, dsn.Port)
	suite.Equal(true, dsn.Autocommit)
	suite.Equal(true, dsn.Encryption)
	suite.Equal(false, dsn.Compression)
}

func (suite *DsnTestSuite) TestParseValidDsnWithSpecialChars2() {
	dsn, err := parseDSN(
		`exa:localhost:1234;user=sys;password=exasol!,@#$%^&*!;autocommit=1;encryption=1;compression=0`)
	suite.NoError(err)
	suite.Equal("sys", dsn.User)
	suite.Equal("exasol!,@#$%^&*!", dsn.Password)
	suite.Equal("localhost", dsn.Host)
	suite.Equal(true, dsn.Autocommit)
	suite.Equal(true, dsn.Encryption)
	suite.Equal(false, dsn.Compression)
}

func (suite *DsnTestSuite) TestInvalidPrefix() {
	dsn, err := parseDSN("exaa:localhost:1234")
	suite.Nil(dsn)
	suite.EqualError(err, "invalid connection string, must start with 'exa:'")
}

func (suite *DsnTestSuite) TestInvalidHostPortFormat() {
	dsn, err := parseDSN("exa:localhost")
	suite.Nil(dsn)
	suite.EqualError(err, "invalid host or port, expected format: <host>:<port>")
}

func (suite *DsnTestSuite) TestInvalidParameter() {
	dsn, err := parseDSN("exa:localhost:1234;user")
	suite.Nil(dsn)
	suite.EqualError(err, "invalid parameter user, expected format <parameter>=<value>")
}

func (suite *DsnTestSuite) TestInvalidFetchsize() {
	dsn, err := parseDSN("exa:localhost:1234;fetchsize=size")
	suite.Nil(dsn)
	suite.EqualError(err, "invalid `fetchsize` value, numeric expected")
}

func (suite *DsnTestSuite) TestInvalidValidateservercertificateUsesDefaultValue() {
	dsn, err := parseDSN("exa:localhost:1234;validateservercertificate=false")
	suite.NoError(err)
	suite.Equal(true, dsn.ValidateServerCertificate)
}

func (suite *DsnTestSuite) TestInvalidResultsetmaxrows() {
	dsn, err := parseDSN("exa:localhost:1234;resultsetmaxrows=size")
	suite.Nil(dsn)
	suite.EqualError(err, "invalid `resultsetmaxrows` value, numeric expected")
}

func (suite *DriverTestSuite) TestConfigToDsnCustomValues() {
	dsn, err := parseDSN(
		"exa:localhost:1234;user=sys;password=exasol;autocommit=0;encryption=0;compression=1;validateservercertificate=0")
	suite.NoError(err)
	suite.Equal("sys", dsn.User)
	suite.Equal("exasol", dsn.Password)
	suite.Equal("localhost", dsn.Host)
	suite.Equal(1234, dsn.Port)
	suite.Equal(false, dsn.Autocommit)
	suite.Equal(false, dsn.Encryption)
	suite.Equal(true, dsn.Compression)
	suite.Equal(false, dsn.ValidateServerCertificate)
}

func (suite *DriverTestSuite) TestConfigToDsnWithBooleanValuesTrue() {
	config := NewConfig("sys", "exasol")
	config.Compression(true)
	config.Encryption(true)
	config.Autocommit(true)
	config.ValidateServerCertificate(true)
	suite.Equal("exa:localhost:8563;user=sys;password=exasol;autocommit=1;compression=1;encryption=1;validateservercertificate=1", config.String())
}

func (suite *DriverTestSuite) TestConfigToDsnWithBooleanValuesFalse() {
	config := NewConfig("sys", "exasol")
	config.Compression(false)
	config.Encryption(false)
	config.Autocommit(false)
	config.ValidateServerCertificate(false)
	suite.Equal("exa:localhost:8563;user=sys;password=exasol;autocommit=0;compression=0;encryption=0;validateservercertificate=0", config.String())
}

func (suite *DriverTestSuite) TestConfigToDsnWithDefaultValues() {
	config := NewConfig("sys", "exasol")
	suite.Equal("exa:localhost:8563;user=sys;password=exasol", config.String())
}
