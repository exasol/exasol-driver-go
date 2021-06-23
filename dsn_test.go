package exasol

import (
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

type DsnTestSuite struct {
	suite.Suite
}

func TestDsnSuite(t *testing.T) {
	suite.Run(t, new(DsnTestSuite))
}

func (suite *DsnTestSuite) TestParseValidDsnWithoutParameters() {
	dsn, err := ParseDSN("exa:localhost:1234")
	suite.NoError(err)
	suite.Equal(dsn.User, "")
	suite.Equal(dsn.Password, "")
	suite.Equal(dsn.Host, "localhost")
	suite.Equal(dsn.Port, "1234")
	suite.Equal(dsn.Params, map[string]string{})
	suite.Equal(dsn.ApiVersion, 2)
	suite.Equal(dsn.ClientName, "Go client")
	suite.Equal(dsn.ClientVersion, "")
	suite.Equal(dsn.Schema, "")
	suite.Equal(dsn.Autocommit, true)
	suite.Equal(dsn.FetchSize, 128*1024)
	suite.Equal(dsn.Compression, false)
	suite.Equal(dsn.ResultSetMaxRows, 0)
	suite.Equal(dsn.Timeout, time.Time{})
	suite.Equal(dsn.Encryption, true)
}

func (suite *DsnTestSuite) TestParseValidDsnWithParameters() {
	dsn, err := ParseDSN(
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
	suite.Equal(dsn.User, "sys")
	suite.Equal(dsn.Password, "exasol")
	suite.Equal(dsn.Host, "localhost")
	suite.Equal(dsn.Port, "1234")
	suite.Equal(dsn.ClientName, "Exasol Go client")
	suite.Equal(dsn.ClientVersion, "1.0.0")
	suite.Equal(dsn.Schema, "MY_SCHEMA")
	suite.Equal(dsn.Autocommit, false)
	suite.Equal(dsn.FetchSize, 1000)
	suite.Equal(dsn.Compression, true)
	suite.Equal(dsn.ResultSetMaxRows, 100)
	suite.Equal(dsn.Timeout, time.Time{})
	suite.Equal(dsn.Encryption, false)
	suite.Equal(dsn.Params, map[string]string{"mycustomparam": "value"})
}

func (suite *DsnTestSuite) TestParseValidDsnWithParameters2() {
	dsn, err := ParseDSN(
		"exa:localhost:1234;user=sys;password=exasol;autocommit=1;encryption=1;compression=0")
	suite.NoError(err)
	suite.Equal(dsn.User, "sys")
	suite.Equal(dsn.Password, "exasol")
	suite.Equal(dsn.Host, "localhost")
	suite.Equal(dsn.Port, "1234")
	suite.Equal(dsn.Autocommit, true)
	suite.Equal(dsn.Encryption, true)
	suite.Equal(dsn.Compression, false)
}

func (suite *DsnTestSuite) TestInvalidPrefix() {
	dsn, err := ParseDSN("exaa:localhost:1234")
	suite.Nil(dsn)
	suite.EqualError(err, "invalid connection string, must start with 'exa:'")
}

func (suite *DsnTestSuite) TestInvalidHostPortFormat() {
	dsn, err := ParseDSN("exa:localhost")
	suite.Nil(dsn)
	suite.EqualError(err, "invalid host or port, expected format: <host>:<port>")
}

func (suite *DsnTestSuite) TestInvalidParameter() {
	dsn, err := ParseDSN("exa:localhost:1234;user")
	suite.Nil(dsn)
	suite.EqualError(err, "invalid parameter user, expected format <parameter>=<value>")
}

func (suite *DsnTestSuite) TestInvalidFetchsize() {
	dsn, err := ParseDSN("exa:localhost:1234;fetchsize=size")
	suite.Nil(dsn)
	suite.EqualError(err, "invalid `fetchsize` value, numeric expected")
}

func (suite *DsnTestSuite) TestInvalidResultsetmaxrows() {
	dsn, err := ParseDSN("exa:localhost:1234;resultsetmaxrows=size")
	suite.Nil(dsn)
	suite.EqualError(err, "invalid `resultsetmaxrows` value, numeric expected")
}
