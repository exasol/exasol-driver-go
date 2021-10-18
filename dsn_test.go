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
	suite.Equal("", dsn.user)
	suite.Equal("", dsn.password)
	suite.Equal("localhost", dsn.host)
	suite.Equal(1234, dsn.port)
	suite.Equal(map[string]string{}, dsn.params)
	suite.Equal(2, dsn.apiVersion)
	suite.Equal("Go client", dsn.clientName)
	suite.Equal("", dsn.clientVersion)
	suite.Equal("", dsn.schema)
	suite.Equal(true, dsn.autocommit)
	suite.Equal(128*1024, dsn.fetchSize)
	suite.Equal(false, dsn.compression)
	suite.Equal(0, dsn.resultSetMaxRows)
	suite.Equal(time.Time{}, dsn.timeout)
	suite.Equal(true, dsn.encryption)
	suite.Equal(true, dsn.validateServerCertificate)
	suite.Equal("", dsn.certificateFingerprint)
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
			"certificatefingerprint=fingerprint;" +
			"mycustomparam=value")
	suite.NoError(err)
	suite.Equal("sys", dsn.user)
	suite.Equal("exasol", dsn.password)
	suite.Equal("localhost", dsn.host)
	suite.Equal(1234, dsn.port)
	suite.Equal("Exasol Go client", dsn.clientName)
	suite.Equal("1.0.0", dsn.clientVersion)
	suite.Equal("MY_SCHEMA", dsn.schema)
	suite.Equal(false, dsn.autocommit)
	suite.Equal(1000, dsn.fetchSize)
	suite.Equal(true, dsn.compression)
	suite.Equal(100, dsn.resultSetMaxRows)
	suite.Equal(time.Time{}, dsn.timeout)
	suite.Equal(false, dsn.encryption)
	suite.Equal("fingerprint", dsn.certificateFingerprint)
	suite.Equal(map[string]string{"mycustomparam": "value"}, dsn.params)
}

func (suite *DsnTestSuite) TestParseValidDsnWithParameters2() {
	dsn, err := parseDSN(
		"exa:localhost:1234;user=sys;password=exasol;autocommit=1;encryption=1;compression=0")
	suite.NoError(err)
	suite.Equal("sys", dsn.user)
	suite.Equal("exasol", dsn.password)
	suite.Equal("localhost", dsn.host)
	suite.Equal(1234, dsn.port)
	suite.Equal(true, dsn.autocommit)
	suite.Equal(true, dsn.encryption)
	suite.Equal(false, dsn.compression)
}

func (suite *DsnTestSuite) TestParseValidDsnWithSpecialChars() {
	dsn, err := parseDSN(
		`exa:localhost:1234;user=sys;password=exasol!,@#$%^&*\;;autocommit=1;encryption=1;compression=0`)
	suite.NoError(err)
	suite.Equal("sys", dsn.user)
	suite.Equal("exasol!,@#$%^&*;", dsn.password)
	suite.Equal("localhost", dsn.host)
	suite.Equal(1234, dsn.port)
	suite.Equal(true, dsn.autocommit)
	suite.Equal(true, dsn.encryption)
	suite.Equal(false, dsn.compression)
}

func (suite *DsnTestSuite) TestParseValidDsnWithSpecialChars2() {
	dsn, err := parseDSN(
		`exa:localhost:1234;user=sys;password=exasol!,@#$%^&*!;autocommit=1;encryption=1;compression=0`)
	suite.NoError(err)
	suite.Equal("sys", dsn.user)
	suite.Equal("exasol!,@#$%^&*!", dsn.password)
	suite.Equal("localhost", dsn.host)
	suite.Equal(true, dsn.autocommit)
	suite.Equal(true, dsn.encryption)
	suite.Equal(false, dsn.compression)
}

func (suite *DsnTestSuite) TestInvalidPrefix() {
	dsn, err := parseDSN("exaa:localhost:1234")
	suite.Nil(dsn)
	suite.EqualError(err, "E-GOD-21: invalid connection string, must start with 'exa:': 'exaa:localhost:1234'")
}

func (suite *DsnTestSuite) TestInvalidHostPortFormat() {
	dsn, err := parseDSN("exa:localhost")
	suite.Nil(dsn)
	suite.EqualError(err, "E-GOD-22: invalid host or port in 'localhost', expected format: <host>:<port>")
}

func (suite *DsnTestSuite) TestInvalidParameter() {
	dsn, err := parseDSN("exa:localhost:1234;user")
	suite.Nil(dsn)
	suite.EqualError(err, "E-GOD-24: invalid parameter 'user', expected format <parameter>=<value>")
}

func (suite *DsnTestSuite) TestInvalidFetchsize() {
	dsn, err := parseDSN("exa:localhost:1234;fetchsize=size")
	suite.Nil(dsn)
	suite.EqualError(err, "E-GOD-25: invalid 'fetchsize' value 'size', numeric expected")
}

func (suite *DsnTestSuite) TestInvalidValidateservercertificateUsesDefaultValue() {
	dsn, err := parseDSN("exa:localhost:1234;validateservercertificate=false")
	suite.NoError(err)
	suite.Equal(true, dsn.validateServerCertificate)
}

func (suite *DsnTestSuite) TestInvalidResultsetmaxrows() {
	dsn, err := parseDSN("exa:localhost:1234;resultsetmaxrows=size")
	suite.Nil(dsn)
	suite.EqualError(err, "E-GOD-25: invalid 'resultsetmaxrows' value 'size', numeric expected")
}

func (suite *DriverTestSuite) TestConfigToDsnCustomValues() {
	dsn, err := parseDSN(
		"exa:localhost:1234;user=sys;password=exasol;autocommit=0;encryption=0;compression=1;validateservercertificate=0;certificatefingerprint=fingerprint")
	suite.NoError(err)
	suite.Equal("sys", dsn.user)
	suite.Equal("exasol", dsn.password)
	suite.Equal("localhost", dsn.host)
	suite.Equal(1234, dsn.port)
	suite.Equal(false, dsn.autocommit)
	suite.Equal(false, dsn.encryption)
	suite.Equal(true, dsn.compression)
	suite.Equal(false, dsn.validateServerCertificate)
	suite.Equal("fingerprint", dsn.certificateFingerprint)
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
