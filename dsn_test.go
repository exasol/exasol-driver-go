package exasol

import (
	"github.com/stretchr/testify/suite"
	"testing"
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
	suite.Equal("", dsn.User)
	suite.Equal("", dsn.Password)
	suite.Equal("localhost", dsn.Host)
	suite.Equal(1234, dsn.Port)
	suite.Equal(map[string]string{}, dsn.params)
	suite.Equal("Go client", dsn.ClientName)
	suite.Equal("", dsn.ClientVersion)
	suite.Equal("", dsn.Schema)
	suite.Equal(true, *dsn.Autocommit)
	suite.Equal(128*1024, dsn.FetchSize)
	suite.Equal(false, *dsn.Compression)
	suite.Equal(0, dsn.ResultSetMaxRows)
	suite.Equal(true, *dsn.Encryption)
	suite.Equal(true, *dsn.ValidateServerCertificate)
	suite.Equal("", dsn.CertificateFingerprint)
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
			"certificatefingerprint=fingerprint;" +
			"mycustomparam=value")
	suite.NoError(err)
	suite.Equal("sys", dsn.User)
	suite.Equal("exasol", dsn.Password)
	suite.Equal("localhost", dsn.Host)
	suite.Equal(1234, dsn.Port)
	suite.Equal("Exasol Go client", dsn.ClientName)
	suite.Equal("1.0.0", dsn.ClientVersion)
	suite.Equal("MY_SCHEMA", dsn.Schema)
	suite.Equal(false, *dsn.Autocommit)
	suite.Equal(1000, dsn.FetchSize)
	suite.Equal(true, *dsn.Compression)
	suite.Equal(100, dsn.ResultSetMaxRows)
	suite.Equal(false, *dsn.Encryption)
	suite.Equal("fingerprint", dsn.CertificateFingerprint)
	suite.Equal(map[string]string{"mycustomparam": "value"}, dsn.params)
}

func (suite *DsnTestSuite) TestParseValidDsnWithParameters2() {
	dsn, err := ParseDSN(
		"exa:localhost:1234;user=sys;password=exasol;autocommit=1;encryption=1;compression=0")
	suite.NoError(err)
	suite.Equal("sys", dsn.User)
	suite.Equal("exasol", dsn.Password)
	suite.Equal("localhost", dsn.Host)
	suite.Equal(1234, dsn.Port)
	suite.Equal(true, *dsn.Autocommit)
	suite.Equal(true, *dsn.Encryption)
	suite.Equal(false, *dsn.Compression)
}

func (suite *DsnTestSuite) TestParseValidDsnWithSpecialChars() {
	dsn, err := ParseDSN(
		`exa:localhost:1234;user=sys;password=exasol!,@#$%^&*\;;autocommit=1;encryption=1;compression=0`)
	suite.NoError(err)
	suite.Equal("sys", dsn.User)
	suite.Equal("exasol!,@#$%^&*;", dsn.Password)
	suite.Equal("localhost", dsn.Host)
	suite.Equal(1234, dsn.Port)
	suite.Equal(true, *dsn.Autocommit)
	suite.Equal(true, *dsn.Encryption)
	suite.Equal(false, *dsn.Compression)
}

func (suite *DsnTestSuite) TestParseValidDsnWithSpecialChars2() {
	dsn, err := ParseDSN(
		`exa:localhost:1234;user=sys;password=exasol!,@#$%^&*!;autocommit=1;encryption=1;compression=0`)
	suite.NoError(err)
	suite.Equal("sys", dsn.User)
	suite.Equal("exasol!,@#$%^&*!", dsn.Password)
	suite.Equal("localhost", dsn.Host)
	suite.Equal(true, *dsn.Autocommit)
	suite.Equal(true, *dsn.Encryption)
	suite.Equal(false, *dsn.Compression)
}

func (suite *DsnTestSuite) TestInvalidPrefix() {
	dsn, err := ParseDSN("exaa:localhost:1234")
	suite.Nil(dsn)
	suite.EqualError(err, "E-EGOD-21: invalid connection string, must start with 'exa:': 'exaa:localhost:1234'")
}

func (suite *DsnTestSuite) TestInvalidHostPortFormat() {
	dsn, err := ParseDSN("exa:localhost")
	suite.Nil(dsn)
	suite.EqualError(err, "E-EGOD-22: invalid host or port in 'localhost', expected format: <host>:<port>")
}

func (suite *DsnTestSuite) TestInvalidParameter() {
	dsn, err := ParseDSN("exa:localhost:1234;user")
	suite.Nil(dsn)
	suite.EqualError(err, "E-EGOD-24: invalid parameter 'user', expected format <parameter>=<value>")
}

func (suite *DsnTestSuite) TestInvalidFetchsize() {
	dsn, err := ParseDSN("exa:localhost:1234;fetchsize=size")
	suite.Nil(dsn)
	suite.EqualError(err, "E-EGOD-25: invalid 'fetchsize' value 'size', numeric expected")
}

func (suite *DsnTestSuite) TestInvalidValidateservercertificateUsesDefaultValue() {
	dsn, err := ParseDSN("exa:localhost:1234;validateservercertificate=false")
	suite.NoError(err)
	suite.Equal(true, *dsn.ValidateServerCertificate)
}

func (suite *DsnTestSuite) TestInvalidResultsetmaxrows() {
	dsn, err := ParseDSN("exa:localhost:1234;resultsetmaxrows=size")
	suite.Nil(dsn)
	suite.EqualError(err, "E-EGOD-25: invalid 'resultsetmaxrows' value 'size', numeric expected")
}

func (suite *DriverTestSuite) TestConfigToDsnCustomValues() {
	dsn, err := ParseDSN(
		"exa:localhost:1234;user=sys;password=exasol;autocommit=0;encryption=0;compression=1;validateservercertificate=0;certificatefingerprint=fingerprint;clientname=clientName;clientversion=clientVersion")
	suite.NoError(err)
	suite.Equal("sys", dsn.User)
	suite.Equal("exasol", dsn.Password)
	suite.Equal("localhost", dsn.Host)
	suite.Equal(1234, dsn.Port)
	suite.Equal(false, *dsn.Autocommit)
	suite.Equal(false, *dsn.Encryption)
	suite.Equal(true, *dsn.Compression)
	suite.Equal(false, *dsn.ValidateServerCertificate)
	suite.Equal("fingerprint", dsn.CertificateFingerprint)
	suite.Equal("clientName", dsn.ClientName)
	suite.Equal("clientVersion", dsn.ClientVersion)
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
