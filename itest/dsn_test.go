package itest_test

import (
	"fmt"
	"testing"

	"github.com/exasol/exasol-driver-go"
	"github.com/exasol/exasol-driver-go/pkg/dsn"
	"github.com/stretchr/testify/suite"
)

type DsnITestSuite struct {
	suite.Suite
}

func TestDsnITestSuite(t *testing.T) {
	suite.Run(t, new(DsnITestSuite))
}

func (suite *DsnITestSuite) TestStringValues() {
	for i, testCase := range []struct {
		description string
		value       string
	}{{"non empty string", "value"},
		{"string with spaces", "value with spaces"},
		{"string with tabs", "value\twith\ttabs"},
		{"string with comma", "value,with,comma"},
		{"string with semicolon", "semi;colon"}} {
		suite.Run(fmt.Sprintf("Test%02d %s", i, testCase.description), func() {
			suite.Equal(testCase.value, suite.getParsedConfig(exasol.NewConfig(testCase.value, "pass")).User)
			suite.Equal(testCase.value, suite.getParsedConfig(exasol.NewConfig("user", testCase.value)).Password)
			suite.Equal(testCase.value, suite.getParsedConfig(exasol.NewConfigWithAccessToken(testCase.value)).AccessToken)
			suite.Equal(testCase.value, suite.getParsedConfig(exasol.NewConfigWithRefreshToken(testCase.value)).RefreshToken)
			suite.Equal(testCase.value, suite.getParsedConfig(builder().UrlPath(testCase.value)).UrlPath)
			suite.Equal(testCase.value, suite.getParsedConfig(builder().CertificateFingerprint(testCase.value)).CertificateFingerprint)
			suite.Equal(testCase.value, suite.getParsedConfig(builder().ClientName(testCase.value)).ClientName)
			suite.Equal(testCase.value, suite.getParsedConfig(builder().ClientVersion(testCase.value)).ClientVersion)
			suite.Equal(testCase.value, suite.getParsedConfig(builder().Schema(testCase.value)).Schema)
		})
	}
}

func (suite *DsnITestSuite) TestDsnHostWithSemicolon() {
	config, err := dsn.ParseDSN(builder().Host("value with; semicolon").String())
	suite.EqualError(err, `E-EGOD-22: invalid host or port in 'value with', expected format: <host>:<port>`)
	suite.Nil(config)
}

func builder() *dsn.DSNConfigBuilder {
	return exasol.NewConfig("user", "password")
}

func (suite *DsnITestSuite) getParsedConfig(builder *dsn.DSNConfigBuilder) *dsn.DSNConfig {
	parsedDsn, err := dsn.ParseDSN(builder.String())
	suite.NoError(err)
	return parsedDsn
}
