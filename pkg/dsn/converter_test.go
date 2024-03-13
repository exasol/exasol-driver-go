package dsn_test

import (
	"testing"

	"github.com/exasol/exasol-driver-go/internal/config"
	"github.com/exasol/exasol-driver-go/pkg/dsn"
	"github.com/stretchr/testify/suite"
)

type ConverterTestSuite struct {
	suite.Suite
}

func TestConverterSuite(t *testing.T) {
	suite.Run(t, new(ConverterTestSuite))
}

func (suite *ConverterTestSuite) TestConvertUserPassword() {
	config := suite.convert("exa:localhost:1234;user=sys;password=exasol")
	suite.Equal(2, config.ApiVersion)
	suite.Equal("sys", config.User)
	suite.Equal("exasol", config.Password)
	suite.Equal("", config.AccessToken)
	suite.Equal("", config.RefreshToken)
}

func (suite *ConverterTestSuite) TestConvertAccessToken() {
	config := suite.convert("exa:localhost:1234;accesstoken=token")
	suite.Equal(3, config.ApiVersion)
	suite.Equal("token", config.AccessToken)
	suite.Equal("", config.RefreshToken)
	suite.Equal("", config.User)
	suite.Equal("", config.Password)
}

func (suite *ConverterTestSuite) TestConvertRefreshToken() {
	config := suite.convert("exa:localhost:1234;refreshtoken=token")
	suite.Equal(3, config.ApiVersion)
	suite.Equal("", config.AccessToken)
	suite.Equal("token", config.RefreshToken)
	suite.Equal("", config.User)
	suite.Equal("", config.Password)
}

func (suite *ConverterTestSuite) TestConvertFetchSize() {
	config := suite.convert("exa:localhost:1234;fetchsize=42")
	suite.Equal(42, config.FetchSize)
}

func (suite *ConverterTestSuite) TestConvertQueryTimeout() {
	config := suite.convert("exa:localhost:1234;querytimeout=42")
	suite.Equal(42, config.QueryTimeout)
}

func (suite *ConverterTestSuite) TestConvertUrlpath() {
	config := suite.convert("exa:localhost:1234;urlpath=/v1/databases/db123/connect?ticket=123")
	suite.Equal("/v1/databases/db123/connect?ticket=123", config.UrlPath)
}

func (suite *ConverterTestSuite) convert(dsnValue string) *config.Config {
	config, err := dsn.ParseDSN(dsnValue)
	suite.NoError(err)
	return dsn.ToInternalConfig(config)
}
