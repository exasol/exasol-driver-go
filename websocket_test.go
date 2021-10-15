package exasol

import (
	"github.com/stretchr/testify/suite"
	"testing"
)

type WebsocketTestSuite struct {
	suite.Suite
}

func TestWebsocketSuite(t *testing.T) {
	suite.Run(t, new(WebsocketTestSuite))
}

func (suite *WebsocketTestSuite) TestSingleHostResolve() {
	config := config{host: "localhost"}
	connection := connection{config: &config}

	hosts, err := connection.resolveHosts()
	suite.NoError(err)
	suite.Equal(1, len(hosts))
	suite.Equal("localhost", hosts[0])
}

func (suite *WebsocketTestSuite) TestMultipleHostResolve() {
	config := config{host: "exasol1,127.0.0.1,exasol3"}
	connection := connection{config: &config}

	hosts, err := connection.resolveHosts()
	suite.NoError(err)
	suite.Equal(3, len(hosts))
	suite.Equal("exasol1", hosts[0])
	suite.Equal("127.0.0.1", hosts[1])
	suite.Equal("exasol3", hosts[2])
}

func (suite *WebsocketTestSuite) TestHostSuffixRangeResolve() {
	config := config{host: "exasol1..3"}
	connection := connection{config: &config}

	hosts, err := connection.resolveHosts()
	suite.NoError(err)
	suite.Equal(3, len(hosts))
	suite.Equal("exasol1", hosts[0])
	suite.Equal("exasol2", hosts[1])
	suite.Equal("exasol3", hosts[2])
}

func (suite *WebsocketTestSuite) TestResolvingHostRangeWithCompleteHostnameNotSupported() {
	config := config{host: "exasol1..exasol3"}
	connection := connection{config: &config}

	hosts, err := connection.resolveHosts()
	suite.NoError(err)
	suite.Equal(1, len(hosts))
	suite.Equal("exasol1..exasol3", hosts[0])
}

func (suite *WebsocketTestSuite) TestIPRangeResolve() {
	config := config{host: "127.0.0.1..3"}
	connection := connection{config: &config}

	hosts, err := connection.resolveHosts()
	suite.NoError(err)
	suite.Equal(3, len(hosts))
	suite.Equal("127.0.0.1", hosts[0])
	suite.Equal("127.0.0.2", hosts[1])
	suite.Equal("127.0.0.3", hosts[2])
}
