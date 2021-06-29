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
	config := config{Host: "localhost"}
	connection := connection{config: &config}

	hosts, err := connection.resolveHosts()
	suite.NoError(err)
	suite.Equal(len(hosts), 1)
	suite.Equal(hosts[0], "localhost")
}

func (suite *WebsocketTestSuite) TestMultipleHostResolve() {
	config := config{Host: "exasol1,127.0.0.1,exasol3"}
	connection := connection{config: &config}

	hosts, err := connection.resolveHosts()
	suite.NoError(err)
	suite.Equal(len(hosts), 3)
	suite.Equal(hosts[0], "exasol1")
	suite.Equal(hosts[1], "127.0.0.1")
	suite.Equal(hosts[2], "exasol3")
}

func (suite *WebsocketTestSuite) TestHostRangeResolve() {
	config := config{Host: "exasol1..3"}
	connection := connection{config: &config}

	hosts, err := connection.resolveHosts()
	suite.NoError(err)
	suite.Equal(len(hosts), 3)
	suite.Equal(hosts[0], "exasol1")
	suite.Equal(hosts[1], "exasol2")
	suite.Equal(hosts[2], "exasol3")
}

func (suite *WebsocketTestSuite) TestIPRangeResolve() {
	config := config{Host: "127.0.0.1..3"}
	connection := connection{config: &config}

	hosts, err := connection.resolveHosts()
	suite.NoError(err)
	suite.Equal(len(hosts), 3)
	suite.Equal(hosts[0], "127.0.0.1")
	suite.Equal(hosts[1], "127.0.0.2")
	suite.Equal(hosts[2], "127.0.0.3")
}
