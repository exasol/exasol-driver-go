package exasol_test

import (
	"context"
	"database/sql"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"log"
	"testing"
)

type IntegrationTestSuite struct {
	suite.Suite
}

func TestSuite(t *testing.T) {
	suite.Run(t, new(IntegrationTestSuite))
}

var ctx = getContext()
var exasolContainer = runExasolContainer()
var exasolPort = getExasolPort(exasolContainer)

func (suite *IntegrationTestSuite) TestConnect() {
	exasol, err := sql.Open("exasol", "exa:localhost:"+exasolPort+";user=sys;password=exasol;encryption=0")
	suite.NoError(err)
	rows, err := exasol.Query("SELECT 2 FROM DUAL")
	suite.NoError(err)
	columns, err := rows.Columns()
	suite.NoError(err)
	suite.Equal("2", columns[0])
}

func (suite *IntegrationTestSuite) TestConnectWithWrongPort() {
	exasol, err := sql.Open("exasol", "exa:localhost:1234;user=sys;password=exasol;encryption=0")
	suite.NoError(err)
	suite.EqualError(exasol.Ping(), "dial tcp 127.0.0.1:1234: connect: connection refused")
}

func (suite *IntegrationTestSuite) TestConnectWithWrongUsername() {
	exasol, err := sql.Open("exasol", "exa:localhost:"+exasolPort+";user=wrongUser;password=exasol;encryption=0")
	suite.NoError(err)
	suite.EqualError(exasol.Ping(), "[08004] Connection exception - authentication failed.")
}

func (suite *IntegrationTestSuite) TestConnectWithWrongPassword() {
	exasol, err := sql.Open("exasol", "exa:localhost:"+exasolPort+";user=sys;password=wrongPassword;encryption=0")
	suite.NoError(err)
	suite.EqualError(exasol.Ping(), "[08004] Connection exception - authentication failed.")
}

func (suite *IntegrationTestSuite) TearDownSuite() {
	exasolContainer.Terminate(ctx)
}

func getContext() context.Context {
	return context.Background()
}

func runExasolContainer() testcontainers.Container {
	request := testcontainers.ContainerRequest{
		Image:        "exasol/docker-db:7.0.7",
		ExposedPorts: []string{"8563", "2580"},
		WaitingFor:   wait.ForLog("All stages finished"),
		Privileged:   true,
	}
	exasolContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: request,
		Started:          true,
	})
	onError(err)
	return exasolContainer
}

func getExasolPort(exasolContainer testcontainers.Container) string {
	port, err := exasolContainer.MappedPort(ctx, "8563")
	onError(err)
	return port.Port()
}

func onError(err error) {
	if err != nil {
		log.Printf("Error %s", err)
		panic(err)
	}
}
