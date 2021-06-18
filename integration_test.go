package exasol_test

import (
	"context"
	"database/sql"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"log"
	"strings"
	"testing"
)

type IntegrationTestSuite struct {
	suite.Suite
	ctx             context.Context
	exasolContainer testcontainers.Container
	port            string
}

func TestIntegrationSuite(t *testing.T) {
	suite.Run(t, new(IntegrationTestSuite))
}

func (suite *IntegrationTestSuite) SetupSuite() {
	suite.ctx = getContext()
	suite.exasolContainer = runExasolContainer(suite.ctx)
	suite.port = getExasolPort(suite.exasolContainer, suite.ctx)
}

func (suite *IntegrationTestSuite) TestConnect() {
	exasol, err := sql.Open("exasol", "exa:localhost:"+suite.port+";user=sys;password=exasol;encryption=0")
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
	err = exasol.Ping()
	suite.Error(err)
	suite.Contains(err.Error(), "connect: connection refuse")
}

func (suite *IntegrationTestSuite) TestConnectWithWrongUsername() {
	exasol, err := sql.Open("exasol", "exa:localhost:"+suite.port+";user=wrongUser;password=exasol;encryption=0")
	suite.NoError(err)
	suite.EqualError(exasol.Ping(), "[08004] Connection exception - authentication failed.")
}

func (suite *IntegrationTestSuite) TestConnectWithWrongPassword() {
	exasol, err := sql.Open("exasol", "exa:localhost:"+suite.port+";user=sys;password=wrongPassword;encryption=0")
	suite.NoError(err)
	suite.EqualError(exasol.Ping(), "[08004] Connection exception - authentication failed.")
}

func (suite *IntegrationTestSuite) TestExecAndQuery() {
	exasol, err := sql.Open("exasol", "exa:localhost:"+suite.port+";user=sys;password=exasol;encryption=0")
	suite.NoError(err)
	_, err = exasol.Exec("CREATE SCHEMA TEST_SCHEMA_1")
	suite.NoError(err)
	_, err = exasol.Exec("CREATE TABLE TEST_SCHEMA_1.TEST_TABLE(x INT)")
	suite.NoError(err)
	_, err = exasol.Exec("INSERT INTO TEST_SCHEMA_1.TEST_TABLE VALUES (15)")
	suite.NoError(err)
	rows, err := exasol.Query("SELECT x FROM TEST_SCHEMA_1.TEST_TABLE")
	suite.NoError(err)
	rows.Next()
	var testValue string
	err = rows.Scan(&testValue)
	suite.NoError(err)
	suite.Equal("15", testValue)
}

func (suite *IntegrationTestSuite) TestExecuteWithError() {
	exasol, err := sql.Open("exasol", "exa:localhost:"+suite.port+";user=sys;password=exasol;encryption=0")
	suite.NoError(err)
	_, err = exasol.Exec("CREATE SCHEMAA TEST_SCHEMA")
	suite.Error(err)
	suite.True(strings.Contains(err.Error(), "syntax error"))
}

func (suite *IntegrationTestSuite) TestQueryWithError() {
	exasol, err := sql.Open("exasol", "exa:localhost:"+suite.port+";user=sys;password=exasol;encryption=0")
	suite.NoError(err)
	_, err = exasol.Exec("CREATE SCHEMA TEST_SCHEMA_2")
	suite.NoError(err)
	_, err = exasol.Query("SELECT x FROM TEST_SCHEMA_2.TEST_TABLE")
	suite.Error(err)
	suite.True(strings.Contains(err.Error(), "object TEST_SCHEMA_2.TEST_TABL not found"))
}

func (suite *IntegrationTestSuite) TearDownSuite() {
	err := suite.exasolContainer.Terminate(suite.ctx)
	onError(err)
}

func getContext() context.Context {
	return context.Background()
}

func runExasolContainer(ctx context.Context) testcontainers.Container {
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

func getExasolPort(exasolContainer testcontainers.Container, ctx context.Context) string {
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
