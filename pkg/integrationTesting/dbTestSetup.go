package integrationTesting

import (
	"fmt"
	"net/url"
	"os"
	"testing"

	testSetupAbstraction "github.com/exasol/exasol-test-setup-abstraction-server/go-client"
	"github.com/stretchr/testify/suite"
)

const defaultExasolDbVersion = "8.22.0"

type DbTestSetup struct {
	suite          *suite.Suite
	Exasol         *testSetupAbstraction.TestSetupAbstraction
	ConnectionInfo *testSetupAbstraction.ConnectionInfo
}

func StartDbSetup(suite *suite.Suite) *DbTestSetup {
	if testing.Short() {
		suite.T().Skip()
	}
	exasolDbVersion := getDbVersion()
	suite.T().Logf("Starting Exasol %s...", exasolDbVersion)
	exasol, err := testSetupAbstraction.New().DockerDbVersion(exasolDbVersion).Start()
	if err != nil {
		suite.FailNowf("failed to create test setup abstraction: %v", err.Error())
	}
	connectionInfo, err := exasol.GetConnectionInfo()
	if err != nil {
		suite.FailNowf("error getting connection info: %v", err.Error())
	}
	setup := DbTestSetup{suite: suite, Exasol: exasol, ConnectionInfo: connectionInfo}
	return &setup
}

func getDbVersion() string {
	dbVersion := os.Getenv("DB_VERSION")
	if dbVersion != "" {
		return dbVersion
	}
	return defaultExasolDbVersion
}

// hostAndPort returns a string with host and port
func (setup *DbTestSetup) hostAndPort() string {
	return fmt.Sprintf("%s:%d", setup.ConnectionInfo.Host, setup.ConnectionInfo.Port)
}

// HostAndPort returns a string with host and port
func (setup *DbTestSetup) GetUrl() url.URL {
	return url.URL{Scheme: "wss", Host: setup.hostAndPort()}
}

func (setup *DbTestSetup) StopDb() {
	setup.suite.NoError(setup.Exasol.Stop())
}
