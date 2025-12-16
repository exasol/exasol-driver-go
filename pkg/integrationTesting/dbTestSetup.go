package integrationTesting

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"testing"

	testSetupAbstraction "github.com/exasol/exasol-test-setup-abstraction-server/go-client"
	"github.com/stretchr/testify/suite"
)

const defaultExasolDbVersion = "2025.1.8"

type DbTestSetup struct {
	suite          *suite.Suite
	Exasol         *testSetupAbstraction.TestSetupAbstraction
	ConnectionInfo *testSetupAbstraction.ConnectionInfo
	DbVersion      string
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
	setup := DbTestSetup{suite: suite, Exasol: exasol, ConnectionInfo: connectionInfo, DbVersion: exasolDbVersion}
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

func (setup *DbTestSetup) IsExasolVersion8() bool {
	version, err := setup.getExasolMajorVersion()
	if err != nil {
		setup.suite.FailNow("error getting exasol version: " + err.Error())
	}
	return version == "8"
}

func (setup *DbTestSetup) getExasolMajorVersion() (string, error) {
	db := setup.createConnection()
	defer db.Close()
	result, err := db.Query("SELECT PARAM_VALUE FROM SYS.EXA_METADATA WHERE PARAM_NAME='databaseMajorVersion'")
	if err != nil {
		return "", fmt.Errorf("querying exasol version failed: %w", err)
	}
	defer result.Close()
	if !result.Next() {
		if result.Err() != nil {
			return "", fmt.Errorf("failed to iterate exasol version: %w", result.Err())
		}
		return "", fmt.Errorf("no result found for exasol version query")
	}
	var majorVersion string
	err = result.Scan(&majorVersion)
	if err != nil {
		return "", fmt.Errorf("failed to read exasol version result: %w", err)
	}
	return majorVersion, nil
}

func (setup *DbTestSetup) createConnection() *sql.DB {
	conn, err := setup.Exasol.CreateConnection()
	if err != nil {
		setup.suite.FailNow("failed to create connection: " + err.Error())
	}
	return conn
}

func (setup *DbTestSetup) StopDb() {
	setup.suite.NoError(setup.Exasol.Stop())
}
