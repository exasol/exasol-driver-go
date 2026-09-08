package integrationTesting

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"testing"

	"github.com/exasol/exasol-driver-go/internal/utils"
	testSetupAbstraction "github.com/exasol/exasol-test-setup-abstraction-server/go-client"
	"github.com/stretchr/testify/suite"
)

const defaultExasolDbVersion = "2026.1.1"

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
	return version >= 8
}

// SupportsNativeParquetImport reports whether the server this suite started can
// serve a local Parquet import natively, deciding from the version the setup was
// started with rather than a live query, since that version is already known.
func (setup *DbTestSetup) SupportsNativeParquetImport() bool {
	return utils.SupportsNativeParquetImport(setup.DbVersion)
}

// SupportsPublicKeyPinning reports whether the server this suite started can
// parse the PUBLIC KEY clause an encrypted local import pins its proxy
// connection with, deciding from the version the setup was started with for the
// same reason as SupportsNativeParquetImport.
//
// This is a separate question from native Parquet import and not a stricter one:
// 2025.1.10 parses the clause yet is below the Parquet threshold, so a test of
// the encrypted channel on either format must ask this rather than reuse the
// Parquet predicate.
func (setup *DbTestSetup) SupportsPublicKeyPinning() bool {
	return utils.SupportsPublicKeyPinning(setup.DbVersion)
}

func (setup *DbTestSetup) getExasolMajorVersion() (int, error) {
	db := setup.createConnection()
	defer db.Close()
	result, err := db.Query("SELECT PARAM_VALUE FROM SYS.EXA_METADATA WHERE PARAM_NAME='databaseMajorVersion'")
	if err != nil {
		return -1, fmt.Errorf("querying exasol version failed: %w", err)
	}
	defer result.Close()
	if !result.Next() {
		if result.Err() != nil {
			return -1, fmt.Errorf("failed to iterate exasol version: %w", result.Err())
		}
		return -1, fmt.Errorf("no result found for exasol version query")
	}
	var majorVersion int
	err = result.Scan(&majorVersion)
	if err != nil {
		return -1, fmt.Errorf("failed to read exasol version result: %w", err)
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
