package exasol_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/exasol/exasol-driver-go"

	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type IntegrationTestSuite struct {
	suite.Suite
	ctx             context.Context
	exasolContainer testcontainers.Container
	port            int
	host            string
}

func TestIntegrationSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(IntegrationTestSuite))
}

func (suite *IntegrationTestSuite) SetupSuite() {
	suite.ctx = getContext()
	suite.exasolContainer = runExasolContainer(suite.ctx)
	suite.port = getExasolPort(suite.exasolContainer, suite.ctx)
	suite.host = getExasolHost(suite.exasolContainer, suite.ctx)
}

func getExasolHost(exasolContainer testcontainers.Container, ctx context.Context) string {
	host, err := exasolContainer.Host(ctx)
	onError(err)
	return host
}

func getExasolPort(exasolContainer testcontainers.Container, ctx context.Context) int {
	port, err := exasolContainer.MappedPort(ctx, "8563")
	onError(err)
	return port.Int()
}

func (suite *IntegrationTestSuite) TestConnect() {
	database, _ := sql.Open("exasol", fmt.Sprintf("exa:%s:%d;user=sys;password=exasol;validateservercertificate=0", suite.host, suite.port))
	rows, _ := database.Query("SELECT 2 FROM DUAL")
	columns, _ := rows.Columns()
	suite.Equal("2", columns[0])
}

func (suite *IntegrationTestSuite) TestConnection() {
	actualFingerprint := suite.getActualCertificateFingerprint()
	wrongFingerprint := "wrongFingerprint"

	errorMsgWrongFingerprint := fmt.Sprintf("E-EGOD-2: The server's certificate fingerprint '%s' does not match the expected fingerprint '%s'", actualFingerprint, wrongFingerprint)
	errorMsgAuthFailed := "[08004] Connection exception - authentication failed."
	errorMsgCertWrongHost := "x509: certificate is not valid for any names, but wanted to match localhost"
	noError := ""

	for i, testCase := range []struct {
		description   string
		config        *exasol.DSNConfig
		expectedError string
	}{
		{"wrong port", suite.createDefaultConfig().Port(1234), "connect: connection refuse"},
		{"wrong host", suite.createDefaultConfig().Host("wrong"), "dial tcp: lookup wrong"},
		{"wrong user", exasol.NewConfig("wronguser", "exasol").Host(suite.host).Port(suite.port).ValidateServerCertificate(false), errorMsgAuthFailed},
		{"wrong password", exasol.NewConfig("sys", "wrongPassword").Host(suite.host).Port(suite.port).ValidateServerCertificate(false), errorMsgAuthFailed},

		{"valid credentials", suite.createDefaultConfig(), noError},
		{"multiple invalid hostnames", suite.createDefaultConfig().Host("wrong0,wrong1,wrong2,wrong3,wrong4,wrong5," + suite.host), noError},

		{"compression on", suite.createDefaultConfig().Compression(true), noError},
		{"compression off", suite.createDefaultConfig().Compression(false), noError},

		{"encryption on", suite.createDefaultConfig().Encryption(true), noError},
		{"encryption off", suite.createDefaultConfig().Encryption(false), noError},

		{"don't validate cert, no fingerprint", suite.createDefaultConfig().ValidateServerCertificate(false).CertificateFingerprint(""), noError},
		{"don't validate cert, wrong fingerprint", suite.createDefaultConfig().ValidateServerCertificate(false).CertificateFingerprint(wrongFingerprint), errorMsgWrongFingerprint},
		{"don't validate cert, correct fingerprint", suite.createDefaultConfig().ValidateServerCertificate(false).CertificateFingerprint(actualFingerprint), noError},

		{"validate cert, no fingerprint", suite.createDefaultConfig().ValidateServerCertificate(true).CertificateFingerprint(""), errorMsgCertWrongHost},
		{"validate cert, wrong fingerprint", suite.createDefaultConfig().ValidateServerCertificate(true).CertificateFingerprint(wrongFingerprint), errorMsgWrongFingerprint},
		{"validate cert, correct fingerprint", suite.createDefaultConfig().ValidateServerCertificate(true).CertificateFingerprint(actualFingerprint), noError},
	} {
		suite.Run(fmt.Sprintf("Test%02d %s", i, testCase.description), func() {
			database := suite.openConnection(testCase.config)
			err := database.Ping()
			if testCase.expectedError == "" {
				suite.NoError(err)
				rows, _ := database.Query("SELECT 2 FROM DUAL")
				columns, _ := rows.Columns()
				suite.Equal("2", columns[0])
				suite.assertSingleValueResult(rows, "2")
			} else {
				suite.Error(err)
				suite.Contains(err.Error(), testCase.expectedError)
			}
		})
	}
}

func (suite *IntegrationTestSuite) getActualCertificateFingerprint() string {
	database := suite.openConnection(suite.createDefaultConfig().CertificateFingerprint("wrongFingerprint"))
	err := database.Ping()
	suite.Error(err)
	re := regexp.MustCompile(`E-EGOD-2: The server's certificate fingerprint '([0-9a-z]{64})' does not match the expected fingerprint 'wrongFingerprint'`)
	submatches := re.FindStringSubmatch(err.Error())
	suite.Equal(2, len(submatches), "Error message %q does not match expected message", err)
	return submatches[1]
}

func (suite *IntegrationTestSuite) TestExecAndQuery() {
	database := suite.openConnection(suite.createDefaultConfig())
	schemaName := "TEST_SCHEMA_1"
	_, _ = database.Exec("CREATE SCHEMA " + schemaName)
	_, _ = database.Exec("CREATE TABLE " + schemaName + ".TEST_TABLE(x INT)")
	_, _ = database.Exec("INSERT INTO " + schemaName + ".TEST_TABLE VALUES (15)")
	rows, _ := database.Query("SELECT x FROM " + schemaName + ".TEST_TABLE")
	suite.assertSingleValueResult(rows, "15")
}

func (suite *IntegrationTestSuite) TestFetch() {
	database := suite.openConnection(suite.createDefaultConfig().FetchSize(200))
	schemaName := "TEST_SCHEMA_FETCH"
	_, _ = database.Exec("CREATE SCHEMA " + schemaName)
	_, _ = database.Exec("CREATE TABLE " + schemaName + ".TEST_TABLE(x INT)")
	data := make([]string, 0)
	for i := 0; i < 10000; i++ {
		data = append(data, fmt.Sprintf("(%d)", i))
	}
	_, _ = database.Exec("INSERT INTO " + schemaName + ".TEST_TABLE VALUES " + strings.Join(data, ","))
	rows, _ := database.Query("SELECT x FROM " + schemaName + ".TEST_TABLE")
	result := make([]int, 0)
	for rows.Next() {
		var x int
		if err := rows.Scan(&x); err != nil {
			// Check for a scan error.
			// Query rows will be closed with defer.
			log.Fatal(err)
		}
		result = append(result, x)
	}
	suite.Equal(10000, len(result))
}

func (suite *IntegrationTestSuite) TestExecuteWithError() {
	database := suite.openConnection(suite.createDefaultConfig())
	_, err := database.Exec("CREATE SCHEMAA TEST_SCHEMA")
	suite.Error(err)
	suite.True(strings.Contains(err.Error(), "syntax error"))
}

func (suite *IntegrationTestSuite) TestQueryWithError() {
	database := suite.openConnection(suite.createDefaultConfig())
	schemaName := "TEST_SCHEMA_2"
	_, _ = database.Exec("CREATE SCHEMA " + schemaName)
	_, err := database.Query("SELECT x FROM " + schemaName + ".TEST_TABLE")
	suite.Error(err)
	suite.True(strings.Contains(err.Error(), "object TEST_SCHEMA_2.TEST_TABLE not found"))
}

func (suite *IntegrationTestSuite) TestPreparedStatement() {
	database := suite.openConnection(suite.createDefaultConfig())
	schemaName := "TEST_SCHEMA_3"
	_, _ = database.Exec("CREATE SCHEMA " + schemaName)
	_, _ = database.Exec("CREATE TABLE " + schemaName + ".TEST_TABLE(x INT)")
	preparedStatement, _ := database.Prepare("INSERT INTO " + schemaName + ".TEST_TABLE VALUES (?)")
	_, _ = preparedStatement.Exec(15)
	preparedStatement, _ = database.Prepare("SELECT x FROM " + schemaName + ".TEST_TABLE WHERE x = ?")
	rows, _ := preparedStatement.Query(15)
	suite.assertSingleValueResult(rows, "15")
}

func (suite *IntegrationTestSuite) TestBeginAndCommit() {
	database := suite.openConnection(suite.createDefaultConfig().Autocommit(false))
	schemaName := "TEST_SCHEMA_4"
	transaction, _ := database.Begin()
	_, _ = transaction.Exec("CREATE SCHEMA " + schemaName)
	_, _ = transaction.Exec("CREATE TABLE " + schemaName + ".TEST_TABLE(x INT)")
	_, _ = transaction.Exec("INSERT INTO " + schemaName + ".TEST_TABLE VALUES (15)")
	_ = transaction.Commit()
	rows, _ := database.Query("SELECT x FROM " + schemaName + ".TEST_TABLE")
	suite.assertSingleValueResult(rows, "15")
}

func (suite *IntegrationTestSuite) TestBeginAndRollback() {
	database := suite.openConnection(suite.createDefaultConfig().Autocommit(false))
	schemaName := "TEST_SCHEMA_5"
	transaction, _ := database.Begin()
	_, _ = transaction.Exec("CREATE SCHEMA " + schemaName)
	_, _ = transaction.Exec("CREATE TABLE " + schemaName + ".TEST_TABLE(x INT)")
	_, _ = transaction.Exec("INSERT INTO " + schemaName + ".TEST_TABLE VALUES (15)")
	_ = transaction.Rollback()
	_, err := database.Query("SELECT x FROM " + schemaName + ".TEST_TABLE")
	suite.Error(err)
	suite.True(strings.Contains(err.Error(), "object "+schemaName+".TEST_TABLE not found"))
}

func (suite *IntegrationTestSuite) TestPingWithContext() {
	database := suite.openConnection(suite.createDefaultConfig())
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	suite.NoError(database.PingContext(ctx))
	cancel()
}

func (suite *IntegrationTestSuite) TestExecuteAndQueryWithContext() {
	database := suite.openConnection(suite.createDefaultConfig())
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	schemaName := "TEST_SCHEMA_6"
	_, _ = database.ExecContext(ctx, "CREATE SCHEMA "+schemaName)
	_, _ = database.ExecContext(ctx, "CREATE TABLE "+schemaName+".TEST_TABLE(x INT)")
	_, _ = database.ExecContext(ctx, "INSERT INTO "+schemaName+".TEST_TABLE VALUES (15)")
	rows, _ := database.QueryContext(ctx, "SELECT x FROM "+schemaName+".TEST_TABLE")
	cancel()
	suite.assertSingleValueResult(rows, "15")
}

func (suite *IntegrationTestSuite) TestBeginWithCancelledContext() {
	database := suite.openConnection(suite.createDefaultConfig().Autocommit(false))
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	schemaName := "TEST_SCHEMA_7"
	transaction, _ := database.BeginTx(ctx, nil)
	_, _ = transaction.ExecContext(ctx, "CREATE SCHEMA "+schemaName)
	cancel()
	_, err := transaction.ExecContext(ctx, "CREATE TABLE "+schemaName+".TEST_TABLE(x INT)")
	suite.EqualError(err, "context canceled")
}

func (suite *IntegrationTestSuite) assertSingleValueResult(rows *sql.Rows, expected string) {
	rows.Next()
	var testValue string
	err := rows.Scan(&testValue)
	onError(err)
	suite.Equal(expected, testValue)
}

func (suite *IntegrationTestSuite) TearDownSuite() {
	err := suite.exasolContainer.Terminate(suite.ctx)
	onError(err)
}

func (suite *IntegrationTestSuite) createDefaultConfig() *exasol.DSNConfig {
	return exasol.NewConfig("sys", "exasol").ValidateServerCertificate(false).Host(suite.host).Port(suite.port)
}

func (suite *IntegrationTestSuite) openConnection(config *exasol.DSNConfig) *sql.DB {
	database, err := sql.Open("exasol", config.String())
	if err != nil {
		fmt.Printf("error connecting to database using config %q", config)
		panic(err)
	}
	return database
}

func getContext() context.Context {
	return context.Background()
}

func runExasolContainer(ctx context.Context) testcontainers.Container {

	dbVersion := os.Getenv("DB_VERSION")
	if dbVersion == "" {
		dbVersion = "7.1.1"
	}

	request := testcontainers.ContainerRequest{
		Image:        fmt.Sprintf("exasol/docker-db:%s", dbVersion),
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

func onError(err error) {
	if err != nil {
		log.Printf("Error %s", err)
		panic(err)
	}
}
