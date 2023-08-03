package itest_test

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"os/user"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/exasol/exasol-driver-go"
	"github.com/exasol/exasol-driver-go/pkg/dsn"

	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
	"gopkg.in/yaml.v3"

	"github.com/stretchr/testify/suite"

	testSetupAbstraction "github.com/exasol/exasol-test-setup-abstraction-server/go-client"
)

type IntegrationTestSuite struct {
	suite.Suite
	ctx       context.Context
	exasol    *testSetupAbstraction.TestSetupAbstraction
	dbVersion string
	port      int
	host      string
}

func TestIntegrationSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(IntegrationTestSuite))
}

func (suite *IntegrationTestSuite) SetupSuite() {
	suite.ctx = context.Background()
	suite.dbVersion = getDbVersion()
	var err error
	suite.exasol, err = testSetupAbstraction.New().DockerDbVersion(suite.dbVersion).Start()
	if err != nil {
		suite.FailNowf("setup failed", "failed to start test setup: %v", err)
	}
	connectionInfo, err := suite.exasol.GetConnectionInfo()
	if err != nil {
		suite.FailNowf("setup failed", "failed to get connection info: %v", err)
	}
	suite.port = connectionInfo.Port
	suite.host = connectionInfo.Host
}

func getDbVersion() string {
	dbVersion := os.Getenv("DB_VERSION")
	if dbVersion != "" {
		return dbVersion
	}
	return "8.20.0"
}

func (suite *IntegrationTestSuite) TestConnect() {
	database, _ := sql.Open("exasol", fmt.Sprintf("exa:%s:%d;user=sys;password=exasol;validateservercertificate=0", suite.host, suite.port))
	defer database.Close()
	rows, _ := database.Query("SELECT 2 FROM DUAL")
	columns, _ := rows.Columns()
	suite.Equal("2", columns[0])
}

func (suite *IntegrationTestSuite) isExasol7_0_x() bool {
	return strings.HasPrefix(suite.dbVersion, "7.0.")
}

func (suite *IntegrationTestSuite) TestConnection() {
	actualFingerprint := suite.getActualCertificateFingerprint()
	const wrongFingerprint = "wrongFingerprint"
	const noError = ""

	errorMsgWrongFingerprint := fmt.Sprintf("E-EGOD-10: the server's certificate fingerprint '%s' does not match the expected fingerprint '%s'", actualFingerprint, wrongFingerprint)
	const errorMsgAuthFailed = "E-EGOD-11: execution failed with SQL error code '08004' and message 'Connection exception - authentication failed.'"

	var errorMsgTokenAuthFailed string
	if suite.isExasol7_0_x() {
		errorMsgTokenAuthFailed = "E-EGOD-11: execution failed with SQL error code '00000' and message 'Invalid login request command: loginToken'"
	} else {
		errorMsgTokenAuthFailed = "E-EGOD-11: execution failed with SQL error code '08004' and message 'Connection exception - authentication failed'"
	}

	var errorMsgCertWrongHost string
	if suite.host == "localhost" {
		errorMsgCertWrongHost = "x509: certificate is not valid for any names, but wanted to match localhost"
	} else {
		errorMsgCertWrongHost = "x509: “*.exacluster.local” certificate is not standards compliant"
	}

	var errorMsgEncryptionOff string
	if suite.isExasolVersion8() {
		errorMsgEncryptionOff = "EGOD-11: execution failed with SQL error code '08004' and message 'Connection exception - Only TLS connections are allowed.'"
	} else {
		errorMsgEncryptionOff = noError
	}

	for i, testCase := range []struct {
		description   string
		config        *dsn.DSNConfigBuilder
		expectedError string
	}{
		{"wrong port", suite.createDefaultConfig().Port(1234), "connect: connection refuse"},
		{"wrong host", suite.createDefaultConfig().Host("wrong"), "dial tcp: lookup wrong"},
		{"wrong user", exasol.NewConfig("wronguser", "exasol").Host(suite.host).Port(suite.port).ValidateServerCertificate(false), errorMsgAuthFailed},
		{"wrong password", exasol.NewConfig("sys", "wrongPassword").Host(suite.host).Port(suite.port).ValidateServerCertificate(false), errorMsgAuthFailed},

		{"wrong refresh token", exasol.NewConfigWithRefreshToken("invalid.refresh.token").Host(suite.host).Port(suite.port).ValidateServerCertificate(false), errorMsgTokenAuthFailed},
		{"wrong access token", exasol.NewConfigWithAccessToken("invalid.access.token").Host(suite.host).Port(suite.port).ValidateServerCertificate(false), errorMsgTokenAuthFailed},

		{"valid credentials", suite.createDefaultConfig(), noError},
		{"multiple invalid hostnames", suite.createDefaultConfig().Host("wrong0,wrong1,wrong2,wrong3,wrong4,wrong5," + suite.host), noError},

		{"compression on", suite.createDefaultConfig().Compression(true), noError},
		{"compression off", suite.createDefaultConfig().Compression(false), noError},

		{"encryption on", suite.createDefaultConfig().Encryption(true), noError},
		{"encryption off", suite.createDefaultConfig().Encryption(false), errorMsgEncryptionOff},

		{"don't validate cert, no fingerprint", suite.createDefaultConfig().ValidateServerCertificate(false).CertificateFingerprint(""), noError},
		{"don't validate cert, wrong fingerprint", suite.createDefaultConfig().ValidateServerCertificate(false).CertificateFingerprint(wrongFingerprint), errorMsgWrongFingerprint},
		{"don't validate cert, correct fingerprint", suite.createDefaultConfig().ValidateServerCertificate(false).CertificateFingerprint(actualFingerprint), noError},

		{"validate cert, no fingerprint", suite.createDefaultConfig().ValidateServerCertificate(true).CertificateFingerprint(""), errorMsgCertWrongHost},
		{"validate cert, wrong fingerprint", suite.createDefaultConfig().ValidateServerCertificate(true).CertificateFingerprint(wrongFingerprint), errorMsgWrongFingerprint},
		{"validate cert, correct fingerprint", suite.createDefaultConfig().ValidateServerCertificate(true).CertificateFingerprint(actualFingerprint), noError},
	} {
		suite.Run(fmt.Sprintf("Test%02d %s", i, testCase.description), func() {
			database := suite.openConnection(testCase.config)
			defer database.Close()
			err := database.Ping()
			if testCase.expectedError == "" {
				suite.NoError(err)
				if err == nil {
					rows, err := database.Query("SELECT 2 FROM DUAL")
					suite.NoError(err)
					columns, err := rows.Columns()
					suite.NoError(err)
					suite.Equal("2", columns[0])
					suite.assertSingleValueResult(rows, "2")
				}
			} else {
				suite.Error(err)
				suite.Contains(err.Error(), testCase.expectedError)
			}
		})
	}
}

func (suite *IntegrationTestSuite) getActualCertificateFingerprint() string {
	database := suite.openConnection(suite.createDefaultConfig().CertificateFingerprint("wrongFingerprint"))
	defer database.Close()
	err := database.Ping()
	suite.Error(err)
	re := regexp.MustCompile(`E-EGOD-10: the server's certificate fingerprint '([0-9a-z]{64})' does not match the expected fingerprint 'wrongFingerprint'`)
	submatches := re.FindStringSubmatch(err.Error())
	suite.Equal(2, len(submatches), "Error message %q does not match %q", err, re)
	return submatches[1]
}

func (suite *IntegrationTestSuite) TestExecAndQuery() {
	database := suite.openConnection(suite.createDefaultConfig())
	schemaName := "TEST_SCHEMA_1"
	_, _ = database.Exec("CREATE SCHEMA " + schemaName)
	defer suite.cleanup(database, schemaName)
	_, _ = database.Exec("CREATE TABLE " + schemaName + ".TEST_TABLE(x INT)")
	_, _ = database.Exec("INSERT INTO " + schemaName + ".TEST_TABLE VALUES (15)")
	rows, _ := database.Query("SELECT x FROM " + schemaName + ".TEST_TABLE")
	suite.assertSingleValueResult(rows, "15")
}

func (suite *IntegrationTestSuite) TestFetch() {
	database := suite.openConnection(suite.createDefaultConfig().FetchSize(20))
	schemaName := "TEST_SCHEMA_FETCH"
	_, _ = database.Exec("CREATE SCHEMA " + schemaName)
	defer suite.cleanup(database, schemaName)
	_, _ = database.Exec("CREATE TABLE " + schemaName + ".TEST_TABLE(x INT)")
	data := make([]string, 0)
	for i := 0; i < 10000; i++ {
		data = append(data, fmt.Sprintf("(%d)", i))
	}
	_, _ = database.Exec("INSERT INTO " + schemaName + ".TEST_TABLE VALUES " + strings.Join(data, ","))
	rows, _ := database.Query("SELECT x FROM " + schemaName + ".TEST_TABLE GROUP BY x ORDER BY x")
	result := make([]int, 0)
	counter := 0
	for rows.Next() {
		var x int
		if err := rows.Scan(&x); err != nil {
			// Check for a scan error.
			// Query rows will be closed with defer.
			log.Fatal(err)
		}
		suite.Equal(data[counter], fmt.Sprintf("(%d)", x))
		result = append(result, x)
		counter++
	}
	suite.Equal(10000, len(result))
}

func (suite *IntegrationTestSuite) TestExecuteWithError() {
	database := suite.openConnection(suite.createDefaultConfig())
	defer database.Close()
	_, err := database.Exec("CREATE SCHEMAA TEST_SCHEMA")
	suite.Error(err)
	suite.ErrorContains(err, "syntax error")
}

func (suite *IntegrationTestSuite) TestQueryWithError() {
	database := suite.openConnection(suite.createDefaultConfig())
	schemaName := "TEST_SCHEMA_2"
	_, _ = database.Exec("CREATE SCHEMA " + schemaName)
	defer suite.cleanup(database, schemaName)
	_, err := database.Query("SELECT x FROM " + schemaName + ".TEST_TABLE")
	suite.Error(err)
	suite.ErrorContains(err, "object TEST_SCHEMA_2.TEST_TABLE not found")
}

func (suite *IntegrationTestSuite) TestPreparedStatement() {
	database := suite.openConnection(suite.createDefaultConfig())
	schemaName := "TEST_SCHEMA_3"
	_, _ = database.Exec("CREATE SCHEMA " + schemaName)
	defer suite.cleanup(database, schemaName)
	_, _ = database.Exec("CREATE TABLE " + schemaName + ".TEST_TABLE(x INT)")
	preparedStatement, _ := database.Prepare("INSERT INTO " + schemaName + ".TEST_TABLE VALUES (?)")
	_, _ = preparedStatement.Exec(15)
	preparedStatement, _ = database.Prepare("SELECT x FROM " + schemaName + ".TEST_TABLE WHERE x = ?")
	rows, _ := preparedStatement.Query(15)
	suite.assertSingleValueResult(rows, "15")
}

func (suite *IntegrationTestSuite) TestQueryWithValuesAndContext() {
	database := suite.openConnection(suite.createDefaultConfig())
	schemaName := "TEST_SCHEMA_3_2"
	_, _ = database.ExecContext(context.Background(), "CREATE SCHEMA "+schemaName)
	defer suite.cleanup(database, schemaName)
	_, _ = database.ExecContext(context.Background(), "CREATE TABLE "+schemaName+".TEST_TABLE(x INT)")
	result, _ := database.ExecContext(context.Background(), "INSERT INTO "+schemaName+".TEST_TABLE VALUES (?)", 15)
	affectedRow, _ := result.RowsAffected()
	suite.Assert().Equal(int64(1), affectedRow)
	rows, _ := database.QueryContext(context.Background(), "SELECT x FROM "+schemaName+".TEST_TABLE WHERE x = ?", 15)
	suite.assertSingleValueResult(rows, "15")
}

func (suite *IntegrationTestSuite) TestQueryWithValuesAndNoContext() {
	database := suite.openConnection(suite.createDefaultConfig())
	schemaName := "TEST_SCHEMA_3_3"
	_, _ = database.Exec("CREATE SCHEMA " + schemaName)
	defer suite.cleanup(database, schemaName)
	_, _ = database.Exec("CREATE TABLE " + schemaName + ".TEST_TABLE(x INT)")
	result, _ := database.Exec("INSERT INTO "+schemaName+".TEST_TABLE VALUES (?)", 15)
	affectedRow, _ := result.RowsAffected()
	suite.Assert().Equal(int64(1), affectedRow)
	rows, _ := database.Query("SELECT x FROM "+schemaName+".TEST_TABLE WHERE x = ?", 15)
	suite.assertSingleValueResult(rows, "15")
}

func (suite *IntegrationTestSuite) TestBeginAndCommit() {
	database := suite.openConnection(suite.createDefaultConfig().Autocommit(false))
	schemaName := "TEST_SCHEMA_4"
	transaction, _ := database.Begin()
	_, _ = transaction.Exec("CREATE SCHEMA " + schemaName)
	defer suite.cleanup(database, schemaName)
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
	defer suite.cleanup(database, schemaName)
	_, _ = transaction.Exec("CREATE TABLE " + schemaName + ".TEST_TABLE(x INT)")
	_, _ = transaction.Exec("INSERT INTO " + schemaName + ".TEST_TABLE VALUES (15)")
	_ = transaction.Rollback()
	_, err := database.Query("SELECT x FROM " + schemaName + ".TEST_TABLE")
	suite.Error(err)
	suite.ErrorContains(err, "object "+schemaName+".TEST_TABLE not found")
}

func (suite *IntegrationTestSuite) TestPingWithContext() {
	database := suite.openConnection(suite.createDefaultConfig())
	defer database.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	suite.NoError(database.PingContext(ctx))
	cancel()
}

func (suite *IntegrationTestSuite) TestExecuteAndQueryWithContext() {
	database := suite.openConnection(suite.createDefaultConfig())
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	schemaName := "TEST_SCHEMA_6"
	_, _ = database.ExecContext(ctx, "CREATE SCHEMA "+schemaName)
	defer suite.cleanup(database, schemaName)
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
	defer suite.cleanup(database, schemaName)
	cancel()
	_, err := transaction.ExecContext(ctx, "CREATE TABLE "+schemaName+".TEST_TABLE(x INT)")
	suite.EqualError(err, "context canceled")
}

func (suite *IntegrationTestSuite) TestSimpleImportStatement() {
	database := suite.openConnection(suite.createDefaultConfig())
	ctx := context.Background()
	schemaName := "TEST_SCHEMA_8"
	tableName := "TEST_TABLE"
	_, _ = database.ExecContext(ctx, "CREATE SCHEMA "+schemaName)
	defer suite.cleanup(database, schemaName)
	_, _ = database.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s.%s (a int , b VARCHAR(20))", schemaName, tableName))

	result, err := database.ExecContext(ctx, fmt.Sprintf(`IMPORT INTO %s.%s FROM LOCAL CSV FILE '../testData/data.csv' COLUMN SEPARATOR = ';' ENCODING = 'UTF-8' ROW SEPARATOR = 'LF'`, schemaName, tableName))
	suite.NoError(err, "import should be successful")
	affectedRows, _ := result.RowsAffected()
	suite.Equal(int64(3), affectedRows)

	rows, _ := database.Query(fmt.Sprintf("SELECT * FROM %s.%s", schemaName, tableName))
	suite.assertTableResult(rows,
		[]string{"A", "B"},
		[][]interface{}{
			{float64(11), "test1"},
			{float64(12), "test2"},
			{float64(13), "test3"},
		},
	)
}

func (suite *IntegrationTestSuite) TestSimpleImportStatementBigFile() {
	database := suite.openConnection(suite.createDefaultConfig())
	ctx := context.Background()
	schemaName := "TEST_SCHEMA_8"
	tableName := "TEST_TABLE_HUGE"

	exampleData := time.Now().Format(time.RFC3339)
	file, err := suite.generateExampleCSVFile(exampleData, 20000)
	suite.NoError(err, "should generate csv file")
	defer os.Remove(file.Name())

	_, _ = database.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+schemaName)
	defer suite.cleanup(database, schemaName)
	_, _ = database.ExecContext(ctx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (a int , b VARCHAR(100), c VARCHAR(100), d VARCHAR(100), e VARCHAR(100), f VARCHAR(100), g VARCHAR(100))", schemaName, tableName))

	result, err := database.ExecContext(ctx, fmt.Sprintf(`IMPORT INTO %s.%s FROM LOCAL CSV FILE '%s' COLUMN SEPARATOR = ',' ENCODING = 'UTF-8' ROW SEPARATOR = 'LF'`, schemaName, tableName, file.Name()))
	suite.NoError(err, "import should be successful")

	affectedRows, err := result.RowsAffected()
	suite.NoError(err, "getting rows affected should be successful")
	suite.Equal(int64(20000), affectedRows)

	rows, err := database.Query(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", schemaName, tableName))
	suite.NoError(err, "count query should work")
	suite.assertTableResult(rows, []string{"COUNT(*)"},
		[][]interface{}{
			{float64(20000)},
		},
	)

	rows, err = database.Query(fmt.Sprintf("SELECT * FROM %s.%s ORDER BY a LIMIT 3 ", schemaName, tableName))
	suite.NoError(err, "query should be working")
	suite.assertTableResult(rows,
		[]string{"A", "B", "C", "D", "E", "F", "G"},
		[][]interface{}{
			{float64(0), exampleData, exampleData, exampleData, exampleData, exampleData, exampleData},
			{float64(1), exampleData, exampleData, exampleData, exampleData, exampleData, exampleData},
			{float64(2), exampleData, exampleData, exampleData, exampleData, exampleData, exampleData},
		},
	)
}

// See https://github.com/exasol/exasol-driver-go/issues/79
func (suite *IntegrationTestSuite) TestNoLeakingGoRoutineDuringFileImport() {
	database := suite.openConnection(suite.createDefaultConfig())
	ctx := context.Background()
	schemaName := "TEST_SCHEMA_LEAK"
	tableName := "TEST_TABLE_HUGE"

	exampleData := time.Now().Format(time.RFC3339)
	file, err := suite.generateExampleCSVFile(exampleData, 20000)
	suite.NoError(err, "should generate csv file")

	defer os.Remove(file.Name())
	defer suite.cleanup(database, schemaName)

	_, _ = database.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+schemaName)

	_, _ = database.ExecContext(ctx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (a int , b VARCHAR(100), c VARCHAR(100), d VARCHAR(100), e VARCHAR(100), f VARCHAR(100), g VARCHAR(100))", schemaName, tableName))

	_, err = database.ExecContext(ctx, fmt.Sprintf(`IMPORT INTO %s.%s FROM LOCAL CSV FILE '%s' COLUMNS SEPARATOR = ',' ENCODING = 'UTF-8' ROW SEPARATOR = 'LF'`, schemaName, tableName, file.Name()))
	suite.Error(err, "import should be failing")
}

func (suite *IntegrationTestSuite) generateExampleCSVFile(exampleData string, amount int) (*os.File, error) {
	file, err := os.CreateTemp("", "data*.csv")
	if err != nil {
		log.Fatal(err)
	}

	writer := csv.NewWriter(file)
	writer.Comma = ','

	for i := 0; i < amount; i++ {
		err := writer.Write([]string{fmt.Sprint(i), exampleData, exampleData, exampleData, exampleData, exampleData, exampleData})
		suite.NoError(err, "adding example data should be working")
	}
	writer.Flush()
	return file, err
}

func (suite *IntegrationTestSuite) TestMultiImportStatement() {
	database := suite.openConnection(suite.createDefaultConfig())
	ctx := context.Background()
	schemaName := "TEST_SCHEMA_9"
	tableName := "TEST_TABLE"
	_, _ = database.ExecContext(ctx, "CREATE SCHEMA "+schemaName)
	defer suite.cleanup(database, schemaName)
	_, _ = database.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s.%s (a int , b VARCHAR(20))", schemaName, tableName))

	result, err := database.ExecContext(ctx, fmt.Sprintf(`IMPORT INTO %s.%s FROM LOCAL CSV FILE '../testData/data.csv' FILE '../testData/data_part2.csv' COLUMN SEPARATOR = ';' ENCODING = 'UTF-8' ROW SEPARATOR = 'LF'`, schemaName, tableName))
	suite.NoError(err, "import should be successful")
	affectedRows, _ := result.RowsAffected()
	suite.Equal(int64(6), affectedRows)

	rows, _ := database.Query(fmt.Sprintf("SELECT * FROM %s.%s", schemaName, tableName))
	suite.assertTableResult(rows,
		[]string{"A", "B"},
		[][]interface{}{
			{float64(11), "test1"},
			{float64(12), "test2"},
			{float64(13), "test3"},
			{float64(21), "test4"},
			{float64(22), "test5"},
			{float64(23), "test6"},
		},
	)
}

func (suite *IntegrationTestSuite) assertTableResult(rows *sql.Rows, expectedCols []string, expectedRows [][]interface{}) {
	i := 0
	cols, _ := rows.Columns()
	suite.Equal(expectedCols, cols)
	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}
		err := rows.Scan(columnPointers...)
		onError(err)

		suite.Equal(expectedRows[i], columns)
		i = i + 1
	}
}

func (suite *IntegrationTestSuite) TestImportStatementWithCRFile() {
	database := suite.openConnection(suite.createDefaultConfig())
	ctx := context.Background()
	schemaName := "TEST_SCHEMA_10"
	tableName := "TEST_TABLE"
	_, _ = database.ExecContext(ctx, "CREATE SCHEMA "+schemaName)
	defer suite.cleanup(database, schemaName)
	_, _ = database.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s.%s (a int , b VARCHAR(20))", schemaName, tableName))

	result, err := database.ExecContext(ctx, fmt.Sprintf(`IMPORT INTO %s.%s FROM LOCAL CSV FILE '../testData/data_cr.csv' COLUMN SEPARATOR = ';' ENCODING = 'UTF-8' ROW SEPARATOR = 'CR'`, schemaName, tableName))
	suite.NoError(err, "import should be successful")
	affectedRows, _ := result.RowsAffected()
	suite.Equal(int64(3), affectedRows)

	rows, _ := database.Query(fmt.Sprintf("SELECT * FROM %s.%s", schemaName, tableName))
	suite.assertTableResult(rows,
		[]string{"A", "B"},
		[][]interface{}{
			{float64(11), "test1"},
			{float64(12), "test2"},
			{float64(13), "test3"},
		},
	)
}

func (suite *IntegrationTestSuite) TestCustomClientName() {
	expectedClientName := "My Client Name"
	database := suite.openConnection(suite.createDefaultConfig().ClientName(expectedClientName))
	defer database.Close()
	rows, err := database.Query("select client from exa_user_sessions where session_id = current_session")
	suite.NoError(err)
	suite.True(rows.Next())
	var client string
	err = rows.Scan(&client)
	suite.NoError(err)
	suite.Equal(expectedClientName+" (unknown version)", client)
}

func (suite *IntegrationTestSuite) TestClientMetadataWithDefaultClientName() {
	expectedOsUser, err := user.Current()
	suite.NoError(err)
	suite.NotNil(expectedOsUser)
	database := suite.openConnection(suite.createDefaultConfig())
	defer database.Close()
	rows, err := database.Query("select client, driver, os_user, os_name from exa_user_sessions where session_id = current_session")
	suite.NoError(err)
	suite.True(rows.Next())
	var client, driver, osUser, osName string
	err = rows.Scan(&client, &driver, &osUser, &osName)
	suite.NoError(err)
	suite.Equal("Go client (unknown version)", client)
	compareDriverVersion(suite.T(), driver)
	suite.Equal(expectedOsUser.Username, osUser)
	suite.Equal(runtime.GOOS, osName)
}

func (suite *IntegrationTestSuite) TestQueryTimeoutExpired() {
	database := suite.openConnection(suite.createDefaultConfig().QueryTimeout(1))
	defer database.Close()
	rows, err := database.Query(`SELECT "$SLEEP"(2)`)
	suite.ErrorContains(err, "E-EGOD-11: execution failed with SQL error code 'R0001' and message 'Query terminated because timeout has been reached.")
	suite.Nil(rows)
}

func (suite *IntegrationTestSuite) assertSingleValueResult(rows *sql.Rows, expected string) {
	rows.Next()
	var testValue string
	err := rows.Scan(&testValue)
	onError(err)
	suite.Equal(expected, testValue)
}

func (suite *IntegrationTestSuite) cleanup(db *sql.DB, schemaName string) {
	_, err := db.Exec("DROP SCHEMA IF EXISTS " + schemaName + " CASCADE")
	suite.NoError(err, "Failed to drop schema "+schemaName)

	suite.NoError(db.Close(), "Failed to close driver ")
}

func (suite *IntegrationTestSuite) TearDownSuite() {
	defer goleak.VerifyNone(suite.T())
	if suite.exasol != nil {
		err := suite.exasol.Stop()
		onError(err)
	}
}

func (suite *IntegrationTestSuite) createDefaultConfig() *dsn.DSNConfigBuilder {
	return exasol.NewConfig("sys", "exasol").ValidateServerCertificate(false).Host(suite.host).Port(suite.port)
}

func (suite *IntegrationTestSuite) openConnection(config *dsn.DSNConfigBuilder) *sql.DB {
	database, err := sql.Open("exasol", config.String())
	if err != nil {
		fmt.Printf("error connecting to database using config %q", config)
		panic(err)
	}
	return database
}

func (suite *IntegrationTestSuite) isExasolVersion8() bool {
	version, err := suite.getExasolMajorVersion()
	if err != nil {
		suite.FailNow("error getting exasol version: " + err.Error())
	}
	return version == "8"
}

func (suite *IntegrationTestSuite) getExasolMajorVersion() (string, error) {
	db, err := suite.exasol.CreateConnection()
	suite.NoError(err)
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
	suite.T().Logf("Got Exasol major version %q", majorVersion)
	return majorVersion, nil
}

func onError(err error) {
	if err != nil {
		log.Printf("Error %s", err)
		panic(err)
	}
}

type projectKeeper struct {
	Version string `yaml:"version"`
}

func compareDriverVersion(t *testing.T, actualVersion string) {
	yamlFile, err := os.ReadFile("../.project-keeper.yml")
	assert.NoError(t, err)
	keeperContent := &projectKeeper{}
	err = yaml.Unmarshal(yamlFile, keeperContent)
	assert.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("exasol-driver-go v%s", keeperContent.Version), strings.TrimRight(actualVersion, " "))
}
