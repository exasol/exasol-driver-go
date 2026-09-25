package itest_test

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/exasol/exasol-driver-go"
	"github.com/exasol/exasol-driver-go/internal/testutil"
	"github.com/exasol/exasol-driver-go/pkg/connection"
	"github.com/exasol/exasol-driver-go/pkg/dsn"
	"github.com/exasol/exasol-driver-go/pkg/integrationTesting"
	"github.com/parquet-go/parquet-go"

	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
	"gopkg.in/yaml.v3"

	"github.com/stretchr/testify/suite"
)

// importDeadline bounds how long an IMPORT statement run through
// execImportWithinDeadline may take. Parquet's pull-based transport and both
// formats' encrypted-channel handshake can deadlock rather than error when the
// driver and server disagree about who acts first, so a call still running
// after this deadline is stuck waiting on a peer that will never answer.
const (
	importDeadline                  = 30 * time.Second
	generateParquetFileErrorMessage = "should generate parquet file"
	generateCSVFileErrorMessage     = "should generate csv file"
	smallParquetRowCount            = 3
	largeParquetRowCount            = 20000
	aIntBVarchar20                  = "a int, b VARCHAR(20)"
	multipleColumns                 = "a int, b VARCHAR(100), c VARCHAR(100), d VARCHAR(100), e VARCHAR(100), f VARCHAR(100), g VARCHAR(100)"
)

func parquetVersionErrorMessage(serverVersion string) string {
	return fmt.Sprintf("E-EGOD-31: local Parquet import requires Exasol version '2025.1.11' or later, but the server reported version '%s'", serverVersion)
}

type IntegrationTestSuite struct {
	suite.Suite
	ctx    context.Context
	exasol *integrationTesting.DbTestSetup
	port   int
	host   string
}

func TestIntegrationSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(IntegrationTestSuite))
}

func (suite *IntegrationTestSuite) SetupSuite() {
	suite.ctx = context.Background()
	var err error
	suite.exasol = integrationTesting.StartDbSetup(&suite.Suite)
	connectionInfo := suite.exasol.ConnectionInfo
	if err != nil {
		suite.FailNowf("setup failed", "failed to get connection info: %v", err)
	}
	suite.port = connectionInfo.Port
	suite.host = connectionInfo.Host
}

func (suite *IntegrationTestSuite) TestConnectWithDsn() {
	database, _ := sql.Open("exasol", fmt.Sprintf("exa:%s:%d;user=sys;password=exasol;validateservercertificate=0", suite.host, suite.port))
	defer database.Close()
	suite.assertQueryWorks(database)
}

func (suite *IntegrationTestSuite) assertQueryWorks(database *sql.DB) {
	rows, err := database.Query("SELECT 2 FROM DUAL")
	suite.NoError(err)
	columns, err := rows.Columns()
	suite.NoError(err)
	suite.Equal("2", columns[0])
	suite.assertSingleValueResult(rows, "2")
}

func (suite *IntegrationTestSuite) TestConnectWithUrlPath() {
	database, _ := sql.Open("exasol", exasol.NewConfig("sys", "exasol").Host(suite.host).Port(suite.port).UrlPath("/v1/databases/db123/connect?ticket=123").ValidateServerCertificate(false).String())
	defer database.Close()
	suite.assertQueryWorks(database)
}

func (suite *IntegrationTestSuite) TestConnectionParameters() {
	actualFingerprint := suite.getActualCertificateFingerprint()
	const wrongFingerprint = "wrongFingerprint"
	const noError = ""

	errorMsgWrongFingerprint := fmt.Sprintf("E-EGOD-10: the server's certificate fingerprint '%s' does not match the expected fingerprint '%s'", actualFingerprint, wrongFingerprint)
	const errorMsgAuthFailed = "E-EGOD-11: execution failed with SQL error code '08004' and message 'Connection exception - authentication failed.'"
	const errorMsgTokenAuthFailed = "E-EGOD-11: execution failed with SQL error code '08004' and message 'Connection exception - authentication failed'"
	const errorMsgCertWrongHost = "tls: failed to verify certificate: x509:"

	var errorMsgEncryptionOff string
	if suite.exasol.IsExasolVersion8() {
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
					suite.assertQueryWorks(database)
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

func unquoted(value string) string {
	return strings.ReplaceAll(value, "\"", "")
}

func insertInto(table string) string {
	return "INSERT INTO " + table
}

type tableSpec struct {
	schema  string
	name    string
	columns string
}

func (t *tableSpec) fqn() string {
	return fmt.Sprintf("%q.%q", t.schema, t.name)
}

func (t *tableSpec) create() string {
	return fmt.Sprintf("CREATE TABLE %s (%s)", t.fqn(), t.columns)
}

func (t *tableSpec) insert(value any) string {
	return fmt.Sprintf("%s VALUES (%v)", insertInto(t.fqn()), value)
}

func (t *tableSpec) selectX(where any) string {
	selectX := "SELECT x FROM " + t.fqn()
	if where == "" {
		return selectX
	} else {
		return fmt.Sprintf("%s WHERE x = %s", selectX, where)
	}
}

func xIntTable(schema string) tableSpec {
	return tableSpec{
		schema:  schema,
		name:    "TEST_TABLE",
		columns: "x INT",
	}
}

func createXIntTable(transaction *sql.Tx, schema string) tableSpec {
	table := xIntTable(schema)
	_, _ = transaction.Exec(table.create())
	_, _ = transaction.Exec(table.insert(15))
	return table
}

func (suite *IntegrationTestSuite) TestExecAndQuery() {
	database := suite.openConnection(suite.createDefaultConfig())
	schemaName := "TEST_SCHEMA_1"
	_ = suite.createDbSchema(database, schemaName, "", "")
	table := xIntTable(schemaName)
	database.ExecContext(suite.ctx, table.create())
	defer suite.cleanup(database, schemaName)
	database.ExecContext(suite.ctx, table.insert(15))
	rows, _ := database.Query(table.selectX(""))
	suite.assertSingleValueResult(rows, "15")
}

func (suite *IntegrationTestSuite) TestFetch() {
	database := suite.openConnection(suite.createDefaultConfig().FetchSize(20))
	schemaName := "TEST_SCHEMA_FETCH"
	_ = suite.createDbSchema(database, schemaName, "", "")
	table := xIntTable(schemaName)
	database.ExecContext(suite.ctx, table.create())
	defer suite.cleanup(database, schemaName)
	data := make([]string, 0)
	for i := 0; i < 10000; i++ {
		data = append(data, fmt.Sprintf("(%d)", i))
	}
	_, _ = database.Exec(insertInto(table.fqn()) + " VALUES " + strings.Join(data, ","))
	rows, _ := database.Query(table.selectX("") + " GROUP BY x ORDER BY x")
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

// https://github.com/exasol/exasol-driver-go/issues/113
func (suite *IntegrationTestSuite) TestFetchLargeInteger() {
	database := suite.openConnection(suite.createDefaultConfig())
	defer database.Close()
	number := 100000000
	rows, err := database.Query(fmt.Sprintf("SELECT %d", number))
	suite.NoError(err)
	suite.True(rows.Next())
	var result int64
	err = rows.Scan(&result)
	suite.NoError(err)
	defer rows.Close()
	suite.Equal(int64(number), result)
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
	_ = suite.createDbSchema(database, schemaName, "", "")
	defer suite.cleanup(database, schemaName)
	table := xIntTable(schemaName)
	_, err := database.Query(table.selectX(""))
	suite.Error(err)
	suite.ErrorContains(err, "object "+unquoted(table.fqn())+" not found")
}

func (suite *IntegrationTestSuite) TestPreparedStatement() {
	database := suite.openConnection(suite.createDefaultConfig())
	schemaName := "TEST_SCHEMA_3"
	_ = suite.createDbSchema(database, schemaName, "", "")
	table := xIntTable(schemaName)
	database.ExecContext(suite.ctx, table.create())
	defer suite.cleanup(database, schemaName)
	preparedStatement, _ := database.Prepare(table.insert("?"))
	_, _ = preparedStatement.Exec(15)
	preparedStatement, _ = database.Prepare(table.selectX("?"))
	rows, _ := preparedStatement.Query(15)
	suite.assertSingleValueResult(rows, "15")
}

func (suite *IntegrationTestSuite) TestPreparedStatementWithoutArgs() {
	database := suite.openConnection(suite.createDefaultConfig())
	schemaName := "TEST_SCHEMA_3"
	_ = suite.createDbSchema(database, schemaName, "", "")
	defer suite.cleanup(database, schemaName)
	table := xIntTable(schemaName)
	database.ExecContext(suite.ctx, table.create())
	preparedStatement, _ := database.Prepare(table.insert(25))
	_, _ = preparedStatement.Exec()
	preparedStatement, _ = database.Prepare(table.selectX(25))
	rows, _ := preparedStatement.Query()
	suite.assertSingleValueResult(rows, "25")
}

var dereferenceString = func(v any) any { return *(v.(*string)) }
var dereferenceFloat32 = func(v any) any { return *(v.(*float32)) }
var dereferenceFloat64 = func(v any) any { return *(v.(*float64)) }
var dereferenceInt32 = func(v any) any { return *(v.(*int32)) }
var dereferenceInt64 = func(v any) any { return *(v.(*int64)) }
var dereferenceInt = func(v any) any { return *(v.(*int)) }
var dereferenceBool = func(v any) any { return *(v.(*bool)) }

func (suite *IntegrationTestSuite) TestQueryDataTypesCast() {
	database := suite.openConnection(suite.createDefaultConfig())
	defer database.Close()

	for i, testCase := range []struct {
		testDescription string
		sqlValue        string
		sqlType         string
		scanDest        any
		expectedValue   any
		dereference     func(any) any
	}{
		// DECIMAL
		{"decimal to int64", "1", "DECIMAL(18,0)", new(int64), int64(1), dereferenceInt64},
		{"large decimal to int64", "100000000", "DECIMAL(18,0)", new(int64), int64(100000000), dereferenceInt64},
		{"large negative decimal to int64", "-100000000", "DECIMAL(18,0)", new(int64), int64(-100000000), dereferenceInt64},
		{"decimal to int", "1", "DECIMAL(18,0)", new(int), 1, dereferenceInt},
		{"decimal to float", "1", "DECIMAL(18,0)", new(float64), 1.0, dereferenceFloat64},
		{"decimal to string", "1", "DECIMAL(18,0)", new(string), "1", dereferenceString},
		{"max int64", fmt.Sprintf("%d", math.MaxInt64), "DECIMAL(36,0)", new(int64), int64(math.MaxInt64), dereferenceInt64},
		{"min int64", fmt.Sprintf("%d", math.MinInt64), "DECIMAL(36,0)", new(int64), int64(math.MinInt64), dereferenceInt64},
		{"decimal to float64", "2.2", "DECIMAL(18,2)", new(float64), 2.2, dereferenceFloat64},
		{"decimal to string", "2.2", "DECIMAL(18,2)", new(string), "2.2", dereferenceString},

		{"double to float64", "3.3", "DOUBLE PRECISION", new(float64), 3.3, dereferenceFloat64},
		{"double to float64", "-3.3", "DOUBLE PRECISION", new(float64), -3.3, dereferenceFloat64},
		{"double to float64", "1.7976e+308", "DOUBLE PRECISION", new(float64), 1.7975999999999999e+308, dereferenceFloat64},
		{"double to float64", "-1.7976e+308", "DOUBLE PRECISION", new(float64), -1.7975999999999999e+308, dereferenceFloat64},
		{"double to float64", fmt.Sprintf("%g", math.SmallestNonzeroFloat64), "DOUBLE PRECISION", new(float64), math.SmallestNonzeroFloat64, dereferenceFloat64},
		{"double to float32", fmt.Sprintf("%g", math.MaxFloat32), "DOUBLE PRECISION", new(float32), float32(3.4028235e+38), dereferenceFloat32},
		{"double to float32", fmt.Sprintf("%g", math.SmallestNonzeroFloat32), "DOUBLE PRECISION", new(float32), float32(1e-45), dereferenceFloat32},
		{"double to string", "3.3", "DOUBLE PRECISION", new(string), "3.3", dereferenceString},

		{"varchar to string", "'text'", "VARCHAR(10)", new(string), "text", dereferenceString},
		{"char to string", "'text'", "CHAR(10)", new(string), "text      ", dereferenceString},
		{"date to string", "'2024-06-18'", "DATE", new(string), "2024-06-18", dereferenceString},
		{"timestamp to string", "'2024-06-18 17:22:13.123456'", "TIMESTAMP", new(string), "2024-06-18 17:22:13.123000", dereferenceString},
		{"timestamp with local time zone to string", "'2024-06-18 17:22:13.123456'", "TIMESTAMP WITH LOCAL TIME ZONE", new(string), "2024-06-18 17:22:13.123000", dereferenceString},
		{"geometry to string", "'point(1 2)'", "GEOMETRY", new(string), "POINT (1 2)", dereferenceString},
		{"interval ytm to string", "'5-3'", "INTERVAL YEAR TO MONTH", new(string), "+05-03", dereferenceString},
		{"interval dts to string", "'2 12:50:10.123'", "INTERVAL DAY TO SECOND", new(string), "+02 12:50:10.123", dereferenceString},
		{"hashtype to string", "'550e8400-e29b-11d4-a716-446655440000'", "HASHTYPE", new(string), "550e8400e29b11d4a716446655440000", dereferenceString},
		{"bool to bool", "true", "BOOLEAN", new(bool), true, dereferenceBool},
		{"bool to string", "false", "BOOLEAN", new(string), "false", dereferenceString},
	} {
		suite.Run(fmt.Sprintf("Cast Test %02d %s: %s", i, testCase.testDescription, testCase.sqlType), func() {
			rows, err := database.Query(fmt.Sprintf("SELECT CAST(%s AS %s)", testCase.sqlValue, testCase.sqlType))
			suite.NoError(err, "failed to select")
			defer rows.Close()
			suite.True(rows.Next(), "should have one row")
			err = rows.Scan(testCase.scanDest)
			suite.NoError(err, "failed to scan rows")
			val := testCase.scanDest
			suite.Equal(testCase.expectedValue, testCase.dereference(val))
		})
	}
}

func (suite *IntegrationTestSuite) TestPreparedStatementArgsConverted() {
	database := suite.openConnection(suite.createDefaultConfig().Autocommit(false))
	schemaName := "DATATYPE_TEST"
	_ = suite.createDbSchema(database, schemaName, "", "")
	defer suite.cleanup(database, schemaName)

	type TestCase struct {
		sqlValue      any
		sqlType       string
		scanDest      any
		expectedValue any
		dereference   func(any) any
	}
	int64TestCase := func(sqlValue any, sqlType string, expectedValue int64) TestCase {
		return TestCase{sqlValue: sqlValue, sqlType: sqlType, scanDest: new(int64), expectedValue: expectedValue, dereference: dereferenceInt64}
	}
	int32TestCase := func(sqlValue any, sqlType string, expectedValue int32) TestCase {
		return TestCase{sqlValue: sqlValue, sqlType: sqlType, scanDest: new(int32), expectedValue: expectedValue, dereference: dereferenceInt32}
	}
	float64TestCase := func(sqlValue any, sqlType string, expectedValue float64) TestCase {
		return TestCase{sqlValue: sqlValue, sqlType: sqlType, scanDest: new(float64), expectedValue: expectedValue, dereference: dereferenceFloat64}
	}
	float32TestCase := func(sqlValue any, sqlType string, expectedValue float32) TestCase {
		return TestCase{sqlValue: sqlValue, sqlType: sqlType, scanDest: new(float32), expectedValue: expectedValue, dereference: dereferenceFloat32}
	}
	stringTestCase := func(sqlValue any, sqlType string, expectedValue string) TestCase {
		return TestCase{sqlValue: sqlValue, sqlType: sqlType, scanDest: new(string), expectedValue: expectedValue, dereference: dereferenceString}
	}
	boolTestCase := func(sqlValue any, sqlType string, expectedValue bool) TestCase {
		return TestCase{sqlValue: sqlValue, sqlType: sqlType, scanDest: new(bool), expectedValue: expectedValue, dereference: dereferenceBool}
	}

	for i, testCase := range []TestCase{
		// DECIMAL
		int64TestCase(1, "DECIMAL(18,0)", 1),
		int64TestCase(-1, "DECIMAL(18,0)", -1),
		int64TestCase(1.1, "DECIMAL(18,0)", 1),
		int64TestCase(-1.1, "DECIMAL(18,0)", -1),
		int64TestCase(100000000, "DECIMAL(18,0)", 100000000),
		int64TestCase(-100000000, "DECIMAL(18,0)", -100000000),
		int64TestCase(100000000, "DECIMAL(18,2)", 100000000),
		int64TestCase(-100000000, "DECIMAL(18,2)", -100000000),
		int64TestCase(math.MaxInt64, "DECIMAL(36,0)", math.MaxInt64),
		int64TestCase(math.MinInt64, "DECIMAL(36,0)", math.MinInt64),

		int32TestCase(1, "DECIMAL(18,0)", 1),
		int32TestCase(-1, "DECIMAL(18,0)", -1),
		int32TestCase(1.1, "DECIMAL(18,0)", 1),
		int32TestCase(-1.1, "DECIMAL(18,0)", -1),
		int32TestCase(math.MaxInt32, "DECIMAL(36,0)", math.MaxInt32),
		int32TestCase(math.MinInt32, "DECIMAL(36,0)", math.MinInt32),

		float64TestCase(1, "DECIMAL(18,0)", 1),
		float64TestCase(-1, "DECIMAL(18,0)", -1),
		float64TestCase(1.123, "DECIMAL(18,3)", 1.123),
		float64TestCase(-1.123, "DECIMAL(18,3)", -1.123),
		float64TestCase(100000000.12, "DECIMAL(18,2)", 100000000.12),
		float64TestCase(-100000000.12, "DECIMAL(18,2)", -100000000.12),

		float32TestCase(1, "DECIMAL(18,0)", 1),
		float32TestCase(-1, "DECIMAL(18,0)", -1),
		float32TestCase(1.123, "DECIMAL(18,3)", 1.123),
		float32TestCase(-1.123, "DECIMAL(18,3)", -1.123),

		// DOUBLE
		float64TestCase(3.3, "DOUBLE PRECISION", 3.3),
		float64TestCase(-3.3, "DOUBLE PRECISION", -3.3),
		float64TestCase(3, "DOUBLE PRECISION", 3.0),
		float64TestCase(-3, "DOUBLE PRECISION", -3.0),

		float32TestCase(math.MaxFloat32, "DOUBLE PRECISION", math.MaxFloat32),
		float32TestCase(math.SmallestNonzeroFloat32, "DOUBLE PRECISION", math.SmallestNonzeroFloat32),
		float64TestCase(1.7976e+308, "DOUBLE PRECISION", 1.7975999999999999e+308), // math.MaxFloat64 causes error "data exception - numeric value out of range"
		float64TestCase(math.SmallestNonzeroFloat64, "DOUBLE PRECISION", math.SmallestNonzeroFloat64),

		// VARCHAR
		stringTestCase("text", "VARCHAR(10)", "text"),
		stringTestCase("text", "CHAR(10)", "text      "),
		stringTestCase("2024-06-18", "DATE", "2024-06-18"),
		stringTestCase(time.Date(2024, time.June, 18, 0, 0, 0, 0, time.UTC), "DATE", "2024-06-18"),
		stringTestCase("2024-06-18 17:22:13.123456", "TIMESTAMP", "2024-06-18 17:22:13.123000"),
		stringTestCase(time.Date(2024, time.June, 18, 17, 22, 13, 123456789, time.UTC), "TIMESTAMP", "2024-06-18 17:22:13.123000"),
		stringTestCase("2024-06-18 17:22:13.123456", "TIMESTAMP WITH LOCAL TIME ZONE", "2024-06-18 17:22:13.123000"),
		stringTestCase(time.Date(2024, time.June, 18, 17, 22, 13, 123456789, time.UTC), "TIMESTAMP WITH LOCAL TIME ZONE", "2024-06-18 17:22:13.123000"),
		stringTestCase("point(1 2)", "GEOMETRY", "POINT (1 2)"),
		stringTestCase("5-3", "INTERVAL YEAR TO MONTH", "+05-03"),
		stringTestCase("2 12:50:10.123", "INTERVAL DAY TO SECOND", "+02 12:50:10.123"),
		stringTestCase("550e8400-e29b-11d4-a716-446655440000", "HASHTYPE", "550e8400e29b11d4a716446655440000"),
		boolTestCase(true, "BOOLEAN", true),
		boolTestCase(false, "BOOLEAN", false),
	} {
		suite.Run(fmt.Sprintf("%02d Column type %s accepts values of type %T", i, testCase.sqlType, testCase.sqlValue), func() {
			tableName := fmt.Sprintf("%s.TAB_%d", schemaName, i)
			_, err := database.Exec(fmt.Sprintf("CREATE TABLE %s (col %s)", tableName, testCase.sqlType))
			suite.NoError(err, "failed to create table "+tableName)
			stmt, err := database.Prepare(insertInto(tableName) + " values (?)")
			suite.NoError(err, "failed to insert into table "+tableName)
			_, err = stmt.Exec(testCase.sqlValue)
			suite.NoError(err, "failed to evaluate SQL expression")
			rows, err := database.Query(fmt.Sprintf("select * from %s", tableName))
			suite.NoError(err, "failed to query table "+tableName)
			defer rows.Close()
			suite.True(rows.Next(), "should have at least one row")
			err = rows.Scan(testCase.scanDest)
			suite.NoError(err, "failed to scan rows")
			suite.False(rows.Next(), "should have at most one row")
			val := testCase.scanDest
			suite.Equal(testCase.expectedValue, testCase.dereference(val))
		})
	}
}

func (suite *IntegrationTestSuite) TestPreparedStatementArgsConversionFails() {
	database := suite.openConnection(suite.createDefaultConfig().Autocommit(false))
	schemaName := "DATATYPE_TEST"
	fqn := suite.createDbSchema(database, schemaName, "TAB", "col TIMESTAMP")
	defer suite.cleanup(database, schemaName)
	stmt, err := database.Prepare(insertInto(fqn) + " values (?)")
	suite.NoError(err, "failed to insert into table "+fqn)
	_, err = stmt.Exec(true)
	suite.EqualError(err, "E-EGOD-30: cannot convert argument 'true' of type 'bool' to 'TIMESTAMP' type")
}

func (suite *IntegrationTestSuite) TestScanTypeUnsupported() {
	database := suite.openConnection(suite.createDefaultConfig().Autocommit(false))
	schemaName := "DATATYPE_TEST"
	_ = suite.createDbSchema(database, schemaName, "", "")
	defer suite.cleanup(database, schemaName)

	for i, testCase := range []struct {
		sqlValue      any
		sqlType       string
		scanDest      any
		expectedError string
	}{
		{1.1, "DECIMAL(4,2)", new(int64), `converting driver.Value type string ("1.1") to a int64: invalid syntax`},
		{time.Date(2024, time.June, 18, 17, 22, 13, 123456789, time.UTC), "TIMESTAMP", new(time.Time), `unsupported Scan, storing driver.Value type string into type *time.Time`},
		{time.Date(2024, time.June, 18, 17, 22, 13, 123456789, time.UTC), "TIMESTAMP WITH LOCAL TIME ZONE", new(time.Time), `unsupported Scan, storing driver.Value type string into type *time.Time`},
	} {
		suite.Run(fmt.Sprintf("Scan fails %02d %s", i, testCase.sqlType), func() {
			tableName := fmt.Sprintf("%s.TAB_%d", schemaName, i)
			_, err := database.Exec(fmt.Sprintf("CREATE TABLE %s (col %s)", tableName, testCase.sqlType))
			suite.NoError(err, "failed to create table "+tableName)
			stmt, err := database.Prepare(insertInto(tableName) + " values (?)")
			suite.NoError(err, "failed to prepare statement ")
			_, err = stmt.Exec(testCase.sqlValue)
			suite.NoError(err, "failed to evaluate SQL expression")
			rows, err := database.Query(fmt.Sprintf("select * from %s", tableName))
			suite.NoError(err, "failed to query table")
			defer rows.Close()
			suite.True(rows.Next(), "should have one row")
			err = rows.Scan(testCase.scanDest)
			suite.EqualError(err, `sql: Scan error on column index 0, name "COL": `+testCase.expectedError)
		})
	}
}

// https://github.com/exasol/exasol-driver-go/issues/108
func (suite *IntegrationTestSuite) TestPreparedStatementIntConvertedToFloat() {
	database := suite.openConnection(suite.createDefaultConfig())
	schemaName := "TEST_SCHEMA_3"
	fqn := suite.createDbSchema(database, schemaName, "DUMMY", "a integer, b float")
	defer suite.cleanup(database, schemaName)
	stmt, err := database.Prepare(insertInto(fqn) + " values(?,?)")
	suite.NoError(err, "failed to insert values")
	_, err = stmt.Exec(1, 2)
	suite.NoError(err, "failed to execute statement")
	rows, err := database.Query(fmt.Sprintf("select a || ':' || b from %s", fqn))
	suite.NoError(err, "failed to run query")
	suite.assertSingleValueResult(rows, "1:2")
}

func (suite *IntegrationTestSuite) TestQueryWithValuesAndContext() {
	database := suite.openConnection(suite.createDefaultConfig())
	schemaName := "TEST_SCHEMA_3_2"
	_ = suite.createDbSchema(database, schemaName, "", "")
	table := xIntTable(schemaName)
	database.ExecContext(suite.ctx, table.create())
	defer suite.cleanup(database, schemaName)
	result, _ := database.ExecContext(suite.ctx, table.insert("?"), 25)
	affectedRow, _ := result.RowsAffected()
	suite.Assert().Equal(int64(1), affectedRow)
	rows, _ := database.QueryContext(suite.ctx, table.selectX("?"), 25)
	suite.assertSingleValueResult(rows, "25")
}

func (suite *IntegrationTestSuite) TestQueryWithValuesAndNoContext() {
	database := suite.openConnection(suite.createDefaultConfig())
	schemaName := "TEST_SCHEMA_3_3"
	_ = suite.createDbSchema(database, schemaName, "", "")
	table := xIntTable(schemaName)
	database.ExecContext(suite.ctx, table.create())
	defer suite.cleanup(database, schemaName)
	result, _ := database.Exec(table.insert(15))
	affectedRow, _ := result.RowsAffected()
	suite.Assert().Equal(int64(1), affectedRow)
	rows, _ := database.Query(table.selectX("?"), 15)
	suite.assertSingleValueResult(rows, "15")
}

func (suite *IntegrationTestSuite) TestBeginAndCommit() {
	database := suite.openConnection(suite.createDefaultConfig().Autocommit(false))
	schemaName := "TEST_SCHEMA_4"
	transaction, _ := database.Begin()
	_, _ = transaction.Exec("CREATE SCHEMA " + schemaName)
	defer suite.cleanup(database, schemaName)
	table := createXIntTable(transaction, schemaName)
	_ = transaction.Commit()
	rows, _ := database.Query(table.selectX(""))
	suite.assertSingleValueResult(rows, "15")
}

func (suite *IntegrationTestSuite) TestBeginAndRollback() {
	database := suite.openConnection(suite.createDefaultConfig().Autocommit(false))
	schemaName := "TEST_SCHEMA_5"
	transaction, _ := database.Begin()
	_, _ = transaction.Exec("CREATE SCHEMA " + schemaName)
	table := createXIntTable(transaction, schemaName)
	defer suite.cleanup(database, schemaName)
	_ = transaction.Rollback()
	_, err := database.Query(table.selectX(""))
	suite.Error(err)
	suite.ErrorContains(err, "object "+unquoted(table.fqn())+" not found")
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
	table := xIntTable(schemaName)
	_, _ = database.ExecContext(ctx, table.create())
	_, _ = database.ExecContext(ctx, table.insert(15))
	rows, _ := database.QueryContext(ctx, table.selectX(""))
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
	table := xIntTable(schemaName)
	_, err := transaction.ExecContext(ctx, table.create())
	suite.EqualError(err, "context canceled")
}

func (suite *IntegrationTestSuite) TestSimpleImportStatement() {
	database := suite.openConnection(suite.createDefaultConfig())
	schemaName := "TEST_SCHEMA_8"
	fqn := suite.createDbSchema(database, schemaName, "TEST_TABLE", aIntBVarchar20)
	defer suite.cleanup(database, schemaName)

	result, err := database.ExecContext(
		suite.ctx,
		fmt.Sprintf(`IMPORT INTO %s FROM LOCAL CSV FILE `+
			`'../testData/data.csv' COLUMN SEPARATOR = ';' `+
			`ENCODING = 'UTF-8' ROW SEPARATOR = 'LF'`, fqn),
	)
	suite.NoError(err, "import should be successful")
	affectedRows, _ := result.RowsAffected()
	suite.Equal(int64(3), affectedRows)

	rows, _ := database.Query(fmt.Sprintf("SELECT * FROM %s", fqn))
	suite.assertTableResult(rows,
		[]string{"A", "B"},
		[][]interface{}{
			{int64(11), "test1"},
			{int64(12), "test2"},
			{int64(13), "test3"},
		},
	)
}

func (suite *IntegrationTestSuite) TestSimpleParquetImportStatement() {
	database := suite.openConnection(suite.createDefaultConfig())
	file := suite.generateExampleParquetFile(smallParquetRowCount)
	defer file.Close()
	schemaName := "TEST_SCHEMA_8"
	fqn := suite.createDbSchema(database, schemaName, "TEST_TABLE", aIntBVarchar20)
	defer suite.cleanup(database, schemaName)

	result, err := database.ExecContext(
		suite.ctx,
		fmt.Sprintf(`IMPORT INTO %s FROM LOCAL PARQUET FILE '%s'`, fqn, file.Name()),
	)
	if suite.exasol.SupportsNativeParquetImport() {
		suite.NoError(err, "import should be successful")
		affectedRows, _ := result.RowsAffected()
		suite.Equal(int64(3), affectedRows)

		rows, _ := database.Query(fmt.Sprintf("SELECT * FROM %s", fqn))
		suite.assertTableResult(rows,
			[]string{"A", "B"},
			[][]interface{}{
				{int64(11), "test1"},
				{int64(12), "test2"},
				{int64(13), "test3"},
			},
		)
	} else {
		suite.EqualError(err, parquetVersionErrorMessage(suite.exasol.DbVersion))
	}
}

// A large Parquet file makes Exasol fetch multiple sections of the file and
// exercises the proxy's byte-range serving loop rather than only its setup.
func (suite *IntegrationTestSuite) TestParquetImportStatementBigFile() {
	if !suite.exasol.SupportsNativeParquetImport() {
		suite.T().Skipf("native Parquet import is not supported by Exasol %s", suite.exasol.DbVersion)
	}

	database := suite.openConnection(suite.createDefaultConfig())
	file := suite.generateExampleParquetFile(largeParquetRowCount)
	defer file.Close()

	schemaName := "TEST_SCHEMA_LARGE_PARQUET"
	fqn := suite.createDbSchema(database, schemaName, "TEST_TABLE", aIntBVarchar20)
	defer suite.cleanup(database, schemaName)

	affectedRows, err := suite.execImportWithinDeadline(
		database,
		fmt.Sprintf(`IMPORT INTO %s FROM LOCAL PARQUET FILE '%s'`, fqn, file.Name()),
	)
	suite.NoError(err, "large Parquet import should be successful")
	suite.Equal(int64(largeParquetRowCount), affectedRows)

	rows, err := database.Query(fmt.Sprintf("SELECT COUNT(*) FROM %s", fqn))
	suite.NoError(err, "count query should work")
	suite.assertTableResult(rows, []string{"COUNT(*)"}, [][]interface{}{{int64(largeParquetRowCount)}})

	rows, err = database.Query(fmt.Sprintf("SELECT * FROM %s ORDER BY a LIMIT 3", fqn))
	suite.NoError(err, "sample query should work")
	suite.assertTableResult(rows,
		[]string{"A", "B"},
		[][]interface{}{
			{int64(11), "test1"},
			{int64(12), "test2"},
			{int64(13), "test3"},
		},
	)
}

func (suite *IntegrationTestSuite) TestParquetImportWrongColumns() {
	database := suite.openConnection(suite.createDefaultConfig())
	file := suite.generateExampleParquetFile(smallParquetRowCount)
	defer file.Close()
	schemaName := "TEST_SCHEMA_8"
	fqn := suite.createDbSchema(database, schemaName, "TEST_TABLE", "a int, b VARCHAR(20), c int")
	defer suite.cleanup(database, schemaName)
	_, err := database.ExecContext(suite.ctx, fmt.Sprintf(`IMPORT INTO %s FROM LOCAL PARQUET FILE '%s'`, fqn, file.Name()))

	if suite.exasol.SupportsNativeParquetImport() {
		suite.ErrorContains(err, "E-EGOD-11: execution failed with SQL error code '42636' and message 'ETL-6009: Number of columns in source (=2) and destination (=3)")
	} else {
		suite.EqualError(err, parquetVersionErrorMessage(suite.exasol.DbVersion))
	}
}

func (suite *IntegrationTestSuite) TestParquetImportNotExistentFile() {
	database := suite.openConnection(suite.createDefaultConfig())
	schemaName := "TEST_SCHEMA_8"
	fqn := suite.createDbSchema(database, schemaName, "TEST_TABLE", "a int")
	defer suite.cleanup(database, schemaName)
	_, err := database.ExecContext(suite.ctx, fmt.Sprintf(`IMPORT INTO %s FROM LOCAL PARQUET FILE 'wrong.parquet'`, fqn))

	if suite.exasol.SupportsNativeParquetImport() {
		suite.EqualError(err, "E-EGOD-28: file 'wrong.parquet' not found")
	} else {
		suite.EqualError(err, parquetVersionErrorMessage(suite.exasol.DbVersion))
	}
}

func (suite *IntegrationTestSuite) TestParquetImportStatementInString() {
	database := suite.openConnection(suite.createDefaultConfig())
	schemaName := "TEST_SCHEMA_8"
	fqn := suite.createDbSchema(database, schemaName, "TEST_TABLE", "text VARCHAR(200)")
	defer suite.cleanup(database, schemaName)

	result, err := database.ExecContext(
		suite.ctx,
		insertInto(fqn)+` values ('import into `+
			`{{dest.schema}}.{{dest.table}} ) `+
			`from local parquet file ''{{file.path}}'' ');`,
	)
	suite.NoError(err, "insert should be successful")
	affectedRows, _ := result.RowsAffected()
	suite.Equal(int64(1), affectedRows)

	rows, _ := database.Query(fmt.Sprintf("SELECT * FROM %s", fqn))
	suite.assertTableResult(rows,
		[]string{"TEXT"},
		[][]interface{}{{"import into {{dest.schema}}.{{dest.table}} ) from local parquet file '{{file.path}}' "}},
	)
}

func (suite *IntegrationTestSuite) TestParquetImportMultipleFilesRejected() {
	database := suite.openConnection(suite.createDefaultConfig())
	file := suite.generateExampleParquetFile(smallParquetRowCount)
	defer file.Close()
	schemaName := "TEST_SCHEMA_8"
	fqn := suite.createDbSchema(database, schemaName, "TEST_TABLE", aIntBVarchar20)
	defer suite.cleanup(database, schemaName)

	_, err := database.ExecContext(suite.ctx, fmt.Sprintf(`IMPORT INTO %s FROM LOCAL PARQUET FILE '%s' FILE '%s'`, fqn, file.Name(), file.Name()))
	suite.EqualError(err, "E-EGOD-32: local Parquet import supports exactly one file, but the statement named 2 files")
}

// execImportWithinDeadline returns the affected-row count while bounding import
// execution. Local-import protocol mismatches can otherwise deadlock the test.
func (suite *IntegrationTestSuite) execImportWithinDeadline(database *sql.DB, statement string) (int64, error) {
	return testutil.RunWithDeadline(suite.T(), importDeadline, func() (int64, error) {
		result, err := database.ExecContext(context.Background(), statement)
		if err != nil {
			return 0, err
		}
		affectedRows, _ := result.RowsAffected()
		return affectedRows, nil
	}, "statement %q did not finish within %s", statement, importDeadline)
}

// See https://github.com/exasol/exasol-driver-go/issues/79
//
// A Parquet import waits for the server to ask for the file, so a statement the
// server rejects before asking anything leaves that wait with nothing to end it
// but the driver taking the connection away. Naming a table that does not exist
// gets the statement rejected while the file server is still parked on its very
// first request. The suite's goleak check reports the goroutine left behind if
// the driver fails to close the connection, and the deadline here turns the
// deadlock that would cause into a test failure rather than a job that hangs.
func (suite *IntegrationTestSuite) TestNoLeakingGoRoutineDuringParquetImport() {
	database := suite.openConnection(suite.createDefaultConfig())
	file := suite.generateExampleParquetFile(smallParquetRowCount)
	defer file.Close()
	schemaName := "TEST_SCHEMA_LEAK_PARQUET"
	_ = suite.createDbSchema(database, schemaName, "", "")
	defer suite.cleanup(database, schemaName)

	_, err := suite.execImportWithinDeadline(
		database,
		fmt.Sprintf(`IMPORT INTO %s.MISSING_TABLE FROM LOCAL PARQUET FILE '%s'`, schemaName, file.Name()),
	)
	suite.Error(err, "import into a missing table should be failing")
}

// TestParquetImportServerVersionGate checks the E-EGOD-31 gate itself, distinct
// from TestSimpleParquetImportStatement's happy-path coverage: on a server below
// the threshold it asserts the full message names both the required version and
// the version the server actually reported, and on a supporting server it
// asserts a valid import raises no E-EGOD-31 error at all.
func (suite *IntegrationTestSuite) TestParquetImportServerVersionGate() {
	database := suite.openConnection(suite.createDefaultConfig())
	file := suite.generateExampleParquetFile(smallParquetRowCount)
	defer file.Close()
	schemaName := "TEST_SCHEMA_8"
	fqn := suite.createDbSchema(database, schemaName, "TEST_TABLE", aIntBVarchar20)
	defer suite.cleanup(database, schemaName)

	_, err := database.ExecContext(suite.ctx, fmt.Sprintf(`IMPORT INTO %s FROM LOCAL PARQUET FILE '%s'`, fqn, file.Name()))

	if suite.exasol.SupportsNativeParquetImport() {
		suite.NoError(err, "a supporting server should not raise the version gate error")
	} else {
		suite.EqualError(err, parquetVersionErrorMessage(suite.exasol.DbVersion))
	}
}

// TestServerVersionCapturedAtLogin asserts the driver captures a non-empty
// release version at login and that it matches the version the server this
// suite started actually reports, not just some placeholder value. database/sql
// hides the underlying driver.Conn behind *sql.DB, so this reaches it through
// the standard library's own escape hatch, *sql.Conn.Raw, rather than any
// driver-specific accessor.
func (suite *IntegrationTestSuite) TestServerVersionCapturedAtLogin() {
	database := suite.openConnection(suite.createDefaultConfig())
	defer database.Close()
	sqlConn, err := database.Conn(suite.ctx)
	suite.NoError(err)
	defer sqlConn.Close()

	var serverVersion string
	err = sqlConn.Raw(func(driverConn interface{}) error {
		exasolConn, ok := driverConn.(*connection.Connection)
		if !ok {
			return fmt.Errorf("expected *connection.Connection, got %T", driverConn)
		}
		serverVersion = exasolConn.ServerVersion
		return nil
	})
	suite.NoError(err)

	suite.NotEmpty(serverVersion, "the driver should capture a non-empty release version at login")
	suite.Equal(suite.exasol.DbVersion, serverVersion, "the captured version should match the server this suite started")
}

// TestParquetImportWithEncryptedProxy verifies a local Parquet import over an
// encrypted proxy connection against a live server. The deadline prevents TLS
// handshake regressions from hanging the integration suite.
func (suite *IntegrationTestSuite) TestParquetImportWithEncryptedProxy() {
	database := suite.openConnection(suite.createDefaultConfig().LocalImportEncryption(true))
	file := suite.generateExampleParquetFile(smallParquetRowCount)
	defer file.Close()
	schemaName := "TEST_SCHEMA_ENCRYPTED_PARQUET"
	fqn := suite.createDbSchema(database, schemaName, "TEST_TABLE", aIntBVarchar20)
	defer suite.cleanup(database, schemaName)
	suite.assertImportsAreEncrypted(database)

	affectedRows, err := suite.execImportWithinDeadline(database, fmt.Sprintf(`IMPORT INTO %s FROM LOCAL PARQUET FILE '%s'`, fqn, file.Name()))

	if !suite.exasol.SupportsNativeParquetImport() {
		suite.EqualError(err, parquetVersionErrorMessage(suite.exasol.DbVersion))
		return
	}
	suite.NoError(err, "import over an encrypted proxy connection should be successful")
	suite.Equal(int64(3), affectedRows)

	rows, _ := database.Query(fmt.Sprintf("SELECT * FROM %s", fqn))
	suite.assertTableResult(rows,
		[]string{"A", "B"},
		[][]interface{}{
			{int64(11), "test1"},
			{int64(12), "test2"},
			{int64(13), "test3"},
		},
	)
}

// assertImportsAreEncrypted fails unless the connection that will run the import
// carries the local-import encryption option, so a test of the encrypted channel
// cannot quietly pass over a plaintext one. The option travels from the builder
// through a connection string and back out of the parser before it reaches the
// driver, and only the connection at the far end of that trip decides whether an
// import is encrypted.
func (suite *IntegrationTestSuite) assertImportsAreEncrypted(database *sql.DB) {
	sqlConn, err := database.Conn(suite.ctx)
	suite.NoError(err)
	defer sqlConn.Close()

	suite.NoError(sqlConn.Raw(func(driverConn interface{}) error {
		exasolConn, ok := driverConn.(*connection.Connection)
		if !ok {
			return fmt.Errorf("expected *connection.Connection, got %T", driverConn)
		}
		if !exasolConn.Config.LocalImportEncryption {
			return fmt.Errorf("the connection reports local-import encryption as disabled, so the import would never reach the encrypted channel this test covers")
		}
		return nil
	}))
}

// TestCsvImportWithEncryptedProxy proves the encrypted proxy channel is
// format-agnostic. TestParquetImportWithEncryptedProxy already proved the
// driver answers Exasol's TLS handshake correctly for Parquet's pull-based
// transport, where the serve loop blocks on a read waiting for a request.
// The CSV push transport blocks on a different operation instead: the TLS
// handshake happens inside the write that sends the file's headers, before
// any request is read. A driver that only handled the pull side correctly
// could still hang or fail here, so this repeats the same live-server proof
// for the write path.
//
// CSV import carries no server-version gate of its own, unlike Parquet. Exasol
// 8 cannot parse the PUBLIC KEY clause that pins the encrypted proxy connection,
// so that supported server family runs CSV imports over plaintext instead.
func (suite *IntegrationTestSuite) TestCsvImportWithEncryptedProxy() {
	if !suite.exasol.SupportsPublicKeyPinning() {
		suite.T().Skipf("Exasol %s cannot parse the PUBLIC KEY clause that pins an encrypted local import", suite.exasol.DbVersion)
	}

	database := suite.openConnection(suite.createDefaultConfig().LocalImportEncryption(true))
	schemaName := "TEST_SCHEMA_ENCRYPTED_CSV"
	fqn := suite.createDbSchema(database, schemaName, "TEST_TABLE", aIntBVarchar20)
	defer suite.cleanup(database, schemaName)
	suite.assertImportsAreEncrypted(database)

	affectedRows, err := suite.execImportWithinDeadline(
		database,
		fmt.Sprintf(
			`IMPORT INTO %s FROM LOCAL CSV FILE '../testData/data.csv' `+
				`COLUMN SEPARATOR = ';' ENCODING = 'UTF-8' ROW SEPARATOR = 'LF'`,
			fqn,
		))

	suite.NoError(err, "import over an encrypted proxy connection should be successful")
	suite.Equal(int64(3), affectedRows)

	rows, _ := database.Query(fmt.Sprintf("SELECT * FROM %s", fqn))
	suite.assertTableResult(rows,
		[]string{"A", "B"},
		[][]interface{}{
			{int64(11), "test1"},
			{int64(12), "test2"},
			{int64(13), "test3"},
		},
	)
}

func (suite *IntegrationTestSuite) TestImportStatementWrongColumns() {
	database := suite.openConnection(suite.createDefaultConfig())
	schemaName := "TEST_SCHEMA_8"
	fqn := suite.createDbSchema(database, schemaName, "TEST_TABLE", "a int, b VARCHAR(20), c int")
	defer suite.cleanup(database, schemaName)

	_, err := database.ExecContext(
		suite.ctx,
		fmt.Sprintf(`IMPORT INTO %s FROM LOCAL CSV FILE '../testData/data.csv' `+
			`COLUMN SEPARATOR = ';' ENCODING = 'UTF-8' ROW SEPARATOR = 'LF'`,
			fqn),
	)
	suite.ErrorContains(err, "E-EGOD-11: execution failed with SQL error code '42636' "+
		"and message 'ETL-6009: Number of columns in source (=2) and destination (=3)")
}

func (suite *IntegrationTestSuite) TestImportStatementNotExistentFile() {
	database := suite.openConnection(suite.createDefaultConfig())
	schemaName := "TEST_SCHEMA_8"
	fqn := suite.createDbSchema(database, schemaName, "TEST_TABLE", "a int")
	defer suite.cleanup(database, schemaName)

	_, err := database.ExecContext(suite.ctx, fmt.Sprintf(`IMPORT INTO %s FROM LOCAL CSV FILE 'wrong.csv'`, fqn))
	suite.ErrorContains(err, "E-EGOD-11: execution failed with SQL error code '42636' and message"+
		" 'ETL-5105: Following error occured while reading data from external connection")
}

func (suite *IntegrationTestSuite) TestImportStatementInString() {
	database := suite.openConnection(suite.createDefaultConfig())
	schemaName := "TEST_SCHEMA_8"
	fqn := suite.createDbSchema(database, schemaName, "TEST_TABLE", "text VARCHAR(200)")
	defer suite.cleanup(database, schemaName)

	result, err := database.ExecContext(
		suite.ctx,
		insertInto(fqn)+` values ('import into {{dest.schema}}.{{dest.table}} )`+
			` from local csv file ''{{file.path}}'' ');`,
	)
	suite.NoError(err, "insert should be successful")
	affectedRows, _ := result.RowsAffected()
	suite.Equal(int64(1), affectedRows)

	rows, _ := database.Query(fmt.Sprintf("SELECT * FROM %s", fqn))
	suite.assertTableResult(rows,
		[]string{"TEXT"},
		[][]interface{}{{"import into {{dest.schema}}.{{dest.table}} ) " +
			"from local csv file '{{file.path}}' "}},
	)
}

func (suite *IntegrationTestSuite) TestSimpleImportStatementBigFile() {
	database := suite.openConnection(suite.createDefaultConfig())
	schemaName := "TEST_SCHEMA_8"
	exampleData := time.Now().Format(time.RFC3339)
	file, err := suite.generateExampleCSVFile(exampleData, 20000)
	suite.NoError(err, generateCSVFileErrorMessage)
	defer os.Remove(file.Name())

	fqn := suite.createDbSchema(database, schemaName, "TEST_TABLE_HUGE", multipleColumns)
	defer suite.cleanup(database, schemaName)

	result, err := database.ExecContext(
		suite.ctx,
		fmt.Sprintf(`IMPORT INTO %s FROM LOCAL CSV FILE '%s' `+
			`COLUMN SEPARATOR = ',' ENCODING = 'UTF-8' ROW SEPARATOR = 'LF'`,
			fqn, file.Name()))
	suite.NoError(err, "import should be successful")

	affectedRows, err := result.RowsAffected()
	suite.NoError(err, "getting rows affected should be successful")
	suite.Equal(int64(20000), affectedRows)

	rows, err := database.Query(fmt.Sprintf("SELECT COUNT(*) FROM %s", fqn))
	suite.NoError(err, "count query should work")
	suite.assertTableResult(rows, []string{"COUNT(*)"},
		[][]interface{}{
			{int64(20000)},
		},
	)

	rows, err = database.Query(fmt.Sprintf("SELECT * FROM %s ORDER BY a LIMIT 3 ", fqn))
	suite.NoError(err, "query should be working")
	suite.assertTableResult(rows,
		[]string{"A", "B", "C", "D", "E", "F", "G"},
		[][]interface{}{
			{int64(0), exampleData, exampleData, exampleData, exampleData, exampleData, exampleData},
			{int64(1), exampleData, exampleData, exampleData, exampleData, exampleData, exampleData},
			{int64(2), exampleData, exampleData, exampleData, exampleData, exampleData, exampleData},
		},
	)
}

// TestCancelRunningImport verifies that cancelling the context of an active
// local import also stops the file transfer. The file is deliberately large so
// the import is still transferring when the context is cancelled; a small
// "$SLEEP" query would only exercise statement cancellation, not the transfer
// context used by imports.
func (suite *IntegrationTestSuite) TestCancelRunningImport() {
	database := suite.openConnection(suite.createDefaultConfig())
	defer database.Close()

	schemaName := "TEST_SCHEMA_CANCEL_IMPORT"
	fqn := suite.createDbSchema(database, schemaName, "TEST_TABLE", multipleColumns)
	defer suite.cleanup(database, schemaName)

	file, err := suite.generateExampleCSVFile(time.Now().Format(time.RFC3339), 200000)
	suite.NoError(err, generateCSVFileErrorMessage)
	suite.NoError(file.Close(), "should close generated csv file")
	defer os.Remove(file.Name())

	statement := fmt.Sprintf(`IMPORT INTO %s FROM LOCAL CSV FILE '%s' `+
		`COLUMN SEPARATOR = ',' ENCODING = 'UTF-8' ROW SEPARATOR = 'LF'`, fqn, file.Name())
	cancelCtx, cancel := context.WithCancel(suite.ctx)
	defer cancel()

	result := make(chan error, 1)
	go func() {
		_, execErr := database.ExecContext(cancelCtx, statement)
		result <- execErr
	}()

	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	cancel()

	select {
	case err := <-result:
		suite.ErrorIs(err, context.Canceled)
		suite.Less(time.Since(start), 5*time.Second, "cancelled import should return promptly")
	case <-time.After(5 * time.Second):
		suite.Fail("cancelled import did not return promptly")
	}
}

// See https://github.com/exasol/exasol-driver-go/issues/79
func (suite *IntegrationTestSuite) TestNoLeakingGoRoutineDuringFileImport() {
	database := suite.openConnection(suite.createDefaultConfig())
	schemaName := "TEST_SCHEMA_LEAK"
	fqn := suite.createDbSchema(database, schemaName, "TEST_TABLE", multipleColumns)
	defer suite.cleanup(database, schemaName)

	exampleData := time.Now().Format(time.RFC3339)
	file, err := suite.generateExampleCSVFile(exampleData, 20000)
	suite.NoError(err, generateCSVFileErrorMessage)

	defer os.Remove(file.Name())

	_, err = database.ExecContext(suite.ctx, fmt.Sprintf(`IMPORT INTO %s FROM LOCAL CSV FILE '%s' `+
		`COLUMNS SEPARATOR = ',' ENCODING = 'UTF-8' ROW SEPARATOR = 'LF'`, fqn, file.Name()))
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

type exampleParquetRow struct {
	A int64  `parquet:"a"`
	B string `parquet:"b"`
}

func (suite *IntegrationTestSuite) generateExampleParquetFile(amount int) *os.File {
	rows := make([]exampleParquetRow, 0, amount)
	for i := 0; i < amount; i++ {
		rows = append(rows, exampleParquetRow{
			A: int64(i + 11),
			B: fmt.Sprintf("test%d", i+1),
		})
	}
	return writeSampleParquetFile(suite, rows)
}

type enhancedParquetRow struct {
	Int32             int32     `parquet:"int32"`
	Int64             int64     `parquet:"int64"`
	Timestamp         time.Time `parquet:"timestamp"`
	Boolean           bool      `parquet:"boolean"`
	ByteArray         string    `parquet:"bytearray"`
	FixedLenByteArray [30]byte  `parquet:"fixedlenbytearray"`
	Float             float32   `parquet:"float"`
	Double            float64   `parquet:"double"`
}

func (suite *IntegrationTestSuite) TestCreateEnhancedParquetSampleFile() {
	file := suite.createEnhancedParquetSampleFile()
	defer file.Close()
}

// see https://github.com/xitongsys/parquet-go/blob/master/example/type.go
func (suite *IntegrationTestSuite) createEnhancedParquetSampleFile() *os.File {
	timestamp := time.Date(2024, time.June, 18, 17, 22, 13, 123456789, time.UTC)
	var array [30]byte
	_ = copy(array[:], "fixed length byte array")
	rows := []enhancedParquetRow{{
		Int32:             33,
		Int64:             65,
		Timestamp:         timestamp,
		Boolean:           true,
		ByteArray:         "A byte array of variable length",
		FixedLenByteArray: array,
		Float:             1.123,
		Double:            123456789.987654321,
	}}
	return writeSampleParquetFile(suite, rows)
}

// Cannot use a method, as methods do not allow type parameters in go.
func writeSampleParquetFile[T any](suite *IntegrationTestSuite, rows []T) *os.File {
	path := filepath.Join(suite.T().TempDir(), "sample.parquet")
	err := parquet.WriteFile(path, rows)
	suite.NoError(err, "failed to write sample parquet file "+path)
	file, err := os.Open(path)
	suite.NoError(err, "failed to open sample parquet file for reading "+path)
	return file
}

func (suite *IntegrationTestSuite) TestMultiImportStatement() {
	database := suite.openConnection(suite.createDefaultConfig())
	schemaName := "TEST_SCHEMA_9"
	fqn := suite.createDbSchema(database, schemaName, "TEST_TABLE", aIntBVarchar20)
	defer suite.cleanup(database, schemaName)

	result, err := database.ExecContext(suite.ctx, fmt.Sprintf(`IMPORT INTO %s FROM LOCAL CSV FILE '../testData/data.csv' `+
		`FILE '../testData/data_part2.csv' COLUMN SEPARATOR = ';' ENCODING = 'UTF-8' ROW SEPARATOR = 'LF'`, fqn))
	suite.NoError(err, "import should be successful")
	affectedRows, _ := result.RowsAffected()
	suite.Equal(int64(6), affectedRows)

	rows, _ := database.Query(fmt.Sprintf("SELECT * FROM %s", fqn))
	suite.assertTableResult(rows,
		[]string{"A", "B"},
		[][]interface{}{
			{int64(11), "test1"},
			{int64(12), "test2"},
			{int64(13), "test3"},
			{int64(21), "test4"},
			{int64(22), "test5"},
			{int64(23), "test6"},
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
		suite.NoError(err, "failed to scan rows")
		suite.Equal(expectedRows[i], columns)
		i = i + 1
	}
}

func (suite *IntegrationTestSuite) TestImportStatementWithCRFile() {
	database := suite.openConnection(suite.createDefaultConfig())
	schemaName := "TEST_SCHEMA_10"
	fqn := suite.createDbSchema(database, schemaName, "TEST_TABLE", aIntBVarchar20)
	defer suite.cleanup(database, schemaName)

	result, err := database.ExecContext(
		suite.ctx,
		fmt.Sprintf(`IMPORT INTO %s FROM LOCAL CSV FILE '../testData/data_cr.csv' `+
			`COLUMN SEPARATOR = ';' ENCODING = 'UTF-8' ROW SEPARATOR = 'CR'`, fqn))
	suite.NoError(err, "import should be successful")
	affectedRows, _ := result.RowsAffected()
	suite.Equal(int64(3), affectedRows)

	rows, _ := database.Query(fmt.Sprintf("SELECT * FROM %s", fqn))
	suite.assertTableResult(rows,
		[]string{"A", "B"},
		[][]interface{}{
			{int64(11), "test1"},
			{int64(12), "test2"},
			{int64(13), "test3"},
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
	suite.NoError(err, "failed to scan rows")
	suite.Equal(expected, testValue)
}

// createDbSchema creates a database schema for the current test. If parameter
// table is not an empty string, then also create a table with the specified
// column declaration.
//
// Returns the fully qualified and quoted name of the database object:
// schema.table.
func (suite *IntegrationTestSuite) createDbSchema(
	db *sql.DB,
	schema string,
	table string,
	columns string,
) string {
	_, err := db.ExecContext(suite.ctx, "CREATE SCHEMA IF NOT EXISTS "+schema)
	suite.NoError(err, "Failed to create database schema "+schema)
	if table != "" {
		statement := fmt.Sprintf("CREATE TABLE %q.%q (%s)", schema, table, columns)
		_, err = db.ExecContext(suite.ctx, statement)
		suite.NoError(err, "Failed to create database table "+table)
	}
	return fmt.Sprintf("%q.%q", schema, table)
}

func (suite *IntegrationTestSuite) cleanup(db *sql.DB, schemaName string) {
	_, err := db.Exec("DROP SCHEMA IF EXISTS " + schemaName + " CASCADE")
	suite.NoError(err, "Failed to drop schema "+schemaName)
	suite.NoError(db.Close(), "Failed to close driver ")
}

func (suite *IntegrationTestSuite) TearDownSuite() {
	defer goleak.VerifyNone(suite.T())
	if suite.exasol != nil {
		suite.exasol.StopDb()
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
