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
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/exasol/exasol-driver-go"
	"github.com/exasol/exasol-driver-go/pkg/dsn"
	"github.com/exasol/exasol-driver-go/pkg/integrationTesting"

	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
	"gopkg.in/yaml.v3"

	"github.com/stretchr/testify/suite"
)

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
	suite.exasol = integrationTesting.StartDbSetup(&suite.Suite)
	connectionInfo := suite.exasol.ConnectionInfo
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

	var errorMsgCertWrongHost string
	if suite.host == "localhost" {
		errorMsgCertWrongHost = "x509: certificate is not valid for any names, but wanted to match localhost"
	} else {
		errorMsgCertWrongHost = "x509: “*.exacluster.local” certificate is not standards compliant"
	}

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

var dereferenceString = func(v any) any { return *(v.(*string)) }
var dereferenceFloat32 = func(v any) any { return *(v.(*float32)) }
var dereferenceFloat64 = func(v any) any { return *(v.(*float64)) }
var dereferenceInt32 = func(v any) any { return *(v.(*int32)) }
var dereferenceInt64 = func(v any) any { return *(v.(*int64)) }
var dereferenceInt = func(v any) any { return *(v.(*int)) }
var dereferenceBool = func(v any) any { return *(v.(*bool)) }

func (suite *IntegrationTestSuite) TestQueryDataTypesCast() {
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
		database := suite.openConnection(suite.createDefaultConfig())
		defer database.Close()
		suite.Run(fmt.Sprintf("Cast Test %02d %s: %s", i, testCase.testDescription, testCase.sqlType), func() {
			rows, err := database.Query(fmt.Sprintf("SELECT CAST(%s AS %s)", testCase.sqlValue, testCase.sqlType))
			onError(err)
			defer rows.Close()
			suite.True(rows.Next(), "should have one row")
			onError(rows.Scan(testCase.scanDest))
			val := testCase.scanDest
			suite.Equal(testCase.expectedValue, testCase.dereference(val))
		})
	}
}

func (suite *IntegrationTestSuite) TestPreparedStatementArgsConverted() {
	type TestCase struct {
		sqlValue      any
		sqlType       string
		scanDest      any
		expectedValue any
		dereference   func(any) any
		//delta         float64
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
		int64TestCase(1.1, "DECIMAL(18,2)", 1),
		int64TestCase(-1.1, "DECIMAL(18,2)", -1),
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
		database := suite.openConnection(suite.createDefaultConfig().Autocommit(false))
		schemaName := "DATATYPE_TEST"
		_, err := database.Exec("CREATE SCHEMA " + schemaName)
		onError(err)
		defer suite.cleanup(database, schemaName)

		suite.Run(fmt.Sprintf("%02d Column type %s accepts values of type %T", i, testCase.sqlType, testCase.sqlValue), func() {
			tableName := fmt.Sprintf("%s.TAB_%d", schemaName, i)
			_, err = database.Exec(fmt.Sprintf("CREATE TABLE %s (col %s)", tableName, testCase.sqlType))
			onError(err)
			stmt, err := database.Prepare(fmt.Sprintf("insert into %s values (?)", tableName))
			onError(err)
			_, err = stmt.Exec(testCase.sqlValue)
			onError(err)
			rows, err := database.Query(fmt.Sprintf("select * from %s", tableName))
			onError(err)
			defer rows.Close()
			suite.True(rows.Next(), "should have at least one row")
			onError(rows.Scan(testCase.scanDest))
			suite.False(rows.Next(), "should have at most one row")
			val := testCase.scanDest
			suite.Equal(testCase.expectedValue, testCase.dereference(val))
		})
	}
}

func (suite *IntegrationTestSuite) TestPreparedStatementArgsConversionFails() {
	database := suite.openConnection(suite.createDefaultConfig().Autocommit(false))
	schemaName := "DATATYPE_TEST"
	_, err := database.Exec("CREATE SCHEMA " + schemaName)
	onError(err)
	defer suite.cleanup(database, schemaName)

	tableName := fmt.Sprintf("%s.TAB", schemaName)
	_, err = database.Exec(fmt.Sprintf("CREATE TABLE %s (col TIMESTAMP)", tableName))
	onError(err)
	stmt, err := database.Prepare(fmt.Sprintf("insert into %s values (?)", tableName))
	onError(err)
	_, err = stmt.Exec(true)
	suite.EqualError(err, "E-EGOD-30: cannot convert argument 'true' of type 'bool' to 'TIMESTAMP' type")
}

func (suite *IntegrationTestSuite) TestScanTypeUnsupported() {
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
		database := suite.openConnection(suite.createDefaultConfig().Autocommit(false))
		schemaName := "DATATYPE_TEST"
		_, err := database.Exec("CREATE SCHEMA " + schemaName)
		onError(err)
		defer suite.cleanup(database, schemaName)

		suite.Run(fmt.Sprintf("Scan fails %02d %s", i, testCase.sqlType), func() {
			tableName := fmt.Sprintf("%s.TAB_%d", schemaName, i)
			_, err = database.Exec(fmt.Sprintf("CREATE TABLE %s (col %s)", tableName, testCase.sqlType))
			onError(err)
			stmt, err := database.Prepare(fmt.Sprintf("insert into %s values (?)", tableName))
			onError(err)
			_, err = stmt.Exec(testCase.sqlValue)
			onError(err)
			rows, err := database.Query(fmt.Sprintf("select * from %s", tableName))
			onError(err)
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
	_, err := database.Exec("CREATE SCHEMA " + schemaName)
	onError(err)
	_, err = database.Exec(fmt.Sprintf("create or replace table %s.dummy (a integer, b float)", schemaName))
	onError(err)
	defer suite.cleanup(database, schemaName)
	stmt, err := database.Prepare(fmt.Sprintf("insert into %s.dummy values(?,?)", schemaName))
	onError(err)
	_, err = stmt.Exec(1, 2)
	onError(err)
	rows, err := database.Query(fmt.Sprintf("select a || ':' || b from %s.dummy", schemaName))
	onError(err)
	suite.assertSingleValueResult(rows, "1:2")
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
			{int64(11), "test1"},
			{int64(12), "test2"},
			{int64(13), "test3"},
		},
	)
}

func (suite *IntegrationTestSuite) TestImportStatementInString() {
	database := suite.openConnection(suite.createDefaultConfig())
	ctx := context.Background()
	schemaName := "TEST_SCHEMA_8"
	tableName := "table1"
	_, _ = database.ExecContext(ctx, "CREATE SCHEMA "+schemaName)
	defer suite.cleanup(database, schemaName)
	_, _ = database.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s.%s (text VARCHAR(200))", schemaName, tableName))

	result, err := database.ExecContext(ctx, `insert into table1 values ('import into {{dest.schema}}.{{dest.table}} ) from local csv file ''{{file.path}}'' ');`)
	suite.NoError(err, "insert should be successful")
	affectedRows, _ := result.RowsAffected()
	suite.Equal(int64(1), affectedRows)

	rows, _ := database.Query(fmt.Sprintf("SELECT * FROM %s.%s", schemaName, tableName))
	suite.assertTableResult(rows,
		[]string{"TEXT"},
		[][]interface{}{{"import into {{dest.schema}}.{{dest.table}} ) from local csv file '{{file.path}}' "}},
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
			{int64(20000)},
		},
	)

	rows, err = database.Query(fmt.Sprintf("SELECT * FROM %s.%s ORDER BY a LIMIT 3 ", schemaName, tableName))
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
	_, _ = database.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s.%s (a int, b VARCHAR(20))", schemaName, tableName))

	result, err := database.ExecContext(ctx, fmt.Sprintf(`IMPORT INTO %s.%s FROM LOCAL CSV FILE '../testData/data_cr.csv' COLUMN SEPARATOR = ';' ENCODING = 'UTF-8' ROW SEPARATOR = 'CR'`, schemaName, tableName))
	suite.NoError(err, "import should be successful")
	affectedRows, _ := result.RowsAffected()
	suite.Equal(int64(3), affectedRows)

	rows, _ := database.Query(fmt.Sprintf("SELECT * FROM %s.%s", schemaName, tableName))
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
