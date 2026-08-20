package utils

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	"github.com/exasol/exasol-driver-go/pkg/errors"

	"github.com/stretchr/testify/assert"
)

const (
	withOptionsTestCase     = "with options"
	csvLocalImportQuery     = "IMPORT into table FROM LOCAL CSV file '/path/to/filename.csv'"
	parquetLocalImportQuery = "IMPORT into table FROM LOCAL PARQUET file '/path/to/filename.parquet'"
	loopbackHost            = "127.0.0.1"
	testFingerprint         = "sha256//abc123"
)

func TestNamedValuesToValues(t *testing.T) {
	namedValues := []driver.NamedValue{{Name: ""}, {Name: ""}}
	values, err := NamedValuesToValues(namedValues)
	assert.Equal(t, []driver.Value{driver.Value(nil), driver.Value(nil)}, values)
	assert.NoError(t, err)
}

func TestNamedValuesToValuesInvalidName(t *testing.T) {
	namedValues := []driver.NamedValue{{Name: "some name"}}
	values, err := NamedValuesToValues(namedValues)
	assert.Nil(t, values)
	assert.EqualError(t, err, "E-EGOD-7: named parameters not supported")
}

func TestGetImportFormatCsv(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		expectedResult ImportFormat
	}{
		{name: withOptionsTestCase, query: "IMPORT into <targettable> from local CSV file '/path/to/filename.csv' <optional options>;\n", expectedResult: ImportFormatCSV},
		{name: "upper case", query: "IMPORT INTO SCHEMA.TABLE FROM LOCAL CSV FILE '/path/to/filename.csv'", expectedResult: ImportFormatCSV},
		{name: "with brackets", query: "IMPORT(something) INTO SCHEMA.TABLE FROM LOCAL CSV FILE '/path/to/filename.csv'", expectedResult: ImportFormatCSV},
		{name: "lower case", query: "import into schema.table from local csv file '/path/to/filename.csv'", expectedResult: ImportFormatCSV},
		{name: "with additional whitespace", query: " IMPORT \t INTO SCHEMA.TABLE\n\tFROM  LOCAL  CSV  FILE  '/path/to/filename.csv'", expectedResult: ImportFormatCSV},
		{name: "import in string with placeholders", query: "insert into table1 values ('import into {{dest.schema}}.{{dest.table}} ) from local csv file ''{{file.path}}'' ');", expectedResult: ImportFormatNone},
		{name: "import in string", query: "insert into table1 values ('import into schema.table from local csv file ''/path/to/filename.csv''');", expectedResult: ImportFormatNone},
		{name: "import in string with schema", query: "insert into schema.tab1 values ('IMPORT into schema.table FROM LOCAL CSV file ''/path/to/filename.csv'';')", expectedResult: ImportFormatNone},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedResult, GetImportFormat(test.query))
		})
	}
}

func TestGetImportFormatParquet(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		expectedResult ImportFormat
	}{
		{name: withOptionsTestCase, query: "IMPORT into <targettable> from local PARQUET file '/path/to/filename.parquet' <optional options>;\n", expectedResult: ImportFormatParquet},
		{name: "upper case", query: "IMPORT INTO SCHEMA.TABLE FROM LOCAL PARQUET FILE '/path/to/filename.parquet'", expectedResult: ImportFormatParquet},
		{name: "with brackets", query: "IMPORT(something) INTO SCHEMA.TABLE FROM LOCAL PARQUET FILE '/path/to/filename.parquet'", expectedResult: ImportFormatParquet},
		{name: "lower case", query: "import into schema.table from local parquet file '/path/to/filename.parquet'", expectedResult: ImportFormatParquet},
		{name: "with additional whitespace", query: " IMPORT \t INTO SCHEMA.TABLE\n\tFROM  LOCAL  PARQUET  FILE  '/path/to/filename.parquet'", expectedResult: ImportFormatParquet},
		{name: "import in string", query: "insert into table1 values ('import into schema.table from local parquet file ''/path/to/filename.parquet''');", expectedResult: ImportFormatNone},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedResult, GetImportFormat(test.query))
		})
	}
}

func TestGetImportFormatNone(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		expectedResult ImportFormat
	}{
		{name: "FBV not supported", query: "IMPORT INTO SCHEMA.TABLE FROM LOCAL FBV FILE '/path/to/filename.fbf'", expectedResult: ImportFormatNone},
		{name: "select query unsupported", query: "select * from schema.table", expectedResult: ImportFormatNone},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedResult, GetImportFormat(test.query))
		})
	}
}

func TestGetFilePathNotFound(t *testing.T) {
	query := "SELECT * FROM table"
	_, err := GetFilePaths(query)
	assert.ErrorIs(t, err, errors.ErrInvalidImportQuery)
}

func TestOpenFileNotFound(t *testing.T) {
	_, err := OpenFile("./.does_not_exist")
	assert.EqualError(t, err, "E-EGOD-28: file './.does_not_exist' not found")
}

func TestOpenFile(t *testing.T) {
	file, err := OpenFile("../../testData/data.csv")
	assert.NoError(t, err)
	assert.NotNil(t, file)
}

func TestUpdateImportQuery(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{name: "non import query",
			query:    "select * from table",
			expected: "select * from table"},
		{name: "import statement in a string",
			query:    "insert into tab1 values ('IMPORT into table FROM LOCAL CSV file ''/path/to/filename.csv'';')",
			expected: "insert into tab1 values ('IMPORT into table FROM LOCAL CSV file ''/path/to/filename.csv'';')"},
		{name: "single file",
			query:    csvLocalImportQuery,
			expected: "IMPORT into table FROM CSV AT 'http://127.0.0.1:4333' FILE 'data.csv' "},
		{name: "multi",
			query:    "IMPORT into table FROM LOCAL CSV file '/path/to/filename.csv' file '/path/to/filename2.csv'",
			expected: "IMPORT into table FROM CSV AT 'http://127.0.0.1:4333' FILE 'data.csv' "},
		{name: withOptionsTestCase,
			query:    "IMPORT INTO table_1 FROM LOCAL CSV USER 'agent_007' IDENTIFIED BY 'secret' FILE 'tab1_part1.csv' FILE 'tab1_part2.csv' COLUMN SEPARATOR = ';' SKIP = 5;",
			expected: "IMPORT INTO table_1 FROM CSV AT 'http://127.0.0.1:4333' USER 'agent_007' IDENTIFIED BY 'secret' FILE 'data.csv' COLUMN SEPARATOR = ';' SKIP = 5;"},
		{name: "with newline",
			query:    "IMPORT INTO table_1\nFROM LOCAL CSV USER 'agent_007' IDENTIFIED BY 'secret' FILE 'tab1_part1.csv' FILE 'tab1_part2.csv' COLUMN SEPARATOR = ';'\r\nSKIP = 5;",
			expected: "IMPORT INTO table_1\nFROM CSV AT 'http://127.0.0.1:4333' USER 'agent_007' IDENTIFIED BY 'secret' FILE 'data.csv' COLUMN SEPARATOR = ';'\r\nSKIP = 5;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			updatedQuery := UpdateImportQuery(test.query, ProxyTarget{Host: loopbackHost, Port: 4333})
			assert.Equal(t, test.expected, updatedQuery)
		})
	}
}

func TestUpdateImportQueryParquetSourceClause(t *testing.T) {
	query := parquetLocalImportQuery
	updatedQuery := UpdateImportQuery(query, ProxyTarget{Host: loopbackHost, Port: 4333})
	assert.Equal(t, "IMPORT into table FROM PARQUET AT 'http://127.0.0.1:4333;MaxConcurrentReads=1' FILE 'data.parquet' ", updatedQuery)
}

func TestUpdateImportQueryParquetFileClause(t *testing.T) {
	query := "IMPORT INTO table_1 FROM LOCAL PARQUET USER 'agent_007' IDENTIFIED BY 'secret' FILE 'tab1_part1.parquet' FILE 'tab1_part2.parquet' COLUMN SEPARATOR = ';' SKIP = 5;"
	updatedQuery := UpdateImportQuery(query, ProxyTarget{Host: loopbackHost, Port: 4333})
	assert.Equal(t, "IMPORT INTO table_1 FROM PARQUET AT 'http://127.0.0.1:4333;MaxConcurrentReads=1' USER 'agent_007' IDENTIFIED BY 'secret' FILE 'data.parquet' COLUMN SEPARATOR = ';' SKIP = 5;", updatedQuery)
}

func TestUpdateImportQueryParquetPlaintext(t *testing.T) {
	query := parquetLocalImportQuery
	updatedQuery := UpdateImportQuery(query, ProxyTarget{Host: loopbackHost, Port: 4333})
	assert.Contains(t, updatedQuery, "PARQUET AT 'http://127.0.0.1:4333;MaxConcurrentReads=1'")
	assert.NotContains(t, updatedQuery, "PUBLIC KEY")
}

func TestUpdateImportQueryParquetTls(t *testing.T) {
	query := parquetLocalImportQuery
	updatedQuery := UpdateImportQuery(query, ProxyTarget{Host: loopbackHost, Port: 4333, Fingerprint: testFingerprint})
	assert.Equal(t, "IMPORT into table FROM PARQUET AT 'https://127.0.0.1:4333;MaxConcurrentReads=1' PUBLIC KEY '"+testFingerprint+"' FILE 'data.parquet' ", updatedQuery)
}

func TestUpdateImportQueryCsvTls(t *testing.T) {
	query := csvLocalImportQuery
	updatedQuery := UpdateImportQuery(query, ProxyTarget{Host: loopbackHost, Port: 4333, Fingerprint: testFingerprint})
	assert.Equal(t, "IMPORT into table FROM CSV AT 'https://127.0.0.1:4333' PUBLIC KEY '"+testFingerprint+"' FILE 'data.csv' ", updatedQuery)
}

func TestUpdateImportQuerySchemeFollowsFingerprint(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		fingerprint    string
		expectedScheme string
	}{
		{name: "csv without fingerprint", query: csvLocalImportQuery, fingerprint: "", expectedScheme: "http"},
		{name: "parquet without fingerprint", query: parquetLocalImportQuery, fingerprint: "", expectedScheme: "http"},
		{name: "csv with fingerprint", query: csvLocalImportQuery, fingerprint: testFingerprint, expectedScheme: "https"},
		{name: "parquet with fingerprint", query: parquetLocalImportQuery, fingerprint: testFingerprint, expectedScheme: "https"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			updatedQuery := UpdateImportQuery(test.query, ProxyTarget{Host: loopbackHost, Port: 4333, Fingerprint: test.fingerprint})
			assert.Contains(t, updatedQuery, fmt.Sprintf("AT '%s://127.0.0.1:4333", test.expectedScheme))
			if test.fingerprint == "" {
				assert.NotContains(t, updatedQuery, "PUBLIC KEY")
			} else {
				assert.Contains(t, updatedQuery, fmt.Sprintf("PUBLIC KEY '%s'", test.fingerprint))
			}
		})
	}
}

func TestGetFilePaths(t *testing.T) {
	quotes := []struct {
		name  string
		value string
	}{
		{name: "SingleQuote", value: "'"},
		{name: "DoubleQuote", value: `"`},
	}

	tests := []struct {
		name  string
		paths []string
	}{
		{name: "Single file", paths: []string{"/path/to/filename.csv"}},
		{name: "Multi file", paths: []string{"/path/to/filename.csv", "/path/to/filename2.csv"}},
		{name: "Relative paths", paths: []string{"./tab1_part1.csv", "./tab1_part2.csv"}},
		{name: "Local Dir", paths: []string{"tab1_part1.csv", "tab1_part2.csv"}},
		{name: "Windows paths", paths: []string{"C:\\Documents\\Newsletters\\Summer2018.csv", "\\Program Files\\Custom Utilities\\StringFinder.csv"}},
		{name: "Unix paths", paths: []string{"/Users/User/Documents/Data/test.csv"}},
		{name: "With dash", paths: []string{"/Users/User/Documents/Data/test-1.csv"}},
		{name: "With tilde", paths: []string{"C:\\Users\\User~1.U\\AppData\\Local\\Temp\\1479825193.csv", "~/Documents/Data/test.csv"}},
	}

	for _, quote := range quotes {
		for _, test := range tests {
			t.Run(fmt.Sprintf("%s %s", test.name, quote.name), func(t *testing.T) {
				var preparedPaths []string
				for _, path := range test.paths {
					preparedPaths = append(preparedPaths, fmt.Sprintf("%s%s%s", quote.value, path, quote.value))
				}

				foundPaths, err := GetFilePaths(fmt.Sprintf(`IMPORT INTO table_1 FROM CSV
       			AT 'http://192.168.1.1:8080/' USER 'agent_007' IDENTIFIED BY 'secret'
       			FILE %s 
       			COLUMN SEPARATOR = ';'
       			SKIP = 5;`, strings.Join(preparedPaths, " FILE ")))
				assert.NoError(t, err)
				assert.ElementsMatch(t, test.paths, foundPaths)
			})
		}
	}
}

func TestGetRowSeparatorCompleteQuery(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{name: "LF", query: "IMPORT into table FROM LOCAL CSV file '/path/to/filename.csv' ROW SEPARATOR = 'LF'", expected: "\n"},
		{name: "CR", query: "IMPORT into table FROM LOCAL CSV file '/path/to/filename.csv' ROW SEPARATOR = 'CR'", expected: "\r"},
		{name: "CRLF", query: "IMPORT into table FROM LOCAL CSV file '/path/to/filename.csv' ROW SEPARATOR = 'CRLF'", expected: "\r\n"},
		{name: "only row separator fragment", query: "ROW SEPARATOR = 'CRLF'", expected: "\r\n"},
		{name: "pipe as quote char not supported", query: "ROW SEPARATOR = |CRLF|", expected: "\n"},
		{name: "unknown value returns default", query: "IMPORT into table FROM LOCAL CSV file '/path/to/filename.csv' ROW SEPARATOR = 'unknown'", expected: "\n"},
		{name: "missing expression returns default", query: csvLocalImportQuery, expected: "\n"},
		{name: "trailing text", query: "IMPORT into table FROM LOCAL CSV file '/path/to/filename.csv' ROW SEPARATOR = 'CRLF' trailing text", expected: "\r\n"},
		{name: "multiple spaces", query: "IMPORT into table FROM LOCAL CSV file '/path/to/filename.csv' ROW SEPARATOR \t = \t 'CRLF';", expected: "\r\n"},
		{name: "no spaces returns default", query: "IMPORT into table FROM LOCAL CSV file '/path/to/filename.csv' ROW SEPARATOR='CRLF';", expected: "\n"},
		{name: "with line breaks", query: "IMPORT into table\nFROM LOCAL CSV file '/path/to/filename.csv'\nROW\r\nSEPARATOR = 'CRLF';", expected: "\r\n"},
		{name: "unknown query returns default", query: "select * from table", expected: "\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, GetRowSeparator(test.query))
		})
	}
}

func TestGetRowSeparator(t *testing.T) {
	tests := []struct {
		name      string
		separator string
		want      string
	}{
		{name: "LF", separator: "LF", want: "\n"},
		{name: "LF lowercase", separator: "lf", want: "\n"},
		{name: "Lf mixed case returns default", separator: "Lf", want: "\n"},
		{name: "CRLF", separator: "CRLF", want: "\r\n"},
		{name: "CRLF lowercase", separator: "crlf", want: "\r\n"},
		{name: "CrLf mixed case returns default", separator: "CrLf", want: "\n"},
		{name: "CR", separator: "CR", want: "\r"},
		{name: "CR lowercase", separator: "cr", want: "\r"},
		{name: "Cr mixed case returns default", separator: "Cr", want: "\n"},
	}
	for _, tt := range tests {
		query := fmt.Sprintf("IMPORT into table FROM LOCAL CSV file '/path/to/filename.csv' ROW SEPARATOR =  '%s'", tt.separator)

		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, GetRowSeparator(query))
		})
	}
}

func TestSingleHostResolve(t *testing.T) {
	hosts, err := ResolveHosts("localhost")

	assert.NoError(t, err)
	assert.Equal(t, 1, len(hosts))
	assert.Equal(t, "localhost", hosts[0])
}

func TestMultipleHostResolve(t *testing.T) {
	hosts, err := ResolveHosts("exasol1,127.0.0.1,exasol3")

	assert.NoError(t, err)
	assert.Equal(t, 3, len(hosts))
	assert.Equal(t, "exasol1", hosts[0])
	assert.Equal(t, loopbackHost, hosts[1])
	assert.Equal(t, "exasol3", hosts[2])
}

func TestHostSuffixRangeResolve(t *testing.T) {
	hosts, err := ResolveHosts("exasol1..3")

	assert.NoError(t, err)
	assert.Equal(t, 3, len(hosts))
	assert.Equal(t, "exasol1", hosts[0])
	assert.Equal(t, "exasol2", hosts[1])
	assert.Equal(t, "exasol3", hosts[2])
}

func TestResolvingHostRangeWithCompleteHostnameNotSupported(t *testing.T) {
	hosts, err := ResolveHosts("exasol1..exasol3")

	assert.NoError(t, err)
	assert.Equal(t, 1, len(hosts))
	assert.Equal(t, "exasol1..exasol3", hosts[0])
}

func TestResolvingHostRangeWithInvalidRangeNotSupported(t *testing.T) {
	hosts, err := ResolveHosts("exasolX..Y")

	assert.NoError(t, err)
	assert.Equal(t, 1, len(hosts))
	assert.Equal(t, "exasolX..Y", hosts[0])
}

func TestResolvingHostRangeWithInvalidRangeLimits(t *testing.T) {
	hosts, err := ResolveHosts("exasol3..1")
	assert.EqualError(t, err, "E-EGOD-20: invalid host range limits: 'exasol3..1'")
	assert.Nil(t, hosts)
}

func TestIPRangeResolve(t *testing.T) {
	hosts, err := ResolveHosts("127.0.0.1..3")
	assert.NoError(t, err)
	assert.Equal(t, 3, len(hosts))
	assert.Equal(t, loopbackHost, hosts[0])
	assert.Equal(t, "127.0.0.2", hosts[1])
	assert.Equal(t, "127.0.0.3", hosts[2])
}
