package exasol

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNamedValuesToValues(t *testing.T) {
	namedValues := []driver.NamedValue{{Name: ""}, {Name: ""}}
	values, err := namedValuesToValues(namedValues)
	assert.Equal(t, []driver.Value{driver.Value(nil), driver.Value(nil)}, values)
	assert.NoError(t, err)
}

func TestNamedValuesToValuesInvalidName(t *testing.T) {
	namedValues := []driver.NamedValue{{Name: "some name"}}
	values, err := namedValuesToValues(namedValues)
	assert.Nil(t, values)
	assert.EqualError(t, err, "E-EGOD-7: named parameters not supported")
}

func TestIsImportQuery(t *testing.T) {
	assert.True(t, isImportQuery("IMPORT into <targettable> from local CSV file '/path/to/filename.csv' <optional options>;\n"))
}

func TestGetFilePathNotFound(t *testing.T) {
	query := "SELECT * FROM table"
	_, err := getFilePaths(query)
	assert.ErrorIs(t, err, ErrInvalidImportQuery)
}

func TestOpenFileNotFound(t *testing.T) {
	_, err := openFile("./.does_not_exist")
	assert.EqualError(t, err, "E-EGOD-28: file './.does_not_exist' not found")
}

func TestOpenFile(t *testing.T) {
	file, err := openFile("./testData/data.csv")
	assert.NoError(t, err)
	assert.NotNil(t, file)
}

func TestUpdateImportQuery(t *testing.T) {
	p := &proxy{Host: "127.0.0.1", Port: 4333}
	query := "IMPORT into table FROM LOCAL CSV file '/path/to/filename.csv'"
	newQuery := updateImportQuery(query, p)
	assert.Equal(t, "IMPORT into table FROM CSV AT 'http://127.0.0.1:4333' FILE 'data.csv' ", newQuery)
}

func TestUpdateImportQueryMulti(t *testing.T) {
	p := &proxy{Host: "127.0.0.1", Port: 4333}
	query := "IMPORT into table FROM LOCAL CSV file '/path/to/filename.csv' file '/path/to/filename2.csv'"
	newQuery := updateImportQuery(query, p)
	assert.Equal(t, "IMPORT into table FROM CSV AT 'http://127.0.0.1:4333' FILE 'data.csv' ", newQuery)
}

func TestUpdateImportQueryMulti2(t *testing.T) {
	p := &proxy{Host: "127.0.0.1", Port: 4333}
	query := "IMPORT INTO table_1 FROM LOCAL CSV USER 'agent_007' IDENTIFIED BY 'secret' FILE 'tab1_part1.csv' FILE 'tab1_part2.csv' COLUMN SEPARATOR = ';' SKIP = 5;"
	newQuery := updateImportQuery(query, p)
	assert.Equal(t, "IMPORT INTO table_1 FROM CSV AT 'http://127.0.0.1:4333' USER 'agent_007' IDENTIFIED BY 'secret' FILE 'data.csv' COLUMN SEPARATOR = ';' SKIP = 5;", newQuery)
}

func TestGetFilePaths(t *testing.T) {

	quotes := []struct {
		name  string
		value string
	}{
		{name: "SingleQuote",
			value: "'"},
		{name: "DoubleQuote",
			value: `"`},
	}

	tests := []struct {
		name  string
		paths []string
	}{
		{name: "Single file", paths: []string{"/path/to/filename.csv"}},
		{name: "Multi file", paths: []string{"/path/to/filename.csv", "/path/to/filename2.csv"}},
		{name: "Relative paths", paths: []string{"./tab1_part1.csv", "./tab1_part2.csv"}},
		{name: "Windows paths", paths: []string{"C:\\Documents\\Newsletters\\Summer2018.csv", "\\Program Files\\Custom Utilities\\StringFinder.csv"}},
		{name: "Unix paths", paths: []string{"/Users/User/Documents/Data/test.csv"}},
	}

	for _, quote := range quotes {
		for _, tt := range tests {
			t.Run(fmt.Sprintf("%s %s", tt.name, quote.name), func(t *testing.T) {

				var preparedPaths []string
				for _, path := range tt.paths {
					preparedPaths = append(preparedPaths, fmt.Sprintf("%s%s%s", quote.value, path, quote.value))
				}

				foundPaths, err := getFilePaths(fmt.Sprintf(`IMPORT INTO table_1 FROM CSV
       			AT 'http://192.168.1.1:8080/' USER 'agent_007' IDENTIFIED BY 'secret'
       			FILE %s 
       			COLUMN SEPARATOR = ';'
       			SKIP = 5;`, strings.Join(preparedPaths, " FILE ")))
				assert.NoError(t, err)
				assert.ElementsMatch(t, tt.paths, foundPaths)
			})
		}
	}
}

func TestGetRowSeparatorLF(t *testing.T) {
	query := "IMPORT into table FROM LOCAL CSV file '/path/to/filename.csv' ROW SEPARATOR = 'LF'"
	assert.Equal(t, getRowSeparator(query), "\n")
}

func TestGetRowSeparatorCR(t *testing.T) {
	query := "IMPORT into table FROM LOCAL CSV file '/path/to/filename.csv' ROW SEPARATOR = 'CR'"
	assert.Equal(t, getRowSeparator(query), "\r")
}

func TestGetRowSeparatorCRLF(t *testing.T) {
	query := "IMPORT into table FROM LOCAL CSV file '/path/to/filename.csv' ROW SEPARATOR =  'CRLF'"
	assert.Equal(t, getRowSeparator(query), "\r\n")
}

func TestGetRowSeparator(t *testing.T) {
	tests := []struct {
		name      string
		separator string
		want      string
	}{
		{name: "LF", separator: "LF", want: "\n"},
		{name: "LF lowercase", separator: "lf", want: "\n"},
		{name: "CRLF", separator: "CRLF", want: "\r\n"},
		{name: "CRLF lowercase", separator: "crlf", want: "\r\n"},
		{name: "CR", separator: "CR", want: "\r"},
		{name: "CR lowercase", separator: "cr", want: "\r"},
	}
	for _, tt := range tests {

		query := fmt.Sprintf("IMPORT into table FROM LOCAL CSV file '/path/to/filename.csv' ROW SEPARATOR =  '%s'", tt.separator)

		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, getRowSeparator(query))
		})
	}
}
