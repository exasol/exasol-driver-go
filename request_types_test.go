package exasol

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/suite"
)

type RequestTypesTestSuite struct {
	suite.Suite
}

func TestRequestTypesTestSuite(t *testing.T) {
	suite.Run(t, new(RequestTypesTestSuite))
}

func (suite *RequestTypesTestSuite) TestMarshallLoginCommand() {
	for i, testCase := range []struct {
		testName string
		value    interface{}
		expected string
	}{
		{"login command", loginCommand{
			command:         command{"login"},
			ProtocolVersion: 42}, `{"command":"login","protocolVersion":42,"attributes":{}}`},
		{"login token command", loginTokenCommand{
			command:         command{"loginToken"},
			ProtocolVersion: 42}, `{"command":"loginToken","protocolVersion":42,"attributes":{}}`},
		{"login command with attributes", loginCommand{
			command:         command{"login"},
			ProtocolVersion: 42,
			Attributes: attributes{
				ResultSetMaxRows: 100,
			}}, `{"command":"login","protocolVersion":42,"attributes":{"resultSetMaxRows":100}}`},
		{"auth command", authCommand{
			Username: "user", Password: "pass", UseCompression: false, SessionID: 1234,
			Attributes: attributes{Autocommit: boolToPtr(true)},
		}, `{"username":"user","password":"pass","useCompression":false,"sessionId":1234,"attributes":{"autocommit":true}}`},
		{"sql command", sqlCommand{
			command: command{"command"}, SQLText: "sql", Attributes: attributes{FeedbackInterval: 2},
		}, `{"command":"command","sqlText":"sql","attributes":{"feedbackInterval":2}}`},
		{"empty attributes", attributes{}, `{}`},
		{"execute prepared statement", executePreparedStatementCommand{
			command: command{"command"}, StatementHandle: 321, NumColumns: 4, NumRows: 6, Columns: []sqlQueryColumn{{Name: "col"}},
			Data:       [][]interface{}{{"a", "b"}, {1, 2}},
			Attributes: attributes{DateLanguage: "format"},
		}, `{"command":"command","statementHandle":321,"numColumns":4,"numRows":6,"columns":[{"name":"col","dataType":{"type":""}}],"data":[["a","b"],[1,2]],"attributes":{"dateLanguage":"format"}}`},
		{"fetch command", fetchCommand{
			command: command{"cmd"}, ResultSetHandle: 4321, StartPosition: 5, NumBytes: 100,
		}, `{"command":"cmd","resultSetHandle":4321,"startPosition":5,"numBytes":100}`},
		{"create prepared statement", createPreparedStatementCommand{
			command: command{"cmd"}, SQLText: "sql", Attributes: attributes{TimestampUtcEnabled: boolToPtr(false)},
		}, `{"command":"cmd","sqlText":"sql","attributes":{"timestampUtcEnabled":false}}`},
		{"close prepared statement", closePreparedStatementCommand{
			command: command{"cmd"}, StatementHandle: 4321, Attributes: attributes{Timezone: "abc"},
		}, `{"command":"cmd","statementHandle":4321,"attributes":{"timezone":"abc"}}`},
		{"close result set command", closeResultSetCommand{
			command: command{"cmd"}, ResultSetHandles: []int{123, 321}, Attributes: attributes{NumericCharacters: "abc"},
		}, `{"command":"cmd","resultSetHandles":[123,321],"attributes":{"numericCharacters":"abc"}}`},
	} {
		valueType := reflect.TypeOf(testCase.value)
		suite.Run(fmt.Sprintf("Test %02d %v %v", i, valueType, testCase.testName), func() {
			marshalled, err := json.Marshal(testCase.value)
			suite.NoError(err)
			suite.Equal(testCase.expected, string(marshalled))
		})
	}
}
