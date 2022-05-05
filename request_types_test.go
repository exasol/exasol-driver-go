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
		{"login command", LoginCommand{
			Command:         Command{"login"},
			ProtocolVersion: 42}, `{"command":"login","protocolVersion":42,"attributes":{}}`},
		{"login token command", LoginTokenCommand{
			Command:         Command{"loginToken"},
			ProtocolVersion: 42}, `{"command":"loginToken","protocolVersion":42,"attributes":{}}`},
		{"login command with attributes", LoginCommand{
			Command:         Command{"login"},
			ProtocolVersion: 42,
			Attributes: Attributes{
				ResultSetMaxRows: 100,
			}}, `{"command":"login","protocolVersion":42,"attributes":{"resultSetMaxRows":100}}`},
		{"auth command", AuthCommand{
			Username: "user", Password: "pass", UseCompression: false, SessionID: 1234,
			Attributes: Attributes{Autocommit: boolToPtr(true)},
		}, `{"username":"user","password":"pass","useCompression":false,"sessionId":1234,"attributes":{"autocommit":true}}`},
		{"sql command", SQLCommand{
			Command: Command{"command"}, SQLText: "sql", Attributes: Attributes{FeedbackInterval: 2},
		}, `{"command":"command","sqlText":"sql","attributes":{"feedbackInterval":2}}`},
		{"empty attributes", Attributes{}, `{}`},
		{"execute prepared statement", ExecutePreparedStatementCommand{
			Command: Command{"command"}, StatementHandle: 321, NumColumns: 4, NumRows: 6, Columns: []SQLQueryColumn{{Name: "col"}},
			Data:       [][]interface{}{{"a", "b"}, {1, 2}},
			Attributes: Attributes{DateLanguage: "format"},
		}, `{"command":"command","statementHandle":321,"numColumns":4,"numRows":6,"columns":[{"name":"col","dataType":{"type":""}}],"data":[["a","b"],[1,2]],"attributes":{"dateLanguage":"format"}}`},
		{"fetch command", FetchCommand{
			Command: Command{"cmd"}, ResultSetHandle: 4321, StartPosition: 5, NumBytes: 100,
		}, `{"command":"cmd","resultSetHandle":4321,"startPosition":5,"numBytes":100}`},
		{"create prepared statement", CreatePreparedStatementCommand{
			Command: Command{"cmd"}, SQLText: "sql", Attributes: Attributes{TimestampUtcEnabled: boolToPtr(false)},
		}, `{"command":"cmd","sqlText":"sql","attributes":{"timestampUtcEnabled":false}}`},
		{"close prepared statement", ClosePreparedStatementCommand{
			Command: Command{"cmd"}, StatementHandle: 4321, Attributes: Attributes{Timezone: "abc"},
		}, `{"command":"cmd","statementHandle":4321,"attributes":{"timezone":"abc"}}`},
		{"close result set command", CloseResultSetCommand{
			Command: Command{"cmd"}, ResultSetHandles: []int{123, 321}, Attributes: Attributes{NumericCharacters: "abc"},
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
