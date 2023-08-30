package connection

import (
	"context"
	"database/sql/driver"
	"fmt"
	"testing"

	"github.com/exasol/exasol-driver-go/internal/config"
	"github.com/exasol/exasol-driver-go/pkg/connection/wsconn"
	"github.com/exasol/exasol-driver-go/pkg/errors"
	"github.com/exasol/exasol-driver-go/pkg/types"
	"github.com/stretchr/testify/suite"
)

var mockException = types.Exception{Text: "mock error", SQLCode: "mock sql code"}

func mockExceptionError(exception types.Exception) string {
	return errors.NewSqlErr(exception.SQLCode, exception.Text).Error()
}

type ConnectionTestSuite struct {
	suite.Suite
	websocketMock *wsconn.WebsocketConnectionMock
}

func TestConnectionSuite(t *testing.T) {
	suite.Run(t, new(ConnectionTestSuite))
}

func (suite *ConnectionTestSuite) SetupTest() {
	suite.websocketMock = wsconn.CreateWebsocketConnectionMock()
}

func (suite *ConnectionTestSuite) TestConnectFails() {
	conn := &Connection{
		Config:   &config.Config{Host: "invalid", Port: 12345},
		Ctx:      context.Background(),
		IsClosed: true,
	}
	err := conn.Connect()
	suite.ErrorContains(err, `failed to connect to URL "ws://invalid:12345": dial tcp`)
}

func (suite *ConnectionTestSuite) TestQueryContextNamedParametersNotSupported() {
	rows, err := suite.createOpenConnection().QueryContext(context.Background(), "query", []driver.NamedValue{{Name: "arg", Ordinal: 1, Value: "value"}})
	suite.EqualError(err, "E-EGOD-7: named parameters not supported")
	suite.Nil(rows)
}

func (suite *ConnectionTestSuite) TestQueryContext() {
	suite.websocketMock.SimulateOKResponse(
		types.SqlCommand{
			Command:    types.Command{Command: "createPreparedStatement"},
			SQLText:    "query",
			Attributes: types.Attributes{},
		},
		types.CreatePreparedStatementResponse{
			ParameterData: types.ParameterData{Columns: []types.SqlQueryColumn{{Name: "col", DataType: types.SqlQueryColumnType{Type: "type"}}}}})
	suite.websocketMock.SimulateSQLQueriesResponse(
		types.ExecutePreparedStatementCommand{Command: types.Command{Command: "executePreparedStatement"},
			StatementHandle: 0, NumColumns: 1, NumRows: 1,
			Columns: []types.SqlQueryColumn{{Name: "col", DataType: types.SqlQueryColumnType{Type: "type"}}},
			Data:    [][]interface{}{{"value"}},
		},
		types.SqlQueryResponseResultSet{ResultType: "resultType", ResultSet: types.SqlQueryResponseResultSetData{}})
	suite.websocketMock.SimulateOKResponse(types.ClosePreparedStatementCommand{Command: types.Command{Command: "closePreparedStatement"}, StatementHandle: 0, Attributes: types.Attributes{}}, nil)

	rows, err := suite.createOpenConnection().QueryContext(context.Background(), "query", []driver.NamedValue{{Ordinal: 1, Value: "value"}})
	suite.NoError(err)
	suite.Equal([]string{}, rows.Columns())
}

func (suite *ConnectionTestSuite) TestQuery() {
	suite.websocketMock.SimulateOKResponse(
		types.SqlCommand{
			Command:    types.Command{Command: "createPreparedStatement"},
			SQLText:    "query",
			Attributes: types.Attributes{},
		},
		types.CreatePreparedStatementResponse{
			ParameterData: types.ParameterData{Columns: []types.SqlQueryColumn{{Name: "col", DataType: types.SqlQueryColumnType{Type: "type"}}}}})
	suite.websocketMock.SimulateSQLQueriesResponse(
		types.ExecutePreparedStatementCommand{Command: types.Command{Command: "executePreparedStatement"},
			StatementHandle: 0, NumColumns: 1, NumRows: 1,
			Columns: []types.SqlQueryColumn{{Name: "col", DataType: types.SqlQueryColumnType{Type: "type"}}},
			Data:    [][]interface{}{{"value"}},
		},
		types.SqlQueryResponseResultSet{ResultType: "resultType", ResultSet: types.SqlQueryResponseResultSetData{}})
	suite.websocketMock.SimulateOKResponse(types.ClosePreparedStatementCommand{Command: types.Command{Command: "closePreparedStatement"}, StatementHandle: 0, Attributes: types.Attributes{}}, nil)

	rows, err := suite.createOpenConnection().Query("query", []driver.Value{"value"})
	suite.NoError(err)
	suite.Equal([]string{}, rows.Columns())
}

func (suite *ConnectionTestSuite) TestExecContextNamedParametersNotSupported() {
	rows, err := suite.createOpenConnection().ExecContext(context.Background(), "query", []driver.NamedValue{{Name: "arg", Ordinal: 1, Value: "value"}})
	suite.EqualError(err, "E-EGOD-7: named parameters not supported")
	suite.Nil(rows)
}

func (suite *ConnectionTestSuite) TestExecContext() {
	suite.websocketMock.SimulateOKResponse(
		types.SqlCommand{
			Command:    types.Command{Command: "createPreparedStatement"},
			SQLText:    "query",
			Attributes: types.Attributes{},
		},
		types.CreatePreparedStatementResponse{
			ParameterData: types.ParameterData{Columns: []types.SqlQueryColumn{{Name: "col", DataType: types.SqlQueryColumnType{Type: "type"}}}}})
	suite.websocketMock.SimulateSQLQueriesResponse(
		types.ExecutePreparedStatementCommand{Command: types.Command{Command: "executePreparedStatement"},
			StatementHandle: 0, NumColumns: 1, NumRows: 1,
			Columns: []types.SqlQueryColumn{{Name: "col", DataType: types.SqlQueryColumnType{Type: "type"}}},
			Data:    [][]interface{}{{"value"}},
		},
		types.SqlQueryResponseRowCount{ResultType: "resultType", RowCount: 42})
	suite.websocketMock.SimulateOKResponse(types.ClosePreparedStatementCommand{Command: types.Command{Command: "closePreparedStatement"}, StatementHandle: 0, Attributes: types.Attributes{}}, nil)

	rows, err := suite.createOpenConnection().ExecContext(context.Background(), "query", []driver.NamedValue{{Ordinal: 1, Value: "value"}})
	suite.NoError(err)
	rowsAffected, err := rows.RowsAffected()
	suite.NoError(err)
	suite.Equal(int64(42), rowsAffected)
}

func (suite *ConnectionTestSuite) TestExec() {
	suite.websocketMock.SimulateOKResponse(
		types.SqlCommand{
			Command:    types.Command{Command: "createPreparedStatement"},
			SQLText:    "query",
			Attributes: types.Attributes{},
		},
		types.CreatePreparedStatementResponse{
			ParameterData: types.ParameterData{Columns: []types.SqlQueryColumn{{Name: "col", DataType: types.SqlQueryColumnType{Type: "type"}}}}})
	suite.websocketMock.SimulateSQLQueriesResponse(
		types.ExecutePreparedStatementCommand{Command: types.Command{Command: "executePreparedStatement"},
			StatementHandle: 0, NumColumns: 1, NumRows: 1,
			Columns: []types.SqlQueryColumn{{Name: "col", DataType: types.SqlQueryColumnType{Type: "type"}}},
			Data:    [][]interface{}{{"value"}},
		},
		types.SqlQueryResponseRowCount{ResultType: "resultType", RowCount: 42})
	suite.websocketMock.SimulateOKResponse(types.ClosePreparedStatementCommand{Command: types.Command{Command: "closePreparedStatement"}, StatementHandle: 0, Attributes: types.Attributes{}}, nil)

	rows, err := suite.createOpenConnection().Exec("query", []driver.Value{"value"})
	suite.NoError(err)
	rowsAffected, err := rows.RowsAffected()
	suite.NoError(err)
	suite.Equal(int64(42), rowsAffected)
}

func (suite *ConnectionTestSuite) TestPrepareContextFailsClosed() {
	conn := suite.createOpenConnection()
	conn.IsClosed = true
	stmt, err := conn.PrepareContext(context.Background(), "query")
	suite.EqualError(err, driver.ErrBadConn.Error())
	suite.Nil(stmt)
}

func (suite *ConnectionTestSuite) TestPrepareContextPreparedStatementFails() {
	suite.websocketMock.SimulateErrorResponse(
		types.SqlCommand{
			Command:    types.Command{Command: "createPreparedStatement"},
			SQLText:    "query",
			Attributes: types.Attributes{},
		},
		mockException)
	stmt, err := suite.createOpenConnection().PrepareContext(context.Background(), "query")
	suite.EqualError(err, mockExceptionError(mockException))
	suite.Nil(stmt)
}

func (suite *ConnectionTestSuite) TestPrepareContextSuccess() {
	suite.websocketMock.SimulateOKResponse(
		types.SqlCommand{
			Command:    types.Command{Command: "createPreparedStatement"},
			SQLText:    "query",
			Attributes: types.Attributes{},
		},
		types.CreatePreparedStatementResponse{
			ParameterData: types.ParameterData{Columns: []types.SqlQueryColumn{{Name: "col", DataType: types.SqlQueryColumnType{Type: "type"}}}}})
	stmt, err := suite.createOpenConnection().PrepareContext(context.Background(), "query")
	suite.NoError(err)
	suite.NotNil(stmt)
}

func (suite *ConnectionTestSuite) TestPrepareSuccess() {
	suite.websocketMock.SimulateOKResponse(
		types.SqlCommand{
			Command:    types.Command{Command: "createPreparedStatement"},
			SQLText:    "query",
			Attributes: types.Attributes{},
		},
		types.CreatePreparedStatementResponse{
			ParameterData: types.ParameterData{Columns: []types.SqlQueryColumn{{Name: "col", DataType: types.SqlQueryColumnType{Type: "type"}}}}})
	stmt, err := suite.createOpenConnection().Prepare("query")
	suite.NoError(err)
	suite.NotNil(stmt)
}

func (suite *ConnectionTestSuite) TestCloseSuccess() {
	suite.websocketMock.SimulateOKResponse(types.Command{Command: "disconnect"}, nil)
	suite.websocketMock.OnClose(nil)
	conn := suite.createOpenConnection()
	suite.False(conn.IsClosed)
	err := conn.Close()
	suite.NoError(err)
	suite.True(conn.IsClosed)
}

func (suite *ConnectionTestSuite) TestCloseDisconnectFails() {
	suite.websocketMock.SimulateErrorResponse(types.Command{Command: "disconnect"}, mockException)
	suite.websocketMock.OnClose(nil)
	err := suite.createOpenConnection().Close()
	suite.EqualError(err, mockExceptionError(mockException))
}

func (suite *ConnectionTestSuite) TestCloseWebsocketCloseFails() {
	suite.websocketMock.SimulateOKResponse(types.Command{Command: "disconnect"}, nil)
	suite.websocketMock.OnClose(fmt.Errorf("mock error"))
	err := suite.createOpenConnection().Close()
	suite.EqualError(err, "failed to close websocket: mock error")
}

func (suite *ConnectionTestSuite) TestBeginSuccess() {
	tx, err := suite.createOpenConnection().Begin()
	suite.NoError(err)
	suite.NotNil(tx)
}

func (suite *ConnectionTestSuite) TestBeginFailsWithConnectionClosed() {
	conn := suite.createOpenConnection()
	conn.IsClosed = true
	tx, err := conn.Begin()
	suite.EqualError(err, driver.ErrBadConn.Error())
	suite.Nil(tx)
}

func (suite *ConnectionTestSuite) TestBeginFailsWithAutocommitEnabled() {
	conn := suite.createOpenConnection()
	conn.Config.Autocommit = true
	tx, err := conn.Begin()
	suite.EqualError(err, "E-EGOD-4: begin not working when autocommit is enabled")
	suite.Nil(tx)
}

func (suite *ConnectionTestSuite) TestQueryFailsConnectionClosed() {
	conn := suite.createOpenConnection()
	conn.IsClosed = true
	rows, err := conn.query(context.Background(), "query", nil)
	suite.EqualError(err, driver.ErrBadConn.Error())
	suite.Nil(rows)
}

func (suite *ConnectionTestSuite) TestQueryNoArgs() {
	suite.websocketMock.SimulateSQLQueriesResponse(
		types.SqlCommand{Command: types.Command{Command: "execute"}, SQLText: "query", Attributes: types.Attributes{}},
		types.SqlQueryResponseResultSet{ResultType: "resultType", ResultSet: types.SqlQueryResponseResultSetData{}})
	rows, err := suite.createOpenConnection().query(context.Background(), "query", []driver.Value{})
	suite.NoError(err)
	suite.NotNil(rows)
}

func (suite *ConnectionTestSuite) TestQueryNoArgsFails() {
	suite.websocketMock.SimulateErrorResponse(
		types.SqlCommand{Command: types.Command{Command: "execute"}, SQLText: "query", Attributes: types.Attributes{}},
		mockException)
	rows, err := suite.createOpenConnection().query(context.Background(), "query", []driver.Value{})
	suite.EqualError(err, mockExceptionError(mockException))
	suite.Nil(rows)
}

func (suite *ConnectionTestSuite) TestQueryWithArgs() {
	suite.websocketMock.SimulateOKResponse(
		types.SqlCommand{
			Command:    types.Command{Command: "createPreparedStatement"},
			SQLText:    "query",
			Attributes: types.Attributes{},
		},
		types.CreatePreparedStatementResponse{
			ParameterData: types.ParameterData{Columns: []types.SqlQueryColumn{{Name: "col", DataType: types.SqlQueryColumnType{Type: "type"}}}}})
	suite.websocketMock.SimulateSQLQueriesResponse(
		types.ExecutePreparedStatementCommand{Command: types.Command{Command: "executePreparedStatement"},
			StatementHandle: 0, NumColumns: 1, NumRows: 1,
			Columns: []types.SqlQueryColumn{{Name: "col", DataType: types.SqlQueryColumnType{Type: "type"}}},
			Data:    [][]interface{}{{"value"}},
		},
		types.SqlQueryResponseResultSet{ResultType: "resultType", ResultSet: types.SqlQueryResponseResultSetData{}})
	suite.websocketMock.SimulateOKResponse(types.ClosePreparedStatementCommand{Command: types.Command{Command: "closePreparedStatement"}, StatementHandle: 0, Attributes: types.Attributes{}}, nil)

	rows, err := suite.createOpenConnection().query(context.Background(), "query", []driver.Value{"value"})
	suite.NoError(err)
	suite.NotNil(rows)
}

func (suite *ConnectionTestSuite) TestQueryWithArgsFailsInPrepare() {
	suite.websocketMock.SimulateErrorResponse(
		types.SqlCommand{
			Command:    types.Command{Command: "createPreparedStatement"},
			SQLText:    "query",
			Attributes: types.Attributes{},
		},
		mockException)

	rows, err := suite.createOpenConnection().query(context.Background(), "query", []driver.Value{"value"})
	suite.EqualError(err, mockExceptionError(mockException))
	suite.Nil(rows)
}

func (suite *ConnectionTestSuite) TestQueryWithArgsFailsInExecute() {
	suite.websocketMock.SimulateOKResponse(
		types.SqlCommand{
			Command:    types.Command{Command: "createPreparedStatement"},
			SQLText:    "query",
			Attributes: types.Attributes{},
		},
		types.CreatePreparedStatementResponse{
			ParameterData: types.ParameterData{Columns: []types.SqlQueryColumn{{Name: "col", DataType: types.SqlQueryColumnType{Type: "type"}}}}})
	suite.websocketMock.SimulateErrorResponse(
		types.ExecutePreparedStatementCommand{Command: types.Command{Command: "executePreparedStatement"},
			StatementHandle: 0, NumColumns: 1, NumRows: 1,
			Columns: []types.SqlQueryColumn{{Name: "col", DataType: types.SqlQueryColumnType{Type: "type"}}},
			Data:    [][]interface{}{{"value"}},
		},
		mockException)

	rows, err := suite.createOpenConnection().query(context.Background(), "query", []driver.Value{"value"})
	suite.EqualError(err, mockExceptionError(mockException))
	suite.Nil(rows)
}

func (suite *ConnectionTestSuite) TestPasswordLoginFailsInitialRequest() {
	suite.websocketMock.SimulateErrorResponse(types.LoginCommand{Command: types.Command{Command: "login"}, ProtocolVersion: 42},
		mockException)
	conn := suite.createOpenConnection()
	conn.Config.ApiVersion = 42
	err := conn.Login(context.Background())
	suite.EqualError(err, mockExceptionError(mockException))
}

func (suite *ConnectionTestSuite) TestPasswordLoginFailsEncryptingPasswordRequest() {
	suite.websocketMock.SimulateOKResponse(types.LoginCommand{Command: types.Command{Command: "login"}, ProtocolVersion: 42},
		types.PublicKeyResponse{PublicKeyPem: "", PublicKeyModulus: "", PublicKeyExponent: ""})
	conn := suite.createOpenConnection()
	conn.Config.ApiVersion = 42
	err := conn.Login(context.Background())
	suite.EqualError(err, "driver: bad connection")
}

func (suite *ConnectionTestSuite) createOpenConnection() *Connection {
	conn := &Connection{
		Config:    &config.Config{Host: "invalid", Port: 12345},
		Ctx:       context.Background(),
		IsClosed:  false,
		websocket: suite.websocketMock,
	}
	return conn
}
