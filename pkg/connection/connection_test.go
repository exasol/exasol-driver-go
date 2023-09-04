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
	err := conn.Login(context.Background())
	suite.EqualError(err, mockExceptionError(mockException))
}

func (suite *ConnectionTestSuite) TestPasswordLoginFailsEncryptingPasswordRequest() {
	suite.websocketMock.SimulateOKResponse(types.LoginCommand{Command: types.Command{Command: "login"}, ProtocolVersion: 42},
		types.PublicKeyResponse{PublicKeyPem: "", PublicKeyModulus: "", PublicKeyExponent: ""})
	conn := suite.createOpenConnection()
	err := conn.Login(context.Background())
	suite.EqualError(err, driver.ErrBadConn.Error())
}

func (suite *ConnectionTestSuite) TestPasswordLoginSuccess() {
	suite.simulatePasswordLoginSuccess()
	conn := suite.createOpenConnection()
	conn.IsClosed = true

	suite.True(conn.IsClosed)
	err := conn.Login(context.Background())
	suite.False(conn.IsClosed)
	suite.NoError(err)
}

func (suite *ConnectionTestSuite) TestAccessTokenLoginSuccess() {
	suite.simulateTokenLoginSuccess()
	conn := suite.createOpenConnection()
	conn.IsClosed = true
	conn.Config.AccessToken = "accessToken"

	suite.True(conn.IsClosed)
	err := conn.Login(context.Background())
	suite.False(conn.IsClosed)
	suite.NoError(err)
}

func (suite *ConnectionTestSuite) TestAccessTokenLoginPrepareFails() {
	suite.websocketMock.SimulateErrorResponse(types.LoginCommand{Command: types.Command{Command: "loginToken"}, ProtocolVersion: 42}, mockException)
	conn := suite.createOpenConnection()
	conn.IsClosed = true
	conn.Config.AccessToken = "accessToken"

	suite.True(conn.IsClosed)
	err := conn.Login(context.Background())
	suite.True(conn.IsClosed)
	suite.EqualError(err, "access token login failed: E-EGOD-11: execution failed with SQL error code 'mock sql code' and message 'mock error'")
}

func (suite *ConnectionTestSuite) TestRefreshTokenLoginSuccess() {
	suite.simulateTokenLoginSuccess()
	conn := suite.createOpenConnection()
	conn.IsClosed = true
	conn.Config.RefreshToken = "refreshToken"

	suite.True(conn.IsClosed)
	err := conn.Login(context.Background())
	suite.False(conn.IsClosed)
	suite.NoError(err)
}

func (suite *ConnectionTestSuite) TestRefreshTokenLoginPrepareFails() {
	suite.websocketMock.SimulateErrorResponse(types.LoginCommand{Command: types.Command{Command: "loginToken"}, ProtocolVersion: 42}, mockException)
	conn := suite.createOpenConnection()
	conn.IsClosed = true
	conn.Config.RefreshToken = "refreshToken"

	suite.True(conn.IsClosed)
	err := conn.Login(context.Background())
	suite.True(conn.IsClosed)
	suite.EqualError(err, "refresh token login failed: E-EGOD-11: execution failed with SQL error code 'mock sql code' and message 'mock error'")
}

func (suite *ConnectionTestSuite) TestLoginRestoresCompressionToTrue() {
	suite.simulatePasswordLoginSuccess()
	conn := suite.createOpenConnection()
	conn.Config.Compression = true

	err := conn.Login(context.Background())
	suite.True(conn.Config.Compression)
	suite.NoError(err)
}
func (suite *ConnectionTestSuite) TestLoginRestoresCompressionToFalse() {
	suite.simulatePasswordLoginSuccess()
	conn := suite.createOpenConnection()
	conn.Config.Compression = false

	err := conn.Login(context.Background())
	suite.False(conn.Config.Compression)
	suite.NoError(err)
}

func (suite *ConnectionTestSuite) TestLoginFails() {
	suite.simulatePasswordLoginFailure(&mockException)
	conn := suite.createOpenConnection()
	conn.IsClosed = false

	err := conn.Login(context.Background())
	suite.True(conn.IsClosed)
	suite.EqualError(err, "failed to login: E-EGOD-11: execution failed with SQL error code 'mock sql code' and message 'mock error'")
}

func (suite *ConnectionTestSuite) TestLoginFailureRestoresCompressionToTrue() {
	suite.simulatePasswordLoginFailure(&mockException)
	conn := suite.createOpenConnection()
	conn.Config.Compression = true

	conn.Login(context.Background())
	suite.True(conn.Config.Compression)
}

func (suite *ConnectionTestSuite) TestLoginFailureRestoresCompressionToFalse() {
	suite.simulatePasswordLoginFailure(&mockException)
	conn := suite.createOpenConnection()
	conn.Config.Compression = false

	conn.Login(context.Background())
	suite.False(conn.Config.Compression)
}

func (suite *ConnectionTestSuite) simulatePasswordLoginSuccess() {
	suite.websocketMock.SimulateOKResponse(types.LoginCommand{Command: types.Command{Command: "login"}, ProtocolVersion: 42},
		types.PublicKeyResponse{
			PublicKeyPem: `-----BEGIN RSA PUBLIC KEY-----
MIGJAoGBAK4nFBtH5EBOFw+yqga1XS1G/eCkVSBYDDxMXVEHsUMqAcyH1M2khKFX
ZZqyqPzyU+Gm9Hn0K9YuoteX2l/Ruf4AsvMfm9JujB11bobk9isILutKMfdJ7Pmu
uYIhswioGpmyPXr/wqz1NFkt5wMzm6sU3lFfCjD5SxU6arQ1zVY3AgMBAAE=
-----END RSA PUBLIC KEY-----`,
			PublicKeyModulus:  `AE27141B47E4404E170FB2AA06B55D2D46FDE0A45520580C3C4C5D5107B1432A01CC87D4CDA484A157659AB2A8FCF253E1A6F479F42BD62EA2D797DA5FD1B9FE00B2F31F9BD26E8C1D756E86E4F62B082EEB4A31F749ECF9AEB98221B308A81A99B23D7AFFC2ACF534592DE703339BAB14DE515F0A30F94B153A6AB435CD5637`,
			PublicKeyExponent: "010001"})
	suite.websocketMock.SimulateOKResponseOnAnyMessage(types.AuthResponse{})
}

func (suite *ConnectionTestSuite) simulatePasswordLoginFailure(exception *types.Exception) {
	suite.websocketMock.SimulateOKResponse(types.LoginCommand{Command: types.Command{Command: "login"}, ProtocolVersion: 42},
		types.PublicKeyResponse{
			PublicKeyPem: `-----BEGIN RSA PUBLIC KEY-----
MIGJAoGBAK4nFBtH5EBOFw+yqga1XS1G/eCkVSBYDDxMXVEHsUMqAcyH1M2khKFX
ZZqyqPzyU+Gm9Hn0K9YuoteX2l/Ruf4AsvMfm9JujB11bobk9isILutKMfdJ7Pmu
uYIhswioGpmyPXr/wqz1NFkt5wMzm6sU3lFfCjD5SxU6arQ1zVY3AgMBAAE=
-----END RSA PUBLIC KEY-----`,
			PublicKeyModulus:  `AE27141B47E4404E170FB2AA06B55D2D46FDE0A45520580C3C4C5D5107B1432A01CC87D4CDA484A157659AB2A8FCF253E1A6F479F42BD62EA2D797DA5FD1B9FE00B2F31F9BD26E8C1D756E86E4F62B082EEB4A31F749ECF9AEB98221B308A81A99B23D7AFFC2ACF534592DE703339BAB14DE515F0A30F94B153A6AB435CD5637`,
			PublicKeyExponent: "010001"})
	suite.websocketMock.SimulateErrorResponseOnAnyMessage(*exception)
}

func (suite *ConnectionTestSuite) simulateTokenLoginSuccess() {
	suite.websocketMock.SimulateOKResponse(types.LoginCommand{Command: types.Command{Command: "loginToken"}, ProtocolVersion: 42},
		types.PublicKeyResponse{
			PublicKeyPem: `-----BEGIN RSA PUBLIC KEY-----
MIGJAoGBAK4nFBtH5EBOFw+yqga1XS1G/eCkVSBYDDxMXVEHsUMqAcyH1M2khKFX
ZZqyqPzyU+Gm9Hn0K9YuoteX2l/Ruf4AsvMfm9JujB11bobk9isILutKMfdJ7Pmu
uYIhswioGpmyPXr/wqz1NFkt5wMzm6sU3lFfCjD5SxU6arQ1zVY3AgMBAAE=
-----END RSA PUBLIC KEY-----`,
			PublicKeyModulus:  `AE27141B47E4404E170FB2AA06B55D2D46FDE0A45520580C3C4C5D5107B1432A01CC87D4CDA484A157659AB2A8FCF253E1A6F479F42BD62EA2D797DA5FD1B9FE00B2F31F9BD26E8C1D756E86E4F62B082EEB4A31F749ECF9AEB98221B308A81A99B23D7AFFC2ACF534592DE703339BAB14DE515F0A30F94B153A6AB435CD5637`,
			PublicKeyExponent: "010001"})
	suite.websocketMock.SimulateOKResponseOnAnyMessage(types.AuthResponse{})
}

func (suite *ConnectionTestSuite) createOpenConnection() *Connection {
	conn := &Connection{
		Config:    &config.Config{Host: "invalid", Port: 12345, User: "user", Password: "password", ApiVersion: 42},
		Ctx:       context.Background(),
		IsClosed:  false,
		websocket: suite.websocketMock,
	}
	return conn
}
