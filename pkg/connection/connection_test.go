package connection

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/exasol/exasol-driver-go/internal/config"
	"github.com/exasol/exasol-driver-go/pkg/connection/wsconn"
	"github.com/exasol/exasol-driver-go/pkg/types"
	"github.com/stretchr/testify/suite"
)

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
		[]types.SqlQueryResponseResultSet{{ResultType: "resultType", ResultSet: types.SqlQueryResponseResultSetData{}}})

	suite.websocketMock.SimulateOKResponse(types.ClosePreparedStatementCommand{Command: types.Command{Command: "closePreparedStatement"}, StatementHandle: 0, Attributes: types.Attributes{}}, nil)
	rows, err := suite.createOpenConnection().QueryContext(context.Background(), "query", []driver.NamedValue{{Ordinal: 1, Value: "value"}})
	suite.NoError(err)
	suite.Equal([]string{}, rows.Columns())
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
