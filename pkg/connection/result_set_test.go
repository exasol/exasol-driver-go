package connection

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"testing"

	"github.com/exasol/exasol-driver-go/internal/config"
	"github.com/exasol/exasol-driver-go/pkg/connection/wsconn"
	"github.com/exasol/exasol-driver-go/pkg/logger"
	"github.com/exasol/exasol-driver-go/pkg/types"

	"github.com/stretchr/testify/suite"
)

type ResultSetTestSuite struct {
	suite.Suite
	websocketMock *wsconn.WebsocketConnectionMock
}

func TestResultSetSuite(t *testing.T) {
	suite.Run(t, new(ResultSetTestSuite))
}

func (suite *ResultSetTestSuite) SetupTest() {
	suite.websocketMock = wsconn.CreateWebsocketConnectionMock()
	logger.SetTraceLogger(log.New(os.Stderr, "[TestResultSetSuite] ", log.LstdFlags|log.Lshortfile))
}

func (suite *ResultSetTestSuite) TearDownTest() {
	logger.SetTraceLogger(nil)
}

func (suite *ResultSetTestSuite) TestColumnTypeDatabaseTypeName() {
	data := types.SqlQueryResponseResultSetData{Columns: []types.SqlQueryColumn{
		{DataType: types.SqlQueryColumnType{Type: "boolean"}},
		{DataType: types.SqlQueryColumnType{Type: "char"}},
		{DataType: types.SqlQueryColumnType{}},
	}}
	queryResults := QueryResults{data: &data}
	suite.Equal("boolean", queryResults.ColumnTypeDatabaseTypeName(0))
	suite.Equal("char", queryResults.ColumnTypeDatabaseTypeName(1))
	suite.Equal("", queryResults.ColumnTypeDatabaseTypeName(2))
}

func (suite *ResultSetTestSuite) TestColumnTypePrecisionScale() {
	expectedPrecision := int64(10)
	expectedScale := int64(3)
	data := types.SqlQueryResponseResultSetData{Columns: []types.SqlQueryColumn{
		{DataType: types.SqlQueryColumnType{Precision: &expectedPrecision, Scale: &expectedScale}},
	}}
	queryResults := QueryResults{data: &data}
	precision, scale, ok := queryResults.ColumnTypePrecisionScale(0)
	suite.Equal(expectedPrecision, precision)
	suite.Equal(expectedScale, scale)
	suite.Equal(true, ok)
}

func (suite *ResultSetTestSuite) TestColumnTypePrecisionScaleWithoutPrecision() {
	expectedScale := int64(3)
	data := types.SqlQueryResponseResultSetData{Columns: []types.SqlQueryColumn{
		{DataType: types.SqlQueryColumnType{Scale: &expectedScale}},
	}}
	queryResults := QueryResults{data: &data}
	precision, scale, ok := queryResults.ColumnTypePrecisionScale(0)
	suite.Equal(int64(0), precision)
	suite.Equal(int64(0), scale)
	suite.Equal(false, ok)
}

func (suite *ResultSetTestSuite) TestColumnTypePrecisionScaleWithoutScale() {
	expectedPrecision := int64(3)
	data := types.SqlQueryResponseResultSetData{Columns: []types.SqlQueryColumn{
		{DataType: types.SqlQueryColumnType{Precision: &expectedPrecision}},
	}}
	queryResults := QueryResults{data: &data}
	precision, scale, ok := queryResults.ColumnTypePrecisionScale(0)
	suite.Equal(int64(0), precision)
	suite.Equal(int64(0), scale)
	suite.Equal(false, ok)
}

func (suite *ResultSetTestSuite) TestColumnTypeNullable() {
	queryResults := QueryResults{}
	nullable, ok := queryResults.ColumnTypeNullable(0)
	suite.True(nullable)
	suite.True(ok)
}

func (suite *ResultSetTestSuite) TestColumnTypeScanTypeVarchar() {
	suite.assertColumnType("VARCHAR", sql.RawBytes{})
}

func (suite *ResultSetTestSuite) assertColumnType(columnType string, sqlType interface{}) {
	data := types.SqlQueryResponseResultSetData{Columns: []types.SqlQueryColumn{
		{DataType: types.SqlQueryColumnType{Type: columnType}},
	}}
	queryResults := QueryResults{data: &data}
	suite.Equal(reflect.TypeOf(sqlType), queryResults.ColumnTypeScanType(0))
}

func (suite *ResultSetTestSuite) TestColumnTypeScanTypeChar() {
	suite.assertColumnType("CHAR", sql.RawBytes{})
}

func (suite *ResultSetTestSuite) TestColumnTypeScanTypeGeometry() {
	suite.assertColumnType("GEOMETRY", sql.RawBytes{})
}

func (suite *ResultSetTestSuite) TestColumnTypeScanTypeHashtype() {
	suite.assertColumnType("HASHTYPE", sql.RawBytes{})
}

func (suite *ResultSetTestSuite) TestColumnTypeScanTypeIntervalDayToSecond() {
	suite.assertColumnType("INTERVAL DAY TO SECOND", sql.RawBytes{})
}

func (suite *ResultSetTestSuite) TestColumnTypeScanTypeIntervalYearToMonth() {
	suite.assertColumnType("INTERVAL YEAR TO MONTH", sql.RawBytes{})
}

func (suite *ResultSetTestSuite) TestColumnTypeScanTypeBoolean() {
	suite.assertColumnType("BOOLEAN", sql.NullBool{})
}

func (suite *ResultSetTestSuite) TestColumnTypeScanTypeDouble() {
	suite.assertColumnType("DOUBLE", sql.NullFloat64{})
}

func (suite *ResultSetTestSuite) TestColumnTypeScanTypeDefault() {
	suite.assertColumnType("UNKNOWN", new(interface{}))
}

func (suite *ResultSetTestSuite) TestColumnTypeLength() {
	expectedLength := int64(3)
	data := types.SqlQueryResponseResultSetData{Columns: []types.SqlQueryColumn{
		{DataType: types.SqlQueryColumnType{Size: &expectedLength}},
	}}
	queryResults := QueryResults{data: &data}
	length, ok := queryResults.ColumnTypeLength(0)
	suite.Equal(expectedLength, length)
	suite.Equal(true, ok)
}

func (suite *ResultSetTestSuite) TestColumnTypeLengthInvalid() {
	data := types.SqlQueryResponseResultSetData{Columns: []types.SqlQueryColumn{
		{DataType: types.SqlQueryColumnType{}},
	}}
	queryResults := QueryResults{data: &data}
	length, ok := queryResults.ColumnTypeLength(0)
	suite.Equal(int64(0), length)
	suite.Equal(false, ok)
}

func (suite *ResultSetTestSuite) TestColumns() {
	data := types.SqlQueryResponseResultSetData{Columns: []types.SqlQueryColumn{
		{Name: "col_1"},
		{Name: "col_2"},
		{Name: "col_3"},
	}}
	queryResults := QueryResults{data: &data}
	suite.Equal([]string{"col_1", "col_2", "col_3"}, queryResults.Columns())
}

func (suite *ResultSetTestSuite) TestColumnsEmpty() {
	data := types.SqlQueryResponseResultSetData{Columns: []types.SqlQueryColumn{}}
	queryResults := QueryResults{data: &data}
	suite.Empty(queryResults.Columns())
}

func (suite *ResultSetTestSuite) TestNextWithoutRows() {
	data := types.SqlQueryResponseResultSetData{NumRows: 0}
	queryResults := QueryResults{data: &data}
	suite.EqualError(queryResults.Next(nil), "EOF")
}

func (suite *ResultSetTestSuite) TestNextPointerDoesNotMatch() {
	data := types.SqlQueryResponseResultSetData{NumRows: 1}
	queryResults := QueryResults{data: &data, totalRowPointer: 2}
	suite.EqualError(queryResults.Next(nil), "EOF")
}

func (suite *ResultSetTestSuite) TestNextFetchesNextChunk() {
	queryResults := suite.createResultSet()
	queryResults.con.Config.FetchSize = 2
	queryResults.totalRowPointer = 0
	queryResults.data.ResultSetHandle = 17
	queryResults.data.NumRows = 2
	queryResults.data.NumRowsInMessage = 1
	queryResults.fetchedRows = 0

	suite.websocketMock.SimulateOKResponse(&types.FetchCommand{
		Command:         types.Command{Command: "fetch"},
		ResultSetHandle: 17,
		StartPosition:   0,
		NumBytes:        2048,
	}, types.SqlQueryResponseResultSetData{
		ResultSetHandle: 17, NumRows: 2, NumRowsInMessage: 1, Columns: []types.SqlQueryColumn{{}, {}}, Data: [][]interface{}{{"c1r1", "c1r2"}, {"c2r1", "c2r2"}},
	})
	dest := make([]driver.Value, 2)
	suite.NoError(queryResults.Next(dest))
	suite.Equal([]driver.Value{"c1r1", "c2r1"}, dest)
	suite.Equal(1, queryResults.rowPointer)
	suite.Equal(2, queryResults.fetchedRows)
	suite.Equal(1, queryResults.rowPointer)
	suite.Equal(1, queryResults.totalRowPointer)
}

func (suite *ResultSetTestSuite) TestNextReturnsCurrentData() {
	queryResults := suite.createResultSet()
	queryResults.con.Config.FetchSize = 2
	queryResults.totalRowPointer = 0
	queryResults.data.ResultSetHandle = 17
	queryResults.data.NumRows = 2
	queryResults.data.NumRowsInMessage = 1
	queryResults.data.Data = [][]interface{}{{"c1r1", "c1r2"}, {"c2r1", "c2r2"}}
	queryResults.fetchedRows = 1

	dest := make([]driver.Value, 2)
	suite.NoError(queryResults.Next(dest))
	suite.Equal([]driver.Value{"c1r1", "c2r1"}, dest)
}

func (suite *ResultSetTestSuite) TestNextReturnsIncrementsCounters() {
	queryResults := suite.createResultSet()
	queryResults.con.Config.FetchSize = 2
	queryResults.totalRowPointer = 0
	queryResults.data.ResultSetHandle = 17
	queryResults.data.NumRows = 2
	queryResults.data.NumRowsInMessage = 1
	queryResults.data.Data = [][]interface{}{{"c1r1", "c1r2"}, {"c2r1", "c2r2"}}
	queryResults.fetchedRows = 1

	dest := make([]driver.Value, 0)
	suite.NoError(queryResults.Next(dest))
	suite.Equal(1, queryResults.rowPointer)
	suite.Equal(1, queryResults.totalRowPointer)
	suite.Equal(1, queryResults.rowPointer)
	suite.Equal(1, queryResults.fetchedRows)
}

func (suite *ResultSetTestSuite) TestNextFetchFailsWithSqlError() {
	queryResults := suite.createResultSet()
	queryResults.con.Config.FetchSize = 2
	queryResults.totalRowPointer = 0
	queryResults.data.ResultSetHandle = 17
	queryResults.data.NumRows = 2
	queryResults.data.NumRowsInMessage = 1
	queryResults.fetchedRows = 0

	suite.websocketMock.SimulateErrorResponse(&types.FetchCommand{
		Command:         types.Command{Command: "fetch"},
		ResultSetHandle: 17,
		StartPosition:   0,
		NumBytes:        2048,
	}, types.Exception{SQLCode: "sql-code", Text: "mock error"})

	suite.EqualError(queryResults.Next(nil), "E-EGOD-11: execution failed with SQL error code 'sql-code' and message 'mock error'")
}

func (suite *ResultSetTestSuite) TestNextFetchFailsWithError() {
	queryResults := suite.createResultSet()
	queryResults.con.Config.FetchSize = 2
	queryResults.totalRowPointer = 0
	queryResults.data.ResultSetHandle = 17
	queryResults.data.NumRows = 2
	queryResults.data.NumRowsInMessage = 1
	queryResults.fetchedRows = 0

	suite.websocketMock.SimulateWriteFails(&types.FetchCommand{
		Command:         types.Command{Command: "fetch"},
		ResultSetHandle: 17,
		StartPosition:   0,
		NumBytes:        2048,
	}, fmt.Errorf("mock error"))

	err := queryResults.Next(nil)
	suite.EqualError(err, "W-EGOD-16: could not send request: 'mock error'")
	suite.True(errors.Is(err, driver.ErrBadConn))
}

func (suite *ResultSetTestSuite) TestCloseIgnoresResultHandleZero() {
	queryResults := suite.createResultSet()
	queryResults.data.ResultSetHandle = 0
	suite.NoError(queryResults.Close())
}

func (suite *ResultSetTestSuite) TestCloseSendsCloseResultSetCommand() {
	queryResults := suite.createResultSet()
	queryResults.data.ResultSetHandle = 17
	suite.websocketMock.SimulateOKResponse(types.CloseResultSetCommand{
		Command:          types.Command{Command: "closeResultSet"},
		ResultSetHandles: []int{17},
	}, nil)
	suite.NoError(queryResults.Close())
}

func (suite *ResultSetTestSuite) createResultSet() QueryResults {
	return QueryResults{
		data: &types.SqlQueryResponseResultSetData{
			ResultSetHandle: 1, NumRows: 2, NumRowsInMessage: 2, Columns: []types.SqlQueryColumn{{}, {}},
		},
		con: &Connection{
			websocket: suite.websocketMock, Config: &config.Config{}, Ctx: context.Background(), IsClosed: false,
		},
		fetchedRows:     0,
		totalRowPointer: 0,
		rowPointer:      0,
	}
}
