package exasol

import (
	"database/sql"
	"github.com/stretchr/testify/suite"
	"reflect"
	"testing"
)

type ResultSetTestSuite struct {
	suite.Suite
}

func TestResultSetSuite(t *testing.T) {
	suite.Run(t, new(ResultSetTestSuite))
}

func (suite *ResultSetTestSuite) TestColumnTypeDatabaseTypeName() {
	data := SQLQueryResponseResultSetData{Columns: []SQLQueryColumn{
		{DataType: SQLQueryColumnType{Type: "boolean"}},
		{DataType: SQLQueryColumnType{Type: "char"}},
		{DataType: SQLQueryColumnType{}},
	}}
	queryResults := queryResults{data: &data}
	suite.Equal("boolean", queryResults.ColumnTypeDatabaseTypeName(0))
	suite.Equal("char", queryResults.ColumnTypeDatabaseTypeName(1))
	suite.Equal("", queryResults.ColumnTypeDatabaseTypeName(2))
}

func (suite *ResultSetTestSuite) TestColumnTypePrecisionScale() {
	expectedPrecision := int64(10)
	expectedScale := int64(3)
	data := SQLQueryResponseResultSetData{Columns: []SQLQueryColumn{
		{DataType: SQLQueryColumnType{Precision: &expectedPrecision, Scale: &expectedScale}},
	}}
	queryResults := queryResults{data: &data}
	precision, scale, ok := queryResults.ColumnTypePrecisionScale(0)
	suite.Equal(expectedPrecision, precision)
	suite.Equal(expectedScale, scale)
	suite.Equal(true, ok)
}

func (suite *ResultSetTestSuite) TestColumnTypePrecisionScaleWithoutPrecision() {
	expectedScale := int64(3)
	data := SQLQueryResponseResultSetData{Columns: []SQLQueryColumn{
		{DataType: SQLQueryColumnType{Scale: &expectedScale}},
	}}
	queryResults := queryResults{data: &data}
	precision, scale, ok := queryResults.ColumnTypePrecisionScale(0)
	suite.Equal(int64(0), precision)
	suite.Equal(int64(0), scale)
	suite.Equal(false, ok)
}

func (suite *ResultSetTestSuite) TestColumnTypePrecisionScaleWithoutScale() {
	expectedScale := int64(3)
	data := SQLQueryResponseResultSetData{Columns: []SQLQueryColumn{
		{DataType: SQLQueryColumnType{Scale: &expectedScale}},
	}}
	queryResults := queryResults{data: &data}
	precision, scale, ok := queryResults.ColumnTypePrecisionScale(0)
	suite.Equal(int64(0), precision)
	suite.Equal(int64(0), scale)
	suite.Equal(false, ok)
}

func (suite *ResultSetTestSuite) TestColumnTypeNullable() {
	queryResults := queryResults{}
	nullable, ok := queryResults.ColumnTypeNullable(0)
	suite.True(nullable)
	suite.True(ok)
}

func (suite *ResultSetTestSuite) TestColumnTypeScanTypeVarchar() {
	suite.assertColumnType("VARCHAR", sql.RawBytes{})
}

func (suite *ResultSetTestSuite) assertColumnType(columnType string, sqlType interface{}) {
	data := SQLQueryResponseResultSetData{Columns: []SQLQueryColumn{
		{DataType: SQLQueryColumnType{Type: columnType}},
	}}
	queryResults := queryResults{data: &data}
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
	data := SQLQueryResponseResultSetData{Columns: []SQLQueryColumn{
		{DataType: SQLQueryColumnType{Size: &expectedLength}},
	}}
	queryResults := queryResults{data: &data}
	length, ok := queryResults.ColumnTypeLength(0)
	suite.Equal(expectedLength, length)
	suite.Equal(true, ok)
}

func (suite *ResultSetTestSuite) TestColumnTypeLengthInvalid() {
	data := SQLQueryResponseResultSetData{Columns: []SQLQueryColumn{
		{DataType: SQLQueryColumnType{}},
	}}
	queryResults := queryResults{data: &data}
	length, ok := queryResults.ColumnTypeLength(0)
	suite.Equal(int64(0), length)
	suite.Equal(false, ok)
}

func (suite *ResultSetTestSuite) TestColumns() {
	data := SQLQueryResponseResultSetData{Columns: []SQLQueryColumn{
		{Name: "col_1"},
		{Name: "col_2"},
		{Name: "col_3"},
	}}
	queryResults := queryResults{data: &data}
	suite.Equal([]string{"col_1", "col_2", "col_3"}, queryResults.Columns())
}

func (suite *ResultSetTestSuite) TestNextWithoutRows() {
	data := SQLQueryResponseResultSetData{NumRows: 0}
	queryResults := queryResults{data: &data}
	suite.EqualError(queryResults.Next(nil), "EOF")
}

func (suite *ResultSetTestSuite) TestNextPointerDoesNotMatch() {
	data := SQLQueryResponseResultSetData{NumRows: 1}
	queryResults := queryResults{data: &data, rowPointer: 2}
	suite.EqualError(queryResults.Next(nil), "EOF")
}
