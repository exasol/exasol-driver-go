package connection

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"reflect"
	"sync"

	"github.com/exasol/exasol-driver-go/pkg/logger"
	"github.com/exasol/exasol-driver-go/pkg/types"
)

type QueryResults struct {
	sync.Mutex      // guards following
	data            *types.SqlQueryResponseResultSetData
	con             *Connection
	fetchedRows     int
	totalRowPointer int
	rowPointer      int
}

func (results *QueryResults) ColumnTypeDatabaseTypeName(index int) string {
	return results.data.Columns[index].DataType.Type
}

func (results *QueryResults) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	if results.data.Columns[index].DataType.Precision != nil && results.data.Columns[index].DataType.Scale != nil {
		return *results.data.Columns[index].DataType.Precision, *results.data.Columns[index].DataType.Scale, true
	}
	return 0, 0, false
}

func (results *QueryResults) ColumnTypeNullable(index int) (nullable, ok bool) {
	return true, true
}

func (results *QueryResults) ColumnTypeScanType(index int) reflect.Type {
	switch results.ColumnTypeDatabaseTypeName(index) {
	case "VARCHAR", "CHAR", "GEOMETRY", "HASHTYPE", "INTERVAL DAY TO SECOND", "INTERVAL YEAR TO MONTH":
		return reflect.TypeOf(sql.RawBytes{})
	case "BOOLEAN":
		return reflect.TypeOf(sql.NullBool{})
	case "DOUBLE":
		return reflect.TypeOf(sql.NullFloat64{})
	default:
		return reflect.TypeOf(new(interface{}))
	}
}

func (results *QueryResults) ColumnTypeLength(index int) (length int64, ok bool) {
	if results.data.Columns[index].DataType.Size != nil {
		return *results.data.Columns[index].DataType.Size, true
	}
	return 0, false
}

func (results *QueryResults) Columns() []string {
	col := make([]string, 0)
	for _, column := range results.data.Columns {
		col = append(col, column.Name)
	}
	return col
}

func (results *QueryResults) Close() error {
	if results.data.ResultSetHandle == 0 {
		return nil
	}
	return results.con.Send(context.Background(), &types.CloseResultSetCommand{
		Command:          types.Command{Command: "closeResultSet"},
		ResultSetHandles: []int{results.data.ResultSetHandle},
	}, nil)
}

func (results *QueryResults) Next(dest []driver.Value) error {
	if results.data.NumRows == 0 {
		return io.EOF
	}

	if results.totalRowPointer >= results.data.NumRows {
		return io.EOF
	}

	if results.data.NumRowsInMessage < results.data.NumRows && results.totalRowPointer == results.fetchedRows {
		err := results.fetchNextRowChunk()
		if err != nil {
			return err
		}
	}

	for columnIndex := range dest {
		dest[columnIndex] = results.getColumnValue(columnIndex)
	}

	results.rowPointer = results.rowPointer + 1
	results.totalRowPointer = results.totalRowPointer + 1

	return nil
}

func (results *QueryResults) getColumnValue(columnIndex int) driver.Value {
	value := results.data.Data[columnIndex][results.rowPointer]
	columnType := results.data.Columns[columnIndex].DataType
	return convertValue(value, columnType)
}

func convertValue(value any, columnType types.SqlQueryColumnType) driver.Value {
	if columnType.Type == "DECIMAL" && columnType.Scale != nil && *columnType.Scale == 0 {
		if floatValue, ok := value.(float64); ok {
			return int64(floatValue)
		}
	}
	return value
}

func (results *QueryResults) fetchNextRowChunk() error {
	chunk := &types.SqlQueryResponseResultSetData{}
	err := results.con.Send(context.Background(), &types.FetchCommand{
		Command:         types.Command{Command: "fetch"},
		ResultSetHandle: results.data.ResultSetHandle,
		StartPosition:   results.totalRowPointer,
		NumBytes:        results.con.Config.FetchSize * 1024,
	}, chunk)
	if err != nil {
		return err
	}
	results.rowPointer = 0
	results.fetchedRows += chunk.NumRows
	logger.TraceLogger.Printf("Fetched %d rows from result set %d with fetch size %d kB at start pos %d\n", chunk.NumRows, results.data.ResultSetHandle, results.con.Config.FetchSize, results.totalRowPointer)

	// Overwrite old data, user needs to collect the whole data if needed
	results.data.Data = chunk.Data
	return nil
}
