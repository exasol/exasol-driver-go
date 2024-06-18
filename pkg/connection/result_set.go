package connection

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"reflect"
	"sync"

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
		result := &types.SqlQueryResponseResultSetData{}
		err := results.con.Send(context.Background(), &types.FetchCommand{
			Command:         types.Command{Command: "fetch"},
			ResultSetHandle: results.data.ResultSetHandle,
			StartPosition:   results.totalRowPointer,
			NumBytes:        results.con.Config.FetchSize * 1024,
		}, result)
		if err != nil {
			return err
		}
		results.rowPointer = 0
		results.fetchedRows = results.fetchedRows + result.NumRows

		// Overwrite old data, user needs to collect the whole data if needed
		results.data.Data = result.Data
	}

	for i := range dest {
		dataType := results.data.Columns[i].DataType
		value := results.data.Data[i][results.rowPointer]
		dest[i] = convertResultSetValue(dataType, value)
	}

	results.rowPointer = results.rowPointer + 1
	results.totalRowPointer = results.totalRowPointer + 1

	return nil
}

func convertResultSetValue(dataType types.SqlQueryColumnType, value any) driver.Value {
	return value
}
