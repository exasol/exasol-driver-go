package exasol

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"reflect"
)

type queryResults struct {
	data        *SQLQueryResponseResultSetData
	con         *connection
	fetchedRows int
	rowPointer  int
}

func (results *queryResults) ColumnTypeDatabaseTypeName(index int) string {
	return results.data.Columns[index].DataType.Type
}

func (results *queryResults) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	if results.data.Columns[index].DataType.Precision != nil && results.data.Columns[index].DataType.Scale != nil {
		return *results.data.Columns[index].DataType.Precision, *results.data.Columns[index].DataType.Scale, true
	}
	return 0, 0, false
}

func (results *queryResults) ColumnTypeNullable(index int) (nullable, ok bool) {
	return true, true
}

func (results *queryResults) ColumnTypeScanType(index int) reflect.Type {
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

func (results *queryResults) ColumnTypeLength(index int) (length int64, ok bool) {
	if results.data.Columns[index].DataType.Size != nil {
		return *results.data.Columns[index].DataType.Size, true
	}
	return 0, false
}

func (results *queryResults) Columns() []string {
	col := make([]string, 0)
	for _, column := range results.data.Columns {
		col = append(col, column.Name)
	}
	return col
}

func (results *queryResults) Close() error {
	if results.data.ResultSetHandle == 0 {
		return nil
	}
	return results.con.send(context.Background(), &CloseResultSetCommand{
		Command:          Command{"closeResultSet"},
		ResultSetHandles: []int{results.data.ResultSetHandle},
	}, nil)
}

func (results *queryResults) Next(dest []driver.Value) error {
	if results.data.NumRows == 0 {
		return io.EOF
	}

	if results.rowPointer >= results.data.NumRows {
		return io.EOF
	}

	if results.data.NumRowsInMessage < results.data.NumRows && results.rowPointer == results.fetchedRows {
		result := &SQLQueryResponseResultSetData{}
		err := results.con.send(context.Background(), &FetchCommand{
			Command:         Command{"fetch"},
			ResultSetHandle: results.data.ResultSetHandle,
			StartPosition:   results.rowPointer,
			NumBytes:        results.con.config.fetchSize,
		}, result)
		if err != nil {
			return err
		}

		results.fetchedRows = results.fetchedRows + result.NumRows

		if results.data.Data == nil {
			results.data.Data = result.Data
		} else {
			for i, d := range result.Data {
				results.data.Data[i] = append(results.data.Data[i], d...)
			}
		}

	}

	for i := range dest {
		dest[i] = results.data.Data[i][results.rowPointer]
	}
	results.rowPointer = results.rowPointer + 1
	return nil
}
