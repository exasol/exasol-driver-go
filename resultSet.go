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

func (r *queryResults) ColumnTypeDatabaseTypeName(index int) string {
	return r.data.Columns[index].DataType.Type
}

func (r *queryResults) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	if r.data.Columns[index].DataType.Precision != nil && r.data.Columns[index].DataType.Scale != nil {
		return *r.data.Columns[index].DataType.Precision, *r.data.Columns[index].DataType.Scale, true
	}
	return 0, 0, false
}

func (r *queryResults) ColumnTypeNullable(index int) (nullable, ok bool) {
	return true, true
}

func (r *queryResults) ColumnTypeScanType(index int) reflect.Type {
	switch r.ColumnTypeDatabaseTypeName(index) {
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

func (r *queryResults) ColumnTypeLength(index int) (length int64, ok bool) {
	if r.data.Columns[index].DataType.Size != nil {
		return *r.data.Columns[index].DataType.Size, true
	}
	return 0, false
}

func (r *queryResults) Columns() []string {
	col := make([]string, 0)
	for _, column := range r.data.Columns {
		col = append(col, column.Name)
	}
	return col
}

func (r *queryResults) Close() error {
	if r.data.ResultSetHandle == 0 {
		return nil
	}
	return r.con.send(context.Background(), &CloseResultSetCommand{
		Command:          Command{"closeResultSet"},
		ResultSetHandles: []int{r.data.ResultSetHandle},
	}, nil)
}

func (r *queryResults) Next(dest []driver.Value) error {
	if r.data.NumRows == 0 {
		return io.EOF
	}

	if r.rowPointer >= r.data.NumRows {
		return io.EOF
	}

	if r.data.NumRowsInMessage < r.data.NumRows && r.rowPointer == r.fetchedRows {
		result := &SQLQueryResponseResultSetData{}
		err := r.con.send(context.Background(), &FetchCommand{
			Command:         Command{"fetch"},
			ResultSetHandle: r.data.ResultSetHandle,
			StartPosition:   r.rowPointer,
			NumBytes:        r.con.config.FetchSize,
		}, result)
		if err != nil {
			return err
		}

		r.fetchedRows = r.fetchedRows + result.NumRowsInMessage

		if r.data.Data == nil {
			r.data.Data = result.Data
		} else {
			for i, d := range result.Data {
				r.data.Data[i] = append(r.data.Data[i], d...)
			}
		}

	}

	for i := range dest {
		dest[i] = r.data.Data[i][r.rowPointer]
	}
	r.rowPointer = r.rowPointer + 1
	return nil
}
