package exasol

import (
	"database/sql/driver"
	"encoding/json"
	"io"
)

type queryResults struct {
	data        *SQLQueryResponseResultSetData
	con         *connection
	fetchedRows int
	rowPointer  int
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
	return r.con.send(&CloseResultSetCommand{
		Command:          Command{"resultSetHandles"},
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

	if r.data.NumRowsInMessage < r.data.NumRows {
		result := &SQLQueriesResponse{}
		err := r.con.send(&FetchCommand{
			Command:         Command{"fetch"},
			ResultSetHandle: r.data.ResultSetHandle,
			StartPosition:   r.rowPointer,
			NumBytes:        r.con.Config.FetchSize,
		}, result)
		if err != nil {
			return err
		}

		resultSet := &SQLQueryResponseResultSet{}
		err = json.Unmarshal(result.Results[0], resultSet)
		if err != nil {
			return err
		}
		for i, d := range resultSet.ResultSet.Data {
			r.data.Data[i] = append(r.data.Data[i], d...)
		}
	}

	for i := range dest {
		dest[i] = r.data.Data[i][r.rowPointer]
	}
	r.rowPointer = r.rowPointer + 1
	return nil
}
