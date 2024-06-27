package connection

import (
	"context"
	"database/sql/driver"
	"encoding/json"

	"github.com/exasol/exasol-driver-go/pkg/types"
)

func ToRow(ctx context.Context, result *types.SqlQueriesResponse, con *Connection) (driver.Rows, error) {
	resultSet := &types.SqlQueryResponseResultSet{}
	err := json.Unmarshal(result.Results[0], resultSet)
	if err != nil {
		return nil, err
	}

	return &QueryResults{ctx: ctx, data: &resultSet.ResultSet, con: con}, nil
}

func ToResult(result *types.SqlQueriesResponse) (driver.Result, error) {
	rowCountResult := &types.SqlQueryResponseRowCount{}
	err := json.Unmarshal(result.Results[0], rowCountResult)
	if err != nil {
		return nil, err
	}

	return &RowCount{affectedRows: int64(rowCountResult.RowCount)}, nil
}
