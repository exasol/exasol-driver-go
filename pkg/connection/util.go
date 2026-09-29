package connection

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/exasol/exasol-driver-go/pkg/types"
)

func ToRow(result *types.SqlQueriesResponse, con *Connection) (driver.Rows, error) {
	resultSet := &types.SqlQueryResponseResultSet{}
	err := json.Unmarshal(result.Results[0], resultSet)
	if err != nil {
		return nil, err
	}

	return &QueryResults{data: &resultSet.ResultSet, con: con}, nil
}

func ToResult(result *types.SqlQueriesResponse) (driver.Result, error) {
	rowCountResult := &types.SqlQueryResponseRowCount{}
	err := json.Unmarshal(result.Results[0], rowCountResult)
	if err != nil {
		return nil, err
	}

	return &RowCount{affectedRows: int64(rowCountResult.RowCount)}, nil
}

// quoteIdentifier escapes double quotes in the specified identifier by
// duplicating them and returns the result enclosed in additional double quote
// characters.
func QuoteIdentifier(raw string) string {
	escaped := strings.ReplaceAll(raw, `"`, `""`)
	return fmt.Sprintf(`"%s"`, escaped)
}
