package connection

import (
	"context"
	"database/sql/driver"

	"github.com/exasol/exasol-driver-go/internal/utils"
	"github.com/exasol/exasol-driver-go/pkg/errors"
	"github.com/exasol/exasol-driver-go/pkg/logger"
	"github.com/exasol/exasol-driver-go/pkg/types"
)

type Statement struct {
	connection      *Connection
	statementHandle int
	columns         []types.SqlQueryColumn
	numInput        int
}

func NewStatement(connection *Connection, response *types.CreatePreparedStatementResponse) *Statement {
	return &Statement{connection: connection, statementHandle: response.StatementHandle, columns: response.ParameterData.Columns, numInput: response.ParameterData.NumColumns}
}

func (s *Statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	values, err := utils.NamedValuesToValues(args)
	if err != nil {
		return nil, err
	}
	result, err := s.executePreparedStatement(ctx, values)
	if err != nil {
		return nil, err
	}
	return ToRow(result, s.connection)
}

func (s *Statement) Query(args []driver.Value) (driver.Rows, error) {
	result, err := s.executePreparedStatement(context.Background(), args)
	if err != nil {
		return nil, err
	}
	return ToRow(result, s.connection)
}

func (s *Statement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	values, err := utils.NamedValuesToValues(args)
	if err != nil {
		return nil, err
	}
	result, err := s.executePreparedStatement(ctx, values)
	if err != nil {
		return nil, err
	}
	return ToResult(result)
}

func (s *Statement) Exec(args []driver.Value) (driver.Result, error) {
	result, err := s.executePreparedStatement(context.Background(), args)
	if err != nil {
		return nil, err
	}
	return ToResult(result)
}

func (s *Statement) Close() error {
	if s.connection.IsClosed {
		return driver.ErrBadConn
	}
	return s.connection.Send(context.Background(), &types.ClosePreparedStatementCommand{
		Command:         types.Command{Command: "closePreparedStatement"},
		StatementHandle: s.statementHandle,
	}, nil)
}

func (s *Statement) NumInput() int {
	return s.numInput
}

func (s *Statement) executePreparedStatement(ctx context.Context, args []driver.Value) (*types.SqlQueriesResponse, error) {
	columns := s.columns

	var numRows int
	if len(columns) == 0 {
		if len(args) > 0 {
			return nil, errors.ErrInvalidValuesCount
		}
		numRows = 0
	} else {
		if len(args)%len(columns) != 0 {
			return nil, errors.ErrInvalidValuesCount
		}
		numRows = len(args) / len(columns)
	}

	data := make([][]interface{}, len(columns))
	for i, arg := range args {
		col := i % len(columns)
		colType := columns[col]
		if data[col] == nil {
			data[col] = make([]interface{}, 0)
		}
		convertedArg, err := convertArg(arg, colType.DataType)
		if err != nil {
			return nil, err
		}
		data[col] = append(data[col], convertedArg)
	}

	command := &types.ExecutePreparedStatementCommand{
		Command:         types.Command{Command: "executePreparedStatement"},
		StatementHandle: s.statementHandle,
		Columns:         columns,
		NumColumns:      len(columns),
		NumRows:         numRows,
		Data:            data,
		Attributes: types.Attributes{
			ResultSetMaxRows: s.connection.Config.ResultSetMaxRows,
		},
	}
	result := &types.SqlQueriesResponse{}
	err := s.connection.Send(ctx, command, result)
	if err != nil {
		return nil, err
	}
	if result.NumResults == 0 {
		logger.ErrorLogger.Printf("Got empty result of type %t: %v", result, result)
		return nil, errors.ErrMalformedData
	}
	return result, err
}
