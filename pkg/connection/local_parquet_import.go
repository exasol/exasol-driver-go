package connection

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/exasol/exasol-driver-go/pkg/logger"
)

var LOG logger.Logger = logger.DebugLogger

type ParquetImportOptions struct {
	Name string
}

func checkIfTableExists(
	ctx context.Context,
	database *sql.DB,
	schema string,
	table string,
) (result bool, err error) {
	rows, err := database.QueryContext(
		ctx,
		"SELECT count(1) FROM SYS.EXA_ALL_TABLES "+
			"WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		schema,
		table,
	)
	defer rows.Close()
	if err != nil {
		return
	}
	if !rows.Next() {
		return false, nil
	}

	var x int
	err = rows.Scan(&x)
	return x > 0, err
}

func ImportParquetWithInferredSchema(
	ctx context.Context,
	database *sql.DB,
	schema string,
	table string,
	filePath string,
	options ParquetImportOptions,
) (result sql.Result, err error) {
	tableFqn := fmt.Sprintf("%q.%q", schema, table)
	exists, err := checkIfTableExists(ctx, database, schema, table)
	if err != nil {
		return result, fmt.Errorf("Failed to check if table %s exists", tableFqn)
	}
	if !exists {
		file, err := os.Open(filePath)
		if err != nil {
			return result, fmt.Errorf("Failed to open Parquet file %s for import %w", filePath, err)
		}
		columns, err := retrieveParquetColumns(file)
		if err != nil {
			return result,
				fmt.Errorf("Failed to retrieve column declarations from Parquet file %s %w", filePath, err)
		}
		statement, err := createTableStatement(tableFqn, columns)
		if err != nil {
			return result,
				fmt.Errorf("Failed to build the CREATE TABLE statement for Parquet file %s %w", filePath, err)
		}
		LOG.Print(statement)
		_, err = database.ExecContext(ctx, statement)
		if err != nil {
			return result, err
		}
	}
	statement := fmt.Sprintf("IMPORT INTO %s FROM LOCAL PARQUET FILE '%s'", tableFqn, filePath)
	LOG.Print(statement)
	return database.ExecContext(ctx, statement)
}
