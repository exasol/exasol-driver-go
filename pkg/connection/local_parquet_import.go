package connection

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"regexp"

	"github.com/exasol/exasol-driver-go/pkg/errors"
	"github.com/exasol/exasol-driver-go/pkg/logger"
)

var LOG logger.Logger = logger.TraceLogger

type ParquetImportOptions struct {
	ColumnNames int
}

const (
	RawParquetNames = 0
	UpperSnakeCase  = 1
)

// regularIdentifier matches an Exasol regular (unquoted) identifier.
// See https://docs.exasol.com/db/latest/sql_references/basiclanguageelements.htm
//
// First character:  Unicode classes Lu, Ll, Lt, Lm, Lo (all covered by \p{L}) and Nl.
// Later characters: the same, plus Mn, Mc, Nd, Pc, Cf and U+00B7 (middle dot).
// Length:           1 to 128 characters.
var regularIdentifier = regexp.MustCompile(
	`\A[\p{L}\p{Nl}][\p{L}\p{Nl}\p{Mn}\p{Mc}\p{Nd}\p{Pc}\p{Cf}\x{00B7}]{0,127}\z`)

// illegalPathCharacters defines a regular expression for illegal characters
// in the path of a file to be imported via ImportParquetWithInferredSchema.
// EGOD deliberately forbids these characters to avoid SQL injection.
var illegalPathCharacters = regexp.MustCompile(`[;'\\]`)

// checkIfTableExists checks if the specified table exists by querying
// EXA_ALL_TABLES.
func checkIfTableExists(ctx context.Context, database *sql.DB, schema string, table string) (bool, error) {
	rows, err := database.QueryContext(
		ctx,
		"SELECT count(1) FROM SYS.EXA_ALL_TABLES "+
			"WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		schema,
		table,
	)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return false, nil
	}

	var x int
	err = rows.Scan(&x)
	return x > 0, err
}

// createTableForLocalParquetImport creates the SQL table for importing a
// Parquet file from local using the column descriptions in the Parquet
// file's metadata.
func createTableForLocalParquetImport(
	ctx context.Context,
	database *sql.DB,
	tableFqn string,
	filePath string,
	options ParquetImportOptions,
) error {
	file, err := os.Open(filePath)
	if err != nil {
		return errors.OpenParquetFile(filePath, err)
	}
	defer file.Close()
	columns, err := retrieveParquetColumns(file)
	if err != nil {
		return errors.RetrieveParqueColumns(filePath, err)
	}
	if options.ColumnNames == UpperSnakeCase {
		columns = renameColumns(columns)
	}
	statement, err := createTableStatement(tableFqn, columns)
	if err != nil {
		return errors.InferSqlColumns(filePath, err)
	}
	LOG.Print(statement)
	_, err = database.ExecContext(ctx, statement)
	if err != nil {
		return errors.CreateSqlTable(filePath, err)
	}
	return nil
}

func verifyInputParameters(schema, table, filePath string) error {
	if !regularIdentifier.MatchString(schema) {
		return errors.SqlSchemaName(schema)
	}
	if !regularIdentifier.MatchString(table) {
		return errors.SqlTableName(table)
	}
	if illegal := illegalPathCharacters.FindString(filePath); illegal != "" {
		return errors.ParquetFilePath(filePath, `"`+illegal+`"`)
	}
	return nil
}

// ImportParquetWithInferredSchema imports a local Parquet file incl.
// creating the target SQL table based on the column definitions retrieved
// from the Parquet file.
//
// The function returns the number of affected rows.
//
// If the SQL table already exists then a warning is sent to the logger.
// Import may fail if the schema of the existing table differs from the
// Parquet file.
func ImportParquetWithInferredSchema(
	ctx context.Context,
	database *sql.DB,
	schema string,
	table string,
	filePath string,
	options ParquetImportOptions,
) (rowsCount int64, err error) {
	if err := verifyInputParameters(schema, table, filePath); err != nil {
		return 0, err
	}
	tableFqn := QuoteIdentifier(schema) + "." + QuoteIdentifier(table)
	exists, err := checkIfTableExists(ctx, database, schema, table)
	if err != nil {
		return 0, errors.CheckTable(tableFqn)
	}
	if exists {
		logger.WarningLogger.Print(errors.TableExists(tableFqn).Error())
	} else {
		err = createTableForLocalParquetImport(ctx, database, tableFqn, filePath, options)
		if err != nil {
			return 0, err
		}
	}
	statement := fmt.Sprintf("IMPORT INTO %s FROM LOCAL PARQUET FILE '%s'", tableFqn, filePath)
	LOG.Print(statement)
	// suppress sonar findings as values are sanitized at the beginning of the function
	result, err := database.ExecContext(ctx, statement) // NOSONAR
	if err != nil {
		return 0, errors.ImportStatement(statement, err)
	}
	rowsCount, err = result.RowsAffected()
	if err != nil {
		return 0, errors.RetrieveAffectedRows(err)
	}
	return rowsCount, nil
}
