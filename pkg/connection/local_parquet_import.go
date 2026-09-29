package connection

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"regexp"

	"github.com/exasol/exasol-driver-go/pkg/logger"
)

var LOG logger.Logger = logger.TraceLogger

type ParquetImportOptions struct {
	Name string
}

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
	if err != nil {
		return
	}
	defer rows.Close()
	if !rows.Next() {
		return false, nil
	}

	var x int
	err = rows.Scan(&x)
	return x > 0, err
}

// ImportParquetWithInferredSchema imports a local Parquet file incl.
// creating the target SQL table based on the column definitions retrieved
// from the Parquet file.
//
// If the SQL table already exists then a warning is sent to the logger.
func ImportParquetWithInferredSchema(
	ctx context.Context,
	database *sql.DB,
	schema string,
	table string,
	filePath string,
	options ParquetImportOptions,
) (result sql.Result, err error) {
	if !regularIdentifier.MatchString(schema) {
		return result, fmt.Errorf("invalid schema name %q", schema)
	}
	if !regularIdentifier.MatchString(table) {
		return result, fmt.Errorf("invalid table name %q", table)
	}
	if illegal := illegalPathCharacters.FindString(filePath); illegal != "" {
		return result, fmt.Errorf("file path contains illegal character %q: %s", illegal, filePath)
	}
	tableFqn := fmt.Sprintf("%q.%q", schema, table)
	exists, err := checkIfTableExists(ctx, database, schema, table)
	if err != nil {
		return result, fmt.Errorf("failed to check if table %s exists", tableFqn)
	}
	if exists {
		logger.WarningLogger.Printf("The specified table %s already exists", tableFqn)
	} else {
		file, err := os.Open(filePath)
		if err != nil {
			return result, fmt.Errorf("failed to open Parquet file %s for import %w", filePath, err)
		}
		defer file.Close()
		columns, err := retrieveParquetColumns(file)
		if err != nil {
			return result,
				fmt.Errorf("failed to retrieve column declarations from Parquet file %s %w", filePath, err)
		}
		statement, err := createTableStatement(tableFqn, columns)
		if err != nil {
			return result,
				fmt.Errorf("failed to build the CREATE TABLE statement for Parquet file %s %w", filePath, err)
		}
		LOG.Print(statement)
		_, err = database.ExecContext(ctx, statement)
		if err != nil {
			return result, err
		}
	}
	statement := fmt.Sprintf("IMPORT INTO %s FROM LOCAL PARQUET FILE '%s'", tableFqn, filePath)
	LOG.Print(statement)
	// suppress sonar findings as values are sanitized at the beginning of the function
	return database.ExecContext(ctx, statement) // NOSONAR
}
