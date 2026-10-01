# Importing Local Files

**Limitations:**
* The driver supports only CSV and Parquet files. It does not support FBV.
* The driver does not support the SQL `SECURE` option. Instead, it encrypts the proxy connection that transfers a local CSV or Parquet file by default when the server supports it.

Encrypting the proxy connection used for a local CSV or Parquet import:
* Encrypted import from local is only supported by Exasol &ge; 2025.1.11.
* For older versions set driver property `localimportencryption` to `0`

Open questions:
* What is a SQL `SECURE` option?

## Importing Local CSV Files

Use the Exasol Go SQL Driver to load data from one or more CSV files into your Exasol Database. These files must be local to the machine where you execute the `IMPORT` statement.

```go
result, err := database.Exec(`
IMPORT INTO CUSTOMERS FROM LOCAL CSV FILE './testData/data.csv' FILE './testData/data_part2.csv'
  COLUMN SEPARATOR = ';'
  ENCODING = 'UTF-8'
  ROW SEPARATOR = 'LF'
`)
```

See also the [usage notes](https://docs.exasol.com/db/latest/sql/import.htm#UsageNotes) about the `file_src` element for local files of the `IMPORT` statement.

## Importing Local Parquet Files

Use the Exasol Go SQL Driver to load data from a local Parquet file into your Exasol Database:

```go
result, err := database.Exec(`
IMPORT INTO CUSTOMERS FROM LOCAL PARQUET FILE '../testData/data.parquet'
`)
```

**Notes:**
* A statement can name exactly one Parquet file.
* The statement requires Exasol 2025.1.11 or later. Against an older server, the import fails at once with error `E-EGOD-31`. This error names the required version and the reported version.
* The driver streams the byte ranges requested by Exasol without loading the whole file into memory.

See comments and open questions regarding `localimportencryption` above.

### Automated Table Schema Inference

When importing a local Parquet file, Exasol Go SQL Driver supports creating the SQL table on the fly based on the column declarations contained in the Parquet file.

This feature has been added with version 1.2.0 and requires using the following function:

```go
func ImportParquetWithInferredSchema(
    ctx context.Context,
    database *sql.DB,
    schema string,
    table string,
    filePath string,
    options ParquetImportOptions,
) (rowCount int64, err error)
```
