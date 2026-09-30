# Importing Local Files

## Importing Local CSV Files

Use the Exasol Go SQL Driver to load data from one or more CSV files into your Exasol Database. These files must be local to the machine where you execute the `IMPORT` statement.

**Limitations:**
* The driver supports only CSV and Parquet files. It does not support FBV.
* The driver does not support the SQL `SECURE` option. Instead, it encrypts the proxy connection that transfers a local CSV or Parquet file by default when the server supports it. Exasol 8 does not support encrypted local imports, so the driver automatically uses plaintext there. The [`localimportencryption`](#connection-string) driver property is deprecated; set it to `0` only when plaintext is explicitly required.

```go
result, err := exasol.Exec(`
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
result, err := exasol.Exec(`
IMPORT INTO CUSTOMERS FROM LOCAL PARQUET FILE '../testData/data.parquet'
`)
```

**Limitations:**
* A statement can name exactly one Parquet file.
* The statement requires Exasol 2025.1.11 or later. Against an older server, the import fails at once with error `E-EGOD-31`. This error names the required version and the reported version.
* The driver streams the byte ranges requested by Exasol without loading the whole file into memory.
* By default, the proxy connection that carries the file is encrypted when the server supports it. Exasol 8 does not support encrypted local imports, so the driver automatically uses plaintext there. The `localimportencryption` driver property is deprecated; set it to `0` only when plaintext is explicitly required.

### Automated Table Schema Inference

When importing a local Parquet file, Exasol Go SQL Driver supports creating the SQL table on the fly based on the column declaration contained in the Parquet file.

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
