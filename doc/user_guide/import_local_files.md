# Importing Local Files

**Limitations:**
* The driver supports only CSV and Parquet files. It does not support FBV.
* The driver does not support the SQL `SECURE` option. Instead, it encrypts the proxy connection that transfers a local CSV or Parquet file by default when the server supports it. Exasol 8 does not support encrypted local imports, so the driver automatically uses plaintext there. The [`localimportencryption`](#connection-string) driver property is deprecated; set it to `0` only when plaintext is explicitly required.

The current understanding is
* Exasol &le; `2025.1.10` did not support encrypted import from local
* Hence for older Exasol versions the Go driver should set `localimportencryption` to `0` Otherwise the import will fail.
* Exasol &ge; `2025.1.11` either always uses encrypted local import or leaves the decision up to the user.

Open questions:
* What is a SQL `SECURE` option?
* How to detect whether the server supports encryption for import from local?
* Can EGOD detect this?

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
