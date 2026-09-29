## Automated Table Schema Inference

When importing a local Parquet file, Exasol Go SQL Driver supports creating the SQL table on the fly based on the column declaration contained in the Parquet file.

This feature has been added with version 1.2.0 and requires using the following function:

```golang
func ImportParquetWithInferredSchema(
        ctx context.Context,
        database *sql.DB,
        schema string,
        table string,
        filePath string,
        options ParquetImportOptions,
) (result sql.Result, err error)
```
