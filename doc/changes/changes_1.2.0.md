# Exasol Driver go 1.2.0, released 2026-10-02

Code name: Schema Inference

## Summary

When importing a local Parquet file, it is now possible to create the SQL table on the fly based on the column declarations from the Parquet metadata.

See the [User Guide](../user_guide/import_local_files.md#automated-table-schema-inference) for details.

## Features

* #165: Added support to compute column sizes
* #168: Added support for mapping parquet physical types to SQL data types
* #172: Added support to build a `CREATE TABLE` SQL statement
* #174: Added support to map Parquet logical types
* #178: Combined logical and physical types of Parquet columns
* #180: Added top-level function `ImportParquetWithInferredSchema` incl. integration test
* #183: Supported option for friendly column names when infering table schema from Parquet

## Documentation

* #162: Described executing single tests and added file `error_code_config.yml`
* #182: Described `ImportParquetWithInferredSchema` in the User Guide

## Refactorings

* #170: Refactored `mapPhysicalType` to return data type `string`
* #176: Enabled creating multiple sample files for integration tests
* #187: Added test for table already exists in `ImportParquetWithInferredSchema`
