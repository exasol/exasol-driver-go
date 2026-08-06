# Exasol Driver go 1.0.18, released 2026-08-05

Code name: Import local Parquet files

## Summary

This release adds support for importing local Parquet files with `IMPORT INTO <table> FROM LOCAL PARQUET FILE '<path>'`, alongside the existing local CSV import. A statement may name exactly one Parquet file. The driver serves the file to Exasol over HTTP range requests instead of streaming it, and reads the whole file into memory for the duration of the statement.

Native local Parquet import requires Exasol 2025.1.11 or later. Against an older server the statement fails immediately with `E-EGOD-31`, naming both the required and the reported server version, before the driver opens any connection.

This release also adds the `localimportencryption` driver property. When enabled, the driver encrypts the proxy connection that carries a local import file (CSV or Parquet) with a throwaway, self-signed TLS key pinned via a `PUBLIC KEY` clause on the rewritten statement. It is off by default and does not change the behavior of an existing connection string.

No Parquet-parsing dependency was added; `go.mod` and `dependencies.md` are unchanged.

## Features

* Added native import of local Parquet files
* Added the `localimportencryption` driver property to encrypt the local-import proxy connection
