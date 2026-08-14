# Exasol Driver go 1.1.0, released 2026-??-??

Code name: Import local Parquet files

## Summary

This release adds support for local Parquet import, with `IMPORT INTO <table> FROM LOCAL PARQUET FILE '<path>'`. This works alongside the existing local CSV import. A statement can name exactly one Parquet file.

The driver sends the file to Exasol over HTTP range requests. It does not stream the file. The driver reads the whole file into memory for the duration of the statement.

Native local Parquet import requires Exasol 2025.1.11 or later. Against an older server, the driver does not open a connection. The statement fails at once with error `E-EGOD-31`. This error names the required version and the reported version.

This release also adds the `localimportencryption` driver property. When enabled, the driver encrypts the proxy connection. This connection carries the local import file (CSV or Parquet). The driver uses a throwaway, self-signed TLS key for this connection, and pins the key with a `PUBLIC KEY` clause on the rewritten statement. The property is off by default. It does not change the behavior of an existing connection string.

This release does not add a Parquet-parsing dependency. `dependencies.md` is unchanged.

## Features

* #152: Added native import of local Parquet files
* Added the `localimportencryption` driver property to encrypt the local-import proxy connection

## Dependency Updates

### Other Dependency Updates

* Updated `toolchain:go1.26.4` to `go1.26.6`
