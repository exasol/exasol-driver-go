# Exasol Driver go 1.1.0, released 2026-??-??

Code name: Import local Parquet files

## Summary

This release adds support for local Parquet import, with `IMPORT INTO <table> FROM LOCAL PARQUET FILE '<path>'`. This works alongside the existing local CSV import. A statement can name exactly one Parquet file.

The driver streams the byte ranges requested by Exasol without loading the whole file into memory.

Native local Parquet import requires Exasol 2025.1.11 or later. Against an older server, the driver does not open a connection. The statement fails at once with error `E-EGOD-31`. This error names the required version and the reported version.

This release also adds the `localimportencryption` driver property. By default, the driver encrypts the proxy connection that carries a local CSV or Parquet file when the server supports `PUBLIC KEY` pinning. On older servers, the driver automatically falls back to plaintext. The driver uses a throwaway, self-signed TLS key for encrypted proxy connections and pins the key with a `PUBLIC KEY` clause on the rewritten statement. Set the property to `0` to disable local-import encryption explicitly.

This release does not add a Parquet-parsing dependency. `dependencies.md` is unchanged.

## Features

* #152: Added native import of local Parquet files
* Added the `localimportencryption` driver property to encrypt the local-import proxy connection

## Dependency Updates

### Test Dependency Updates

* Added `github.com/parquet-go/parquet-go:v0.32.0`

### Other Dependency Updates

* Updated `toolchain:go1.26.4` to `go1.26.6`
