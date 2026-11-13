# Exasol Driver go 1.3.0, released 2026-11-13

Code name: Quoted identifiers in Parquet import and vulnerability fixes

## Summary

`ImportParquetWithInferredSchema` now accepts any schema and table name that is valid as a quoted Exasol identifier, including names with spaces, mixed case and special characters. Error messages now use `error-reporting-go`.

This release also updates dependencies to fix vulnerabilities reported by `govulncheck`.

**Notes:** Starting with this release, the driver requires Go 1.26.3 or later, and the Go versions used for building and testing are upgraded from 1.26.8 and 1.25.14 to 1.26.11. Go 1.27.1 is still tested. We also test the driver against Exasol versions 2025.1.17, 2026.1.2 and 8.29.13.

## Bugfixes

* #195: Supported schema and table names with spaces and other special characters in `ImportParquetWithInferredSchema`

## Refactorings

* #192: Used `error-reporting-go` for error messages

## Dependency Updates

### Compile Dependency Updates

* Updated `golang:1.25.0` to `1.26.3`

### Test Dependency Updates

* Updated `golang.org/x/sync:v0.22.0` to `v0.25.0`

### Other Dependency Updates

* Updated `toolchain:go1.26.6` to `go1.26.11`
