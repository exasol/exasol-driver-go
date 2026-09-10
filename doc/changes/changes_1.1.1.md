# Exasol Driver go 1.1.1, released 2026-??-??

Code name: Serialize WebSocket access and fix local IMPORT

## Summary

This release fixes local IMPORT detection for SQL statements containing multiline string literals.

This release prevents a runtime panic when multiple operations access one WebSocket connection concurrently and malformed frames are received. The driver now serializes WebSocket reads and writes. Protocol errors continue to be returned as connection errors.

When trace logging is enabled, the driver logs the first detected concurrent read and write per connection. These diagnostics contain no request or response payloads.

## Bug Fixes

* #151: Fixed false detection of IMPORT statements inside multiline SQL string literals
* #139: Prevented a slice-bounds panic caused by concurrent WebSocket access
* #151: Fixed false detection of IMPORT statements inside multiline SQL string literals

## Dependency Updates

### Compile Dependency Updates

* Updated `github.com/stretchr/testify:v1.11.1` to `v1.12.1`
* Updated `github.com/exasol/error-reporting-go:v0.2.0` to `v0.2.1`

### Test Dependency Updates

* Updated `golang.org/x/sync:v0.21.0` to `v0.22.0`
