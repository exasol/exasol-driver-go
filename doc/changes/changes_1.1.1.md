# Exasol Driver go 1.1.1, released 2026-09-11

Code name: Serialize WebSocket access and fix local IMPORT

## Summary

This release fixes local IMPORT detection for SQL statements containing multiline string literals.

This release prevents a runtime panic when multiple operations access one WebSocket connection concurrently and malformed frames are received. The driver now serializes WebSocket reads and writes. Protocol errors continue to be returned as connection errors.

When trace logging is enabled, the driver logs the first detected concurrent read and write per connection. These diagnostics contain no request or response payloads.

**Note:** Starting with this release, the Go driver is not tested with Exasol version 7.1 any more. Only the latest LTS release 2025.1.x and 8.29.x and the latest release 2026.1.1 are supported. Exasol 7.1 is officially past end-of-life since 2026-06-30 (see https://docs.exasol.com/db/latest/planning/life_cycle/life_cycle_policy.htm).

## Bug Fixes

* #151: Fixed false detection of IMPORT statements inside multiline SQL string literals
* #139: Prevented a slice-bounds panic caused by concurrent WebSocket access

## Dependency Updates

### Compile Dependency Updates

* Updated `github.com/stretchr/testify:v1.11.1` to `v1.12.1`
* Updated `github.com/exasol/error-reporting-go:v0.2.0` to `v0.2.1`

### Test Dependency Updates

* Updated `golang.org/x/sync:v0.21.0` to `v0.22.0`
