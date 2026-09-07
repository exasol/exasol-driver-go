# Exasol Driver go 1.1.1, released 2026-??-??

Code name:

## Summary

This release fixes local IMPORT detection for SQL statements containing multiline string literals.

## Bug Fixes

* #151: Fixed false detection of IMPORT statements inside multiline SQL string literals

## Dependency Updates

### Compile Dependency Updates

* Updated `github.com/stretchr/testify:v1.11.1` to `v1.12.1`
* Updated `github.com/exasol/error-reporting-go:v0.2.0` to `v0.2.1`

### Test Dependency Updates

* Updated `golang.org/x/sync:v0.21.0` to `v0.22.0`
