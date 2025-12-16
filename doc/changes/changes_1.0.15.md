# Exasol Driver go 1.0.15, released 2025-12-16

Code name: IMPORT error handling

## Summary

This release fixes the error handling for failing `IMPORT` commands.

We also updated the SonarQube GitHub Action to version 7 to fix CVE-2025-59844, a command injection vulnerability in SonarQube GitHub Action prior to v6.0.0.

This release is now tested using the latest versions of Go 1.25 and 1.24.

## Bugfixes

* #138: IMPORT error handling

## Security

* CVE-2025-59844: Update SonarQube GitHub Action to version 6
* #142: Fixed vulnerability check

## Dependency Updates

### Compile Dependency Updates

* Updated `golang:1.23.0` to `1.24.0`
* Updated `github.com/stretchr/testify:v1.10.0` to `v1.11.1`
* Updated `github.com/exasol/exasol-test-setup-abstraction-server/go-client:v0.3.11` to `v1.0.0`

### Test Dependency Updates

* Updated `golang.org/x/sync:v0.14.0` to `v0.19.0`

### Other Dependency Updates

* Updated `toolchain:go1.24.3` to `go1.25.5`
