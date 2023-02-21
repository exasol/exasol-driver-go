# exasol-driver-go 0.4.7, released 2023-02-21

Code name: Improve documentation and make internal APIs private.

## Summary

This release improves the Godoc documentation available at [pkg.go.dev](https://pkg.go.dev/github.com/exasol/exasol-driver-go). It also makes internal types private that are used for the communication with the database and uses [exasol-test-setup-abstraction-server](https://pkg.go.dev/github.com/exasol/exasol-test-setup-abstraction-server/go-client) to speed up running integration tests during development.

## Features

* #82: Improve Godoc documentation.

## Refactoring

* #83: Use exasol test setup in integration tests.

## Dependency Updates

### Test Dependency Updates

* Updated `go.uber.org/goleak:v1.2.0` to `v1.2.1`
* Updated `golang.org/x/sync:v0.0.0-20220722155255-886fb9371eb4` to `v0.1.0`
* Updated `github.com/stretchr/testify:v1.8.0` to `v1.8.1`
* Added `github.com/exasol/exasol-test-setup-abstraction-server/go-client:v0.3.2`

### Other Dependency Updates

* Removed `github.com/testcontainers/testcontainers-go:v0.13.0`
