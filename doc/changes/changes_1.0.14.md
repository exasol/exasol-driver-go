# Exasol Driver go 1.0.14, released 2025-06-02

Code name: Fix vulnerability GO-2025-3563 in net/http/internal@go1.22.12

## Summary

This release fixes vulnerability GO-2025-3563 in `net/http/internal@go1.22.12` and updates dependencies.

## Security

* #135: Fixed vulnerability GO-2025-3563 in `net/http/internal@go1.22.12`

## Dependency Updates

### Compile Dependency Updates

* Updated `golang:1.22` to `1.23.0`
* Updated `github.com/exasol/exasol-test-setup-abstraction-server/go-client:v0.3.10` to `v0.3.11`

### Test Dependency Updates

* Updated `golang.org/x/sync:v0.11.0` to `v0.14.0`

### Other Dependency Updates

* Added `toolchain:go1.24.3`
