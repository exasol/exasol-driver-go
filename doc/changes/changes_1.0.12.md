# Exasol Driver go 1.0.12, released 2025-02-10

Code name: Update to go 1.22

## Summary

This release updates to go 1.22 and updates version used in CI pipeline to fix vulnerabilities in the go standard library:
 - [GO-2024-3107](https://pkg.go.dev/vuln/GO-2024-3107)
 - [GO-2024-3105](https://pkg.go.dev/vuln/GO-2024-3105)
 - [GO-2024-3106](https://pkg.go.dev/vuln/GO-2024-3106)

## Security

* #128: Fix security issues in dependencies / standard library

## Dependency Updates

### Compile Dependency Updates

* Updated `golang:1.21` to `1.22`
* Updated `github.com/stretchr/testify:v1.9.0` to `v1.10.0`
* Updated `github.com/exasol/exasol-test-setup-abstraction-server/go-client:v0.3.9` to `v0.3.10`

### Test Dependency Updates

* Updated `golang.org/x/sync:v0.7.0` to `v0.11.0`
