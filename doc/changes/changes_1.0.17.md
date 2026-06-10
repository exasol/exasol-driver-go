# Exasol Driver go 1.0.17, released 2026-06-10

Code name: Fix 9 vulnerabilities in dependencies

## Summary

This release fixes the following 9 vulnerabilities in Go dependencies:

* [GO-2026-5039](https://pkg.go.dev/vuln/GO-2026-5039) in `net/textproto@go1.25.7`
* [GO-2026-5037](https://pkg.go.dev/vuln/GO-2026-5037) in `crypto/x509@go1.25.7`
* [GO-2026-4971](https://pkg.go.dev/vuln/GO-2026-4971) in `net@go1.25.7`
* [GO-2026-4947](https://pkg.go.dev/vuln/GO-2026-4947) in `crypto/x509@go1.25.7`
* [GO-2026-4980](https://pkg.go.dev/vuln/GO-2026-4980) in `stdlib@go1.25.7`
* [GO-2026-4977](https://pkg.go.dev/vuln/GO-2026-4977) in `stdlib@go1.25.7`
* [GO-2026-4869](https://pkg.go.dev/vuln/GO-2026-4869) in `stdlib@go1.25.7`
* [GO-2026-4865](https://pkg.go.dev/vuln/GO-2026-4865) in `stdlib@go1.25.7`
* [GO-2026-4603](https://pkg.go.dev/vuln/GO-2026-4603) in `stdlib@go1.25.7`

**Notes:** Starting with this release, upgrade the Go version used for building and testing from 1.25.7 and 1.24.13 to 1.26.4 and 1.25.11. We also test the driver against Exasol versions 2025.1.10, 2026.1.0 and 7.1.30.

## Security

* #148: Fixed vulnerabilities in Go dependencies

## Dependency Updates

### Compile Dependency Updates

* Updated `golang:1.24.0` to `1.25.0`
* Updated `github.com/exasol/exasol-test-setup-abstraction-server/go-client:v1.0.0` to `v1.0.1`

### Test Dependency Updates

* Updated `golang.org/x/sync:v0.19.0` to `v0.21.0`

### Other Dependency Updates

* Updated `toolchain:go1.25.7` to `go1.26.4`
