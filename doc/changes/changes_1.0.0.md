# Exasol Driver go 1.0.0, released 2023-04-04

Code name: Upgrade to Go 1.19

## Summary

This release upgrades the Go version to 1.19. It also restructures the code and moves it to to the `pkg` directory. Client code that uses internal data structures might need to be adapted.

## Dependency Updates

### Compile Dependency Updates

* Updated `golang:1.18` to `1.19`

### Test Dependency Updates

* Updated `github.com/stretchr/testify:v1.8.1` to `v1.8.2`
