# Exasol Driver go 1.0.16, released 2026-02-09

Code name: Fix partial file transfer error during import statement execution

## Summary

This release fixes a bug when running IMPORT statements with large files.

This release is now tested using the latest versions of Go 1.25 and 1.24.

## Bugfixes

* #146: Partial file transfer error during import statement execution

## Dependency Updates

### Other Dependency Updates

* Updated `toolchain:go1.25.5` to `go1.25.7`
