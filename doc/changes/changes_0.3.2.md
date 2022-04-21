# Exasol Go SQL Driver 0.3.2, released 2022-04-21

Code name: Fix `ToDSN()` to add the schema

## Summary

This release fixes a bug in the `ToDSN()` that caused the schema to be missing in the generated DSN.

## Bugfixes

* #56: Added the schema to `ToDSN()`.
