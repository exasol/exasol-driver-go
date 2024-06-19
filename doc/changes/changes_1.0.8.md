# Exasol Driver go 1.0.8, released 2024-06-19

Code name: Fix inserting double values

## Summary

This release fixes an issue inserting double values with a prepared statement.

Details of the fix:
The driver serializes commands to the database as JSON. When inserting values into a `DOUBLE` column, the database expects the JSON to contain numbers with a decimal point, e.g. `42.0`. When using values like `42` or `42.0` in `stmt.Exec()`, the driver omitted the decimal point. This caused the query to fail with error

```
E-EGOD-11: execution failed with SQL error code '00000' and message 'getDouble: JSON value is not a double
```

## Bugfixes

* #108: Fixed inserting double values with a prepared statement

## Dependency Updates

### Compile Dependency Updates

* Updated `github.com/gorilla/websocket:v1.5.1` to `v1.5.3`
* Updated `github.com/exasol/exasol-test-setup-abstraction-server/go-client:v0.3.6` to `v0.3.9`
