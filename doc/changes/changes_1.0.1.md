# Exasol Driver go 1.0.1, released 2023-08-03

Code name: Test with Exasol v8

## Summary

This release adds support for specifying the query timeout in seconds when connecting to an Exasol database. The default timeout is 0, i.e. no timeout.

The release also adds integration tests with Exasol v8. Exasol version 7.0 is deprecated and no longer supported.

## Features

* #92: Added tests with Exasol v8
* #89: Added query timeout to DSN

