# Exasol Driver go 1.0.11, released 2025-01-15

Code name: Fix import file name parsing

## Summary

This release fixes an issue parsing file names in IMPORT statements.
It also fixes vulnerability CVE-2024-45338 in transitive dependency golang.org/x/net 

## Bugfixes

* #123: Fixed parsing of file names in IMPORT statements

## Security

* #125: Fixed vulnerability CVE-2024-45338 in transitive dependency golang.org/x/net