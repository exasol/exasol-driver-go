# Exasol Go SQL Driver 0.4.2, released 2022-05-11

Code name: Fix upload of large files

## Summary

This release fixes the issue that files bigger than 1MB could not be uploaded.

## Bugfixes

* #65: Import hangs for large files. Thanks to [@cyrixsimon](https://github.com/cyrixsimon) for reporting this!
