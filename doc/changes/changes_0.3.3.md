# Exasol Go SQL Driver 0.3.3, released 2022-04-27

Code name: Fix OS user name

## Summary

This release fixes a bug that caused the OS user name not being sent to the database at login, causing value `UNKNOWN` in column `EXA_USER_SESSIONS.OS_USER`.

## Bugfixes

* #58: Fixed clientOsUserName not being sent to the database
