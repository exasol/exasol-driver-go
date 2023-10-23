# Exasol Driver go 1.0.4, released 2023-10-23

Code name: Fixed `IMPORT LOCAL CSV FILE`

## Summary

This release fixes the detection of `IMPORT LOCAL CSV FILE`. Before, the Go driver also detected this inside strings which broke e.g. running the following `INSERT` statement:

```sql
insert into table1 values ('import into {{dest.schema}}.{{dest.table}} ) from local csv file ''{{file.path}}'' ');
```

Thanks to [@cyrixsimon](https://github.com/cyrixsimon) and [@ssteinhauser](https://github.com/ssteinhauser) for reporting this.

## Bugfixes

* #98: Fixed detection of `IMPORT LOCAL CSV FILE`

## Dependency Updates

### Compile Dependency Updates

* Updated `github.com/exasol/exasol-test-setup-abstraction-server/go-client:v0.3.3` to `v0.3.4`
