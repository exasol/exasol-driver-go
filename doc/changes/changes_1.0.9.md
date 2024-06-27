# Exasol Driver go 1.0.9, released 2024-06-??

Code name: Fix reading int values

## Summary

This release fixes an issue when calling `rows.Scan(&result)` with an int value. This failed for large values like 100000000 with the following error:

```
sql: Scan error on column index 0, name "100000000": converting driver.Value type float64 ("1e+08") to a int64: invalid syntax
```

## Bugfixes

* #113: Fixed `Scan()` with large integer numbers
