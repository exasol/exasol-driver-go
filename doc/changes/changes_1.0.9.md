# Exasol Driver go 1.0.9, released 2024-06-28

Code name: Fix reading int values

## Summary

This release fixes an issue when calling `rows.Scan(&result)` with an int value. This failed for large values like 100000000 with the following error:

```
sql: Scan error on column index 0, name "COL": converting driver.Value type float64 ("1e+08") to a int64: invalid syntax
```

Please note that reading non-integer numbers like `1.1` into a `int64` variable will still fail with the following error message:

```
sql: Scan error on column index 0, name "COL": converting driver.Value type string ("1.1") to a int64: invalid syntax
```

The release also now returns the correct error from `rows.Err()`. Before, this only returned `driver.ErrBadConn`.

## Bugfixes

* #113: Fixed `Scan()` with large integer numbers
* #111: Return correct error from `rows.Err()`
