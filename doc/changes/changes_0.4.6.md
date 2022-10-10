# exasol-driver-go 0.4.6, released 2022-10-10

Code name: Fix resource leak and FetchSize

## Summary

This release fixes a resource leak using invalid `IMPORT` statements and the wrong calculation of the fetch size.

## Bugfixes

* #79: Fixed goroutine leak for invalid import statement. Thanks to [@cyrixsimon](https://github.com/cyrixsimon) reporting this!
* #80: Fixed wrong calculation of fetch size. Thanks to [@stangelandcl](https://github.com/stangelandcl) for reporting this!

## Dependency Updates

### Test Dependency Updates

* Added `go.uber.org/goleak:v1.2.0`
