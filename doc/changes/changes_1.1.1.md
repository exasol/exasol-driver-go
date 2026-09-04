# Exasol Driver go 1.1.1, released 2026-09-04

Code name: Serialize WebSocket access

## Summary

This release prevents a runtime panic when multiple operations access one WebSocket connection concurrently and malformed frames are received. The driver now serializes WebSocket reads and writes. Protocol errors continue to be returned as connection errors.

When trace logging is enabled, the driver logs the first detected concurrent read and write per connection. These diagnostics contain no request or response payloads.

## Bugfixes

* #139: Prevented a slice-bounds panic caused by concurrent WebSocket access
