# Exasol Driver go 1.0.5, released 2024-03-13

Code name: Escape connection string

## Summary

This release supports using `;` in the connection string. When using the builder like this, it will automatically escape the semicolon:

```go
connectionString := exasol.NewConfig("<username>", "<password>").
                           Host("<host>").
                           Port(8563).
                           ClientName("My Client; Version abc").
                           String()
```

When using the connection string directly, you need to escape `;` with a `\`:

```go
connectionString := `exa:localhost:1234;user=sys;password=exasol;clientname=My Client\; Version abc`
```

Additionally this release also support specifying an URL path when creating the WebSocket connection to the database. This is only required in special situations and should not be used when connecting to an Exasol database.

## Features

* #103: Added support for URL Path when connecting

## Dependency Updates

### Compile Dependency Updates

* Updated `golang:1.20` to `1.21`
* Updated `github.com/gorilla/websocket:v1.5.0` to `v1.5.1`
* Updated `github.com/stretchr/testify:v1.8.4` to `v1.9.0`
* Updated `github.com/exasol/exasol-test-setup-abstraction-server/go-client:v0.3.4` to `v0.3.6`

### Test Dependency Updates

* Updated `go.uber.org/goleak:v1.2.1` to `v1.3.0`
* Updated `golang.org/x/sync:v0.4.0` to `v0.6.0`
