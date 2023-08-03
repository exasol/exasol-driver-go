# Developer Guide

## Testing / Development

Run unit tests only:

```shell
go test ./... -short
```

Run unit tests and integration tests:

For running the integrations tests you need [Docker](https://www.docker.com/) and [Java](https://adoptium.net/) installed.

```shell
go test ./...
```

Integration tests use [exasol-test-setup-abstraction-server](https://github.com/exasol/exasol-test-setup-abstraction-server) and thus indirectly [exasol-testcontainers](https://github.com/exasol/exasol-testcontainers/). To speedup tests during development you need to enable reusing of test containers by creating file `~/.testcontainers.properties` with the following content:

```properties
testcontainers.reuse.enable=true
```
