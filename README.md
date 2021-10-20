# Exasol Go SQL Driver

[![Go Reference](https://pkg.go.dev/badge/github.com/exasol/exasol-driver-go.svg)](https://pkg.go.dev/github.com/exasol/exasol-driver-go)

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Aexasol-driver-go&metric=alert_status)](https://sonarcloud.io/dashboard?id=com.exasol%3Aexasol-driver-go)

[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Aexasol-driver-go&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=com.exasol%3Aexasol-driver-go)
[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Aexasol-driver-go&metric=bugs)](https://sonarcloud.io/dashboard?id=com.exasol%3Aexasol-driver-go)
[![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Aexasol-driver-go&metric=code_smells)](https://sonarcloud.io/dashboard?id=com.exasol%3Aexasol-driver-go)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Aexasol-driver-go&metric=coverage)](https://sonarcloud.io/dashboard?id=com.exasol%3Aexasol-driver-go)

This repository contains a Go library for connection to the [Exasol](https://www.exasol.com/) database.

This library uses the standard Golang [SQL driver interface](https://golang.org/pkg/database/sql/) for easy use.

## Usage

### Create Connection

#### With Exasol Config

We recommend using a provided builder to build a connection string:

```go
package main

import (
    "database/sql"
    "github.com/exasol/exasol-driver-go"
)

func main() {
    database, err := sql.Open("exasol", exasol.NewConfig("<username>", "<password>")
                                              .Port(<port>)
                                              .Host("<host>")
                                              .String())
    // ...
}
```

#### With Exasol DSN

There is also a way to build the connection string without the builder:

```go
package main

import (
    "database/sql"
    _ "github.com/exasol/exasol-driver-go"
)

func main() {
    database, err := sql.Open("exasol",
            "exa:<host>:<port>;user=<username>;password=<password>")
    // ...
}
```

### Execute Statement

```go
result, err := exasol.Exec(`
    INSERT INTO CUSTOMERS
    (NAME, CITY)
    VALUES('Bob', 'Berlin');`)
```

### Query Statement

```go
rows, err := exasol.Query("SELECT * FROM CUSTOMERS")
```

### Use Prepared Statements

```go
preparedStatement, err := exasol.Prepare(`
    INSERT INTO CUSTOMERS
    (NAME, CITY)
    VALUES(?, ?)`)
result, err = preparedStatement.Exec("Bob", "Berlin")
```

```go
preparedStatement, err := exasol.Prepare("SELECT * FROM CUSTOMERS WHERE NAME = ?")
rows, err := preparedStatement.Query("Bob")
```

## Transaction Commit and Rollback

To control a transaction state manually, you would need to disable autocommit (enabled by default):

```go
database, err := sql.Open("exasol",
                "exa:<host>:<port>;user=<username>;password=<password>;autocommit=0")
// or
database, err := sql.Open("exasol", exasol.NewConfig("<username>", "<password>")
                                          .Port(<port>)
                                          .Host("<host>")
                                          .Autocommit(false)
                                          .String())
```

After that you can begin a transaction:

```go
transaction, err := exasol.Begin()
result, err := transaction.Exec( ... )
result2, err := transaction.Exec( ... )
```

To commit a transaction use `Commit()`:

```go
err = transaction.Commit()
```

To rollback a transaction use `Rollback()`:

```go
err = transaction.Rollback()
```

## Connection String

The golang Driver uses the following URL structure for Exasol:

`exa:<host>[,<host_1>]...[,<host_n>]:<port>[;<prop_1>=<value_1>]...[;<prop_n>=<value_n>]`

Host-Range-Syntax is supported (e.g. `exasol1..3`). A range like `exasol1..exasol3` is not valid.

### Supported Driver Properties

| Property                    | Value         | Default     | Description                                     |
| :-------------------------- | :-----------: | :---------: | :---------------------------------------------- |
| `autocommit`                |  0=off, 1=on  | `1`         | Switch autocommit on or off.                    |
| `clientname`                |  string       | `Go client` | Tell the server the application name.           |
| `clientversion`             |  string       |             | Tell the server the version of the application. |
| `compression`               |  0=off, 1=on  | `0`         | Switch data compression on or off.              |
| `encryption`                |  0=off, 1=on  | `1`         | Switch automatic encryption on or off.          |
| `validateservercertificate` |  0=off, 1=on  | `1`         | TLS certificate verification. Disable it if you want to use a self-signed or invalid certificate (server side). |
| `certificatefingerprint`    |  string       |             | Expected fingerprint of the server's TLS certificate. See below for details. |
| `fetchsize`                 | numeric, >0   | `128*1024`  | Amount of data in kB which should be obtained by Exasol during a fetch. The application can run out of memory if the value is too high. |
| `password`                  |  string       |             | Exasol password.                                |
| `resultsetmaxrows`          |  numeric      |             | Set the max amount of rows in the result set.   |
| `schema`                    |  string       |             | Exasol schema name.                             |
| `user`                      |  string       |             | Exasol username.                                |

### Configuring TLS

We recommend to always enable TLS encryption. This is on by default, but you can enable it explicitly via driver property `encryption=1` or `config.Encryption(true)`.

There are two driver properties that control how TLS certificates are verified: `validateservercertificate` and `certificatefingerprint`. You have these three options depending on your setup:

* With `validateservercertificate=1` (or `config.ValidateServerCertificate(true)`) the driver will return an error for any TLS errors (e.g. unknown certificate or invalid hostname).

    Use this when the database has a CA-signed certificate. This is the default behavior.
* With `validateservercertificate=1;certificatefingerprint=<fingerprint>` (or `config.ValidateServerCertificate(true).CertificateFingerprint("<fingerprint>")`) you can specify the fingerprint (i.e. the SHA256 checksum) of the server's certificate.

    This is useful when the database has a self-signed certificate with invalid hostname but you still want to verify connecting to the corrrect host.

    **Note:** You can find the fingerprint by first specifiying an invalid fingerprint and connecting to the database. The error will contain the actual fingerprint.
* With `validateservercertificate=1` (or `config.ValidateServerCertificate(false)`) the driver will ignore any TLS certificate errors.

    Use this if the server uses a self-signed certificate and you don't know the fingerprint. **This is not recommended.**

## Information for Users

* [Examples](examples)
* [Changelog](doc/changes/changelog.md)

## Testing / Development

Run unit tests only:

```shell
go test ./... -short
```

Run unit tests and integration tests:

For running the integrations tests you need [Docker](https://www.docker.com/) installed.

```shell
go test ./...
```
