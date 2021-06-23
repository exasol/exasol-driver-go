# Exasol Go Client [ALPHA]

This repository contains a Go library for connection to the [Exasol](https://www.exasol.com/) database.

This library uses the standard Golang [SQL driver interface](https://golang.org/pkg/database/sql/) for easy use.

## Usage

### Create Connection

```go
package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/exasol/go-exasol"
)

func main() {
	exasol, err := sql.Open("exasol", "exa:<host>:<port>;user=<username>;password=<password>")
	...
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
exasol, err := sql.Open("exasol", "exa:<host>:<port>;user=<username>;password=<password>;autocommit=0")
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

`exa:<host>:<port>[;<prop_1>=<value_1>]...[;<prop_n>=<value_n>]`

Limitations: Only single ips or dns is supported

### Supported Driver Properties

| Property         | Value         | Default   | Description                                     |
| :--------------: | :-----------: | :-------: | :---------------------------------------------- |
| autocommit       |  0=off, 1=on  | 1         | Switch autocommit on or off.                    |
| clientname       |  string       | Go client | Tell the server the application name.           |
| clientversion    |  string       |           | Tell the server the version of the application. |
| compression      |  0=off, 1=on  | 0         | Switch data compression on or off.              |
| encryption       |  0=off, 1=on  | 1         | Switch automatic encryption on or off.          |
| fetchsize        | numeric, >0   | 128*1024  | Amount of data in kB which should be obtained by Exasol during a fetch. The JVM can run out of memory if the value is too high. |
| password         |  string       |           | Exasol password.                                |
| resultsetmaxrows |  numeric      |           | Set the max amount of rows in the result set.   |
| schema           |  string       |           | Exasol schema name.                             |
| user             |  string       |           | Exasol username.                                |

## Examples

See [examples](examples)