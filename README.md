# go-exasol [ALPHA]

A go library for connection to [Exasol](https://www.exasol.com/)

This library uses the standard golang [sql driver interface ](https://golang.org/pkg/database/sql/) for easy use.

## Usage

### Create connection

```go
package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/nightapes/go-exasol"
)

func main() {
    db, err := sql.Open("exasol", "exa:localhost:8563;user=sys;password=<password>")
    ...
}
```

### Execute a statement

```go
result, err := db.Exec(`
    INSERT INTO t
    (Name, AValue)
    VALUES('MyName', '12');`)
```

### Query a statement

```go
rows, err := db.Query("SELECT * FROM t")
```

## Connection string

The golang Driver uses the following URL structure for Exasol:

`exa:<host>:<port>[;<prop_1>=<value_1>]...[;<prop_n>=<value_n>]`

Limitations: Only single ips or dns is supported

Supported Driver Properties

| Property    |     Value     |                                         Description |
| :---------- | :-----------: | --------------------------------------------------: |
| user |  string  |           DB username |
| password |  string  |          DB password |
| autocommit |  0=off, 1=on  |           Switches autocommit on or off. Default: 1 |
| encryption |  0=off, 1=on  | Switches automatic encryption on or off. Default: 1 |
| fetchsize   | numeric, >0 | Amount of data in kB which should be obtained by Exasol during a fetch. The JVM can run out of memory if the value is too high. Default: 2000|
| clientname |  string  | Tells the server what the application is called.. Default: Go client |
| clientversion |  string  | Tells the server the version of the application. Default: "" |


## Examples

See [./examples](https://github.com/Nightapes/go-exasol/blob/main/examples)

## TODO

* Add tests
