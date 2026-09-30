# Connecting to Exasol

## With Exasol Config

We recommend using the provided builder to build a connection string. The builder ensures all values are escaped properly.

```go
package main

import (
    "database/sql"
    "github.com/exasol/exasol-driver-go"
)

func main() {
    config := exasol.NewConfig("<username>", "<password>").
        Host("<host>").
        Port(8563).
        String()
    database, err := sql.Open("exasol", config)
    // ...
}
```

If you want to login via [OpenID tokens](https://github.com/exasol/websocket-api/blob/master/docs/commands/loginTokenV3.md) use `exasol.NewConfigWithRefreshToken("token")` or `exasol.NewConfigWithAccessToken("token")`. See the [documentation](https://docs.exasol.com/db/latest/sql/create_user.htm#AuthenticationusingOpenID) about how to configure OpenID authentication in Exasol.

## With an Exasol DSN

An [ODBC Data Source Name](https://en.wikipedia.org/wiki/Data_source_name) (DSN) is a string identifying a database connection incl. the detailed protocol, i.e.  the type of the database for selecting the appropriate database driver.

You can create a connection by using a simple string representing the DSN:

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

If a value in the connection string contains a `;` you need to escape it with `\;`. This ensures that the driver can parse the connection string as expected.

See more details in [Connection String](connection_string.md).
