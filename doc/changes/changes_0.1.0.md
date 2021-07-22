# Exasol Go SQL Driver 0.1.0, released 2021-07-14

Code name: Initial implementation of Exasol Go SQL Driver

## Summary

This is an initial implementation of the Exasol Go SQL Driver for connection to the Exasol database.
This library implements the [standard Golang SQL driver interface](https://github.com/golang/go/wiki/SQLInterface) for easy use.

How to use it In a nutshell:

```
package main

import (
	"database/sql"
	
	"github.com/exasol/exasol-driver-go"
)

func main() {
	database, err := sql.Open("exasol", exasol.NewConfig("<username>", "<password>").Port(<port>).Host("<host>").String())
	...
}
```

For additional information please refer to the documentation: https://github.com/exasol/exasol-driver-go

## Features

* #3: Added error logging.
* #5: Added first integration test.
* #6: Added DSN builder.
* #14: Implemented host with multiple values.

## Refactoring

* #9: Improved code quality.
* #10: Renamed the repository to `exasol-driver-go`.
* #20: Fixed GitHub workflow and setup SonarCloud intergation.