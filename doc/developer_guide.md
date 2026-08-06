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

## Regenerating the Parquet test fixture

`testData/data.parquet` holds the same three rows as `testData/data.csv` — `(11, "test1")`, `(12, "test2")`, `(13, "test3")` — as an `INT64` column named `a` and a UTF-8 string column named `b`. This driver never parses Parquet and therefore never depends on a Parquet library, so the fixture is generated once, out of band, in a throwaway module outside this repository, and only the resulting binary file is committed.

To regenerate it, run the following in a temporary directory (this leaves no trace in this repository's `go.mod` or `go.sum`):

```shell
mkdir -p /tmp/parquet-fixture-gen && cd /tmp/parquet-fixture-gen
go mod init parquetfixturegen
go get github.com/parquet-go/parquet-go@latest
cat > main.go <<'EOF'
package main

import (
	"log"
	"os"

	"github.com/parquet-go/parquet-go"
)

type row struct {
	A int64  `parquet:"a"`
	B string `parquet:"b"`
}

func main() {
	rows := []row{
		{A: 11, B: "test1"},
		{A: 12, B: "test2"},
		{A: 13, B: "test3"},
	}
	if err := parquet.WriteFile(os.Args[1], rows); err != nil {
		log.Fatalf("failed to write parquet file: %v", err)
	}
}
EOF
go run . /path/to/exasol-driver-go/testData/data.parquet
```

Then remove the temporary directory. Verify the regenerated file before committing it, for example by reading it back with `parquet.ReadFile[row]` from the same throwaway module and comparing the rows against the three expected above.
