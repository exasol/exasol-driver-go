# Transaction Commit and Rollback

To control the transaction state manually, you need to disable autocommit (enabled by default):

```go
database, err := sql.Open("exasol",
   "exa:<host>:<port>;user=<username>;password=<password>;autocommit=0")
// or
config := exasol.NewConfig("<username>", "<password>").
  Port(int(port)).
  Host("<host>").
  Autocommit(false).
  String()
database, err := sql.Open("exasol", config)
```

After that you can begin a transaction:
```go
transaction, err := database.Begin()
result, err := transaction.Exec( ... )
result2, err := transaction.Exec( ... )
```

`Commit()` commits a transaction:
```go
err = transaction.Commit()
```

`Rollback()` rolls back a transaction:
```go
err = transaction.Rollback()
```

