# Basic Statements

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

### Prepared Statements

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
