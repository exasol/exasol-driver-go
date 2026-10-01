# Basic Statements

### Execute Statement

```go
result, err := database.Exec(`
    INSERT INTO CUSTOMERS
    (NAME, CITY)
    VALUES('Bob', 'Berlin');`)
```

### Query Statement

```go
rows, err := database.Query("SELECT * FROM CUSTOMERS")
```

### Prepared Statements

```go
preparedStatement, err := database.Prepare(`
    INSERT INTO CUSTOMERS
    (NAME, CITY)
    VALUES(?, ?)`)
result, err = preparedStatement.Exec("Bob", "Berlin")
```

```go
preparedStatement, err := database.Prepare("SELECT * FROM CUSTOMERS WHERE NAME = ?")
rows, err := preparedStatement.Query("Bob")
```

Please note: You can only use positional `?` placeholders as Exasol Go SQL Driver does not support named parameters in prepared statements.
