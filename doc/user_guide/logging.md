# Logging Configuration

## Error Logger

By default the driver will log warnings and error messages to `stderr`. You can configure a custom error logger with
```go
logger.SetLogger(log.New(os.Stderr, "[exasol] ", log.LstdFlags|log.Lshortfile))
```

## Trace Logger

By default the driver does not log any trace or debug messages. To investigate problems you can configure a custom trace logger with
```go
logger.SetTraceLogger(log.New(os.Stderr, "[exasol-trace] ", log.LstdFlags|log.Lshortfile))
```

You can deactivate trace logging with
```go
logger.SetTraceLogger(nil)
```
