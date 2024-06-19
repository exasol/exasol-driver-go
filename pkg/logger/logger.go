package logger

import (
	"log"
	"os"

	"github.com/exasol/exasol-driver-go/pkg/errors"
)

var ErrorLogger Logger = log.New(os.Stderr, "[exasol] ", log.LstdFlags|log.Lshortfile)
var TraceLogger Logger = noOpLogger{}

// Logger is used to log critical error messages.
type Logger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
}

// SetLogger is used to set the logger for critical errors.
// The initial logger is os.Stderr.
func SetLogger(logger Logger) error {
	if logger == nil {
		return errors.ErrLoggerNil
	}
	ErrorLogger = logger
	return nil
}

// SetTraceLogger is used to set the logger for tracing.
// The initial logger is a no-op logger. Please note that this will generate a lot of output.
// Set the logger to nil to disable tracing.
func SetTraceLogger(logger Logger) {
	if logger == nil {
		TraceLogger = noOpLogger{}
	} else {
		TraceLogger = logger
	}
}

type noOpLogger struct{}

func (noOpLogger) Print(v ...interface{})                 { /* no-op */ }
func (noOpLogger) Printf(format string, v ...interface{}) { /* no-op */ }
