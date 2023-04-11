package logger

import (
	"log"
	"os"

	"github.com/exasol/exasol-driver-go/pkg/errors"
)

var ErrorLogger = Logger(log.New(os.Stderr, "[exasol] ", log.LstdFlags|log.Lshortfile))

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
