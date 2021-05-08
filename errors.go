package exasol

import (
	"errors"
	"log"
	"os"
)

// Various errors the driver might return. Can change between driver versions.
var (
	ErrInvalidConn             = errors.New("invalid connection")
	ErrClosed                  = errors.New("connection was closed")
	ErrMalformData             = errors.New("malformed result")
	ErrAutocommitEnabled       = errors.New("begin not working when autocommit is enabled")
	ErrInvalidValuesCount      = errors.New("invalid value count for prepared status")
	ErrNoLastInsertID          = errors.New("no LastInsertId not available ")
	ErrNamedValuesNotSupported = errors.New("named parameters not supported")
)

var errorLogger = log.New(os.Stderr, "[exasol] ", log.LstdFlags|log.Lshortfile)
