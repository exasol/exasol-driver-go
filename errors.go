package exasol

// Various errors the driver might return. Can change between driver versions.
const (
	ErrInvalidConn             = DriverErr("invalid connection")
	ErrClosed                  = DriverErr("connection was closed")
	ErrMalformedData           = DriverErr("malformed result")
	ErrAutocommitEnabled       = DriverErr("begin not working when autocommit is enabled")
	ErrInvalidValuesCount      = DriverErr("invalid value count for prepared status")
	ErrNoLastInsertID          = DriverErr("no LastInsertId available")
	ErrNamedValuesNotSupported = DriverErr("named parameters not supported")
	ErrLoggerNil               = DriverErr("logger is nil")
)

type DriverErr string

func (e DriverErr) Error() string {
	return string(e)
}
