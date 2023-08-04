package errors

import (
	"net/url"

	exaerror "github.com/exasol/error-reporting-go"
)

// Various errors the driver might return. Can change between driver versions.
var (
	ErrInvalidConn              = NewDriverErr(exaerror.New("E-EGOD-1").Message("invalid connection"))
	ErrClosed                   = NewDriverErr(exaerror.New("E-EGOD-2").Message("connection was closed"))
	ErrMalformedData            = NewDriverErr(exaerror.New("E-EGOD-3").Message("malformed result"))
	ErrAutocommitEnabled        = NewDriverErr(exaerror.New("E-EGOD-4").Message("begin not working when autocommit is enabled"))
	ErrInvalidValuesCount       = NewDriverErr(exaerror.New("E-EGOD-5").Message("invalid value count for prepared status"))
	ErrNoLastInsertID           = NewDriverErr(exaerror.New("E-EGOD-6").Message("no LastInsertId available"))
	ErrNamedValuesNotSupported  = NewDriverErr(exaerror.New("E-EGOD-7").Message("named parameters not supported"))
	ErrLoggerNil                = NewDriverErr(exaerror.New("E-EGOD-8").Message("logger is nil"))
	ErrMissingServerCertificate = NewDriverErr(exaerror.New("E-EGOD-9").
					Message("server did not return certificates"))

	ErrInvalidProxyConn = NewDriverErr(exaerror.New("E-EGOD-26").
				Message("could not create proxy connection to import file"))
	ErrInvalidImportQuery = NewDriverErr(exaerror.New("E-EGOD-27").
				Message("could not parse import query"))
)

func NewErrCertificateFingerprintMismatch(actualFingerprint, expectedFingerprint string) DriverErr {
	return NewDriverErr(exaerror.New("E-EGOD-10").
		Message("the server's certificate fingerprint {{server fingerprint}} does not match the expected fingerprint {{expected fingerprint}}").
		ParameterWithDescription("server fingerprint", actualFingerprint, "The SHA256 sum of the server's certificate").
		ParameterWithDescription("expected fingerprint", expectedFingerprint, "The expected fingerprint"))
}

func NewSqlErr(sqlCode string, msg string) DriverErr {
	return NewDriverErr(exaerror.New("E-EGOD-11").
		Message("execution failed with SQL error code {{sql code}} and message {{text}}").
		Parameter("sql code", sqlCode).
		Parameter("text", msg))
}

func NewErrCouldNotAbort(rootCause error) DriverErr {
	return NewDriverErr(exaerror.New("E-EGOD-12").
		Message("could not abort query: {{root cause}}").
		Parameter("root cause", rootCause))
}

func NewDriverErr(error *exaerror.ExaError) DriverErr {
	return DriverErr(error.String())
}

func NewPasswordEncryptionError(err error) DriverErr {
	return NewDriverErr(exaerror.New("E-EGOD-13").
		Message("password encryption error: {{root cause}}").
		Parameter("root cause", err))
}

func NewConnectionFailedError(url url.URL, err error) DriverErr {
	return NewDriverErr(exaerror.New("W-EGOD-14").
		Message("connection to {{url}} failed: {{error}}").
		Parameter("url", url.String()).
		Parameter("error", err))
}

func NewMarshallingError(request interface{}, err error) DriverErr {
	return NewDriverErr(exaerror.New("W-EGOD-15").
		Message("could not marshal request {{request}}: {{error}}").
		Parameter("request", request).
		Parameter("error", err))
}

func NewRequestSendingError(err error) DriverErr {
	return NewDriverErr(exaerror.New("W-EGOD-16").
		Message("could not send request: {{error}}").
		Parameter("error", err))
}

func NewReceivingError(err error) DriverErr {
	return NewDriverErr(exaerror.New("W-EGOD-17").
		Message("could not receive data: {{error}}").
		Parameter("error", err))
}

func NewUncompressingError(err error) DriverErr {
	return NewDriverErr(exaerror.New("W-EGOD-18").
		Message("could not decode compressed data: {{error}}").
		Parameter("error", err))
}

func NewJsonDecodingError(err error) DriverErr {
	return NewDriverErr(exaerror.New("W-EGOD-19").
		Message("could not decode json data: {{error}}").
		Parameter("error", err))
}

func NewInvalidHostRangeLimits(host string) DriverErr {
	return NewDriverErr(exaerror.New("E-EGOD-20").
		Message("invalid host range limits: {{host name}}").
		Parameter("host name", host))
}

func NewInvalidConnectionString(connectionString string) DriverErr {
	return NewDriverErr(exaerror.New("E-EGOD-21").
		Message("invalid connection string, must start with 'exa:': {{connection string}}").
		Parameter("connection string", connectionString))
}

func NewInvalidConnectionStringHostOrPort(connectionString string) DriverErr {
	return NewDriverErr(exaerror.New("E-EGOD-22").
		Message("invalid host or port in {{connection string}}, expected format: <host>:<port>").
		Parameter("connection string", connectionString))
}

func NewInvalidConnectionStringInvalidPort(port string) DriverErr {
	return NewDriverErr(exaerror.New("E-EGOD-23").
		Message("invalid `port` value {{port}}, numeric port expected").
		Parameter("port", port))
}
func NewInvalidConnectionStringInvalidParameter(parameter string) DriverErr {
	return NewDriverErr(exaerror.New("E-EGOD-24").
		Message("invalid parameter {{parameter}}, expected format <parameter>=<value>").
		Parameter("parameter", parameter))
}
func NewInvalidConnectionStringInvalidIntParam(paramName, value string) DriverErr {
	return NewDriverErr(exaerror.New("E-EGOD-25").
		Message("invalid {{parameter name}} value {{value}}, numeric expected").
		Parameter("parameter name", paramName).
		Parameter("value", value))
}

func NewFileNotFound(path string) DriverErr {
	return NewDriverErr(exaerror.New("E-EGOD-28").
		Message("file {{path}} not found").
		Parameter("path", path))
}

func NewCouldNotGetOsUser(err error) DriverErr {
	return NewDriverErr(exaerror.New("W-EGOD-28").
		Message("could not get current OS user: {{error}}").
		Parameter("error", err))
}

func NewWebsocketNotConnected(request interface{}) DriverErr {
	return NewDriverErr(exaerror.New("E-EGOD-29").
		Message("could not send request {{request}}: not connected to server").
		Parameter("request", request))
}

// DriverErr This type represents an error that can occur when working with a database connection.
type DriverErr string

// Error converts the error to a string.
func (e DriverErr) Error() string {
	return string(e)
}
