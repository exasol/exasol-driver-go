package exasol

import (
	"net/url"

	"github.com/exasol/error-reporting-go"
)

// Various errors the driver might return. Can change between driver versions.
var (
	ErrInvalidConn              = newDriverErr(exaerror.New("E-EGOD-1").Message("invalid connection"))
	ErrClosed                   = newDriverErr(exaerror.New("E-EGOD-2").Message("connection was closed"))
	ErrMalformedData            = newDriverErr(exaerror.New("E-EGOD-3").Message("malformed result"))
	ErrAutocommitEnabled        = newDriverErr(exaerror.New("E-EGOD-4").Message("begin not working when autocommit is enabled"))
	ErrInvalidValuesCount       = newDriverErr(exaerror.New("E-EGOD-5").Message("invalid value count for prepared status"))
	ErrNoLastInsertID           = newDriverErr(exaerror.New("E-EGOD-6").Message("no LastInsertId available"))
	ErrNamedValuesNotSupported  = newDriverErr(exaerror.New("E-EGOD-7").Message("named parameters not supported"))
	ErrLoggerNil                = newDriverErr(exaerror.New("E-EGOD-8").Message("logger is nil"))
	ErrMissingServerCertificate = newDriverErr(exaerror.New("E-EGOD-9").
					Message("server did not return certificates"))
	ErrInvalidProxyConn = newDriverErr(exaerror.New("E-EGOD-26").
				Message("could not create proxy connection to import file"))
	ErrInvalidImportQuery = newDriverErr(exaerror.New("E-EGOD-27").
				Message("could not parse import query"))
)

func newErrCertificateFingerprintMismatch(actualFingerprint, expectedFingerprint string) DriverErr {
	return newDriverErr(exaerror.New("E-EGOD-10").
		Message("the server's certificate fingerprint {{server fingerprint}} does not match the expected fingerprint {{expected fingerprint}}").
		ParameterWithDescription("server fingerprint", actualFingerprint, "The SHA256 sum of the server's certificate").
		ParameterWithDescription("expected fingerprint", expectedFingerprint, "The expected fingerprint"))
}

func newSqlErr(exception *Exception) DriverErr {
	return newDriverErr(exaerror.New("E-EGOD-11").
		Message("execution failed with SQL error code {{sql code}} and message {{text}}").
		Parameter("sql code", exception.SQLCode).
		Parameter("text", exception.Text))
}

func newErrCouldNotAbort(rootCause error) DriverErr {
	return newDriverErr(exaerror.New("E-EGOD-12").
		Message("could not abort query: {{root cause}}").
		Parameter("root cause", rootCause))
}

func newDriverErr(error *exaerror.ExaError) DriverErr {
	return DriverErr(error.String())
}

func logPasswordEncryptionError(err error) {
	errorLogger.Print(exaerror.New("E-EGOD-13").
		Message("password encryption error: {{root cause}}").
		Parameter("root cause", err).
		String())
}

func logConnectionFailedError(url url.URL, err error) {
	errorLogger.Print(exaerror.New("W-EGOD-14").
		Message("connection to {{url}} failed: {{error}}").
		Parameter("url", url.String()).
		Parameter("error", err).
		String())
}

func logMarshallingError(request interface{}, err error) {
	errorLogger.Print(exaerror.New("W-EGOD-15").
		Message("could not marshal request {{request}}: {{error}}").
		Parameter("request", request).
		Parameter("error", err).
		String())
}

func logRequestSendingError(err error) {
	errorLogger.Print(exaerror.New("W-EGOD-16").
		Message("could not send request: {{error}}").
		Parameter("error", err).
		String())
}

func logReceivingError(err error) {
	errorLogger.Print(exaerror.New("W-EGOD-17").
		Message("could not receive data: {{error}}").
		Parameter("error", err).
		String())
}

func logUncompressingError(err error) {
	errorLogger.Print(exaerror.New("W-EGOD-18").
		Message("could not decode compressed data: {{error}}").
		Parameter("error", err).
		String())
}

func logJsonDecodingError(err error) {
	errorLogger.Print(exaerror.New("W-EGOD-19").
		Message("could not decode json data: {{error}}").
		Parameter("error", err).
		String())
}

func newInvalidHostRangeLimits(host string) DriverErr {
	return newDriverErr(exaerror.New("E-EGOD-20").
		Message("invalid host range limits: {{host name}}").
		Parameter("host name", host))
}

func newInvalidConnectionString(connectionString string) DriverErr {
	return newDriverErr(exaerror.New("E-EGOD-21").
		Message("invalid connection string, must start with 'exa:': {{connection string}}").
		Parameter("connection string", connectionString))
}

func newInvalidConnectionStringHostOrPort(connectionString string) DriverErr {
	return newDriverErr(exaerror.New("E-EGOD-22").
		Message("invalid host or port in {{connection string}}, expected format: <host>:<port>").
		Parameter("connection string", connectionString))
}

func newInvalidConnectionStringInvalidPort(port string) DriverErr {
	return newDriverErr(exaerror.New("E-EGOD-23").
		Message("invalid `port` value {{port}}, numeric port expected").
		Parameter("port", port))
}
func newInvalidConnectionStringInvalidParameter(parameter string) DriverErr {
	return newDriverErr(exaerror.New("E-EGOD-24").
		Message("invalid parameter {{parameter}}, expected format <parameter>=<value>").
		Parameter("parameter", parameter))
}
func newInvalidConnectionStringInvalidIntParam(paramName, value string) DriverErr {
	return newDriverErr(exaerror.New("E-EGOD-25").
		Message("invalid {{parameter name}} value {{value}}, numeric expected").
		Parameter("parameter name", paramName).
		Parameter("value", value))
}

func newFileNotFound(path string) DriverErr {
	return newDriverErr(exaerror.New("E-EGOD-28").
		Message("file {{path}} not found").
		Parameter("path", path))
}

func logCouldNotGetOsUser(err error) {
	errorLogger.Print(exaerror.New("W-EGOD-28").
		Message("could not get current OS user: {{error}}").
		Parameter("error", err).
		String())
}

type DriverErr string

func (e DriverErr) Error() string {
	return string(e)
}
