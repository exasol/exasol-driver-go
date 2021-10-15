package exasol

import (
	"net/url"

	error_msg "github.com/exasol/error-reporting-go"
)

// Various errors the driver might return. Can change between driver versions.
var (
	ErrInvalidConn              = newDriverErr(error_msg.ExaError("E-EGOD-1").Message("invalid connection"))
	ErrClosed                   = newDriverErr(error_msg.ExaError("E-EGOD-2").Message("connection was closed"))
	ErrMalformedData            = newDriverErr(error_msg.ExaError("E-EGOD-3").Message("malformed result"))
	ErrAutocommitEnabled        = newDriverErr(error_msg.ExaError("E-EGOD-4").Message("begin not working when autocommit is enabled"))
	ErrInvalidValuesCount       = newDriverErr(error_msg.ExaError("E-EGOD-5").Message("invalid value count for prepared status"))
	ErrNoLastInsertID           = newDriverErr(error_msg.ExaError("E-EGOD-6").Message("no LastInsertId available"))
	ErrNamedValuesNotSupported  = newDriverErr(error_msg.ExaError("E-EGOD-7").Message("named parameters not supported"))
	ErrLoggerNil                = newDriverErr(error_msg.ExaError("E-EGOD-8").Message("logger is nil"))
	ErrMissingServerCertificate = newDriverErr(error_msg.ExaError("E-EGOD-9").
					Message("server did not return certificates"))
)

func newErrCertificateFingerprintMismatch(actualFingerprint, expectedFingerprint string) DriverErr {
	return newDriverErr(error_msg.ExaError("E-EGOD-10").
		Message("the server's certificate fingerprint {{server fingerprint}} does not match the expected fingerprint {{expected fingerprint}}").
		ParameterWithDescription("server fingerprint", actualFingerprint, "The SHA256 sum of the server's certificate").
		ParameterWithDescription("expected fingerprint", expectedFingerprint, "The expected fingerprint"))
}

func newSqlErr(exception *Exception) DriverErr {
	return newDriverErr(error_msg.ExaError("E-GOD-11").
		Message("execution failed with SQL error code {{sql code}} and message {{text}}").
		Parameter("sql code", exception.SQLCode).
		Parameter("text", exception.Text))
}

func newErrCouldNotAbort(rootCause error) DriverErr {
	return newDriverErr(error_msg.ExaError("E-GOD-12").
		Message("could not abort query: {{root cause}}").
		Parameter("root cause", rootCause))
}

func newDriverErr(error *error_msg.ErrorMessageBuilder) DriverErr {
	return DriverErr(error.String())
}

func logPasswordEncryptionError(err error) {
	errorLogger.Print(error_msg.ExaError("E-GOD-13").
		Message("password encryption error: {{root cause}}").
		Parameter("root cause", err).
		String())
}

func logConnectionFailedError(url url.URL, err error) {
	errorLogger.Print(error_msg.ExaError("W-EGOD-14").
		Message("connection to {{url}} failed: {{error}}").
		Parameter("url", url.String()).
		Parameter("error", err).
		String())
}

func logMarshallingError(request interface{}, err error) {
	errorLogger.Print(error_msg.ExaError("W-EGOD-15").
		Message("could not marshal request {{request}}: {{error}}").
		Parameter("request", request).
		Parameter("error", err).
		String())
}

func logRequestSendingError(err error) {
	errorLogger.Printf("could not send request, %s", err)
	errorLogger.Print(error_msg.ExaError("W-EGOD-16").
		Message("could not send request: {{error}}").
		Parameter("error", err).
		String())
}

func logReceivingError(err error) {
	errorLogger.Print(error_msg.ExaError("W-EGOD-17").
		Message("could not receive data: {{error}}").
		Parameter("error", err).
		String())
}

func logUncompressingError(err error) {
	errorLogger.Print(error_msg.ExaError("W-EGOD-18").
		Message("could not decode compressed data: {{error}}").
		Parameter("error", err).
		String())
}

func logJsonDecodingError(err error) {
	errorLogger.Print(error_msg.ExaError("W-EGOD-19").
		Message("could not decode json data: {{error}}").
		Parameter("error", err).
		String())
}

type DriverErr string

func (e DriverErr) Error() string {
	return string(e)
}
