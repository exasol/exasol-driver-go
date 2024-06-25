package errors

import (
	"bytes"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"testing"

	"github.com/stretchr/testify/suite"
)

type ErrorsTestSuite struct {
	suite.Suite
	logBuffer *bytes.Buffer
}

func TestErrorsSuite(t *testing.T) {
	suite.Run(t, new(ErrorsTestSuite))
}

func (suite *ErrorsTestSuite) SetupTest() {
	suite.logBuffer = bytes.NewBuffer(make([]byte, 0, 64))
}

func (suite *ErrorsTestSuite) TestErrInvalidConn() {
	suite.EqualError(ErrInvalidConn, "E-EGOD-1: invalid connection")
}

func (suite *ErrorsTestSuite) TestErrInvalidConnUnwrapReturnsNil() {
	suite.Nil(ErrInvalidConn.Unwrap())
}

func (suite *ErrorsTestSuite) TestErrClosed() {
	suite.EqualError(ErrClosed, "E-EGOD-2: connection was closed")
}

func (suite *ErrorsTestSuite) TestErrMalformedData() {
	suite.EqualError(ErrMalformedData, "E-EGOD-3: malformed empty result")
}

func (suite *ErrorsTestSuite) TestErrAutocommitEnabled() {
	suite.EqualError(ErrAutocommitEnabled, "E-EGOD-4: begin not working when autocommit is enabled")
}

func (suite *ErrorsTestSuite) TestErrInvalidValuesCount() {
	suite.EqualError(ErrInvalidValuesCount, "E-EGOD-5: invalid value count for prepared status")
}

func (suite *ErrorsTestSuite) TestErrNoLastInsertID() {
	suite.EqualError(ErrNoLastInsertID, "E-EGOD-6: no LastInsertId available")
}

func (suite *ErrorsTestSuite) TestErrNamedValuesNotSupported() {
	suite.EqualError(ErrNamedValuesNotSupported, "E-EGOD-7: named parameters not supported")
}

func (suite *ErrorsTestSuite) TestErrLoggerNil() {
	suite.EqualError(ErrLoggerNil, "E-EGOD-8: logger is nil")
}

func (suite *ErrorsTestSuite) TestErrMissingServerCertificate() {
	suite.EqualError(ErrMissingServerCertificate, "E-EGOD-9: server did not return certificates")
}

func (suite *ErrorsTestSuite) TestNewErrCertificateFingerprintMismatch() {
	suite.EqualError(NewErrCertificateFingerprintMismatch("actual", "expected"), "E-EGOD-10: the server's certificate fingerprint 'actual' does not match the expected fingerprint 'expected'")
}

func (suite *ErrorsTestSuite) TestNewSqlErr() {
	suite.EqualError(NewSqlErr("sqlCode", "text"), "E-EGOD-11: execution failed with SQL error code 'sqlCode' and message 'text'")
}

func (suite *ErrorsTestSuite) TestNewErrCouldNotAbort() {
	suite.EqualError(NewErrCouldNotAbort(fmt.Errorf("error")), "E-EGOD-12: could not abort query: 'error'")
}

func (suite *ErrorsTestSuite) TestLogPasswordEncryptionError() {
	suite.EqualError(NewPasswordEncryptionError(fmt.Errorf("error")), "E-EGOD-13: password encryption error: 'error'")
}

func (suite *ErrorsTestSuite) TestLogConnectionFailedError() {
	url := url.URL{Scheme: "scheme", Host: "host", Path: "path"}
	suite.EqualError(NewConnectionFailedError(url, fmt.Errorf("error")), "W-EGOD-14: connection to 'scheme://host/path' failed: 'error'")
}

func (suite *ErrorsTestSuite) TestLogMarshallingError() {
	suite.EqualError(NewMarshallingError("request", fmt.Errorf("error")), "W-EGOD-15: could not marshal request 'request': 'error'")
}

func (suite *ErrorsTestSuite) TestLogRequestSendingError() {
	suite.EqualError(NewRequestSendingError(fmt.Errorf("error")), "W-EGOD-16: could not send request: 'error'")
}
func (suite *ErrorsTestSuite) TestLogReceivingError() {
	suite.EqualError(NewReceivingError(fmt.Errorf("error")), "W-EGOD-17: could not receive data: 'error'")
}

func (suite *ErrorsTestSuite) TestLogReceivingErrorIsBadConnection() {
	err := NewReceivingError(fmt.Errorf("error"))
	suite.True(errors.Is(err, driver.ErrBadConn))
	suite.False(errors.Is(err, driver.ErrSkip))
}

func (suite *ErrorsTestSuite) TestLogReceivingErrorUnwrapBadConnection() {
	err := NewReceivingError(fmt.Errorf("error"))
	suite.Same(driver.ErrBadConn, errors.Unwrap(err))
}

func (suite *ErrorsTestSuite) TestLogUncompressingError() {
	suite.EqualError(NewUncompressingError(fmt.Errorf("error")), "W-EGOD-18: could not decode compressed data: 'error'")
}

func (suite *ErrorsTestSuite) TestLogUncompressingErrorIsBadConnection() {
	err := NewUncompressingError(fmt.Errorf("error"))
	suite.True(errors.Is(err, driver.ErrBadConn))
}

func (suite *ErrorsTestSuite) TestLogUncompressingErrorUnwrapBadConnection() {
	err := NewUncompressingError(fmt.Errorf("error"))
	suite.Same(driver.ErrBadConn, errors.Unwrap(err))
}

func (suite *ErrorsTestSuite) TestLogJsonDecodingError() {
	suite.EqualError(NewJsonDecodingError(fmt.Errorf("error"), []byte("data")), "W-EGOD-19: could not decode json data 'data': 'error'")
}

func (suite *ErrorsTestSuite) TestLogJsonDecodingErrorIsBadConnection() {
	err := NewJsonDecodingError(fmt.Errorf("error"), []byte("data"))
	suite.True(errors.Is(err, driver.ErrBadConn))
}

func (suite *ErrorsTestSuite) TestLogJsonDecodingErrorUnwrapBadConnection() {
	err := NewJsonDecodingError(fmt.Errorf("error"), []byte("data"))
	suite.Same(driver.ErrBadConn, errors.Unwrap(err))
}

func (suite *ErrorsTestSuite) TestNewInvalidConnectionStringInvalidPort() {
	suite.EqualError(NewInvalidConnectionStringInvalidPort("port"), "E-EGOD-23: invalid `port` value 'port', numeric port expected")
}

func (suite *ErrorsTestSuite) TestNewInvalidArgType() {
	suite.EqualError(NewInvalidArgType("arg", "expected Type"), "E-EGOD-30: cannot convert argument 'arg' of type 'string' to 'expected Type' type")
}
