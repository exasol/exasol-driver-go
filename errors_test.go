package exasol

import (
	"bytes"
	"fmt"
	"log"
	"net/url"
	"testing"

	"github.com/stretchr/testify/suite"
)

type ErrorsTestSuite struct {
	suite.Suite
	previousLogger Logger
	logBuffer      *bytes.Buffer
}

func TestErrorsSuite(t *testing.T) {
	suite.Run(t, new(ErrorsTestSuite))
}

func (suite *ErrorsTestSuite) SetupTest() {
	suite.previousLogger = errorLogger

	suite.logBuffer = bytes.NewBuffer(make([]byte, 0, 64))
	logger := log.New(suite.logBuffer, "", 0)
	suite.NoError(SetLogger(logger))
}

func (suite *ErrorsTestSuite) TearDownTest() {
	suite.NoError(SetLogger(suite.previousLogger))
}

func (suite *ErrorsTestSuite) getLogContent() string {
	return suite.logBuffer.String()
}

func (suite *ErrorsTestSuite) TestErrInvalidConn() {
	suite.EqualError(ErrInvalidConn, "E-EGOD-1: invalid connection")
}

func (suite *ErrorsTestSuite) TestErrClosed() {
	suite.EqualError(ErrClosed, "E-EGOD-2: connection was closed")
}

func (suite *ErrorsTestSuite) TestErrMalformedData() {
	suite.EqualError(ErrMalformedData, "E-EGOD-3: malformed result")
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
	suite.EqualError(newErrCertificateFingerprintMismatch("actual", "expected"), "E-EGOD-10: the server's certificate fingerprint 'actual' does not match the expected fingerprint 'expected'")
}

func (suite *ErrorsTestSuite) TestNewSqlErr() {
	exception := exception{SQLCode: "sqlCode", Text: "text"}
	suite.EqualError(newSqlErr(&exception), "E-EGOD-11: execution failed with SQL error code 'sqlCode' and message 'text'")
}

func (suite *ErrorsTestSuite) TestNewErrCouldNotAbort() {
	suite.EqualError(newErrCouldNotAbort(fmt.Errorf("error")), "E-EGOD-12: could not abort query: 'error'")
}

func (suite *ErrorsTestSuite) TestLogPasswordEncryptionError() {
	logPasswordEncryptionError(fmt.Errorf("error"))
	suite.Equal("E-EGOD-13: password encryption error: 'error'\n", suite.getLogContent())
}

func (suite *ErrorsTestSuite) TestLogConnectionFailedError() {
	url := url.URL{Scheme: "scheme", Host: "host", Path: "path"}
	logConnectionFailedError(url, fmt.Errorf("error"))
	suite.Equal("W-EGOD-14: connection to 'scheme://host/path' failed: 'error'\n", suite.getLogContent())
}

func (suite *ErrorsTestSuite) TestLogMarshallingError() {
	logMarshallingError("request", fmt.Errorf("error"))
	suite.Equal("W-EGOD-15: could not marshal request 'request': 'error'\n", suite.getLogContent())
}

func (suite *ErrorsTestSuite) TestLogRequestSendingError() {
	logRequestSendingError(fmt.Errorf("error"))
	suite.Equal("W-EGOD-16: could not send request: 'error'\n", suite.getLogContent())
}
func (suite *ErrorsTestSuite) TestLogReceivingError() {
	logReceivingError(fmt.Errorf("error"))
	suite.Equal("W-EGOD-17: could not receive data: 'error'\n", suite.getLogContent())
}
func (suite *ErrorsTestSuite) TestLogUncompressingError() {
	logUncompressingError(fmt.Errorf("error"))
	suite.Equal("W-EGOD-18: could not decode compressed data: 'error'\n", suite.getLogContent())
}

func (suite *ErrorsTestSuite) TestLogJsonDecodingError() {
	logJsonDecodingError(fmt.Errorf("error"))
	suite.Equal("W-EGOD-19: could not decode json data: 'error'\n", suite.getLogContent())
}

func (suite *ErrorsTestSuite) TestNewInvalidConnectionStringInvalidPort() {
	suite.EqualError(newInvalidConnectionStringInvalidPort("port"), "E-EGOD-23: invalid `port` value 'port', numeric port expected")
}
