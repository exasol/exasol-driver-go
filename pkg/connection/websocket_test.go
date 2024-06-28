package connection

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"

	"github.com/exasol/exasol-driver-go/internal/config"
	"github.com/exasol/exasol-driver-go/pkg/connection/wsconn"
	"github.com/exasol/exasol-driver-go/pkg/types"
	"github.com/stretchr/testify/suite"
)

type WebsocketTestSuite struct {
	suite.Suite
	websocketMock *wsconn.WebsocketConnectionMock
	ctx           context.Context
}

func TestWebsocketSuite(t *testing.T) {
	suite.Run(t, new(WebsocketTestSuite))
}

func (suite *WebsocketTestSuite) SetupTest() {
	suite.websocketMock = wsconn.CreateWebsocketConnectionMock()
	suite.ctx = context.Background()
}

func (suite *WebsocketTestSuite) TestSendSuccess() {
	request := types.LoginCommand{Command: types.Command{Command: "login"}}
	response := &types.PublicKeyResponse{}
	suite.websocketMock.SimulateOKResponse(request, types.PublicKeyResponse{PublicKeyPem: "pem"})

	err := suite.createOpenConnection().Send(suite.ctx, request, response)
	suite.NoError(err)
	suite.Equal("pem", response.PublicKeyPem)
}

func (suite *WebsocketTestSuite) TestSendSuccessWithCompression() {
	request := types.LoginCommand{Command: types.Command{Command: "login"}}
	response := &types.PublicKeyResponse{}
	suite.websocketMock.OnWriteCompressedMessage([]byte(`{"command":"login","protocolVersion":0,"attributes":{}}`), nil)
	suite.websocketMock.OnReadCompressedMessage([]byte(`{"status":"ok","responseData":{"publicKeyPem":"pem"}}`), nil)

	conn := suite.createOpenConnection()
	conn.Config.Compression = true
	err := conn.Send(suite.ctx, request, response)
	suite.NoError(err)
	suite.Equal("pem", response.PublicKeyPem)
}

func (suite *WebsocketTestSuite) TestSendWithCompressionFailsDuringUncompress() {
	request := types.LoginCommand{Command: types.Command{Command: "login"}}
	response := &types.PublicKeyResponse{}
	suite.websocketMock.OnWriteCompressedMessage([]byte(`{"command":"login","protocolVersion":0,"attributes":{}}`), nil)
	suite.websocketMock.OnReadTextMessage([]byte("invalid gzip content"), nil)

	conn := suite.createOpenConnection()
	conn.Config.Compression = true
	err := conn.Send(suite.ctx, request, response)
	suite.EqualError(err, "W-EGOD-18: could not decode compressed data: 'zlib: invalid header'")
	suite.True(errors.Is(err, driver.ErrBadConn))
}

func (suite *WebsocketTestSuite) TestSendSuccessNoResponse() {
	request := types.LoginCommand{Command: types.Command{Command: "login"}}
	suite.websocketMock.SimulateOKResponse(request, types.PublicKeyResponse{PublicKeyPem: "pem"})

	err := suite.createOpenConnection().Send(suite.ctx, request, nil)
	suite.NoError(err)
}

func (suite *WebsocketTestSuite) TestSendFailsNotConnected() {
	request := types.LoginCommand{Command: types.Command{Command: "login"}}
	response := &types.PublicKeyResponse{}

	conn := suite.createOpenConnection()
	conn.websocket = nil
	err := conn.Send(suite.ctx, request, response)
	suite.EqualError(err, `E-EGOD-29: could not send request '{"command":"login","protocolVersion":0,"attributes":{}}': not connected to server`)
}

func (suite *WebsocketTestSuite) TestSendFailsAtWriteMessage() {
	request := types.LoginCommand{Command: types.Command{Command: "login"}}
	response := &types.PublicKeyResponse{}
	suite.websocketMock.OnWriteAnyMessage(fmt.Errorf("mock error"))
	err := suite.createOpenConnection().Send(suite.ctx, request, response)
	suite.EqualError(err, "W-EGOD-16: could not send request: 'mock error'")
	suite.True(errors.Is(err, driver.ErrBadConn))
}

func (suite *WebsocketTestSuite) TestSendFailsAtReadMessage() {
	request := types.LoginCommand{Command: types.Command{Command: "login"}}
	response := &types.PublicKeyResponse{}
	suite.websocketMock.OnWriteAnyMessage(nil)
	suite.websocketMock.OnReadTextMessage(nil, fmt.Errorf("mock error"))

	err := suite.createOpenConnection().Send(suite.ctx, request, response)
	suite.EqualError(err, "W-EGOD-17: could not receive data: 'mock error'")
	suite.True(errors.Is(err, driver.ErrBadConn))
}

func (suite *WebsocketTestSuite) TestSendFailsAtDecodingResponse() {
	request := types.LoginCommand{Command: types.Command{Command: "login"}}
	response := &types.PublicKeyResponse{}
	suite.websocketMock.OnWriteAnyMessage(nil)
	suite.websocketMock.OnReadTextMessage([]byte("invalid json"), nil)

	err := suite.createOpenConnection().Send(suite.ctx, request, response)
	suite.EqualError(err, "W-EGOD-19: could not decode json data 'invalid json': 'invalid character 'i' looking for beginning of value'")
	suite.True(errors.Is(err, driver.ErrBadConn))
}

func (suite *WebsocketTestSuite) TestSendFailsAtNonOKStatusException() {
	request := types.LoginCommand{Command: types.Command{Command: "login"}}
	response := &types.PublicKeyResponse{}
	suite.websocketMock.SimulateErrorResponse(request, mockException)

	err := suite.createOpenConnection().Send(suite.ctx, request, response)
	suite.EqualError(err, "E-EGOD-11: execution failed with SQL error code 'mock sql code' and message 'mock error'")
}

func (suite *WebsocketTestSuite) TestSendFailsAtNonOKStatusMissingException() {
	request := types.LoginCommand{Command: types.Command{Command: "login"}}
	response := &types.PublicKeyResponse{}
	suite.websocketMock.OnWriteTextMessage(wsconn.JsonMarshall(request), nil)
	suite.websocketMock.OnReadTextMessage([]byte(`{"status": "notok"}`), nil)

	err := suite.createOpenConnection().Send(suite.ctx, request, response)
	suite.EqualError(err, `result status is not 'ok': "notok", expected exception in response &{notok [] <nil>}`)
}

func (suite *WebsocketTestSuite) TestSendFailsAtParsingResponseData() {
	request := types.LoginCommand{Command: types.Command{Command: "login"}}
	response := &types.PublicKeyResponse{}
	suite.websocketMock.OnWriteTextMessage(wsconn.JsonMarshall(request), nil)
	suite.websocketMock.OnReadTextMessage([]byte(`{"status": "ok", "responseData": "invalid"}`), nil)

	err := suite.createOpenConnection().Send(suite.ctx, request, response)
	suite.EqualError(err, `failed to parse response data "\"invalid\"": json: cannot unmarshal string into Go value of type types.PublicKeyResponse`)
}

func (suite *WebsocketTestSuite) TestCreateURL() {
	for i, testCase := range []struct {
		description string
		urlPath     string
		expectedURL string
	}{
		{"empty url path", "", "ws://hostName:12345"},
		{"url path with slash", "/", "ws://hostName:12345/"},
		{"url path with leading slash", "/path", "ws://hostName:12345/path"},
		{"url path without leading slash", "path", "ws://hostName:12345/path"},
		{"url path with trailing slash", "path/", "ws://hostName:12345/path/"},
		{"url path with leading and trailing slash", "/path/", "ws://hostName:12345/path/"},
		{"url path with query", "path?query=1", "ws://hostName:12345/path?query=1"},
		{"url with multiple query parameters", "path?query1=1&query2=2", "ws://hostName:12345/path?query1=1&query2=2"},
		{"url path with query and fragment", "path?query=1#fragment", "ws://hostName:12345/path?query=1#fragment"},
		{"url path with fragment", "path#fragment", "ws://hostName:12345/path#fragment"},
	} {
		suite.Run(fmt.Sprintf("Test%02d %s", i, testCase.description), func() {
			connection := suite.createOpenConnection()
			connection.Config.UrlPath = testCase.urlPath
			url, err := connection.createURL("hostName")
			suite.Assert().NoError(err)
			suite.Equal(testCase.expectedURL, url.String())
		})
	}
}

func (suite *WebsocketTestSuite) createOpenConnection() *Connection {
	conn := &Connection{
		Config:    &config.Config{Host: "invalid", Port: 12345, User: "user", Password: "password", ApiVersion: 42},
		Ctx:       suite.ctx,
		IsClosed:  false,
		websocket: suite.websocketMock,
	}
	return conn
}
