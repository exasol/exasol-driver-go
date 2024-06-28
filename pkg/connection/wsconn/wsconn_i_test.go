package wsconn_test

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/exasol/exasol-driver-go/pkg/connection/wsconn"
	"github.com/exasol/exasol-driver-go/pkg/integrationTesting"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/suite"
)

type WebsocketITestSuite struct {
	suite.Suite
	exasol *integrationTesting.DbTestSetup
	ctx    context.Context
}

func TestIntegrationWebsocketSuite(t *testing.T) {
	suite.Run(t, new(WebsocketITestSuite))
}

func (suite *WebsocketITestSuite) SetupSuite() {
	suite.exasol = integrationTesting.StartDbSetup(&suite.Suite)
}

func (suite *WebsocketITestSuite) TearDownSuite() {
	suite.exasol.StopDb()
}

func (suite *WebsocketITestSuite) SetupTest() {
	suite.ctx = context.Background()
}

func (suite *WebsocketITestSuite) TestCreateConnectionSuccess() {
	conn, err := wsconn.CreateConnection(suite.ctx, true, "", suite.exasol.GetUrl())
	suite.NoError(err)
	suite.NotNil(conn)
	conn.Close()
}

func (suite *WebsocketITestSuite) TestCreateConnectionFailed() {
	conn, err := wsconn.CreateConnection(suite.ctx, true, "", url.URL{Scheme: "wss", Host: "invalid:12345"})
	suite.ErrorContains(err, `failed to connect to URL "wss://invalid:12345": dial tcp`)
	suite.Nil(conn)
}

func (suite *WebsocketITestSuite) TestCreateConnectionInvalidCertificate() {
	conn, err := wsconn.CreateConnection(suite.ctx, false, "invalid", suite.exasol.GetUrl())
	suite.ErrorContains(err, fmt.Sprintf(`failed to connect to URL "wss://%s:%d": tls: failed to verify certificate`, suite.exasol.ConnectionInfo.Host, suite.exasol.ConnectionInfo.Port))
	suite.Nil(conn)
}

func (suite *WebsocketITestSuite) TestWrite() {
	conn := suite.createConnection()
	err := conn.WriteMessage(websocket.TextMessage, []byte("hello"))
	suite.NoError(err)
}

func (suite *WebsocketITestSuite) TestRead() {
	conn := suite.createConnection()
	conn.WriteMessage(websocket.TextMessage, []byte(`{"command": "login", "protocolVersion": 3}`))
	messageType, data, err := conn.ReadMessage()
	suite.Equal(1, messageType)
	suite.Contains(string(data), `{"status":"ok","responseData"`)
	suite.NoError(err)
}

func (suite *WebsocketITestSuite) createConnection() wsconn.WebsocketConnection {
	conn, err := wsconn.CreateConnection(suite.ctx, true, "", suite.exasol.GetUrl())
	if err != nil {
		suite.FailNowf("connection failed: %v", err.Error())
	}
	suite.T().Cleanup(func() {
		conn.Close()
	})
	return conn
}
