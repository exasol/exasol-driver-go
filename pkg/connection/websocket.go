package connection

import (
	"bytes"
	"compress/zlib"
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/exasol/exasol-driver-go/internal/utils"
	"github.com/exasol/exasol-driver-go/pkg/connection/wsconn"
	"github.com/exasol/exasol-driver-go/pkg/errors"
	"github.com/exasol/exasol-driver-go/pkg/logger"
	"github.com/exasol/exasol-driver-go/pkg/types"

	"github.com/gorilla/websocket"
)

func (c *Connection) getURIScheme() string {
	if c.Config.Encryption {
		return "wss"
	} else {
		return "ws"
	}
}

func (c *Connection) Connect() error {
	hosts, err := utils.ResolveHosts(c.Config.Host)
	if err != nil {
		return err
	}
	utils.ShuffleHosts(hosts)
	for _, host := range hosts {
		var url *url.URL
		url, err = c.createURL(host)
		if err != nil {
			return err
		}
		c.websocket, err = c.connectToHost(*url)
		if err == nil {
			return nil
		}
	}
	return err
}

func (c *Connection) createURL(host string) (*url.URL, error) {
	urlPath := c.Config.UrlPath
	if len(urlPath) > 0 && !strings.HasPrefix(urlPath, "/") {
		urlPath = "/" + urlPath
	}
	return url.Parse(fmt.Sprintf("%s://%s:%d%s", c.getURIScheme(), host, c.Config.Port, urlPath))
}

func (c *Connection) connectToHost(url url.URL) (wsconn.WebsocketConnection, error) {
	skipVerify := !c.Config.ValidateServerCertificate || c.Config.CertificateFingerprint != ""
	ws, err := wsconn.CreateConnection(c.Ctx, skipVerify, c.Config.CertificateFingerprint, url)
	if err != nil {
		logger.ErrorLogger.Print(errors.NewConnectionFailedError(url, err))
		return nil, err
	}
	return ws, nil
}

func (c *Connection) Send(ctx context.Context, request, response interface{}) error {
	receiver, err := c.asyncSend(request)
	if err != nil {
		return err
	}
	channel := make(chan error, 1)
	go func() { channel <- receiver(response) }()
	select {
	case <-ctx.Done():
		logger.TraceLogger.Printf("Received context done signal. Context error: %v", ctx.Err())
		_, err := c.asyncSend(&types.Command{Command: "abortQuery"})
		if err != nil {
			logger.ErrorLogger.Printf("Could not abort query: %v", err)
			return errors.NewErrCouldNotAbort(ctx.Err())
		}
		return ctx.Err()
	case err := <-channel:
		if err != nil {
			logger.TraceLogger.Printf("Received error from channel: %v", err)
		}
		return err
	}
}

func (c *Connection) asyncSend(request interface{}) (func(interface{}) error, error) {
	message, err := json.Marshal(request)
	if err != nil {
		logger.ErrorLogger.Print(errors.NewMarshallingError(request, err))
		return nil, driver.ErrBadConn
	}
	logger.TraceLogger.Printf("Sending message: %s", message)

	messageType := websocket.TextMessage
	if c.Config.Compression {
		var b bytes.Buffer
		w := zlib.NewWriter(&b)
		_, err = w.Write(message)
		if err != nil {
			return nil, err
		}
		w.Close()
		message = b.Bytes()
		messageType = websocket.BinaryMessage
	}

	if c.websocket == nil {
		return nil, errors.NewWebsocketNotConnected(string(message))
	}
	err = c.websocket.WriteMessage(messageType, message)
	if err != nil {
		wrappedError := errors.NewRequestSendingError(err)
		logger.ErrorLogger.Print(wrappedError)
		return nil, wrappedError
	}

	return c.callback(), nil
}

func (c *Connection) callback() func(response interface{}) error {
	return func(response interface{}) error {
		_, message, err := c.websocket.ReadMessage()
		if err != nil {
			wrappedError := errors.NewReceivingError(err)
			logger.ErrorLogger.Print(wrappedError)
			return wrappedError
		}

		result, err := c.parseResponse(message)
		if err != nil {
			return err
		}

		if result.Status != "ok" {
			if result.Exception != nil {
				return errors.NewSqlErr(result.Exception.SQLCode, result.Exception.Text)
			} else {
				return fmt.Errorf("result status is not 'ok': %q, expected exception in response %v", result.Status, result)
			}
		}

		if response == nil {
			// No response expected
			return nil
		}
		logger.TraceLogger.Printf("Received response with status %q with %d bytes data", result.Status, len(result.ResponseData))
		err = json.Unmarshal(result.ResponseData, response)
		if err != nil {
			return fmt.Errorf("failed to parse response data %q: %w", result.ResponseData, err)
		}
		return nil
	}
}

func (c *Connection) parseResponse(message []byte) (*types.BaseResponse, error) {
	result := &types.BaseResponse{}

	reader, err := c.createResponseReader(message)
	if err != nil {
		return nil, err
	}

	err = json.NewDecoder(reader).Decode(result)
	if err != nil {
		wrappedError := errors.NewJsonDecodingError(err, message)
		logger.ErrorLogger.Print(wrappedError)
		return nil, wrappedError
	}
	return result, nil
}

func (c *Connection) createResponseReader(message []byte) (io.Reader, error) {
	if c.Config.Compression {
		reader, err := zlib.NewReader(bytes.NewReader(message))
		if err != nil {
			wrappedError := errors.NewUncompressingError(err)
			logger.ErrorLogger.Print(wrappedError)
			return nil, wrappedError
		}
		return reader, nil
	} else {
		return bytes.NewReader(message), nil
	}
}
