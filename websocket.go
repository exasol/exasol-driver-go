package exasol

import (
	"bytes"
	"compress/zlib"
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/gorilla/websocket"
)

func (c *connection) connect() error {
	uri := fmt.Sprintf("%s:%s", c.config.Host, c.config.Port)

	scheme := "ws"
	if c.config.Encryption {
		scheme = "wss"
	}

	u := url.URL{
		Scheme: scheme,
		Host:   uri,
	}
	log.Printf("Connect to %s", u.String())
	ws, _, err := defaultDialer.DialContext(c.ctx, u.String(), nil)
	if err != nil {
		return err
	}
	c.websocket = ws
	c.websocket.EnableWriteCompression(false)
	return nil
}

func (c *connection) send(ctx context.Context, request, response interface{}) error {
	receiver, err := c.asyncSend(request)
	if err != nil {
		return err
	}
	channel := make(chan error, 1)
	go func() { channel <- receiver(response) }()
	select {
	case <-ctx.Done():
		_, err := c.asyncSend(&Command{Command: "abortQuery"})
		if err != nil {
			return fmt.Errorf("could not abort query %w", ctx.Err())
		}
		return ctx.Err()
	case err := <-channel:
		return err
	}
}

func (c *connection) asyncSend(request interface{}) (func(interface{}) error, error) {
	message, err := json.Marshal(request)
	log.Printf("Do request %s", message)
	if err != nil {
		errorLogger.Printf("could not marshal request, %s", err)
		return nil, driver.ErrBadConn
	}

	messageType := websocket.TextMessage
	if c.config.Compression {
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

	err = c.websocket.WriteMessage(messageType, message)
	if err != nil {
		errorLogger.Printf("could not send request, %s", err)
		return nil, driver.ErrBadConn
	}

	return func(response interface{}) error {

		_, message, err := c.websocket.ReadMessage()
		if err != nil {
			errorLogger.Printf("could not receive data, %s", err)
			return driver.ErrBadConn
		}

		result := &BaseResponse{}
		if c.config.Compression {
			b := bytes.NewReader(message)
			r, err := zlib.NewReader(b)
			if err != nil {
				errorLogger.Printf("could not decode compressed data, %s", err)
				return driver.ErrBadConn
			}
			err = json.NewDecoder(r).Decode(result)
			if err != nil {
				errorLogger.Printf("could not decode data, %s", err)
				return driver.ErrBadConn
			}

		} else {
			err = json.Unmarshal(message, result)
			if err != nil {
				errorLogger.Printf("could not receive data, %s", err)
				return driver.ErrBadConn
			}
		}

		if result.Status != "ok" {
			return fmt.Errorf("[%s] %s", result.Exception.SQLCode, result.Exception.Text)
		}

		log.Printf("Result %s", result.ResponseData)

		if response == nil {
			return nil
		}

		return json.Unmarshal(result.ResponseData, response)

	}, nil
}
