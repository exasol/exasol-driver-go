package exasol

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"net/url"
)

func (c *connection) connect() error {
	uri := fmt.Sprintf("%s:%s", c.Config.Host, c.Config.Port)

	scheme := "ws"
	if c.Config.Encryption {
		scheme = "wss"
	}

	u := url.URL{
		Scheme: scheme,
		Host:   uri,
	}
	ws, _, err := defaultDialer.DialContext(c.ctx, u.String(), nil)
	if err != nil {
		return err
	}
	c.ws = ws
	c.ws.EnableWriteCompression(false)
	return nil
}

func (c *connection) send(request, response interface{}) error {
	receiver, err := c.asyncSend(request)
	if err != nil {
		return err
	}
	return receiver(response)
}

func (c *connection) asyncSend(request interface{}) (func(interface{}) error, error) {
	err := c.ws.WriteJSON(request)
	if err != nil {
		errorLogger.Printf("could not send request, %w", err)
		return nil, driver.ErrBadConn
	}

	return func(response interface{}) error {
		result := &BaseResponse{}
		err = c.ws.ReadJSON(result)
		if err != nil {
			errorLogger.Printf("could not receive data, %w", err)
			return driver.ErrBadConn
		}

		if result.Status != "ok" {
			return fmt.Errorf("[%s] %s", result.Exception.SQLCode, result.Exception.Text)
		}

		if response == nil {
			return nil
		}

		return json.Unmarshal(result.ResponseData, response)

	}, nil
}
