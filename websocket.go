package exasol

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/tls"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

func (c *connection) resolveHosts() ([]string, error) {
	var hosts []string
	hostRangeRegex := regexp.MustCompile(`^((.+?)(\d+))\.\.(\d+)$`)

	for _, host := range strings.Split(c.config.Host, ",") {
		if hostRangeRegex.MatchString(host) {
			matches := hostRangeRegex.FindStringSubmatch(host)
			prefix := matches[2]

			start, err := strconv.Atoi(matches[3])
			if err != nil {
				return nil, err
			}

			stop, err := strconv.Atoi(matches[4])
			if err != nil {
				return nil, err
			}

			if stop < start {
				return nil, fmt.Errorf("invalid range limits")
			}

			for i := start; i <= stop; i++ {
				hosts = append(hosts, fmt.Sprintf("%s%d", prefix, i))
			}
		} else {
			hosts = append(hosts, host)
		}
	}
	return hosts, nil
}

func (c *connection) getURIScheme() string {
	if c.config.Encryption {
		return "wss"
	} else {
		return "ws"
	}
}

func (c *connection) connect() error {
	hosts, err := c.resolveHosts()
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(hosts), func(i, j int) {
		hosts[i], hosts[j] = hosts[j], hosts[i]
	})

	for _, host := range hosts {
		uri := fmt.Sprintf("%s:%d", host, c.config.Port)

		u := url.URL{
			Scheme: c.getURIScheme(),
			Host:   uri,
		}
		dialer := *websocket.DefaultDialer
		dialer.TLSClientConfig = &tls.Config{InsecureSkipVerify: c.config.Insecure}

		ws, _, err := dialer.DialContext(c.ctx, u.String(), nil)
		if err != nil {
			return err
		}

		c.websocket = ws
		c.websocket.EnableWriteCompression(false)
		return nil
	}
	return err
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