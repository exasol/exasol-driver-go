package exasol

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	error_reporting_go "github.com/exasol/error-reporting-go"
	"github.com/gorilla/websocket"
)

func (c *connection) resolveHosts() ([]string, error) {
	var hosts []string
	hostRangeRegex := regexp.MustCompile(`^((.+?)(\d+))\.\.(\d+)$`)

	for _, host := range strings.Split(c.config.host, ",") {
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
	if c.config.encryption {
		return "wss"
	} else {
		return "ws"
	}
}

func (c *connection) connect() error {
	hosts, err := c.resolveHosts()
	if err != nil {
		return err
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(hosts), func(i, j int) {
		hosts[i], hosts[j] = hosts[j], hosts[i]
	})

	for _, host := range hosts {
		uri := fmt.Sprintf("%s:%d", host, c.config.port)

		u := url.URL{
			Scheme: c.getURIScheme(),
			Host:   uri,
		}
		dialer := *websocket.DefaultDialer
		dialer.TLSClientConfig = &tls.Config{
			InsecureSkipVerify: !c.config.validateServerCertificate,
			CipherSuites: []uint16{
				tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384, // Workaround, set db suit in first place to fix handshake issue
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_RSA_WITH_3DES_EDE_CBC_SHA,
				tls.TLS_RSA_WITH_AES_128_CBC_SHA,
				tls.TLS_RSA_WITH_AES_256_CBC_SHA,
				tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_RSA_WITH_AES_256_GCM_SHA384,

				tls.TLS_AES_128_GCM_SHA256,
				tls.TLS_AES_256_GCM_SHA384,
				tls.TLS_CHACHA20_POLY1305_SHA256,

				tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
				tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
				tls.TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA,
				tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
				tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
				tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
			},
			VerifyPeerCertificate: c.verifyPeerCertificate,
		}

		var ws *websocket.Conn
		ws, _, err = dialer.DialContext(c.ctx, u.String(), nil)
		if err == nil {
			c.websocket = ws
			c.websocket.EnableWriteCompression(false)
			break
		}
	}
	return err
}

func (c *connection) verifyPeerCertificate(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	expectedFingerprint := c.config.certificateFingerprint
	if len(expectedFingerprint) == 0 {
		return nil
	}
	if len(rawCerts) == 0 {
		return error_reporting_go.ExaError("E-EGOD-1").Message("Server did not return certificates")
	}
	actualFingerprint := sha256Hex(rawCerts[0])
	if !strings.EqualFold(expectedFingerprint, actualFingerprint) {
		err := error_reporting_go.ExaError("E-EGOD-2")
		err.Message("The server's certificate fingerprint {{server fingerprint}} does not match the expected fingerprint {{expected fingerprint}}")
		err.ParameterWithDescription("server fingerprint", actualFingerprint, "The SHA256 sum of the server's certificate")
		err.ParameterWithDescription("expected fingerprint", expectedFingerprint, "The expected fingerprint")
		return err
	}
	return nil
}

func sha256Hex(data []byte) string {
	sha256Sum := sha256.Sum256(data)
	return bytesToHexString(sha256Sum[:])
}

func bytesToHexString(data []byte) string {
	return hex.EncodeToString(data)
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
	if err != nil {
		errorLogger.Printf("could not marshal request, %s", err)
		return nil, driver.ErrBadConn
	}

	messageType := websocket.TextMessage
	if c.config.compression {
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
		if c.config.compression {
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

		if response == nil {
			return nil
		}

		return json.Unmarshal(result.ResponseData, response)

	}, nil
}
