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
	"io"
	"math/rand"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

func (c *connection) getURIScheme() string {
	if c.config.encryption {
		return "wss"
	} else {
		return "ws"
	}
}

func (c *connection) connect() error {
	hosts, err := resolveHosts(c.config.host)
	if err != nil {
		return err
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(hosts), func(i, j int) {
		hosts[i], hosts[j] = hosts[j], hosts[i]
	})

	for _, host := range hosts {
		url := url.URL{
			Scheme: c.getURIScheme(),
			Host:   fmt.Sprintf("%s:%d", host, c.config.port),
		}
		skipVerify := !c.config.validateServerCertificate || c.config.certificateFingerprint != ""
		dialer := *websocket.DefaultDialer
		dialer.TLSClientConfig = &tls.Config{
			InsecureSkipVerify: skipVerify,
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
		ws, _, err = dialer.DialContext(c.ctx, url.String(), nil)
		if err == nil {
			c.websocket = ws
			c.websocket.EnableWriteCompression(false)
			break
		} else {
			logConnectionFailedError(url, err)
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
		return ErrMissingServerCertificate
	}
	actualFingerprint := sha256Hex(rawCerts[0])
	if !strings.EqualFold(expectedFingerprint, actualFingerprint) {
		return newErrCertificateFingerprintMismatch(actualFingerprint, expectedFingerprint)
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
			return newErrCouldNotAbort(ctx.Err())
		}
		return ctx.Err()
	case err := <-channel:
		return err
	}
}

func (c *connection) asyncSend(request interface{}) (func(interface{}) error, error) {
	message, err := json.Marshal(request)
	if err != nil {
		logMarshallingError(request, err)
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
		logRequestSendingError(err)
		return nil, driver.ErrBadConn
	}

	return c.callback(), nil
}

func (c *connection) callback() func(response interface{}) error {
	return func(response interface{}) error {

		_, message, err := c.websocket.ReadMessage()
		if err != nil {
			logReceivingError(err)
			return driver.ErrBadConn
		}

		result := &BaseResponse{}

		var reader io.Reader
		reader = bytes.NewReader(message)

		if c.config.compression {
			reader, err = zlib.NewReader(bytes.NewReader(message))
			if err != nil {
				logUncompressingError(err)
				return driver.ErrBadConn
			}
		}

		err = json.NewDecoder(reader).Decode(result)
		if err != nil {
			logJsonDecodingError(err)
			return driver.ErrBadConn
		}

		if result.Status != "ok" {
			return newSqlErr(result.Exception)
		}

		if response == nil {
			return nil
		}

		return json.Unmarshal(result.ResponseData, response)
	}
}
