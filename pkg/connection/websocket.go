package connection

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
	"net/url"
	"strings"

	"github.com/exasol/exasol-driver-go/internal/utils"
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
		url := url.URL{
			Scheme: c.getURIScheme(),
			Host:   fmt.Sprintf("%s:%d", host, c.Config.Port),
		}
		skipVerify := !c.Config.ValidateServerCertificate || c.Config.CertificateFingerprint != ""
		dialer := *websocket.DefaultDialer
		dialer.TLSClientConfig = &tls.Config{
			InsecureSkipVerify: skipVerify, //nolint:gosec
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
		ws, _, err = dialer.DialContext(c.Ctx, url.String(), nil)
		if err == nil {
			c.websocket = ws
			c.websocket.EnableWriteCompression(false)
			break
		} else {
			logger.ErrorLogger.Print(errors.NewConnectionFailedError(url, err))
		}
	}
	return err
}

func (c *Connection) verifyPeerCertificate(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	expectedFingerprint := c.Config.CertificateFingerprint
	if len(expectedFingerprint) == 0 {
		return nil
	}
	if len(rawCerts) == 0 {
		return errors.ErrMissingServerCertificate
	}
	actualFingerprint := sha256Hex(rawCerts[0])
	if !strings.EqualFold(expectedFingerprint, actualFingerprint) {
		return errors.NewErrCertificateFingerprintMismatch(actualFingerprint, expectedFingerprint)
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

func (c *Connection) Send(ctx context.Context, request, response interface{}) error {
	receiver, err := c.asyncSend(request)
	if err != nil {
		return err
	}
	channel := make(chan error, 1)
	go func() { channel <- receiver(response) }()
	select {
	case <-ctx.Done():
		_, err := c.asyncSend(&types.Command{Command: "abortQuery"})
		if err != nil {
			return errors.NewErrCouldNotAbort(ctx.Err())
		}
		return ctx.Err()
	case err := <-channel:
		return err
	}
}

func (c *Connection) asyncSend(request interface{}) (func(interface{}) error, error) {
	message, err := json.Marshal(request)
	if err != nil {
		logger.ErrorLogger.Print(errors.NewMarshallingError(request, err))
		return nil, driver.ErrBadConn
	}

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

	err = c.websocket.WriteMessage(messageType, message)
	if err != nil {
		logger.ErrorLogger.Print(errors.NewRequestSendingError(err))

		return nil, driver.ErrBadConn
	}

	return c.callback(), nil
}

func (c *Connection) callback() func(response interface{}) error {
	return func(response interface{}) error {
		_, message, err := c.websocket.ReadMessage()
		if err != nil {
			logger.ErrorLogger.Print(errors.NewReceivingError(err))
			return driver.ErrBadConn
		}

		result := &types.BaseResponse{}

		var reader io.Reader
		reader = bytes.NewReader(message)

		if c.Config.Compression {
			reader, err = zlib.NewReader(bytes.NewReader(message))
			if err != nil {
				logger.ErrorLogger.Print(errors.NewUncompressingError(err))
				return driver.ErrBadConn
			}
		}

		err = json.NewDecoder(reader).Decode(result)
		if err != nil {
			logger.ErrorLogger.Print(errors.NewJsonDecodingError(err))
			return driver.ErrBadConn
		}

		if result.Status != "ok" {
			return errors.NewSqlErr(result.Exception.SQLCode, result.Exception.Text)
		}

		if response == nil {
			return nil
		}

		return json.Unmarshal(result.ResponseData, response)
	}
}
