package wsconn

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"

	"github.com/exasol/exasol-driver-go/pkg/errors"
	"github.com/gorilla/websocket"
)

type WebsocketConnection interface {
	Close() error
	EnableWriteCompression(enable bool)
	WriteMessage(messageType int, data []byte) error
	ReadMessage() (messageType int, p []byte, err error)
}

type wsConnImpl struct {
	socket *websocket.Conn
}

func CreateConnection(ctx context.Context, skipVerify bool, expectedFingerprint string, url url.URL) (WebsocketConnection, error) {
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
		VerifyPeerCertificate: certificateVerifier(expectedFingerprint),
	}
	ws, _, err := dialer.DialContext(ctx, url.String(), nil)
	if err != nil {
		return nil, err
	}
	return &wsConnImpl{socket: ws}, nil
}

func (ws *wsConnImpl) EnableWriteCompression(enable bool) {
	if ws.socket == nil {
		panic(fmt.Errorf("EnableWriteCompression: websocket not available"))
	}
	ws.socket.EnableWriteCompression(enable)
}

func (ws *wsConnImpl) WriteMessage(messageType int, data []byte) error {
	if ws.socket == nil {
		return fmt.Errorf("WriteMessage: websocket not available")
	}
	return ws.socket.WriteMessage(messageType, data)
}

func (ws *wsConnImpl) ReadMessage() (messageType int, p []byte, err error) {
	if ws.socket == nil {
		return 0, nil, fmt.Errorf("ReadMessage: websocket not available")
	}
	return ws.socket.ReadMessage()
}

func (ws *wsConnImpl) Close() error {
	if ws.socket == nil {
		return fmt.Errorf("Close: websocket not available")
	}
	return ws.socket.Close()
}

func certificateVerifier(expectedFingerprint string) func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
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
}

func sha256Hex(data []byte) string {
	sha256Sum := sha256.Sum256(data)
	return bytesToHexString(sha256Sum[:])
}

func bytesToHexString(data []byte) string {
	return hex.EncodeToString(data)
}
