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
	"sync"

	"github.com/exasol/exasol-driver-go/pkg/errors"
	"github.com/exasol/exasol-driver-go/pkg/logger"
	"github.com/gorilla/websocket"
)

// WebsocketConnection is a thin wrapper around [websocket.Conn]
// that allows mocking a websocket connection during unit tests.
type WebsocketConnection interface {
	// WriteMessage is a helper method for getting a writer using NextWriter,
	// writing the message and closing the writer.
	WriteMessage(messageType int, data []byte) error
	// ReadMessage is a helper method for getting a reader using NextReader and
	// reading from that reader to a buffer.
	ReadMessage() (messageType int, p []byte, err error)
	// Close closes the underlying network connection without sending or waiting for a close message.
	Close() error
}

type websocketSocket interface {
	EnableWriteCompression(enable bool)
	WriteMessage(messageType int, data []byte) error
	ReadMessage() (messageType int, p []byte, err error)
	Close() error
}

type wsConnImpl struct {
	socket websocketSocket

	readMutex  sync.Mutex
	writeMutex sync.Mutex

	readContentionLogOnce  sync.Once
	writeContentionLogOnce sync.Once
}

var cipherSuites = []uint16{
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384, // Workaround, set db suite in first place to fix handshake issue
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
}

// CreateConnection creates a websocket connection to the given URL.
// This deactivates write compression for the new connection.
func CreateConnection(ctx context.Context, skipVerify bool, expectedFingerprint string, url url.URL) (WebsocketConnection, error) {
	dialer := *websocket.DefaultDialer
	dialer.TLSClientConfig = &tls.Config{
		InsecureSkipVerify:    skipVerify, //nolint:gosec
		CipherSuites:          cipherSuites,
		VerifyPeerCertificate: certificateVerifier(expectedFingerprint),
	}
	ws, _, err := dialer.DialContext(ctx, url.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to URL %q: %w", url.String(), err)
	}
	ws.EnableWriteCompression(false)
	return &wsConnImpl{socket: ws}, nil
}

func (ws *wsConnImpl) WriteMessage(messageType int, data []byte) error {
	ws.lockWrite()
	defer ws.writeMutex.Unlock()
	return ws.socket.WriteMessage(messageType, data)
}

func (ws *wsConnImpl) ReadMessage() (messageType int, p []byte, err error) {
	ws.lockRead()
	defer ws.readMutex.Unlock()
	return ws.socket.ReadMessage()
}

func (ws *wsConnImpl) Close() error {
	return ws.socket.Close()
}

func (ws *wsConnImpl) lockRead() {
	if ws.readMutex.TryLock() {
		return
	}
	ws.readContentionLogOnce.Do(func() {
		logger.TraceLogger.Print("Concurrent WebSocket reads detected; serializing access")
	})
	ws.readMutex.Lock()
}

func (ws *wsConnImpl) lockWrite() {
	if ws.writeMutex.TryLock() {
		return
	}
	ws.writeContentionLogOnce.Do(func() {
		logger.TraceLogger.Print("Concurrent WebSocket writes detected; serializing access")
	})
	ws.writeMutex.Lock()
}

// certificateVerifier returns a function that verifies that a certificate has the given fingerprint.
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
