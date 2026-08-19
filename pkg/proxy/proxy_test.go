package proxy

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/exasol/exasol-driver-go/pkg/errors"
	"github.com/stretchr/testify/assert"
)

const (
	serveTimeout        = 5 * time.Second
	contentRangeHeader  = "Content-Range"
	closedRangeHeader   = "bytes=2-5"
	partialContentRange = "bytes 2-5/10"
)

type importPeer struct {
	conn    net.Conn
	replies *bufio.Reader
}

type recordedRead struct {
	offset int64
	length int
}

type recordingReaderAt struct {
	data  []byte
	reads []recordedRead
}

func (r *recordingReaderAt) ReadAt(target []byte, offset int64) (int, error) {
	r.reads = append(r.reads, recordedRead{offset: offset, length: len(target)})
	if offset >= int64(len(r.data)) {
		return 0, io.EOF
	}
	read := copy(target, r.data[offset:])
	if read < len(target) {
		return read, io.EOF
	}
	return read, nil
}

func startServeFileRanges(t *testing.T, ctx context.Context, data []byte) (*importPeer, func() error) {
	t.Helper()
	peerConn, proxy := newPipedProxy(t)
	wait := serveInBackground(t, proxy, ctx, data)
	return &importPeer{conn: peerConn, replies: bufio.NewReader(peerConn)}, wait
}

func newPipedProxy(t *testing.T) (net.Conn, *Proxy) {
	t.Helper()
	peerConn, driverConn := net.Pipe()
	t.Cleanup(func() {
		peerConn.Close()
		driverConn.Close()
	})
	if err := peerConn.SetDeadline(time.Now().Add(serveTimeout)); err != nil {
		t.Fatalf("could not set peer deadline: %v", err)
	}
	return peerConn, &Proxy{connection: driverConn}
}

func serveInBackground(t *testing.T, proxy *Proxy, ctx context.Context, data []byte) func() error {
	t.Helper()
	served := make(chan error, 1)
	go func() {
		served <- proxy.ServeFileRanges(ctx, data)
	}()
	return func() error {
		select {
		case err := <-served:
			return err
		case <-time.After(serveTimeout):
			t.Fatal("ServeFileRanges did not return")
			return nil
		}
	}
}

// newStartedProxy returns a proxy that has exchanged the magic words with the
// peer on the other end of the pipe, which is the state encryption requires.
func newStartedProxy(t *testing.T) (*Proxy, net.Conn) {
	t.Helper()
	peerConn, proxy := newPipedProxy(t)

	started := make(chan error, 1)
	go func() {
		started <- proxy.StartProxy()
	}()

	var sentWords [3]uint32
	if err := binary.Read(peerConn, binary.LittleEndian, &sentWords); err != nil {
		t.Fatalf("could not read the magic words: %v", err)
	}
	reply := struct {
		Start uint32
		Port  uint32
		Host  [16]byte
	}{Start: 1, Port: 8563}
	// RFC 5737 reserves 192.0.2.0/24 for documentation. The address is only
	// serialized over the in-memory test pipe and is never used as a destination.
	copy(reply.Host[:], net.IPv4(192, 0, 2, 1).String())
	if err := binary.Write(peerConn, binary.LittleEndian, reply); err != nil {
		t.Fatalf("could not answer the magic words: %v", err)
	}

	select {
	case err := <-started:
		if err != nil {
			t.Fatalf("could not start the proxy: %v", err)
		}
	case <-time.After(serveTimeout):
		t.Fatal("StartProxy did not return")
	}
	return proxy, peerConn
}

// enableEncryption calls EnableTLS and fails the test if the call does not
// return promptly. Blocking means it touched the connection, which no peer can
// answer before the IMPORT statement announces that the connection carries TLS.
func enableEncryption(t *testing.T, proxy *Proxy) (string, error) {
	t.Helper()
	type outcome struct {
		fingerprint string
		err         error
	}
	enabled := make(chan outcome, 1)
	go func() {
		fingerprint, err := proxy.EnableTLS()
		enabled <- outcome{fingerprint, err}
	}()

	select {
	case result := <-enabled:
		return result.fingerprint, result.err
	case <-time.After(serveTimeout):
		t.Fatal("EnableTLS did not return: it must wrap the connection without any network I/O")
		return "", nil
	}
}

func (peer *importPeer) exchange(t *testing.T, request *http.Request) *http.Response {
	t.Helper()
	if err := request.Write(peer.conn); err != nil {
		t.Fatalf("could not send %s request: %v", request.Method, err)
	}
	response, err := http.ReadResponse(peer.replies, request)
	if err != nil {
		t.Fatalf("could not read response to %s request: %v", request.Method, err)
	}
	return response
}

func newImportRequest(t *testing.T, method string) *http.Request {
	t.Helper()
	request, err := http.NewRequest(method, "http://localhost/data.parquet", nil)
	if err != nil {
		t.Fatalf("could not build %s request: %v", method, err)
	}
	return request
}

func newRangeRequest(t *testing.T, requestedRange string) *http.Request {
	t.Helper()
	request := newImportRequest(t, http.MethodGet)
	request.Header.Set("Range", requestedRange)
	return request
}

func readBody(t *testing.T, response *http.Response) []byte {
	t.Helper()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("could not read response body: %v", err)
	}
	return body
}

func TestServeFileRangesHead(t *testing.T) {
	data := []byte("0123456789")
	peer, wait := startServeFileRanges(t, context.Background(), data)

	response := peer.exchange(t, newImportRequest(t, http.MethodHead))

	assert.Equal(t, http.StatusOK, response.StatusCode)
	assert.Equal(t, int64(len(data)), response.ContentLength)
	assert.Empty(t, readBody(t, response))

	assert.NoError(t, peer.conn.Close())
	assert.NoError(t, wait())
}

func TestServeFileRangesPartialContent(t *testing.T) {
	data := []byte("0123456789")
	for _, testCase := range []struct {
		name           string
		requestedRange string
		wantRange      string
		wantBody       string
	}{
		{"closed range", closedRangeHeader, partialContentRange, "2345"},
		{"single byte", "bytes=9-9", "bytes 9-9/10", "9"},
		{"explicit bounds spanning the whole file", "bytes=0-9", "bytes 0-9/10", "0123456789"},
		{"last byte position beyond the end is clamped", "bytes=7-99", "bytes 7-9/10", "789"},
		{"suffix range", "bytes=-3", "bytes 7-9/10", "789"},
		{"suffix longer than the file", "bytes=-99", "bytes 0-9/10", "0123456789"},
		{"bounds padded with spaces", "bytes= 2 - 5 ", partialContentRange, "2345"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			peer, wait := startServeFileRanges(t, context.Background(), data)

			response := peer.exchange(t, newRangeRequest(t, testCase.requestedRange))

			assert.Equal(t, http.StatusPartialContent, response.StatusCode)
			assert.Equal(t, testCase.wantRange, response.Header.Get(contentRangeHeader))
			assert.Equal(t, int64(len(testCase.wantBody)), response.ContentLength)
			assert.Equal(t, testCase.wantBody, string(readBody(t, response)))

			assert.NoError(t, peer.conn.Close())
			assert.NoError(t, wait())
		})
	}
}

func TestServeReaderRangesReadsOnlyRequestedSection(t *testing.T) {
	source := &recordingReaderAt{data: []byte("0123456789")}
	peerConn, proxy := newPipedProxy(t)
	wait := make(chan error, 1)
	go func() {
		wait <- proxy.ServeReaderRanges(context.Background(), source, int64(len(source.data)))
	}()
	peer := &importPeer{conn: peerConn, replies: bufio.NewReader(peerConn)}

	response := peer.exchange(t, newRangeRequest(t, closedRangeHeader))

	assert.Equal(t, "2345", string(readBody(t, response)))
	assert.NoError(t, peerConn.Close())
	assert.NoError(t, <-wait)
	assert.Equal(t, []recordedRead{{offset: 2, length: 4}}, source.reads,
		"serving a range must not read the complete source into memory")
}

func TestServeFileRangesOpenEndedRange(t *testing.T) {
	data := []byte("0123456789")
	peer, wait := startServeFileRanges(t, context.Background(), data)

	response := peer.exchange(t, newRangeRequest(t, "bytes=4-"))

	assert.Equal(t, http.StatusPartialContent, response.StatusCode)
	assert.Equal(t, "bytes 4-9/10", response.Header.Get(contentRangeHeader))
	assert.Equal(t, int64(6), response.ContentLength)
	assert.Equal(t, "456789", string(readBody(t, response)))

	assert.NoError(t, peer.conn.Close())
	assert.NoError(t, wait())
}

func TestServeFileRangesWholeFile(t *testing.T) {
	data := []byte("0123456789")
	peer, wait := startServeFileRanges(t, context.Background(), data)

	response := peer.exchange(t, newImportRequest(t, http.MethodGet))

	assert.Equal(t, http.StatusOK, response.StatusCode)
	assert.Equal(t, int64(len(data)), response.ContentLength)
	assert.Empty(t, response.Header.Get(contentRangeHeader))
	assert.Equal(t, data, readBody(t, response))

	assert.NoError(t, peer.conn.Close())
	assert.NoError(t, wait())
}

func TestServeFileRangesUnsatisfiableRange(t *testing.T) {
	for _, testCase := range []struct {
		name           string
		data           []byte
		requestedRange string
	}{
		{"first byte at the end of the file", []byte("0123456789"), "bytes=10-12"},
		{"first byte beyond the end of the file", []byte("0123456789"), "bytes=99-200"},
		{"open ended range beyond the end of the file", []byte("0123456789"), "bytes=99-"},
		{"zero length suffix", []byte("0123456789"), "bytes=-0"},
		{"last byte before first byte", []byte("0123456789"), "bytes=6-3"},
		{"any range of an empty file", []byte{}, "bytes=0-"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			peer, wait := startServeFileRanges(t, context.Background(), testCase.data)

			response := peer.exchange(t, newRangeRequest(t, testCase.requestedRange))

			assert.Equal(t, http.StatusBadRequest, response.StatusCode)
			assert.Equal(t, int64(0), response.ContentLength)

			stillServing := peer.exchange(t, newImportRequest(t, http.MethodHead))
			assert.Equal(t, http.StatusOK, stillServing.StatusCode)
			assert.Equal(t, int64(len(testCase.data)), stillServing.ContentLength)

			assert.NoError(t, peer.conn.Close())
			assert.NoError(t, wait())
		})
	}
}

func TestServeFileRangesMalformedRange(t *testing.T) {
	data := []byte("0123456789")
	for _, testCase := range []struct {
		name           string
		requestedRange string
	}{
		{"missing byte unit", "0-3"},
		{"unknown unit", "items=0-3"},
		{"no bounds at all", "bytes="},
		{"no separator", "bytes=3"},
		{"non numeric first byte", "bytes=abc-3"},
		{"non numeric last byte", "bytes=1-xyz"},
		{"several ranges", "bytes=0-1,4-5"},
		{"negative first byte", "bytes=--1"},
		{"negative last byte", "bytes=1--5"},
		{"first byte too large to represent", "bytes=99999999999999999999-"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			peer, wait := startServeFileRanges(t, context.Background(), data)

			response := peer.exchange(t, newRangeRequest(t, testCase.requestedRange))

			assert.Equal(t, http.StatusBadRequest, response.StatusCode)
			assert.Equal(t, int64(0), response.ContentLength)

			stillServing := peer.exchange(t, newImportRequest(t, http.MethodHead))
			assert.Equal(t, http.StatusOK, stillServing.StatusCode)
			assert.Equal(t, int64(len(data)), stillServing.ContentLength)

			assert.NoError(t, peer.conn.Close())
			assert.NoError(t, wait())
		})
	}
}

func TestServeFileRangesRejectsPut(t *testing.T) {
	peer, wait := startServeFileRanges(t, context.Background(), []byte("0123456789"))

	response := peer.exchange(t, newImportRequest(t, http.MethodPut))

	assert.Equal(t, http.StatusMethodNotAllowed, response.StatusCode)
	assert.Equal(t, int64(0), response.ContentLength)

	err := wait()
	assert.ErrorIs(t, err, errors.ErrInvalidProxyConn)
	assert.ErrorContains(t, err, "PUT")
}

func TestServeFileRangesSequentialRequests(t *testing.T) {
	data := []byte("0123456789")
	peer, wait := startServeFileRanges(t, context.Background(), data)

	size := peer.exchange(t, newImportRequest(t, http.MethodHead))
	assert.Equal(t, http.StatusOK, size.StatusCode)
	assert.Equal(t, int64(len(data)), size.ContentLength)
	assert.False(t, size.Close)

	head := peer.exchange(t, newRangeRequest(t, "bytes=0-3"))
	assert.Equal(t, http.StatusPartialContent, head.StatusCode)
	assert.Equal(t, "0123", string(readBody(t, head)))
	assert.False(t, head.Close)

	tail := peer.exchange(t, newRangeRequest(t, "bytes=4-7"))
	assert.Equal(t, http.StatusPartialContent, tail.StatusCode)
	assert.Equal(t, "4567", string(readBody(t, tail)))
	assert.False(t, tail.Close)

	assert.NoError(t, peer.conn.Close())
	assert.NoError(t, wait())
}

func TestServeFileRangesStopsOnPeerClose(t *testing.T) {
	data := []byte("0123456789")
	peer, wait := startServeFileRanges(t, context.Background(), data)

	response := peer.exchange(t, newImportRequest(t, http.MethodHead))
	assert.Equal(t, http.StatusOK, response.StatusCode)

	assert.NoError(t, peer.conn.Close())

	assert.NoError(t, wait())
}

func TestPublicKeyFingerprintFormat(t *testing.T) {
	proxy, _ := newStartedProxy(t)

	fingerprint, err := enableEncryption(t, proxy)

	assert.NoError(t, err)
	digest, isSha256 := strings.CutPrefix(fingerprint, "sha256//")
	assert.True(t, isSha256, "fingerprint %q must carry the sha256// prefix", fingerprint)
	decoded, err := base64.StdEncoding.DecodeString(digest)
	assert.NoError(t, err, "fingerprint %q must hold standard base64", fingerprint)
	assert.Len(t, decoded, sha256.Size)
}

func TestEnableTlsRequiresCompletedHandshake(t *testing.T) {
	_, proxy := newPipedProxy(t)

	fingerprint, err := enableEncryption(t, proxy)

	assert.Empty(t, fingerprint)
	assert.ErrorIs(t, err, errors.ErrInvalidProxyConn)
	assert.ErrorContains(t, err, "StartProxy")
}

func TestEnableTlsUsesServerRole(t *testing.T) {
	proxy, peerConn := newStartedProxy(t)
	fingerprint, err := enableEncryption(t, proxy)
	assert.NoError(t, err)
	wait := serveInBackground(t, proxy, context.Background(), []byte("0123456789"))

	certificateRequested := false
	peer := tls.Client(peerConn, &tls.Config{
		ServerName:            proxyHostName,
		InsecureSkipVerify:    true,
		VerifyPeerCertificate: pinnedTo(fingerprint),
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			certificateRequested = true
			return &tls.Certificate{}, nil
		},
	})
	handshaken := make(chan error, 1)
	go func() {
		handshaken <- peer.Handshake()
	}()

	select {
	case err := <-handshaken:
		assert.NoError(t, err)
	case <-time.After(serveTimeout):
		t.Fatal("the peer's handshake did not finish: the driver must answer a ClientHello as the TLS server")
	}
	assert.True(t, peer.ConnectionState().HandshakeComplete)
	assert.False(t, certificateRequested, "the driver must not request a certificate from the peer")

	assert.NoError(t, peer.Close())
	assert.NoError(t, wait())
}

// pinnedTo accepts the driver's certificate the way the server does, by
// matching the advertised fingerprint against the key the driver presents
// instead of following a chain to an authority.
func pinnedTo(fingerprint string) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCertificates [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCertificates) != 1 {
			return fmt.Errorf("the driver presented %d certificates, want exactly one", len(rawCertificates))
		}
		presented, err := x509.ParseCertificate(rawCertificates[0])
		if err != nil {
			return err
		}
		digest := sha256.Sum256(presented.RawSubjectPublicKeyInfo)
		presentedFingerprint := "sha256//" + base64.StdEncoding.EncodeToString(digest[:])
		if presentedFingerprint != fingerprint {
			return fmt.Errorf("the driver presented the key %s, want the advertised %s", presentedFingerprint, fingerprint)
		}
		return nil
	}
}

func TestServeFileRangesOverTls(t *testing.T) {
	data := []byte("0123456789")
	proxy, peerConn := newStartedProxy(t)
	fingerprint, err := enableEncryption(t, proxy)
	assert.NoError(t, err)
	wait := serveInBackground(t, proxy, context.Background(), data)

	encrypted := tls.Client(peerConn, &tls.Config{
		ServerName:            proxyHostName,
		InsecureSkipVerify:    true,
		VerifyPeerCertificate: pinnedTo(fingerprint),
	})
	peer := &importPeer{conn: encrypted, replies: bufio.NewReader(encrypted)}

	response := peer.exchange(t, newRangeRequest(t, closedRangeHeader))

	assert.Equal(t, http.StatusPartialContent, response.StatusCode)
	assert.Equal(t, partialContentRange, response.Header.Get(contentRangeHeader))
	assert.Equal(t, int64(4), response.ContentLength)
	assert.Equal(t, "2345", string(readBody(t, response)))

	assert.NoError(t, encrypted.Close())
	assert.NoError(t, wait())
}

func TestServeFileRangesStopsOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, wait := startServeFileRanges(t, ctx, []byte("0123456789"))

	assert.NoError(t, wait())
}
