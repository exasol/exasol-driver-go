package proxy

import (
	"bufio"
	"bytes"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/exasol/exasol-driver-go/pkg/errors"
	"github.com/exasol/exasol-driver-go/pkg/logger"
)

const (
	// fingerprintPrefix names the digest algorithm of the PUBLIC KEY clause.
	fingerprintPrefix = "sha256//"
	// proxyHostName is the name the certificate carries. The server reaches the
	// driver by address and pins the key by fingerprint, so it never resolves
	// the name, but TLS stacks that require a subject alternative name reject a
	// certificate that carries none.
	proxyHostName = "localhost"
	// certificateClockSkew keeps the certificate valid on a server whose clock
	// runs slightly behind the driver's.
	certificateClockSkew = time.Hour
	// certificateLifetime outlives any single import, and the key is discarded
	// with the connection.
	certificateLifetime = 24 * time.Hour
	serialNumberBits    = 128
)

type Proxy struct {
	isClosed   bool
	isStarted  bool
	connection net.Conn
	Host       string
	Port       int
}

var magicWords = []interface{}{uint32(0x02212102), uint32(1), uint32(1)}

func NewProxy(hosts []string, port int) (*Proxy, error) {
	var wrappedErr error
	for _, host := range hosts {
		uri := net.JoinHostPort(host, fmt.Sprintf("%d", port))
		con, err := net.Dial("tcp", uri)
		if err == nil {
			p := &Proxy{
				connection: con,
				isClosed:   false,
			}

			return p, nil
		} else {
			wrappedErr = fmt.Errorf("%w: could not create TCP connection to %s, %s", errors.ErrInvalidProxyConn, uri, err.Error())
			logger.ErrorLogger.Print(wrappedErr)
		}
	}
	return nil, wrappedErr
}

func (p *Proxy) StartProxy() error {
	for _, word := range magicWords {
		err := binary.Write(p.connection, binary.LittleEndian, word)
		if err != nil {
			wrappedErr := fmt.Errorf("%w: could not send magic words, %s", errors.ErrInvalidProxyConn, err)
			logger.ErrorLogger.Print(wrappedErr)
			return wrappedErr
		}
	}

	var result struct {
		Start uint32 // Not needed
		Port  uint32
		Host  [16]byte
	}
	err := binary.Read(p.connection, binary.LittleEndian, &result)
	if err != nil {
		wrappedErr := fmt.Errorf("%w: could not read from TCP connection to get internal host and port, %s", errors.ErrInvalidProxyConn, err.Error())
		logger.ErrorLogger.Print(wrappedErr)
		return wrappedErr
	}

	p.Port = int(result.Port)
	p.Host = string(bytes.Trim(result.Host[:], "\x00"))
	p.isStarted = true

	return nil
}

// EnableTLS encrypts the proxy connection with a certificate generated for this
// one import, and returns the fingerprint the server pins in the statement's
// PUBLIC KEY clause to accept a certificate no authority vouches for. The
// wrapped connection replaces the plaintext one, so every later read and write
// of the import goes through the TLS layer.
//
// Encryption can only be enabled once the proxy has started, because the magic
// words are exchanged in plaintext.
//
// The driver is the TLS server here, not the client: the server treats the
// proxy connection as an HTTPS endpoint and opens the handshake itself, so a
// client-role driver would reject the ClientHello it receives. There is no peer
// certificate in this direction, and no certificate is requested from the peer.
//
// No handshake happens here, and no other byte crosses the connection either.
// The server learns that the connection carries TLS only from the PUBLIC KEY
// clause of the IMPORT statement, and sends its ClientHello only after reading
// that clause, so a handshake started before the statement is dispatched waits
// for a ClientHello that cannot arrive. Neither side times out, which would
// hang the import instead of failing it. This function therefore wraps and
// returns, leaving the handshake to the first read or write of the transfer
// that runs concurrently with the statement.
func (p *Proxy) EnableTLS() (string, error) {
	if !p.isStarted {
		return "", fmt.Errorf("%w: could not encrypt the connection before the magic words were exchanged in plaintext, call StartProxy first", errors.ErrInvalidProxyConn)
	}
	certificate, fingerprint, err := newPinnedCertificate()
	if err != nil {
		return "", err
	}
	p.connection = tls.Server(p.connection, &tls.Config{Certificates: []tls.Certificate{certificate}})
	return fingerprint, nil
}

// newPinnedCertificate generates the throwaway key pair of one import and
// returns the self-signed certificate the driver presents together with the
// fingerprint the server pins to accept it.
func newPinnedCertificate() (tls.Certificate, string, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, "", fmt.Errorf("%w: could not generate a key pair for the encrypted import connection, %s", errors.ErrInvalidProxyConn, err)
	}
	template, err := certificateTemplate()
	if err != nil {
		return tls.Certificate{}, "", err
	}
	certificate, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	if err != nil {
		return tls.Certificate{}, "", fmt.Errorf("%w: could not create the certificate for the encrypted import connection, %s", errors.ErrInvalidProxyConn, err)
	}
	fingerprint, err := publicKeyFingerprint(key.Public())
	if err != nil {
		return tls.Certificate{}, "", err
	}
	return tls.Certificate{Certificate: [][]byte{certificate}, PrivateKey: key}, fingerprint, nil
}

func certificateTemplate() (*x509.Certificate, error) {
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), serialNumberBits))
	if err != nil {
		return nil, fmt.Errorf("%w: could not draw a serial number for the certificate of the encrypted import connection, %s", errors.ErrInvalidProxyConn, err)
	}
	return &x509.Certificate{
		SerialNumber:          serialNumber,
		Subject:               pkix.Name{CommonName: proxyHostName},
		DNSNames:              []string{proxyHostName},
		NotBefore:             time.Now().Add(-certificateClockSkew),
		NotAfter:              time.Now().Add(certificateLifetime),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}, nil
}

func publicKeyFingerprint(publicKey crypto.PublicKey) (string, error) {
	encodedKey, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		return "", fmt.Errorf("%w: could not encode the public key of the encrypted import connection, %s", errors.ErrInvalidProxyConn, err)
	}
	digest := sha256.Sum256(encodedKey)
	return fingerprintPrefix + base64.StdEncoding.EncodeToString(digest[:]), nil
}

func (p *Proxy) Write(ctx context.Context, files []*os.File, rowSeparator string) error {
	err := p.sendHeaders([]string{
		"HTTP/1.1 200 OK",
		"Content-Type: application/octet-stream",
		"Content-Disposition: attachment; filename=data.csv",
		"Transfer-Encoding: chunked",
		"Connection: close",
	})
	if err != nil {
		return err
	}
	chunkedWriter := httputil.NewChunkedWriter(p.connection)
	for _, file := range files {
		err = p.SendFile(ctx, file, rowSeparator, chunkedWriter)
		if err != nil {
			return err
		}
	}
	_, err = p.connection.Write([]byte("0\r\n\r\n")) // A final zero chunk
	return err
}

func (p *Proxy) SendFile(ctx context.Context, file *os.File, rowSeparator string, chunkedWriter io.WriteCloser) error {
	reader := bufio.NewReader(file)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		delimiter := '\n'
		// Handle files which end on CR
		if rowSeparator == "\r" {
			delimiter = '\r'
		}
		line, err := reader.ReadBytes(byte(delimiter))
		if err != nil && len(line) == 0 {
			break
		}

		if err != nil && len(line) != 0 {
			line = append(line, []byte(rowSeparator)...)
		}

		if len(line) == 0 {
			break
		}
		_, err = chunkedWriter.Write(line)
		if err != nil {
			return err
		}
	}
	return nil
}

// ServeFileRanges answers range requests from an in-memory file. It is kept for
// callers that already hold the content; file-backed imports should use
// ServeReaderRanges to avoid loading the complete file into memory.
func (p *Proxy) ServeFileRanges(ctx context.Context, data []byte) error {
	return p.ServeReaderRanges(ctx, bytes.NewReader(data), int64(len(data)))
}

// ServeReaderRanges answers the HTTP requests Exasol issues while reading a
// local file it pulls rather than receives, and returns once the server stops
// asking. Each response reads only the requested section from source.
//
// Exasol ends the conversation by closing the connection, so a request that
// fails to arrive finishes the transfer successfully, whether the cause is end
// of stream, an unparsable request line, or the connection being closed
// underneath this call. That last cause is what makes cancellation work: this
// function cannot interrupt a read it is already parked in, so the transfer
// boundary unblocks it by closing the connection, and a serve loop that
// reported an error on a deliberate cancellation would mask the caller's own
// reason for stopping. ctx is therefore only polled between requests.
//
// The buffered reader is built here rather than when the Proxy is constructed,
// because the connection may have been wrapped in a TLS layer in the meantime
// and a reader bound to the raw socket would parse encrypted bytes as HTTP.
//
// ServeReaderRanges answers exactly one reader sequentially over one connection;
// it has no way to multiplex a second concurrent reader onto the same
// bufio.Reader. internal/utils.UpdateImportQuery holds the server to that by
// appending the ";MaxConcurrentReads=1" URL suffix, so the two must change
// together: relaxing one without the other breaks the contract silently.
func (p *Proxy) ServeReaderRanges(ctx context.Context, source io.ReaderAt, size int64) error {
	requests := bufio.NewReader(p.connection)
	for {
		if ctx.Err() != nil {
			return nil
		}
		request, err := http.ReadRequest(requests)
		if err != nil {
			return nil
		}
		if err := p.answer(request, source, size); err != nil {
			return err
		}
	}
}

func (p *Proxy) answer(request *http.Request, source io.ReaderAt, size int64) error {
	switch request.Method {
	case http.MethodHead:
		return p.announceFileSize(size)
	case http.MethodGet:
		requestedRange := request.Header.Get("Range")
		if requestedRange == "" {
			return p.sendWholeFile(source, size)
		}
		return p.sendByteRange(requestedRange, source, size)
	default:
		return p.rejectMethod(request.Method)
	}
}

func (p *Proxy) announceFileSize(size int64) error {
	return p.sendHeaders([]string{
		"HTTP/1.1 200 OK",
		"Accept-Ranges: bytes",
		fmt.Sprintf("Content-Length: %d", size),
	})
}

func (p *Proxy) sendWholeFile(source io.ReaderAt, size int64) error {
	if err := p.announceFileSize(size); err != nil {
		return err
	}
	return p.sendBody(io.NewSectionReader(source, 0, size), size)
}

func (p *Proxy) sendByteRange(requestedRange string, source io.ReaderAt, size int64) error {
	first, last, satisfiable := parseByteRange(requestedRange, size)
	if !satisfiable {
		return p.sendStatusWithoutBody("HTTP/1.1 400 Bad Request")
	}
	err := p.sendHeaders([]string{
		"HTTP/1.1 206 Partial Content",
		fmt.Sprintf("Content-Range: bytes %d-%d/%d", first, last, size),
		fmt.Sprintf("Content-Length: %d", last-first+1),
	})
	if err != nil {
		return err
	}
	length := last - first + 1
	return p.sendBody(io.NewSectionReader(source, first, length), length)
}

func (p *Proxy) sendBody(body io.Reader, size int64) error {
	if _, err := io.CopyN(p.connection, body, size); err != nil {
		return fmt.Errorf("%w: could not send %d bytes of the import file to the server, %s", errors.ErrInvalidProxyConn, size, err)
	}
	return nil
}

// sendStatusWithoutBody answers with a status line alone. The explicit zero
// length is what keeps the connection usable afterwards: without it HTTP/1.1
// delimits the body by closing the connection, so the server would read the
// next response as part of this one instead of asking for more of the file.
func (p *Proxy) sendStatusWithoutBody(statusLine string) error {
	return p.sendHeaders([]string{statusLine, "Content-Length: 0"})
}

func (p *Proxy) rejectMethod(method string) error {
	if err := p.sendStatusWithoutBody("HTTP/1.1 405 Method Not Allowed"); err != nil {
		return err
	}
	wrappedErr := fmt.Errorf("%w: the server sent an unexpected %s request, but a local import connection only answers GET and HEAD requests", errors.ErrInvalidProxyConn, method)
	logger.ErrorLogger.Print(wrappedErr)
	return wrappedErr
}

// parseByteRange resolves a single `Range` header value against a file of total
// bytes into an inclusive, in-bounds slice. It reports whether the range can be
// served at all, without distinguishing an unparsable range from an
// unsatisfiable one, because both are answered the same way.
func parseByteRange(requestedRange string, total int64) (first, last int64, satisfiable bool) {
	spec, isByteUnit := strings.CutPrefix(requestedRange, "bytes=")
	if !isByteUnit || total == 0 {
		return 0, 0, false
	}
	firstText, lastText, separated := strings.Cut(spec, "-")
	if !separated {
		return 0, 0, false
	}
	firstText, lastText = strings.TrimSpace(firstText), strings.TrimSpace(lastText)

	if firstText == "" {
		suffixLength, parsed := parsePosition(lastText)
		if !parsed || suffixLength == 0 {
			return 0, 0, false
		}
		return max(0, total-suffixLength), total - 1, true
	}

	first, parsed := parsePosition(firstText)
	if !parsed || first >= total {
		return 0, 0, false
	}
	if lastText == "" {
		return first, total - 1, true
	}
	last, parsed = parsePosition(lastText)
	if !parsed {
		return 0, 0, false
	}
	last = min(last, total-1)
	if last < first {
		return 0, 0, false
	}
	return first, last, true
}

func parsePosition(text string) (int64, bool) {
	position, err := strconv.ParseInt(text, 10, 64)
	if err != nil || position < 0 {
		return 0, false
	}
	return position, true
}

func (p *Proxy) sendHeaders(headers []string) error {
	headers = append(headers, "")
	for _, header := range headers {
		header += "\r\n"
		_, err := p.connection.Write([]byte(header))
		if err != nil {
			return fmt.Errorf("unable to send header <%s>to proxy: %s", header, err)
		}
	}
	return nil
}

func (p *Proxy) Close() {
	if p.isClosed {
		return
	}

	p.connection.Close()
	p.isClosed = true
}
