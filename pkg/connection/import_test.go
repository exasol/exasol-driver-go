package connection

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/exasol/exasol-driver-go/internal/config"
	"github.com/exasol/exasol-driver-go/internal/utils"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
)

const (
	csvImportQuery               = "IMPORT INTO TEST_TABLE FROM LOCAL CSV FILE '../../testData/data.csv'"
	parquetFixtureName           = "../testData/data.parquet"
	missingFilePath              = "../../testData/no-such-file.parquet"
	missingParquetImportQuery    = "IMPORT INTO TEST_TABLE FROM LOCAL PARQUET FILE '" + missingFilePath + "'"
	createImportStatementFailure = "could not create the import statement: %v"
)

type parquetTestRow struct {
	A int64  `parquet:"a"`
	B string `parquet:"b"`
}

func createParquetFixture(t *testing.T) string {
	t.Helper()
	filePath := filepath.Join("..", parquetFixtureName)
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatalf("could not create parquet fixture: %v", err)
	}
	path := filePath
	if err := file.Close(); err != nil {
		t.Fatalf("could not close parquet fixture: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(path) })

	rows := []parquetTestRow{
		{A: 11, B: "test1"},
		{A: 12, B: "test2"},
		{A: 13, B: "test3"},
	}
	if err := parquet.WriteFile(path, rows); err != nil {
		t.Fatalf("could not write parquet fixture: %v", err)
	}
	return path
}

func parquetImportQuery(path string) string {
	return fmt.Sprintf("IMPORT INTO TEST_TABLE FROM LOCAL PARQUET FILE '%s'", path)
}

func parquetImportQueryWithFiles(paths ...string) string {
	query := "IMPORT INTO TEST_TABLE FROM LOCAL PARQUET"
	for _, path := range paths {
		query += fmt.Sprintf(" FILE '%s'", path)
	}
	return query
}

const (
	// cancellationDeadline is the bound a cancelled transfer must meet. Both
	// transports park in a wire operation that cannot observe a context, so
	// missing this bound means closing the connection never reached them and a
	// failing import would hang the caller instead of returning.
	cancellationDeadline = 1 * time.Second
	// transferSettleTime lets the transfer reach the wire operation it can never
	// finish before the test cancels it. Cancelling sooner would still pass while
	// no longer exercising a transfer that is already blocked.
	transferSettleTime = 100 * time.Millisecond
	// peerDeadline bounds every operation of the fake server, so a driver defect
	// fails the test instead of hanging it.
	peerDeadline = 10 * time.Second
	// dialGracePeriod gives a connection the driver should never have opened the
	// time to arrive, so its absence is a finding rather than a scheduling race.
	dialGracePeriod = 100 * time.Millisecond
)

// silentPeer stands in for the Exasol node a local import dials. It answers the
// magic-word handshake and then sends nothing at all, which is the state both
// transports have to survive: a Parquet serve loop waits for a request that
// never arrives, and an encrypted transfer waits for a ClientHello the peer
// never sends, because the driver is the TLS server on this connection.
type silentPeer struct {
	host      string
	port      int
	connected chan handshake
}

// handshake is what the peer's own goroutine learned about the driver's
// connection. It carries the failure instead of reporting it, because the test
// may already have finished by the time the peer notices.
type handshake struct {
	connection net.Conn
	err        error
}

func startSilentPeer(t *testing.T) *silentPeer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("could not listen for the driver: %v", err)
	}
	address := listener.Addr().(*net.TCPAddr)
	peer := &silentPeer{host: address.IP.String(), port: address.Port, connected: make(chan handshake, 1)}
	t.Cleanup(func() {
		listener.Close()
		select {
		case accepted := <-peer.connected:
			if accepted.connection != nil {
				accepted.connection.Close()
			}
		default:
		}
	})

	go func() {
		connection, err := listener.Accept()
		if err != nil {
			return
		}
		if err := answerMagicWords(connection); err != nil {
			connection.Close()
			peer.connected <- handshake{err: err}
			return
		}
		peer.connected <- handshake{connection: connection}
	}()
	return peer
}

func (peer *silentPeer) plaintextConfig() *config.Config {
	return &config.Config{Host: peer.host, Port: peer.port}
}

func (peer *silentPeer) encryptedConfig() *config.Config {
	return &config.Config{Host: peer.host, Port: peer.port, LocalImportEncryption: true}
}

func (peer *silentPeer) acceptedConnection(t *testing.T) net.Conn {
	t.Helper()
	select {
	case accepted := <-peer.connected:
		if accepted.err != nil {
			t.Fatalf("the peer could not answer the magic words: %v", accepted.err)
		}
		t.Cleanup(func() { accepted.connection.Close() })
		return accepted.connection
	case <-time.After(peerDeadline):
		t.Fatal("the driver did not complete the magic-word handshake")
		return nil
	}
}

func (peer *silentPeer) assertNotDialled(t *testing.T) {
	t.Helper()
	select {
	case <-peer.connected:
		t.Fatal("the driver opened a proxy connection for a statement it should have refused before any I/O")
	case <-time.After(dialGracePeriod):
	}
}

func answerMagicWords(connection net.Conn) error {
	if err := connection.SetDeadline(time.Now().Add(peerDeadline)); err != nil {
		return err
	}
	var magicWords [3]uint32
	if err := binary.Read(connection, binary.LittleEndian, &magicWords); err != nil {
		return err
	}
	reply := struct {
		Start uint32
		Port  uint32
		Host  [16]byte
	}{Start: 1, Port: 8563}
	// RFC 5737 reserves 192.0.2.0/24 for documentation. The address is only
	// serialized into this fake peer's reply and is never used as a destination.
	copy(reply.Host[:], net.IPv4(192, 0, 2, 1).String())
	return binary.Write(connection, binary.LittleEndian, reply)
}

// cancelParkedTransfer runs a transfer that can never finish on its own, waits
// for it to park on the wire, cancels it, and returns what the transfer
// reported. It fails the test when the transfer outlives its cancellation.
func cancelParkedTransfer(t *testing.T, statement *ImportStatement) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	transferred := make(chan error, 1)
	go func() { transferred <- statement.Transfer(ctx) }()

	time.Sleep(transferSettleTime)
	cancel()

	select {
	case err := <-transferred:
		return err
	case <-time.After(cancellationDeadline):
		t.Fatalf("the transfer did not stop within %s of cancellation", cancellationDeadline)
		return nil
	}
}

// assertConnectionClosed proves the driver released the proxy connection, which
// is the only lever that frees a blocked transfer. The driver sends this peer
// nothing, so the close is the only event the peer can ever read.
func assertConnectionClosed(t *testing.T, connection net.Conn) {
	t.Helper()
	if err := connection.SetReadDeadline(time.Now().Add(cancellationDeadline)); err != nil {
		t.Fatalf("could not set the read deadline of the peer: %v", err)
	}
	_, err := connection.Read(make([]byte, 1))
	assert.ErrorIs(t, err, io.EOF, "the driver must close the proxy connection when the transfer is cancelled")
}

func TestNewImportStatementReportsMissingParquetFile(t *testing.T) {
	peer := startSilentPeer(t)

	statement, err := NewImportStatement(missingParquetImportQuery, utils.ImportFormatParquet, peer.plaintextConfig())

	assert.Nil(t, statement)
	assert.ErrorContains(t, err, "E-EGOD-28")
	assert.ErrorContains(t, err, missingFilePath, "the error must name the file the caller asked for")
	var pathError *fs.PathError
	assert.NotErrorAs(t, err, &pathError,
		"the caller must be told the file is missing in the driver's own terms, not handed the raw error of the standard library")
	peer.assertNotDialled(t)
}

// TestNewImportStatementRejectsMultipleParquetFiles calls the exported
// constructor directly, which is the one way into the Parquet path that does not
// pass Connection.exec's file-count guard. Without a guard of its own the
// constructor serves the first file and drops the rest, while the rewrite
// collapses the extra FILE clauses so the server is never told: the statement
// reports success over a table that received part of the data. Decision-log § [2]
// allocated E-EGOD-32 to make that outcome unreachable, so it must be
// unreachable here too and not merely documented as the caller's duty.
func TestNewImportStatementRejectsMultipleParquetFiles(t *testing.T) {
	peer := startSilentPeer(t)
	path := createParquetFixture(t)

	statement, err := NewImportStatement(parquetImportQueryWithFiles(path, path), utils.ImportFormatParquet, peer.plaintextConfig())

	assert.Nil(t, statement)
	assert.ErrorContains(t, err, "E-EGOD-32")
	peer.assertNotDialled(t)
}

func TestNewImportStatementIgnoresFileClausesInComments(t *testing.T) {
	peer := startSilentPeer(t)
	path := createParquetFixture(t)
	query := fmt.Sprintf("/* previous FILE 'old.parquet' */IMPORT INTO TEST_TABLE FROM LOCAL PARQUET FILE '%s'", path)

	statement, err := NewImportStatement(query, utils.ImportFormatParquet, peer.plaintextConfig())

	if err != nil {
		t.Fatalf(createImportStatementFailure, err)
	}
	t.Cleanup(statement.Close)
}

func TestGetUpdatedQueryPinsTheEncryptedConnection(t *testing.T) {
	peer := startSilentPeer(t)
	statement, err := NewImportStatement(csvImportQuery, utils.ImportFormatCSV, peer.encryptedConfig())
	if err != nil {
		t.Fatalf(createImportStatementFailure, err)
	}
	t.Cleanup(statement.Close)

	updatedQuery := statement.GetUpdatedQuery()

	assert.Contains(t, updatedQuery, "AT 'https://",
		"the statement is the only place the server learns the connection carries TLS, so an unencrypted scheme leaves it talking plaintext to a driver that expects a handshake")
	assert.Regexp(t, `PUBLIC KEY 'sha256//[A-Za-z0-9+/]+={0,2}'`, updatedQuery,
		"the server accepts a certificate no authority vouches for only against the fingerprint the statement pins")
}

func TestGetUpdatedQueryPinsNothingWithoutEncryption(t *testing.T) {
	peer := startSilentPeer(t)
	statement, err := NewImportStatement(csvImportQuery, utils.ImportFormatCSV, peer.plaintextConfig())
	if err != nil {
		t.Fatalf(createImportStatementFailure, err)
	}
	t.Cleanup(statement.Close)

	updatedQuery := statement.GetUpdatedQuery()

	assert.Contains(t, updatedQuery, "AT 'http://", "encryption is opt-in, so an import that did not ask for it keeps the plaintext connection it has always had")
	assert.NotContains(t, updatedQuery, "PUBLIC KEY", "there is no key to pin on a connection the driver never wrapped")
}

func TestTransferStopsOnContextCancelWithTls(t *testing.T) {
	peer := startSilentPeer(t)

	statement, err := NewImportStatement(csvImportQuery, utils.ImportFormatCSV, peer.encryptedConfig())
	if err != nil {
		t.Fatalf(createImportStatementFailure, err)
	}
	connection := peer.acceptedConnection(t)

	transferErr := cancelParkedTransfer(t, statement)

	assert.Error(t, transferErr, "a transfer cut short in the TLS handshake cannot report that it sent the file")
	assertConnectionClosed(t, connection)
}

func TestTransferStopsOnContextCancelParquet(t *testing.T) {
	peer := startSilentPeer(t)
	path := createParquetFixture(t)

	statement, err := NewImportStatement(parquetImportQuery(path), utils.ImportFormatParquet, peer.plaintextConfig())
	if err != nil {
		t.Fatalf(createImportStatementFailure, err)
	}
	connection := peer.acceptedConnection(t)

	transferErr := cancelParkedTransfer(t, statement)

	assert.NoError(t, transferErr, "a serve loop freed by closing its connection finishes cleanly, so the caller's own error surfaces")
	assertConnectionClosed(t, connection)
}

// TestNewImportStatementDoesNotHandshakeBeforeTransfer guards decision-log § [14]:
// EnableTLS must wrap the connection and return without touching the wire. The
// peer here answers the magic words and then sends nothing at all, never a TLS
// ClientHello, because the driver is the TLS server on this connection and Exasol
// only sends one once it has read the statement's PUBLIC KEY clause. A handshake
// begun this early would park waiting for a ClientHello that cannot arrive, and
// NewImportStatement would never return.
func TestNewImportStatementDoesNotHandshakeBeforeTransfer(t *testing.T) {
	peer := startSilentPeer(t)

	type outcome struct {
		statement *ImportStatement
		err       error
	}
	constructed := make(chan outcome, 1)
	go func() {
		statement, err := NewImportStatement(csvImportQuery, utils.ImportFormatCSV, peer.encryptedConfig())
		constructed <- outcome{statement: statement, err: err}
	}()

	select {
	case result := <-constructed:
		if result.err != nil {
			t.Fatalf(createImportStatementFailure, result.err)
		}
		t.Cleanup(result.statement.Close)
	case <-time.After(cancellationDeadline):
		t.Fatalf("NewImportStatement did not return within %s; an eager TLS handshake would block waiting for a ClientHello the peer never sends", cancellationDeadline)
	}
}
