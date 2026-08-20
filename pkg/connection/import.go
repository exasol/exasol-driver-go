package connection

import (
	"context"
	"os"

	"github.com/exasol/exasol-driver-go/internal/config"
	"github.com/exasol/exasol-driver-go/internal/utils"
	"github.com/exasol/exasol-driver-go/pkg/errors"
	"github.com/exasol/exasol-driver-go/pkg/proxy"
)

// ImportStatement is one local import in progress: the proxy connection that
// carries the file, the format that decides how the bytes move over it, and the
// fingerprint the server needs to accept the encrypted connection. It owns the
// file and the lifecycle of the transfer, while the proxy owns the transport.
type ImportStatement struct {
	query           string
	format          utils.ImportFormat
	parquetFile     *os.File
	parquetFileSize int64
	fingerprint     string
	proxy           *proxy.Proxy
}

// NewImportStatement opens the proxy connection a local import runs over and
// leaves it ready for the transfer, without moving a single byte of the file.
//
// A Parquet file is opened here, before the connection is dialled, so a caller
// who names a file that does not exist is told so directly. The open file is
// retained and streamed during Transfer; its contents are not loaded into
// memory.
//
// The format is supplied rather than derived, because the caller has already
// classified the statement to decide whether the server supports it at all.
// A Parquet statement naming more than one file is refused here as well as by
// Connection.exec, because serving the first file and dropping the rest would
// be a silent partial import and no caller may opt out of that check.
func NewImportStatement(query string, format utils.ImportFormat, cfg *config.Config) (*ImportStatement, error) {
	statement := &ImportStatement{query: query, format: format}
	if format == utils.ImportFormatParquet {
		file, size, err := openImportFile(query)
		if err != nil {
			return nil, err
		}
		statement.parquetFile = file
		statement.parquetFileSize = size
	}

	connection, err := createProxy(cfg.Host, cfg.Port)
	if err != nil {
		statement.closeParquetFile()
		return nil, err
	}
	if err := connection.StartProxy(); err != nil {
		connection.Close()
		statement.closeParquetFile()
		return nil, err
	}
	if cfg.LocalImportEncryption {
		fingerprint, err := connection.EnableTLS()
		if err != nil {
			connection.Close()
			statement.closeParquetFile()
			return nil, err
		}
		statement.fingerprint = fingerprint
	}

	statement.proxy = connection
	return statement, nil
}

// openImportFile opens the single file a Parquet import names and captures its
// size without reading its contents. Keeping the descriptor open lets Transfer
// stream the same file that was validated here.
//
// Serving only the first of several named files would be a silent partial
// import, so the count is refused here rather than assumed: opening the first
// path is safe only once no other path exists.
func openImportFile(query string) (*os.File, int64, error) {
	paths, err := utils.GetFilePaths(query)
	if err != nil {
		return nil, 0, err
	}
	if len(paths) > 1 {
		return nil, 0, errors.NewParquetImportMultipleFiles(len(paths))
	}
	file, err := utils.OpenFile(paths[0])
	if err != nil {
		return nil, 0, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, 0, errors.NewFileNotFound(paths[0])
	}
	return file, info.Size(), nil
}

func createProxy(host string, port int) (*proxy.Proxy, error) {
	hosts, err := utils.ResolveHosts(host)
	if err != nil {
		return nil, err
	}
	utils.ShuffleHosts(hosts)
	return proxy.NewProxy(hosts, port)
}

// GetUpdatedQuery is the statement the server actually receives: the local file
// clause replaced by the address of the proxy connection, and the fingerprint of
// that connection's key when it is encrypted, which is the only way the server
// learns to expect TLS there.
func (i *ImportStatement) GetUpdatedQuery() string {
	return utils.UpdateImportQuery(i.query, utils.ProxyTarget{
		Host:        i.proxy.Host,
		Port:        i.proxy.Port,
		Fingerprint: i.fingerprint,
	})
}

func (i *ImportStatement) Close() {
	if i.proxy != nil {
		i.proxy.Close()
	}
	i.closeParquetFile()
}

func (i *ImportStatement) closeParquetFile() {
	if i.parquetFile != nil {
		i.parquetFile.Close()
		i.parquetFile = nil
	}
}

// Transfer moves the file over the proxy connection and returns once the server
// has taken what it asked for. It must run concurrently with the rewritten
// statement, because neither side of an encrypted connection can start the
// handshake before the server has read the statement's fingerprint.
//
// Cancelling ctx closes the proxy connection, and that is the only way to end a
// transfer already parked on the wire. A Parquet transfer waits in a read only
// the server can satisfy, and an encrypted transfer of either format waits in
// the handshake its first write triggers; neither position can observe a
// context, so both are freed by taking the connection away from them. The close
// is joined before returning, so no goroutine of this transfer outlives it.
func (i *ImportStatement) Transfer(ctx context.Context) error {
	closed := make(chan struct{})
	stopClosingOnCancel := context.AfterFunc(ctx, func() {
		defer close(closed)
		i.proxy.Close()
	})
	defer func() {
		if !stopClosingOnCancel() {
			<-closed
		}
	}()

	if i.format == utils.ImportFormatParquet {
		return i.proxy.ServeReaderRanges(ctx, i.parquetFile, i.parquetFileSize)
	}
	return i.pushFiles(ctx)
}

func (i *ImportStatement) pushFiles(ctx context.Context) error {
	paths, err := utils.GetFilePaths(i.query)
	if err != nil {
		return err
	}

	var files []*os.File
	for _, path := range paths {
		file, err := utils.OpenFile(path)
		if err != nil {
			return err
		}
		files = append(files, file)
	}

	return i.proxy.Write(ctx, files, utils.GetRowSeparator(i.query))
}
