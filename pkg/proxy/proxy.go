package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http/httputil"
	"os"

	"github.com/exasol/exasol-driver-go/pkg/errors"
	"github.com/exasol/exasol-driver-go/pkg/logger"
)

type Proxy struct {
	isClosed   bool
	connection io.ReadWriteCloser
	Host       string
	Port       int
}

var magicWords = []interface{}{uint32(0x02212102), uint32(1), uint32(1)}

func NewProxy(hosts []string, port int) (*Proxy, error) {
	var wrappedErr error
	for _, host := range hosts {
		uri := fmt.Sprintf("%s:%d", host, port)
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

	return nil
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
