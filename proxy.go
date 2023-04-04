package exasol

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
)

type proxy struct {
	isClosed   bool
	connection io.ReadWriteCloser
	Host       string
	Port       uint32
}

var magicWords = []interface{}{uint32(0x02212102), uint32(1), uint32(1)}

func newProxy(hosts []string, port int) (*proxy, error) {
	var wrappedErr error
	for _, host := range hosts {
		uri := fmt.Sprintf("%s:%d", host, port)
		con, err := net.Dial("tcp", uri)
		if err == nil {
			p := &proxy{
				connection: con,
				isClosed:   false,
			}

			return p, nil
		} else {
			wrappedErr = fmt.Errorf("%w: could not create TCP connection to %s, %s", ErrInvalidProxyConn, uri, err.Error())
			errorLogger.Print(wrappedErr)
		}
	}
	return nil, wrappedErr
}

func (p *proxy) startProxy() error {
	for _, word := range magicWords {
		err := binary.Write(p.connection, binary.LittleEndian, word)
		if err != nil {
			wrappedErr := fmt.Errorf("%w: could not send magic words, %s", ErrInvalidProxyConn, err)
			errorLogger.Print(wrappedErr)
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
		wrappedErr := fmt.Errorf("%w: could not read from TCP connection to get internal host and port, %s", ErrInvalidProxyConn, err.Error())
		errorLogger.Print(wrappedErr)
		return wrappedErr
	}

	p.Port = result.Port
	p.Host = string(bytes.Trim(result.Host[:], "\x00"))

	return nil
}

func (p *proxy) write(ctx context.Context, files []*os.File, rowSeparator string) error {
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
		err = p.sendFile(ctx, file, rowSeparator, chunkedWriter)
		if err != nil {
			return err
		}
	}
	_, err = p.connection.Write([]byte("0\r\n\r\n")) // A final zero chunk
	return err
}

func (p *proxy) sendFile(ctx context.Context, file *os.File, rowSeparator string, chunkedWriter io.WriteCloser) error {
	reader := bufio.NewReader(file)

	for {
		if ctx.Err() != nil {
			p.close()
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

func (p *proxy) sendHeaders(headers []string) error {
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

func (p *proxy) close() {
	if p.isClosed {
		return
	}

	p.connection.Close()
	p.isClosed = true
}
