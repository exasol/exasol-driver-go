package wsconn

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/exasol/exasol-driver-go/pkg/logger"
	"github.com/stretchr/testify/suite"
)

type WebsocketTestSuite struct {
	suite.Suite
}

func TestWebsocketSuite(t *testing.T) {
	suite.Run(t, new(WebsocketTestSuite))
}

func (suite *WebsocketTestSuite) TestVerifyPeerCertificate() {
	const errorMsgNoCertificate = "E-EGOD-9: server did not return certificates"
	const noFingerprint = ""
	for i, testCase := range []struct {
		certificate   [][]byte
		fingerprint   string
		expectedError string
	}{
		// Fingerprint configured
		{nil, "expectedFingerprint", errorMsgNoCertificate},
		{[][]byte{}, "expectedFingerprint", errorMsgNoCertificate},
		{[][]byte{[]byte("")}, "expectedFingerprint", "E-EGOD-10: the server's certificate fingerprint 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' does not match the expected fingerprint 'expectedFingerprint'"},
		{[][]byte{[]byte("certificateContent\n")}, "expectedFingerprint", "E-EGOD-10: the server's certificate fingerprint '77805314a4b617393d25bd7cf660963b4d41eee11381b1c5bab30db30710b416' does not match the expected fingerprint 'expectedFingerprint'"},
		{[][]byte{[]byte("certificateContent\n")}, "77805314a4b617393d25bd7cf660963b4d41eee11381b1c5bab30db30710b416", ""},
		{[][]byte{[]byte("certificateContent\n")}, "77805314A4B617393D25BD7CF660963B4D41EEE11381B1C5BAB30DB30710B416", ""},
		// No fingerprint configured
		{nil, noFingerprint, ""},
		{[][]byte{}, noFingerprint, ""},
		{[][]byte{[]byte("")}, noFingerprint, ""},
		{[][]byte{[]byte("certificateContent\n")}, noFingerprint, ""},
	} {
		suite.Run(fmt.Sprintf("Test %v: rawCerts=%q expectedFingerprint=%q", i, testCase.certificate, testCase.fingerprint), func() {
			verifier := certificateVerifier(testCase.fingerprint)
			err := verifier(testCase.certificate, nil)
			if testCase.expectedError == "" {
				suite.NoError(err)
			} else {
				suite.EqualError(err, testCase.expectedError)
			}
		})
	}
}

func (suite *WebsocketTestSuite) TestBytesToHexString() {
	for i, testCase := range []struct {
		data        []byte
		expectedHex string
	}{
		{nil, ""},
		{[]byte{}, ""},
		{[]byte{0}, "00"},
		{[]byte{1}, "01"},
		{[]byte{15}, "0f"},
		{[]byte{16}, "10"},
		{[]byte{255}, "ff"},
		{[]byte{0, 0}, "0000"},
		{[]byte{0, 1}, "0001"},
		{[]byte{255, 255}, "ffff"},
	} {
		suite.Run(fmt.Sprintf("Test %v: data=%q expectedHex=%q", i, testCase.data, testCase.expectedHex), func() {
			suite.Equal(testCase.expectedHex, bytesToHexString(testCase.data))
		})
	}
}

func (suite *WebsocketTestSuite) TestSerializesConcurrentWebsocketAccess() {
	for _, testCase := range []struct {
		name            string
		call            func(*wsConnImpl)
		expectedLogLine string
	}{
		{
			name: "reads",
			call: func(connection *wsConnImpl) {
				_, _, _ = connection.ReadMessage()
			},
			expectedLogLine: "Concurrent WebSocket reads detected; serializing access",
		},
		{
			name: "writes",
			call: func(connection *wsConnImpl) {
				_ = connection.WriteMessage(1, nil)
			},
			expectedLogLine: "Concurrent WebSocket writes detected; serializing access",
		},
	} {
		suite.Run(testCase.name, func() {
			traceLogger := newRecordingLogger()
			previousTraceLogger := logger.TraceLogger
			logger.SetTraceLogger(traceLogger)
			defer logger.SetTraceLogger(previousTraceLogger)

			socket := newBlockingSocket()
			connection := &wsConnImpl{socket: socket}
			var calls sync.WaitGroup
			calls.Add(2)
			go func() {
				defer calls.Done()
				testCase.call(connection)
			}()
			<-socket.started
			go func() {
				defer calls.Done()
				testCase.call(connection)
			}()

			suite.Equal(testCase.expectedLogLine, <-traceLogger.messages)
			close(socket.release)
			calls.Wait()
			suite.Equal(int32(1), socket.maximumConcurrentCalls.Load())
			select {
			case unexpected := <-traceLogger.messages:
				suite.Failf("unexpected trace message", "%q", unexpected)
			default:
			}
		})
	}
}

type blockingSocket struct {
	started                chan struct{}
	release                chan struct{}
	startedOnce            sync.Once
	activeCalls            atomic.Int32
	maximumConcurrentCalls atomic.Int32
}

func newBlockingSocket() *blockingSocket {
	return &blockingSocket{started: make(chan struct{}), release: make(chan struct{})}
}

func (socket *blockingSocket) EnableWriteCompression(bool) {
	// This is a no-op for the blockingSocket used in tests.
}

func (socket *blockingSocket) WriteMessage(int, []byte) error {
	socket.block()
	return nil
}

func (socket *blockingSocket) ReadMessage() (int, []byte, error) {
	socket.block()
	return 0, nil, nil
}

func (socket *blockingSocket) Close() error {
	return nil
}

func (socket *blockingSocket) block() {
	activeCalls := socket.activeCalls.Add(1)
	for maximum := socket.maximumConcurrentCalls.Load(); activeCalls > maximum; {
		if socket.maximumConcurrentCalls.CompareAndSwap(maximum, activeCalls) {
			break
		}
		maximum = socket.maximumConcurrentCalls.Load()
	}
	socket.startedOnce.Do(func() { close(socket.started) })
	<-socket.release
	socket.activeCalls.Add(-1)
}

type recordingLogger struct {
	messages chan string
}

func newRecordingLogger() *recordingLogger {
	return &recordingLogger{messages: make(chan string, 2)}
}

func (logger *recordingLogger) Print(v ...interface{}) {
	logger.messages <- fmt.Sprint(v...)
}

func (logger *recordingLogger) Printf(format string, v ...interface{}) {
	logger.messages <- fmt.Sprintf(format, v...)
}
