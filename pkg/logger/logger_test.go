package logger

import (
	"bytes"
	"fmt"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrorsSetLogger(t *testing.T) {
	previous := ErrorLogger
	defer func() {
		ErrorLogger = previous
	}()

	// set up logger
	const expected = "prefix: test\n"
	buffer := bytes.NewBuffer(make([]byte, 0, 64))
	logger := log.New(buffer, "prefix: ", 0)

	// print
	_ = SetLogger(logger)
	ErrorLogger.Print("test")

	// check result
	if actual := buffer.String(); actual != expected {
		t.Errorf("expected %q, got %q", expected, actual)
	}
}

func TestLoggerIsNil(t *testing.T) {
	assert.EqualError(t, SetLogger(nil), "E-EGOD-8: logger is nil")
}

func TestSetTraceLogger(t *testing.T) {
	// set up logger
	buffer := bytes.NewBuffer(make([]byte, 0, 64))
	logger := log.New(buffer, "prefix: ", 0)

	SetTraceLogger(logger)
	defer SetTraceLogger(nil)
	TraceLogger.Print("test")
	const expected = "prefix: test\n"
	if actual := buffer.String(); actual != expected {
		t.Errorf("expected %q, got %q", expected, actual)
	}
}

func TestDefaultTraceLogger(t *testing.T) {
	TraceLogger.Print("ignored")
	TraceLogger.Print("ignored %s", "arg")
}

type loggerMock struct {
	messages []string
}

func (m *loggerMock) Print(v ...interface{}) { /* no-op */ }
func (m *loggerMock) Printf(format string, v ...interface{}) {
	m.messages = append(m.messages, fmt.Sprintf(format, v...))
}

func TestX1(t *testing.T) {
	mock := loggerMock{messages: make([]string, 0)}
	TraceLogger = &mock
	TraceLogger.Printf("Hello")
	fmt.Printf("Saved Messages: %s\n", mock.messages[0])
}
