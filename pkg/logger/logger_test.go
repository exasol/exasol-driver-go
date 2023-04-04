package logger

import (
	"bytes"
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
