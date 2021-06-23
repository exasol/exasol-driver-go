package exasol

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
)

func TestErrorsSetLogger(t *testing.T) {
	previous := errorLogger
	defer func() {
		errorLogger = previous
	}()

	// set up logger
	const expected = "prefix: test\n"
	buffer := bytes.NewBuffer(make([]byte, 0, 64))
	logger := log.New(buffer, "prefix: ", 0)

	// print
	_ = SetLogger(logger)
	errorLogger.Print("test")

	// check result
	if actual := buffer.String(); actual != expected {
		t.Errorf("expected %q, got %q", expected, actual)
	}
}

func TestLoggerIsNil(t *testing.T) {
	assert.EqualError(t, SetLogger(nil), "logger is nil")
}
