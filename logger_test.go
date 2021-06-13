package exasol

import (
	"bytes"
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
	SetLogger(logger)
	errorLogger.Print("test")

	// check result
	if actual := buffer.String(); actual != expected {
		t.Errorf("expected %q, got %q", expected, actual)
	}
}
