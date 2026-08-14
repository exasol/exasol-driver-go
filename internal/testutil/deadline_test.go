package testutil

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRunWithDeadlineReturnsOperationResult(t *testing.T) {
	result, err := RunWithDeadline(t, time.Second, func() (string, error) {
		return "result", nil
	}, "operation timed out")

	assert.Equal(t, "result", result)
	assert.NoError(t, err)
}

func TestRunWithDeadlineReturnsOperationError(t *testing.T) {
	expectedError := errors.New("expected")
	result, err := RunWithDeadline(t, time.Second, func() (string, error) {
		return "", expectedError
	}, "operation timed out")

	assert.Empty(t, result)
	assert.ErrorIs(t, err, expectedError)
}
