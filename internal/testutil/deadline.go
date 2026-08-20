package testutil

import (
	"testing"
	"time"
)

// RunWithDeadline runs operation asynchronously and returns its result. It
// fails the test if operation does not finish before deadline.
func RunWithDeadline[T any](t testing.TB, deadline time.Duration, operation func() (T, error), timeoutFormat string, timeoutArgs ...any) (T, error) {
	t.Helper()
	type outcome struct {
		value T
		err   error
	}
	completed := make(chan outcome, 1)
	go func() {
		value, err := operation()
		completed <- outcome{value: value, err: err}
	}()

	select {
	case result := <-completed:
		return result.value, result.err
	case <-time.After(deadline):
		var zero T
		t.Fatalf(timeoutFormat, timeoutArgs...)
		return zero, nil
	}
}
