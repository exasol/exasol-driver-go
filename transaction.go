package exasol

import (
	"context"
	"database/sql/driver"
)

type transaction struct {
	connection *connection
}

func (t *transaction) Commit() error {
	if t.connection.isClosed {
		errorLogger.Print(ErrClosed)
		return driver.ErrBadConn
	}
	if t.connection == nil {
		return ErrInvalidConn
	}
	_, err := t.connection.simpleExec(context.Background(), "COMMIT")
	t.connection = nil
	return err
}

func (t *transaction) Rollback() error {
	if t.connection.isClosed {
		errorLogger.Print(ErrClosed)
		return driver.ErrBadConn
	}
	if t.connection == nil {
		return ErrInvalidConn
	}
	_, err := t.connection.simpleExec(context.Background(), "ROLLBACK")
	t.connection = nil
	return err
}
