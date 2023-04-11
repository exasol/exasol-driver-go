package connection

import (
	"context"
	"database/sql/driver"

	"github.com/exasol/exasol-driver-go/pkg/errors"
	"github.com/exasol/exasol-driver-go/pkg/logger"
)

type Transaction struct {
	connection *Connection
}

func NewTransaction(connection *Connection) *Transaction {
	return &Transaction{connection: connection}
}

func (t *Transaction) Commit() error {
	if t.connection == nil {
		return errors.ErrInvalidConn
	}
	if t.connection.IsClosed {
		logger.ErrorLogger.Print(errors.ErrClosed)
		return driver.ErrBadConn
	}
	_, err := t.connection.SimpleExec(context.Background(), "COMMIT")
	t.connection = nil
	return err
}

func (t *Transaction) Rollback() error {
	if t.connection == nil {
		return errors.ErrInvalidConn
	}
	if t.connection.IsClosed {
		logger.ErrorLogger.Print(errors.ErrClosed)
		return driver.ErrBadConn
	}
	_, err := t.connection.SimpleExec(context.Background(), "ROLLBACK")
	t.connection = nil
	return err
}
