package connection

import (
	"context"
	"database/sql/driver"

	"github.com/exasol/exasol-driver-go/pkg/errors"
	"github.com/exasol/exasol-driver-go/pkg/logger"
)

type Transaction struct {
	ctx        context.Context
	connection *Connection
}

func NewTransaction(ctx context.Context, connection *Connection) *Transaction {
	return &Transaction{ctx: ctx, connection: connection}
}

func (t *Transaction) Commit() error {
	if t.connection == nil {
		return errors.ErrInvalidConn
	}
	if t.connection.IsClosed {
		logger.ErrorLogger.Print(errors.ErrClosed)
		return driver.ErrBadConn
	}
	_, err := t.connection.SimpleExec(t.ctx, "COMMIT")
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
	_, err := t.connection.SimpleExec(t.ctx, "ROLLBACK")
	t.connection = nil
	return err
}
