package connection

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/suite"
)

type TransactionTestSuite struct {
	suite.Suite
}

func TestTransactionSuite(t *testing.T) {
	suite.Run(t, new(TransactionTestSuite))
}

func (suite *TransactionTestSuite) TestCommitWithEmptyConnection() {
	transaction := suite.createTransaction()
	transaction.connection = nil
	suite.EqualError(transaction.Commit(), "E-EGOD-1: invalid connection")
}

func (suite *TransactionTestSuite) TestRollbackWithEmptyConnection() {
	transaction := suite.createTransaction()
	transaction.connection = nil
	suite.EqualError(transaction.Rollback(), "E-EGOD-1: invalid connection")
}

func (suite *TransactionTestSuite) TestCommitWithClosedConnection() {
	transaction := suite.createTransaction()
	transaction.connection.IsClosed = true
	suite.EqualError(transaction.Commit(), driver.ErrBadConn.Error())
}

func (suite *TransactionTestSuite) TestRollbackWithClosedConnection() {
	transaction := suite.createTransaction()
	transaction.connection.IsClosed = true
	suite.EqualError(transaction.Rollback(), driver.ErrBadConn.Error())
}

func (suite *TransactionTestSuite) createTransaction() Transaction {
	connection := Connection{IsClosed: true}
	return Transaction{ctx: context.Background(), connection: &connection}
}
