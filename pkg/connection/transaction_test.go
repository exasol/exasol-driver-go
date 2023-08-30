package connection

import (
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
	transaction := Transaction{nil}
	suite.EqualError(transaction.Commit(), "E-EGOD-1: invalid connection")
}

func (suite *TransactionTestSuite) TestRollbackWithEmptyConnection() {
	transaction := Transaction{nil}
	suite.EqualError(transaction.Rollback(), "E-EGOD-1: invalid connection")
}

func (suite *TransactionTestSuite) TestCommitWithClosedConnection() {
	connection := Connection{IsClosed: true}
	transaction := Transaction{connection: &connection}
	suite.EqualError(transaction.Commit(), driver.ErrBadConn.Error())
}

func (suite *TransactionTestSuite) TestRollbackWithClosedConnection() {
	connection := Connection{IsClosed: true}
	transaction := Transaction{connection: &connection}
	suite.EqualError(transaction.Rollback(), driver.ErrBadConn.Error())
}
