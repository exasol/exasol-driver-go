package exasol

import (
	"github.com/stretchr/testify/suite"
	"testing"
)

type TransactionTestSuite struct {
	suite.Suite
}

func TestTransactionSuite(t *testing.T) {
	suite.Run(t, new(TransactionTestSuite))
}

func (suite *TransactionTestSuite) TestCommitWithEmptyConnection() {
	transaction := transaction{nil}
	suite.EqualError(transaction.Commit(), "E-EGOD-1: invalid connection")
}

func (suite *TransactionTestSuite) TestRollbackWithEmptyConnection() {
	transaction := transaction{nil}
	suite.EqualError(transaction.Rollback(), "E-EGOD-1: invalid connection")
}

func (suite *TransactionTestSuite) TestCommitWithClosedConnection() {
	connection := connection{isClosed: true}
	transaction := transaction{connection: &connection}
	suite.EqualError(transaction.Commit(), "driver: bad connection")
}

func (suite *TransactionTestSuite) TestRollbackWithClosedConnection() {
	connection := connection{isClosed: true}
	transaction := transaction{connection: &connection}
	suite.EqualError(transaction.Rollback(), "driver: bad connection")
}
