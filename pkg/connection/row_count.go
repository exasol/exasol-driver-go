package connection

import "github.com/exasol/exasol-driver-go/pkg/errors"

type RowCount struct {
	affectedRows int64
}

func (res *RowCount) LastInsertId() (int64, error) {
	return 0, errors.ErrNoLastInsertID
}

func (res *RowCount) RowsAffected() (int64, error) {
	return res.affectedRows, nil
}
