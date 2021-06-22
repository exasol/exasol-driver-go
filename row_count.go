package exasol

type rowCount struct {
	affectedRows int64
}

func (res *rowCount) LastInsertId() (int64, error) {
	return 0, ErrNoLastInsertID
}

func (res *rowCount) RowsAffected() (int64, error) {
	return res.affectedRows, nil
}
