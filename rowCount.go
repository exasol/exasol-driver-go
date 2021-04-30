package exasol

type rowCount struct {
	affectedRows int64
}

func (res *rowCount) LastInsertId() (int64, error) {
	panic("not implemented")
}

func (res *rowCount) RowsAffected() (int64, error) {
	return res.affectedRows, nil
}
