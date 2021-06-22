package exasol

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLastInsertId(t *testing.T) {
	rows := rowCount{10}
	id, err := rows.LastInsertId()
	assert.Equal(t, int64(0), id)
	assert.EqualError(t, err, "no LastInsertId available")
}

func TestRowsAffected(t *testing.T) {
	rows := rowCount{10}
	affectedRows, err := rows.RowsAffected()
	assert.Equal(t, int64(10), affectedRows)
	assert.NoError(t, err)
}
