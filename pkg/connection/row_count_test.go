package connection

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLastInsertId(t *testing.T) {
	rows := RowCount{10}
	id, err := rows.LastInsertId()
	assert.Equal(t, int64(0), id)
	assert.EqualError(t, err, "E-EGOD-6: no LastInsertId available")
}

func TestRowsAffected(t *testing.T) {
	rows := RowCount{10}
	affectedRows, err := rows.RowsAffected()
	assert.Equal(t, int64(10), affectedRows)
	assert.NoError(t, err)
}
