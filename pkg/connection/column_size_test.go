package connection

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestColumnSizes(t *testing.T) {
	assert.Equal(t, numberOfDigits(32), DecimalSizeInt32)
	assert.Equal(t, numberOfDigits(64), DecimalSizeInt64)
	assert.Equal(t, 2000000, VarCharLength)
}
