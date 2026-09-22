package connection

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestColumnSizes(t *testing.T) {
	assert.Equal(t, numberOfDigits(32), decimalSizeInt32)
	assert.Equal(t, numberOfDigits(64), decimalSizeInt64)
	assert.Equal(t, 2000000, varCharLength)
}
