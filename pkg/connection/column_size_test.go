package connection

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

var flagtests = []struct {
    in  int
    out int
}{
	{2, 1},
	{4, 1},
	{5, 2},
	{32, decimalSizeInt32},
	{64, decimalSizeInt64},
}
func TestColumnSizes(t *testing.T) {
	for _, tt := range flagtests {
		name := fmt.Sprintf("digits_%d", tt.in)
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, numberOfDigits(tt.in), tt.out)
		})
	}
	assert.Equal(t, 2000000, varCharLength)
}
