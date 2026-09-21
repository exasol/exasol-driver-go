package connection

import (
	"math"
)

const (
	DecimalSizeInt32 = 10
	DecimalSizeInt64 = 19
	VarCharLength = 2000000
)

// Return the number of decimal digits required to represent a signed integer
// with the specified number of bits.
//
// The return value can be used to compute the size for SQL data type DECIMAL
// as stored in the constants above.
func numberOfDigits(numberOfBits int) (digits int) {
	n := numberOfBits - 1
	return int(math.Ceil(float64(n) * math.Ln2 / math.Ln10))
}
