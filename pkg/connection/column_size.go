package connection

import (
	"math"
)

const (
	decimalSizeInt32 = 10
	decimalSizeInt64 = 19
	varCharLength    = 2000000
)

// Return the number of decimal digits required to represent a signed integer
// with the specified number of bits.
//
// The return value can be used to compute the size for SQL data type DECIMAL
// as stored in the constants above.
func numberOfDigits(bitCount int) (digits int) {
	// The number of digits in the DECIMAL's scale can ignore one bit
	// representing the +/- sign of the signed integer as the SQL data type
	// stores the sign separately.
	n := bitCount - 1
	return int(math.Ceil(float64(n) * math.Ln2 / math.Ln10))
}
