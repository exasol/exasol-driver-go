package connection

import (
	"fmt"
	"math"

	"github.com/parquet-go/parquet-go"
)

const (
	decimalPrecisionInt32 = 10
	decimalPrecisionInt64 = 19
	maxVarcharLength      = 2000000
	doublePrecisionColumn = "DOUBLE PRECISION"
)

// numberOfDigits returns the number of decimal digits required to represent a
// signed integer with the specified number of bits.
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

func intColumn(precision int64) (string) {
	return fmt.Sprintf("DECIMAL(%d,0)", precision)
}

func varcharColumn(length int64) (result string) {
	return fmt.Sprintf("VARCHAR(%d) CHARACTER SET UTF8", length)
}

// mapPhysicalType maps the physical parquet.Kind and size as optained by
// leaf.Node.Type().Length() from the Parquet file schema columns to a string
// containing the resp. Exasol SQL data type declaration.
//
// See also https://parquet.apache.org/docs/file-format/types/
func mapPhysicalType(physical parquet.Kind, size int64) (result string, err error) {
	switch physical {
	case parquet.Int32:
		result = intColumn(decimalPrecisionInt32)
	case parquet.Int64:
		result = intColumn(decimalPrecisionInt64)
	case parquet.Int96:
		result = "TIMESTAMP(9)"
	case parquet.Boolean:
		result = "BOOLEAN"
	case parquet.ByteArray:
		result = varcharColumn(maxVarcharLength)
	case parquet.FixedLenByteArray:
		if size > maxVarcharLength {
			err = fmt.Errorf(
				"size of parquet.FixedLenByteArray exceeds supported maxiumum of %d",
				maxVarcharLength)
		} else {
			result = varcharColumn(size)
		}
	case parquet.Float:
		fallthrough
	case parquet.Double:
		result = doublePrecisionColumn
	default:
		err = fmt.Errorf("unsupported Parquet physical data type %s", physical)
	}
	return
}
