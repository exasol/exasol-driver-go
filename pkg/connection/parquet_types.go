package connection

import (
	"fmt"
	"math"

	"github.com/parquet-go/parquet-go"
)

const (
	decimalPrecisionInt32 = 10
	decimalPrecisionInt64 = 19
	maxVarcharLength    = 2000000
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


type columnType struct {
	Name string
	Size int64
}

func intColumn(scale int64) (result columnType) {
	return columnType{
		Name: "DECIMAL",
		Size: scale,
	}
}


func varcharColumn(length int64) (result columnType) {
	return columnType{
		Name: "VARCHAR",
		Size: length,
	}
}

// See https://github.com/exasol/parquet-edml-generator/blob/main/src/main/
// java/com/exasol/edmlgenerator/parquet/converter/
// ParquetColumnToMappingDefinitionConverter.java#L62
func mapPhysicalType(physical parquet.Kind, size int64) (result columnType, err error) {
	switch physical {
	case parquet.Int32:
		result = intColumn(decimalPrecisionInt32)
	case parquet.Int64:
		result = intColumn(decimalPrecisionInt64)
	case parquet.Int96:
		result = columnType{Name: "TIMESTAMP"}
	case parquet.Boolean:
		result = columnType{Name: "BOOLEAN"}
	case parquet.ByteArray:
		result = varcharColumn(maxVarcharLength)
	case parquet.FixedLenByteArray:
		// leaf.Node.Type().Length()
		if size > maxVarcharLength {
			err = fmt.Errorf(
				"Size of parquet.FixedLenByteArray" +
					" exceeds supported maxiumum of %d",
				maxVarcharLength)
		} else {
			result = varcharColumn(size)
		}
	case parquet.Float:
		fallthrough
	case parquet.Double:
		// https://parquet.apache.org/docs/file-format/types/
		// FLOAT: IEEE 32-bit floating point values
		// DOUBLE: IEEE 64-bit floating point values
		result = columnType{Name: "DOUBLE PRECISION"}
	default:
		err = fmt.Errorf("Unsupported Parquet physical data type %s", physical)
	}
	return
}
