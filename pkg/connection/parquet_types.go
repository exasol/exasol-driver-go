package connection

import (
	"fmt"
	"math"
	"strings"

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
//
// This function is only used internally and only for calculating the values
// for the constants named above and the related unit tests.
func numberOfDigits(bitCount int) (digits int) {
	// The number of digits in the DECIMAL's scale can ignore one bit
	// representing the +/- sign of the signed integer as the SQL data type
	// stores the sign separately.
	n := bitCount - 1
	return int(math.Ceil(float64(n) * math.Ln2 / math.Ln10))
}

func intColumn(precision int64) string {
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
				"size of parquet.FixedLenByteArray exceeds supported maximum of %d",
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

type parquetColumn struct {
	path   []string
	kind   parquet.Kind
	length int
}

// CreateTableStatement returns the SQL statement to create a table based on
// the column definitions in parameter columns.
func CreateTableStatement(tableFqn string, columns []parquetColumn) (result string, err error) {
	sql := make([]string, 0, len(columns))
	for _, col := range columns {
		sqlType, err := mapPhysicalType(col.kind, int64(col.length))
		if err != nil {
			return "", err
		}
		decl := fmt.Sprintf("%q %s", strings.Join(col.path, "_"), sqlType)
		sql = append(sql, decl)
	}
	result = fmt.Sprintf("CREATE TABLE %s (%s)", tableFqn, strings.Join(sql, ", "))
	return
}
