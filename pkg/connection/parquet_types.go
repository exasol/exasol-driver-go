package connection

import (
	"fmt"
	"math"
	"strings"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

const (
	decimalPrecisionInt32 = 10
	decimalPrecisionInt64 = 19
	maxVarcharLength      = 2000000
	doublePrecisionColumn = "DOUBLE PRECISION"
	maxDecimalPrecision   = 36
	timestamp9Column      = "TIMESTAMP(9)"
)

// numberOfDigits returns the number of decimal digits required to represent
// an unsigned integer with the specified number of bits.
//
// The return value can be used to compute the size for SQL data type DECIMAL
// as stored in the constants above.
//
// See https://parquet.apache.org/docs/file-format/types/logicaltypes/
func numberOfDigits(bitCount int) (digits int) {
	return int(math.Ceil(float64(bitCount) * math.Ln2 / math.Ln10))
}

// numberOfDigitsSigned returns the number of decimal digits required to
// represent a signed integer with the specified number of bits.
//
// For signed integers we can ignore one bit representing the +/- sign as the
// SQL data type stores the sign separately.
func numberOfDigitsSigned(bitCount int) (digits int) {
	return numberOfDigits(bitCount - 1)
}

func intColumn(precision int64) string {
	return fmt.Sprintf("DECIMAL(%d,0)", precision)
}

func varcharColumn(length int64) (result string) {
	return fmt.Sprintf("VARCHAR(%d) CHARACTER SET UTF8", length)
}

// precisionAndScale returns the values of the precision and scale of the
// logical DecimalType and an appropriate error in case one the values does
// not comply to the constraints of Exasol's DECIMAL data type.
func precisionAndScale(decimal *format.DecimalType) (precision int, scale int, err error) {
	var msg string
	precision = int(decimal.Precision)
	scale = int(decimal.Scale)
	p, s := precision, scale
	if p > maxDecimalPrecision || p < 1 {
		msg = fmt.Sprintf("precision %d", p)
	} else if s > p {
		msg = fmt.Sprintf("scale %d > precision %d", s, p)
	} else if s > maxDecimalPrecision {
		msg = fmt.Sprintf("scale %d", s)
	}
	if msg != "" {
		err = fmt.Errorf("unsupported %s for logical DecimalType", msg)
	}
	return
}

// mapLogicalType maps the specified Parquet logical type to a string
// containing the resp. Exasol SQL data type declaration.
func mapLogicalType(logical format.LogicalTypeValue) (result string, err error) {
	switch casted := logical.(type) {
	case *format.UUIDType:
		result = "STRING"
	case *format.StringType:
		result = "STRING"
	case *format.DecimalType:
		p, s, err := precisionAndScale(casted)
		if err != nil {
			return "", err
		}
		result = fmt.Sprintf("DECIMAL(%d,%d)", p, s)
	case *format.IntType:
		bitCount := int(casted.BitWidth)
		if casted.IsSigned {
			bitCount -= 1
		}
		precision := numberOfDigits(bitCount)
		if precision > maxDecimalPrecision {
			return "", fmt.Errorf("logical IntType with %d bits "+
				"requires DECIMAL precision of %d, "+
				"exceeding the supported maximum of %d",
				casted.BitWidth, precision, maxDecimalPrecision)
		}
		result = fmt.Sprintf("DECIMAL(%d,%d)", precision, 0)
	case *format.DateType:
		result = timestamp9Column
	case *format.TimeType:
		result = timestamp9Column
	case *format.TimestampType:
		result = timestamp9Column
	case *format.Float16Type:
		result = "DOUBLE PRECISION"
	// logical types known to be unsupported:
	//
	// - MapType
	// - ListType
	// - EnumType
	// - NullType
	// - JsonType
	// - BsonType
	// - GeometryType
	// - GeographyType
	default:
		return "", fmt.Errorf("unsupported logical type %T", logical)
	}
	return
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
		result = timestamp9Column
	case parquet.Boolean:
		result = "BOOLEAN"
	case parquet.ByteArray:
		result = varcharColumn(maxVarcharLength)
	case parquet.FixedLenByteArray:
		if size > maxVarcharLength {
			err = fmt.Errorf(
				"size %d of parquet.FixedLenByteArray exceeds supported maximum of %d",
				size, maxVarcharLength)
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

// createTableStatement returns the SQL statement to create a table based on
// the column definitions in parameter columns.
func createTableStatement(tableFqn string, columns []parquetColumn) (result string, err error) {
	if len(columns) < 1 {
		return "", fmt.Errorf("empty list of columns is not supported")
	}
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
