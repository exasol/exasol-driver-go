package connection

import (
	"fmt"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
)

var columnSizeTests = []struct {
	in  int
	out int
}{
	{2, 1},
	{4, 1},
	{5, 2},
	{32, decimalPrecisionInt32},
	{64, decimalPrecisionInt64},
}

func TestColumnSizes(t *testing.T) {
	for _, tt := range columnSizeTests {
		name := fmt.Sprintf("digits_%d", tt.in)
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, numberOfDigits(tt.in), tt.out)
		})
	}
	assert.Equal(t, 2000000, maxVarcharLength)
}

var mapPhysicalTypeTests = []struct {
	physical      parquet.Kind
	size          int64
	expectedType  string
	expectedError string
}{
	{parquet.Int32, 0, intColumn(decimalPrecisionInt32), ""},
	{parquet.Int32, 1, intColumn(decimalPrecisionInt32), ""},
	{parquet.Int64, 0, intColumn(decimalPrecisionInt64), ""},
	{parquet.Int64, 1, intColumn(decimalPrecisionInt64), ""},
	{parquet.Int96, 0, "TIMESTAMP(9)", ""},
	{parquet.Int96, 1, "TIMESTAMP(9)", ""},
	{parquet.Boolean, 0, "BOOLEAN", ""},
	{parquet.Boolean, 1, "BOOLEAN", ""},
	{parquet.ByteArray, 0, varcharColumn(maxVarcharLength), ""},
	{parquet.ByteArray, 1, varcharColumn(maxVarcharLength), ""},
	{parquet.FixedLenByteArray, 1, varcharColumn(1), ""},
	{parquet.FixedLenByteArray, 123, varcharColumn(123), ""},
	{parquet.FixedLenByteArray, maxVarcharLength + 1, "", "exceeds supported maximum"},
	{parquet.Float, 0, doublePrecisionColumn, ""},
	{parquet.Float, 1, doublePrecisionColumn, ""},
	{parquet.Double, 0, doublePrecisionColumn, ""},
	{parquet.Double, 1, doublePrecisionColumn, ""},
	{33, 0, "", "unsupported Parquet physical data type"},
}

func TestMapPhysicalType(t *testing.T) {
	for _, tt := range mapPhysicalTypeTests {
		name := fmt.Sprintf("%s_%d", tt.physical, tt.size)
		t.Run(name, func(t *testing.T) {
			result, err := mapPhysicalType(tt.physical, tt.size)
			if tt.expectedError == "" {
				assert.NoError(t, err, "mapPhysicalType failed unexpectedly")
				assert.Equal(t, result, tt.expectedType)
			} else {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			}
		})
	}
}

var createTableStatementTests = []struct {
	name          string
	columns       []parquetColumn
	expected      string
	expectedError string
}{
	{
		"decimal_10", []parquetColumn{{[]string{"d1"}, parquet.Int32, 0}},
		"(\"d1\" DECIMAL(10,0))", "",
	},
	{
		"decimal_19", []parquetColumn{{[]string{"d2"}, parquet.Int64, 0}},
		"(\"d2\" DECIMAL(19,0))", "",
	},
	{
		"timestamp", []parquetColumn{{[]string{"ts"}, parquet.Int96, 0}},
		"(\"ts\" TIMESTAMP(9))", "",
	},
	{
		"boolean", []parquetColumn{{[]string{"b"}, parquet.Boolean, 0}},
		"(\"b\" BOOLEAN)", "",
	},
	{
		"varchar_max", []parquetColumn{{[]string{"v1"}, parquet.ByteArray, 0}},
		"(\"v1\" VARCHAR(2000000) CHARACTER SET UTF8)", "",
	},
	{
		"varchar_flex", []parquetColumn{{[]string{"v2"}, parquet.FixedLenByteArray, 123}},
		"(\"v2\" VARCHAR(123) CHARACTER SET UTF8)", "",
	},
	{
		"error_1", []parquetColumn{{[]string{"e1"}, parquet.FixedLenByteArray, maxVarcharLength + 1}},
		"", "exceeds supported maximum",
	},
	{
		"double_float", []parquetColumn{{[]string{"dp1"}, parquet.Float, 0}},
		"(\"dp1\" DOUBLE PRECISION)", "",
	},
	{
		"double_double", []parquetColumn{{[]string{"dp2"}, parquet.Double, 0}},
		"(\"dp2\" DOUBLE PRECISION)", "",
	},
	{
		"error_2", []parquetColumn{{[]string{"e1"}, 33, 0}},
		"", "unsupported Parquet physical data type",
	},
}

func TestCreateTableStatement(t *testing.T) {
	for _, tt := range createTableStatementTests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := CreateTableStatement("S.T", tt.columns)
			if tt.expectedError == "" {
				assert.NoError(t, err, "CreateTableStatement failed unexpectedly")
				expected := fmt.Sprintf("CREATE TABLE S.T %s", tt.expected)
				assert.Equal(t, result, expected)
			} else {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			}
		})
	}
}
