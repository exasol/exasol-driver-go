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
	physical parquet.Kind
	size     int64
	expected string
	err      string
}{
	{parquet.Int32, 0, intColumn(decimalPrecisionInt32), ""},
	{parquet.Int32, 1, intColumn(decimalPrecisionInt32), ""},
	{parquet.Int64, 0, intColumn(decimalPrecisionInt64), ""},
	{parquet.Int64, 1, intColumn(decimalPrecisionInt64), ""},
	{parquet.Int96, 0, "TIMESTAMP(3)", ""},
	{parquet.Int96, 1, "TIMESTAMP(3)", ""},
	{parquet.Boolean, 0, "BOOLEAN", ""},
	{parquet.Boolean, 1, "BOOLEAN", ""},
	{parquet.ByteArray, 0, varcharColumn(maxVarcharLength), ""},
	{parquet.ByteArray, 1, varcharColumn(maxVarcharLength), ""},
	{parquet.FixedLenByteArray, 1, varcharColumn(1), ""},
	{parquet.FixedLenByteArray, 123, varcharColumn(123), ""},
	{parquet.FixedLenByteArray, maxVarcharLength + 1, "", "exceeds supported maxiumum"},
	{parquet.Float, 0, "DOUBLE PRECISION", ""},
	{parquet.Float, 1, "DOUBLE PRECISION", ""},
	{parquet.Double, 0, "DOUBLE PRECISION", ""},
	{parquet.Double, 1, "DOUBLE PRECISION", ""},
	{33, 0, "", "unsupported Parquet physical data type"},
}

func TestMapPhysicalType(t *testing.T) {
	for _, tt := range mapPhysicalTypeTests {
		name := fmt.Sprintf("%s_%d", tt.physical, tt.size)
		t.Run(name, func(t *testing.T) {
			result, err := mapPhysicalType(tt.physical, tt.size)
			if tt.err == "" {
				assert.NoError(t, err, "mapPhysicalType failed unexpectedly")
				assert.Equal(t, result, tt.expected)
			} else {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.err)
			}
		})
	}
}
