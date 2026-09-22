package connection

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/parquet-go/parquet-go"
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
	err      string
	expected columnType
}{
	{parquet.Int32, 0, "", intColumn(decimalPrecisionInt32)},
	{parquet.Int64, 0, "", intColumn(decimalPrecisionInt64)},
	{parquet.Int96, 0, "", columnType{Name: "TIMESTAMP"}},
	{parquet.Boolean, 0, "", columnType{Name: "BOOLEAN"}},
	{parquet.ByteArray, 0, "", varcharColumn(maxVarcharLength)},
	{parquet.FixedLenByteArray, 1, "", varcharColumn(1)},
	{parquet.FixedLenByteArray, 123, "", varcharColumn(123)},
	{parquet.FixedLenByteArray, maxVarcharLength + 1,
		"exceeds supported maxiumum", columnType{}},
	{parquet.Float, 0, "", columnType{Name: "DOUBLE PRECISION"}},
	{parquet.Double, 0, "", columnType{Name: "DOUBLE PRECISION"}},
	{33, 0, "Unsupported Parquet physical data type", columnType{}},
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
