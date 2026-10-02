package connection

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
	"github.com/stretchr/testify/assert"
)

func TestColumnSizes(t *testing.T) {
	for _, tt := range []struct {
		in  int
		out int
	}{
		{0, 0},
		{1, 0},
		{2, 1},
		{4, 1},
		{5, 2},
		{32, decimalPrecisionInt32},
		{64, decimalPrecisionInt64},
		{117, 35},
		{120, 36},
	} {
		name := fmt.Sprintf("digits_%d", tt.in)
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.out, numberOfDigitsSigned(tt.in))
		})
	}
	assert.Equal(t, 2000000, maxVarcharLength)
}

func TestMapLogicalType(t *testing.T) {
	for _, tt := range []struct {
		logical       format.LogicalTypeValue
		expected      string
		expectedError string
	}{
		{&format.StringType{}, maxVarcharColumn, ""},
		{&format.UUIDType{}, maxVarcharColumn, ""},
		{&format.ListType{}, maxVarcharColumn, ""},
		{&format.MapType{}, maxVarcharColumn, ""},
		{&format.DecimalType{Precision: 2, Scale: 1}, "DECIMAL(2,1)", ""},
		{&format.DecimalType{Precision: 1, Scale: 2}, "", "unsupported scale 2 > precision 1"},
		{&format.DecimalType{Precision: 37}, "", "unsupported precision 37"},
		{&format.DecimalType{Precision: 0}, "", "unsupported precision 0"},
		{&format.IntType{BitWidth: 8, IsSigned: true}, "DECIMAL(3,0)", ""},
		{&format.IntType{BitWidth: 123, IsSigned: true}, "",
			"IntType with 123 bits requires DECIMAL precision of 37," +
				" exceeding the supported maximum of 36"},
		{&format.DateType{}, timestamp9Column, ""},
		{&format.TimeType{}, timestamp9Column, ""},
		{&format.TimestampType{}, timestamp9Column, ""},
		{&format.Float16Type{}, "DOUBLE PRECISION", ""},
		{&format.NullType{}, "", "unsupported logical type"},
	} {
		name := fmt.Sprintf("%T", tt.logical)
		t.Run(name, func(t *testing.T) {
			result, err := mapLogicalType(tt.logical)
			if tt.expectedError == "" {
				assert.NoError(t, err, "mapLogicalType failed unexpectedly")
				assert.Equal(t, result, tt.expected)
			} else {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			}
		})
	}
}

const invalidPhysicalDataType = 33

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
	{parquet.Int96, 0, timestamp9Column, ""},
	{parquet.Int96, 1, timestamp9Column, ""},
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
	{invalidPhysicalDataType, 0, "", "unsupported Parquet physical data type"},
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

func TestCreateTableStatement(t *testing.T) {
	for _, tt := range []struct {
		name          string
		columns       []parquetColumn
		expected      string
		expectedError string
	}{
		{"empty_list", []parquetColumn{}, "", "empty list of columns"},
		{
			"decimal_logical", []parquetColumn{{
				path:     []string{"dL"},
				logical:  &format.DecimalType{Precision: 2, Scale: 1},
				physical: parquet.ByteArray, // deliberately inconsistent
				length:   0,
			}},
			"(\"dL\" DECIMAL(2,1))", "",
		},
		{
			"decimal_physical", []parquetColumn{{[]string{"dp"}, nil, parquet.Int64, 0}},
			"(\"dp\" DECIMAL(19,0))", "",
		},
		{
			"varchar_logical", []parquetColumn{
				{[]string{"vL"}, &format.StringType{}, parquet.Int32, 0},
			}, "(\"vL\" " + maxVarcharColumn + ")", "",
		},
		{
			"varchar_physical", []parquetColumn{
				{[]string{"vP"}, nil, parquet.ByteArray, 0},
			}, "(\"vP\" " + maxVarcharColumn + ")", "",
		},
		{
			"varchar_flex",
			[]parquetColumn{
				{[]string{"v2"}, nil, parquet.FixedLenByteArray, 123},
			}, "(\"v2\" VARCHAR(123) CHARACTER SET UTF8)", "",
		},
		{
			"error_1",
			[]parquetColumn{
				{[]string{"e1"}, nil, parquet.FixedLenByteArray, maxVarcharLength + 1},
			}, "", "exceeds supported maximum",
		},
		{
			"multiple_valid", []parquetColumn{
				{[]string{"dP"}, nil, parquet.Int64, 0},
				{[]string{"vL"}, &format.StringType{}, parquet.FixedLenByteArray, 345},
			},
			"(\"dP\" DECIMAL(19,0), \"vL\" " + maxVarcharColumn + ")",
			"",
		},
		{
			"multiple_2nd_invalid", []parquetColumn{
				{[]string{"d1"}, nil, parquet.Int64, 0},
				{[]string{"e1"}, nil, invalidPhysicalDataType, 0},
			},
			"",
			"unsupported",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result, err := createTableStatement("S.T", tt.columns)
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

func closedFile(t *testing.T) *os.File {
	f, err := os.CreateTemp(t.TempDir(), "broken")
	assert.NoError(t, err)
	f.Close()
	return f
}

func TestImportClosedFile(t *testing.T) {
	_, err := retrieveParquetColumns(closedFile(t))
	assert.ErrorContains(t, err, "could not stat Parquet file")
}

func TestImportInvalidFileFormat(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "broken")
	assert.NoError(t, err)
	defer f.Close()
	_, err = retrieveParquetColumns(f)
	assert.ErrorContains(t, err, "could not open file with Parquet reader")
}

func TestImportIllegalCharacters(t *testing.T) {
	for _, tt := range []struct {
		schema        string
		table         string
		path          string
		expectedError string
	}{
		{`"S1`, "T1", "path", "invalid schema name"},
		{"S1", ".T1", "path", "invalid table name"},
		{"S1", "T1", "pa'th", `file path contains illegal character "'"`},
	} {
		t.Run(tt.expectedError, func(t *testing.T) {
			var ctx context.Context
			var db *sql.DB
			options := ParquetImportOptions{}
			_, err := ImportParquetWithInferredSchema(ctx, db, tt.schema, tt.table, tt.path, options)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedError)
		})
	}
}

func TestRenameSingleColumn(t *testing.T) {
	for _, tt := range []struct {
		name     string
		colName  string
		expected string
	}{
		{"empty_name", "", "_"},
		{"lowercase", "abc", "ABC"},
		{"digit_prefix", "123abc", "ABC"},
		{"underscores", "_123abc_", "_123ABC_"},
		{"multiple_special", "__a,.-b___c", "_A_B_C"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			columns := []parquetColumn{{path: []string{tt.colName}}}
			actual := renameColumns(columns)
			assert.Equal(t, 1, len(actual[0].path))
			assert.Equal(t, tt.expected, actual[0].path[0])
		})
	}
}

func TestRenameDuplicates(t *testing.T) {
	for _, tt := range []struct {
		name        string
		columnNames []string
		expected    []string
	}{
		{"empty_list", []string{}, []string{}},
		{"single_column", []string{"a"}, []string{"A"}},
		{"two_columns", []string{"a", "b"}, []string{"A", "B"}},
		{"duplicate", []string{"a", "a"}, []string{"A", "A_1"}},
		{"duplicate_2", []string{"a", "1a"}, []string{"A", "A_1"}},
		{"duplicate_3", []string{"a_b", "a__b"}, []string{"A_B", "A_B_1"}},
		{"duplicate_4", []string{"a", "a_1", "a_1"}, []string{"A", "A_1", "A_1_1"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			columns := make([]parquetColumn, 0, len(tt.columnNames))
			for _, name := range tt.columnNames {
				columns = append(columns, parquetColumn{path: []string{name}})
			}
			result := renameColumns(columns)
			actual := make([]string, 0, len(result))
			for _, col := range result {
				assert.Equal(t, 1, len(col.path))
				actual = append(actual, col.path[0])
			}
			assert.Equal(t, tt.expected, actual)
		})
	}
}
