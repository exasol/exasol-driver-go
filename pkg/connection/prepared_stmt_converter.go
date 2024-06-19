package connection

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	"github.com/exasol/exasol-driver-go/pkg/errors"
	"github.com/exasol/exasol-driver-go/pkg/types"
)

func convertArg(arg driver.Value, colType types.SqlQueryColumnType) (interface{}, error) {
	dataType := colType.Type
	switch dataType {
	case "DOUBLE":
		switch arg := arg.(type) {
		case int:
			return jsonDoubleValue(float64(arg)), nil
		case int32:
			return jsonDoubleValue(float64(arg)), nil
		case int64:
			return jsonDoubleValue(float64(arg)), nil
		case float32:
			return jsonDoubleValue(float64(arg)), nil
		case float64:
			return jsonDoubleValue(arg), nil
		default:
			return nil, errors.NewInvalidArgType(arg, dataType)
		}
	case "TIMESTAMP", "TIMESTAMP WITH LOCAL TIME ZONE":
		switch arg := arg.(type) {
		case time.Time:
			return jsonTimestampValue(arg), nil
		case string:
			// We assume strings are already formatted correctly
			return arg, nil
		default:
			return nil, errors.NewInvalidArgType(arg, dataType)
		}
	case "DATE":
		switch arg := arg.(type) {
		case time.Time:
			return jsonDateValue(arg), nil
		case string:
			// We assume strings are already formatted correctly
			return arg, nil
		default:
			return nil, errors.NewInvalidArgType(arg, dataType)
		}
	default:
		// No need to convert other types
		return arg, nil
	}
}

func jsonDoubleValue(value float64) json.Marshaler {
	return &jsonDoubleValueStruct{value: value}
}

type jsonDoubleValueStruct struct {
	value float64
}

func (j *jsonDoubleValueStruct) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%f", j.value)), nil
}

func jsonTimestampValue(value time.Time) json.Marshaler {
	return &jsonTimestampValueStruct{value: value}
}

type jsonTimestampValueStruct struct {
	value time.Time
}

func (j *jsonTimestampValueStruct) MarshalJSON() ([]byte, error) {
	// Exasol expects format YYYY-MM-DD HH24:MI:SS.FF6
	return []byte(fmt.Sprintf(`"%s"`, j.value.Format("2006-01-02 15:04:05.000000"))), nil
}

func jsonDateValue(value time.Time) json.Marshaler {
	return &jsonDateValueStruct{value: value}
}

type jsonDateValueStruct struct {
	value time.Time
}

func (j *jsonDateValueStruct) MarshalJSON() ([]byte, error) {
	// Exasol expects format YYYY-MM-DD
	return []byte(fmt.Sprintf(`"%s"`, j.value.Format("2006-01-02"))), nil
}
