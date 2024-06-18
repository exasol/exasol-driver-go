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
	if dataType == "DOUBLE" {
		if intArg, ok := arg.(int64); ok {
			return jsonDoubleValue(float64(intArg)), nil
		}
		if floatArg, ok := arg.(float64); ok {
			return jsonDoubleValue(floatArg), nil
		}
		return nil, errors.NewInvalidArgType(arg, dataType)
	}
	if dataType == "TIMESTAMP" || dataType == "TIMESTAMP WITH LOCAL TIME ZONE" {
		if timeArg, ok := arg.(time.Time); ok {
			return jsonTimestampValue(timeArg), nil
		}
		if stringArg, ok := arg.(string); ok {
			return stringArg, nil
		}
		return nil, errors.NewInvalidArgType(arg, dataType)
	}
	if dataType == "DATE" {
		if timeArg, ok := arg.(time.Time); ok {
			return jsonDateValue(timeArg), nil
		}
		if stringArg, ok := arg.(string); ok {
			return stringArg, nil
		}
		return nil, errors.NewInvalidArgType(arg, dataType)
	}
	if dataType == "BOOLEAN" {
		if boolArg, ok := arg.(bool); ok {
			return boolArg, nil
		}
		return nil, errors.NewInvalidArgType(arg, dataType)
	}
	return arg, nil
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
