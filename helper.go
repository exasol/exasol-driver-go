package exasol

import (
	"database/sql/driver"
)

func namedValuesToValues(namedValues []driver.NamedValue) ([]driver.Value, error) {
	values := make([]driver.Value, len(namedValues))
	for index, namedValue := range namedValues {
		if namedValue.Name != "" {
			return nil, ErrNamedValuesNotSupported
		}
		values[index] = namedValue.Value
	}
	return values, nil
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
