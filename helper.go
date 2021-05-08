package exasol

import (
	"database/sql/driver"
)

func namedValueToValue(args []driver.NamedValue) ([]driver.Value, error) {
	values := make([]driver.Value, len(args))
	for n, namedValue := range args {
		if namedValue.Name != "" {
			return nil, ErrNamedValuesNotSupported
		}
		values[n] = namedValue.Value
	}
	return values, nil
}
