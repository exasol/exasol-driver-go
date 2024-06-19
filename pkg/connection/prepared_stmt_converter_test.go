package connection

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/exasol/exasol-driver-go/pkg/types"
)

func TestConvertArgs(t *testing.T) {

	berlinTimeZone, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		t.Errorf("Error loading Berlin timezone: %v", err)
	}
	for i, testCase := range []struct {
		arg           driver.Value
		exasolType    string
		expectedJson  string
		expectedError string
	}{
		{arg: "text", exasolType: "VARCHAR", expectedJson: `"text"`},
		{arg: 123, exasolType: "VARCHAR", expectedJson: `123`},
		{arg: 123.456, exasolType: "VARCHAR", expectedJson: `123.456`},
		{arg: "text", exasolType: "CHAR", expectedJson: `"text"`},
		// BOOLEAN
		{arg: true, exasolType: "BOOLEAN", expectedJson: `true`},
		{arg: false, exasolType: "BOOLEAN", expectedJson: `false`},
		// DECIMAL
		{arg: 17, exasolType: "DECIMAL", expectedJson: `17`},
		{arg: 123.456, exasolType: "DECIMAL", expectedJson: `123.456`},
		{arg: int(123), exasolType: "DECIMAL", expectedJson: `123`},
		{arg: int32(123), exasolType: "DECIMAL", expectedJson: `123`},
		{arg: int64(123), exasolType: "DECIMAL", expectedJson: `123`},
		{arg: float32(123), exasolType: "DECIMAL", expectedJson: `123`},
		{arg: float64(123), exasolType: "DECIMAL", expectedJson: `123`},
		{arg: float32(123.456), exasolType: "DECIMAL", expectedJson: `123.456`},
		{arg: float64(123.456), exasolType: "DECIMAL", expectedJson: `123.456`},
		{arg: "invalid", exasolType: "DECIMAL", expectedJson: `"invalid"`}, // No special handling for invalid values
		// DOUBLE
		{arg: 123.456, exasolType: "DOUBLE", expectedJson: `123.456000`},
		{arg: 123, exasolType: "DOUBLE", expectedJson: `123.000000`},
		{arg: int(123), exasolType: "DOUBLE", expectedJson: `123.000000`},
		{arg: int32(123), exasolType: "DOUBLE", expectedJson: `123.000000`},
		{arg: int64(123), exasolType: "DOUBLE", expectedJson: `123.000000`},
		{arg: float32(123), exasolType: "DOUBLE", expectedJson: `123.000000`},
		{arg: float64(123), exasolType: "DOUBLE", expectedJson: `123.000000`},
		{arg: float32(123.456), exasolType: "DOUBLE", expectedJson: `123.456001`}, // Float32 rounding error is OK
		{arg: float64(123.456), exasolType: "DOUBLE", expectedJson: `123.456000`},
		{arg: "invalid", exasolType: "DOUBLE", expectedError: "E-EGOD-30: cannot convert argument 'invalid' of type 'string' to 'DOUBLE' type"},
		// TIMESTAMP
		{arg: "some string", exasolType: "TIMESTAMP", expectedJson: `"some string"`}, // We assume strings are already formatted
		{arg: time.Date(2024, time.June, 18, 17, 22, 13, 123456789, time.UTC), exasolType: "TIMESTAMP", expectedJson: `"2024-06-18 17:22:13.123456"`},
		{arg: time.Date(2024, time.June, 18, 17, 22, 13, 123456789, berlinTimeZone), exasolType: "TIMESTAMP", expectedJson: `"2024-06-18 17:22:13.123456"`},
		{arg: 1, exasolType: "TIMESTAMP", expectedError: "E-EGOD-30: cannot convert argument '1' of type 'int' to 'TIMESTAMP' type"},
		// TIMESTAMP WITH LOCAL TIME ZONE
		{arg: "some string", exasolType: "TIMESTAMP WITH LOCAL TIME ZONE", expectedJson: `"some string"`}, // We assume strings are already formatted
		{arg: time.Date(2024, time.June, 18, 17, 22, 13, 123456789, time.UTC), exasolType: "TIMESTAMP WITH LOCAL TIME ZONE", expectedJson: `"2024-06-18 17:22:13.123456"`},
		{arg: time.Date(2024, time.June, 18, 17, 22, 13, 123456789, berlinTimeZone), exasolType: "TIMESTAMP WITH LOCAL TIME ZONE", expectedJson: `"2024-06-18 17:22:13.123456"`},
		{arg: 1, exasolType: "TIMESTAMP WITH LOCAL TIME ZONE", expectedError: "E-EGOD-30: cannot convert argument '1' of type 'int' to 'TIMESTAMP WITH LOCAL TIME ZONE' type"},
		// DATE
		{arg: "some string", exasolType: "DATE", expectedJson: `"some string"`}, // We assume strings are already formatted
		{arg: time.Date(2024, time.June, 18, 17, 22, 13, 123456789, time.UTC), exasolType: "DATE", expectedJson: `"2024-06-18"`},
		{arg: time.Date(2024, time.June, 18, 17, 22, 13, 123456789, berlinTimeZone), exasolType: "DATE", expectedJson: `"2024-06-18"`},
		{arg: 1, exasolType: "DATE", expectedError: "E-EGOD-30: cannot convert argument '1' of type 'int' to 'DATE' type"},
	} {
		t.Run(fmt.Sprintf("Test%02d converting %T to %s returns %q", i, testCase.arg, testCase.exasolType, testCase.expectedJson), func(t *testing.T) {
			converted, err := convertArg(testCase.arg, types.SqlQueryColumnType{Type: testCase.exasolType})
			if testCase.expectedError != "" {
				if err == nil {
					t.Errorf("Expected error %q, got nil", testCase.expectedError)
					return
				} else if err.Error() != testCase.expectedError {
					t.Errorf("Expected error %q, got %q", testCase.expectedError, err.Error())
					return
				}
				return
			}
			if err != nil {
				t.Errorf("Error converting arg: %v", err)
				return
			}
			actualJson, err := json.Marshal(converted)
			if err != nil {
				t.Errorf("Error marshalling converted arg: %v", err)
				return
			}
			if string(actualJson) != testCase.expectedJson {
				t.Errorf("Expected %q, got %q", testCase.expectedJson, string(actualJson))
			}
		})
	}
}
