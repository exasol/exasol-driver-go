package integrationTesting

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestSupportsPublicKeyPinningCoversEveryCiMatrixLeg pins the predicate to what
// each version in the CI matrix was actually observed to do, rather than to the
// documented release of the clause alone. An encrypted CSV import was run
// against all three: 7.1.30 answered "syntax error, unexpected
// IDENTIFIER_PART_, expecting FILE_" at the PUBLIC KEY clause, while 2025.1.10
// and 2026.1.0 loaded the rows. See decision-log § [16].
func TestSupportsPublicKeyPinningCoversEveryCiMatrixLeg(t *testing.T) {
	tests := []struct {
		name      string
		dbVersion string
		supported bool
	}{
		{name: "7.1.30 cannot parse the clause", dbVersion: "7.1.30", supported: false},
		{name: "2025.1.10 parses the clause below the Parquet threshold", dbVersion: "2025.1.10", supported: true},
		{name: "2026.1.0 parses the clause", dbVersion: "2026.1.0", supported: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setup := &DbTestSetup{DbVersion: test.dbVersion}
			assert.Equal(t, test.supported, setup.SupportsPublicKeyPinning())
		})
	}
}
