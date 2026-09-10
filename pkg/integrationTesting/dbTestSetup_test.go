package integrationTesting

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSupportsPublicKeyPinningCoversSupportedDatabaseFamilies(t *testing.T) {
	tests := []struct {
		name      string
		dbVersion string
		supported bool
	}{
		{name: "2025 supports the clause", dbVersion: "2025.1.14", supported: true},
		{name: "2026 supports the clause", dbVersion: "2026.1.1", supported: true},
		{name: "Exasol 8 cannot parse the clause", dbVersion: "8.29.13", supported: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setup := &DbTestSetup{DbVersion: test.dbVersion}
			assert.Equal(t, test.supported, setup.SupportsPublicKeyPinning())
		})
	}
}
