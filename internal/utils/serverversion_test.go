package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseServerVersionThreeParts(t *testing.T) {
	major, minor, patch, ok := parseServerVersion("2025.1.11")
	assert.True(t, ok)
	assert.Equal(t, 2025, major)
	assert.Equal(t, 1, minor)
	assert.Equal(t, 11, patch)
}

func TestParseServerVersionTwoParts(t *testing.T) {
	major, minor, patch, ok := parseServerVersion("2025.1")
	assert.True(t, ok)
	assert.Equal(t, 2025, major)
	assert.Equal(t, 1, minor)
	assert.Equal(t, 0, patch)
}

func TestParseServerVersionTrailingSuffix(t *testing.T) {
	major, minor, patch, ok := parseServerVersion("2026.1.0-rc1")
	assert.True(t, ok)
	assert.Equal(t, 2026, major)
	assert.Equal(t, 1, minor)
	assert.Equal(t, 0, patch)
}

func TestUnparsableServerVersionIsUnsupported(t *testing.T) {
	tests := []struct {
		name           string
		releaseVersion string
	}{
		{name: "single component", releaseVersion: "2025"},
		{name: "empty string", releaseVersion: ""},
		{name: "non-numeric major", releaseVersion: "abc.1.11"},
		{name: "non-numeric minor", releaseVersion: "2025.abc.11"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.False(t, SupportsNativeParquetImport(test.releaseVersion))
		})
	}
}

func TestSupportsNativeParquetImportAtOrAboveThreshold(t *testing.T) {
	tests := []struct {
		name           string
		releaseVersion string
	}{
		{name: "exact threshold", releaseVersion: "2025.1.11"},
		{name: "higher patch", releaseVersion: "2025.1.12"},
		{name: "higher minor", releaseVersion: "2025.2.0"},
		{name: "higher major", releaseVersion: "2026.1.0"},
		{name: "missing patch treated as zero, higher minor", releaseVersion: "2025.2"},
		{name: "non-numeric patch on a higher major", releaseVersion: "2026.1.abc"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.True(t, SupportsNativeParquetImport(test.releaseVersion))
		})
	}
}

// The three versions this suite's CI matrix runs are pinned by name, because
// they are the only ones the PUBLIC KEY threshold was established against: an
// encrypted CSV import was observed to fail on 7.1.30 with a syntax error at the
// clause and to succeed on 2025.1.10 and 2026.1.0. See decision-log § [16].
func TestSupportsPublicKeyPinningAtOrAboveThreshold(t *testing.T) {
	tests := []struct {
		name           string
		releaseVersion string
	}{
		{name: "exact threshold", releaseVersion: "2025.1.0"},
		{name: "CI matrix leg observed to accept the clause", releaseVersion: "2025.1.10"},
		{name: "CI matrix leg observed to accept the clause on a higher major", releaseVersion: "2026.1.0"},
		{name: "higher minor", releaseVersion: "2025.2.1"},
		{name: "missing patch treated as zero at the threshold", releaseVersion: "2025.1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.True(t, SupportsPublicKeyPinning(test.releaseVersion))
		})
	}
}

func TestSupportsPublicKeyPinningBelowThreshold(t *testing.T) {
	tests := []struct {
		name           string
		releaseVersion string
	}{
		{name: "CI matrix leg observed to reject the clause", releaseVersion: "7.1.30"},
		{name: "lower minor", releaseVersion: "2025.0.99"},
		{name: "lower major", releaseVersion: "2024.9.9"},
		{name: "unparsable version is treated as unsupported", releaseVersion: "not-a-version"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.False(t, SupportsPublicKeyPinning(test.releaseVersion))
		})
	}
}

func TestSupportsNativeParquetImportBelowThreshold(t *testing.T) {
	tests := []struct {
		name           string
		releaseVersion string
	}{
		{name: "lower patch", releaseVersion: "2025.1.10"},
		{name: "lower minor", releaseVersion: "2025.0.99"},
		{name: "lower major", releaseVersion: "7.1.30"},
		{name: "missing patch defaults to zero, below threshold", releaseVersion: "2025.1"},
		{name: "non-numeric patch below threshold", releaseVersion: "2025.1.abc"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.False(t, SupportsNativeParquetImport(test.releaseVersion))
		})
	}
}
