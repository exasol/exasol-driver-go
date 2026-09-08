package utils

import (
	"regexp"
	"strconv"
	"strings"
)

const (
	minParquetImportMajor = 2025
	minParquetImportMinor = 1
	minParquetImportPatch = 11
)

const minPublicKeyPinningMajor = 2025

var leadingDigitsRegex = regexp.MustCompile(`^\d+`)

// SupportsNativeParquetImport reports whether a server reporting the given release
// version can serve a local Parquet import natively. An unparsable release version
// is treated as unsupported rather than as an error, since the caller has no
// corrective action beyond refusing the import.
func SupportsNativeParquetImport(releaseVersion string) bool {
	return atLeastVersion(releaseVersion, minParquetImportMajor, minParquetImportMinor, minParquetImportPatch)
}

// SupportsPublicKeyPinning reports whether a server reporting the given release
// version can parse the PUBLIC KEY clause that pins an encrypted local import's
// proxy connection. Exasol 8 releases cannot parse this clause, so imports use
// plaintext there. Unparsable release versions are treated as unsupported.
func SupportsPublicKeyPinning(releaseVersion string) bool {
	major, _, _, ok := parseServerVersion(releaseVersion)
	return ok && major >= minPublicKeyPinningMajor
}

func atLeastVersion(releaseVersion string, major int, minor int, patch int) bool {
	reportedMajor, reportedMinor, reportedPatch, ok := parseServerVersion(releaseVersion)
	if !ok {
		return false
	}
	if reportedMajor != major {
		return reportedMajor > major
	}
	if reportedMinor != minor {
		return reportedMinor > minor
	}
	return reportedPatch >= patch
}

func parseServerVersion(releaseVersion string) (major int, minor int, patch int, ok bool) {
	parts := strings.SplitN(releaseVersion, ".", 3)
	if len(parts) < 2 {
		return 0, 0, 0, false
	}

	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, 0, false
	}

	minor, err = strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, 0, false
	}

	if len(parts) < 3 {
		return major, minor, 0, true
	}

	patch, _ = strconv.Atoi(leadingDigitsRegex.FindString(parts[2]))
	return major, minor, patch, true
}
