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

const (
	minPublicKeyPinningMajor = 2025
	minPublicKeyPinningMinor = 1
	minPublicKeyPinningPatch = 0
)

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
// proxy connection. It sits beside SupportsNativeParquetImport because both
// answer the same kind of question, and the two thresholds genuinely differ:
// 2025.1.10 accepts the clause but cannot serve native Parquet import.
//
// The driver does not gate on this today, so an encrypted import against an
// older server still surfaces the server's own syntax error. Recording the
// threshold here keeps that follow-up a one-line change rather than a
// rediscovery. See decision-log § [16] for the evidence behind the version.
func SupportsPublicKeyPinning(releaseVersion string) bool {
	return atLeastVersion(releaseVersion, minPublicKeyPinningMajor, minPublicKeyPinningMinor, minPublicKeyPinningPatch)
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
