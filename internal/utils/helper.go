package utils

import (
	"database/sql/driver"
	"fmt"
	mathRand "math/rand"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/exasol/exasol-driver-go/pkg/errors"
)

func NamedValuesToValues(namedValues []driver.NamedValue) ([]driver.Value, error) {
	values := make([]driver.Value, len(namedValues))
	for index, namedValue := range namedValues {
		if namedValue.Name != "" {
			return nil, errors.ErrNamedValuesNotSupported
		}
		values[index] = namedValue.Value
	}
	return values, nil
}

func BoolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func BoolToPtr(b bool) *bool {
	return &b
}

const WHITESPACE = `\s+`
const IMPORT_FORMAT_PLACEHOLDER = "ImportFormatPlaceholder"

// ImportFormat is the file format named by a local IMPORT statement's FROM LOCAL clause.
type ImportFormat int

const (
	ImportFormatNone ImportFormat = iota
	ImportFormatCSV
	ImportFormatParquet
)

func namedGroup(name, regexp string) string {
	return fmt.Sprintf("(?P<%s>%s)", name, regexp)
}

var localImportRegex = regexp.MustCompile(`(?is)^\s*IMPORT[\s(]+.+FROM` + WHITESPACE + `LOCAL` + WHITESPACE +
	namedGroup(IMPORT_FORMAT_PLACEHOLDER, "CSV|PARQUET") + `.*$`)

// GetImportFormat reports which file format, if any, a local IMPORT statement names in its
// FROM LOCAL clause. It is the single place that decides this, so the statement rewrite, the
// output filename, and the transport selection all read this one value instead of re-deriving it.
func GetImportFormat(query string) ImportFormat {
	matches := localImportRegex.FindStringSubmatch(skipLeadingSQLComments(query))
	if matches == nil {
		return ImportFormatNone
	}
	if strings.EqualFold(matches[localImportRegex.SubexpIndex(IMPORT_FORMAT_PLACEHOLDER)], "PARQUET") {
		return ImportFormatParquet
	}
	return ImportFormatCSV
}

// skipLeadingSQLComments removes whitespace and SQL comments before the first SQL token.
// Keeping localImportRegex anchored after this step prevents IMPORT text within a string
// literal from being mistaken for a statement, while allowing commented imports.
func skipLeadingSQLComments(query string) string {
	for {
		query = strings.TrimLeft(query, " \t\r\n\f")
		switch {
		case strings.HasPrefix(query, "--"):
			lineEnd := strings.IndexAny(query, "\r\n")
			if lineEnd == -1 {
				return ""
			}
			query = query[lineEnd+1:]
		case strings.HasPrefix(query, "/*"):
			commentEnd := strings.Index(query[2:], "*/")
			if commentEnd == -1 {
				return query
			}
			query = query[commentEnd+4:]
		default:
			return query
		}
	}
}

const ROW_SEPARATOR_PLACEHOLDER = "RowSeparatorPlaceholder"
const QUOTE = `["']`

var rowSeparatorQueryRegex = regexp.MustCompile(`(?i)` +
	`ROW` + WHITESPACE + `SEPARATOR` + WHITESPACE + `=` + WHITESPACE +
	QUOTE + namedGroup(ROW_SEPARATOR_PLACEHOLDER, "[a-zA-Z]+") + QUOTE)

func GetRowSeparator(query string) string {
	r := rowSeparatorQueryRegex.FindStringSubmatch(query)
	separator := "LF"
	for i, name := range rowSeparatorQueryRegex.SubexpNames() {
		if name == ROW_SEPARATOR_PLACEHOLDER && len(r) >= i {
			separator = r[i]
		}
	}

	switch separator {
	case "CR", "cr":
		return "\r"
	case "CRLF", "crlf":
		return "\r\n"
	default:
		return "\n"
	}
}

const FILE_PLACEHOLDER = "FilePlaceholder"

var fileQueryRegex = regexp.MustCompile(`(?i)` + `FILE` + WHITESPACE +
	QUOTE + namedGroup(FILE_PLACEHOLDER, `[a-zA-Z0-9:<> \\/._\-~]+`) + QUOTE + ` ?`)

func GetFilePaths(query string) ([]string, error) {
	r := fileQueryRegex.FindAllStringSubmatch(query, -1)
	var files []string
	for _, matches := range r {
		for i, name := range fileQueryRegex.SubexpNames() {
			if name == FILE_PLACEHOLDER {
				files = append(files, matches[i])
			}
		}
	}
	if len(files) == 0 {
		return nil, errors.ErrInvalidImportQuery
	}
	return files, nil
}

func OpenFile(path string) (*os.File, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.NewFileNotFound(path)
	}
	return file, nil
}

// ProxyTarget carries where the local-import proxy connection listens and, when set, the
// pinned public-key fingerprint of a TLS-wrapped connection. The fingerprint's presence is the
// single decision that selects both the URL scheme and whether a PUBLIC KEY clause is emitted,
// so CSV and Parquet imports can never disagree about whether the connection they are both
// pointed at is encrypted.
type ProxyTarget struct {
	Host        string
	Port        int
	Fingerprint string
}

func (t ProxyTarget) scheme() string {
	if t.Fingerprint == "" {
		return "http"
	}
	return "https"
}

func (t ProxyTarget) publicKeyClause() string {
	if t.Fingerprint == "" {
		return ""
	}
	return fmt.Sprintf(" PUBLIC KEY '%s'", t.Fingerprint)
}

func UpdateImportQuery(query string, target ProxyTarget) string {
	format := GetImportFormat(query)
	if format == ImportFormatNone {
		return query
	}

	fileName := "data.csv"
	formatKeyword := "CSV"
	urlSuffix := ""
	if format == ImportFormatParquet {
		fileName = "data.parquet"
		formatKeyword = "PARQUET"
		urlSuffix = ";MaxConcurrentReads=1"
	}

	r := fileQueryRegex.FindAllStringSubmatch(query, -1)
	for i, matches := range r {
		if i == 0 {
			query = strings.Replace(query, matches[0], fmt.Sprintf("FILE '%s' ", fileName), 1)
		} else {
			query = strings.Replace(query, matches[0], "", 1)
		}
	}

	proxyURL := fmt.Sprintf("%s://%s:%d%s", target.scheme(), target.Host, target.Port, urlSuffix)
	updatedImport := fmt.Sprintf("%s AT '%s'%s", formatKeyword, proxyURL, target.publicKeyClause())
	importQueryRegex := regexp.MustCompile(`(?i)(LOCAL` + WHITESPACE + formatKeyword + `)`)

	return string(importQueryRegex.ReplaceAll([]byte(query), []byte(updatedImport)))
}

func ResolveHosts(h string) ([]string, error) {
	var hosts []string
	hostRangeRegex := regexp.MustCompile(`^((.+?)(\d+))\.\.(\d+)$`)

	for _, host := range strings.Split(h, ",") {
		if hostRangeRegex.MatchString(host) {
			parsedHosts, err := ParseRange(hostRangeRegex, host)
			if err != nil {
				return nil, err
			}
			hosts = append(hosts, parsedHosts...)
		} else {
			hosts = append(hosts, host)
		}
	}
	return hosts, nil
}

func ParseRange(hostRangeRegex *regexp.Regexp, host string) ([]string, error) {
	matches := hostRangeRegex.FindStringSubmatch(host)
	prefix := matches[2]

	start, err := strconv.Atoi(matches[3])
	if err != nil {
		return nil, err
	}

	stop, err := strconv.Atoi(matches[4])
	if err != nil {
		return nil, err
	}

	if stop < start {
		return nil, errors.NewInvalidHostRangeLimits(host)
	}

	var hosts []string
	for i := start; i <= stop; i++ {
		hosts = append(hosts, fmt.Sprintf("%s%d", prefix, i))
	}
	return hosts, nil
}

func ShuffleHosts(hosts []string) {
	r := mathRand.New(mathRand.NewSource(time.Now().UnixNano())) //nolint:gosec
	r.Shuffle(len(hosts), func(i, j int) {
		hosts[i], hosts[j] = hosts[j], hosts[i]
	})
}
