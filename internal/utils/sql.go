package utils

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/exasol/exasol-driver-go/pkg/errors"
)

const WHITESPACE = `\s+`
const IMPORT_FORMAT_PLACEHOLDER = "ImportFormatPlaceholder"

type ImportFormat int

const (
	ImportFormatNone ImportFormat = iota
	ImportFormatCSV
	ImportFormatParquet
)

func namedGroup(name, regexp string) string { return fmt.Sprintf("(?P<%s>%s)", name, regexp) }

var localImportRegex = regexp.MustCompile(`(?is)^\s*IMPORT[\s(]+.+FROM` + WHITESPACE + `LOCAL` + WHITESPACE + namedGroup(IMPORT_FORMAT_PLACEHOLDER, "CSV|PARQUET") + `.*$`)

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

var rowSeparatorQueryRegex = regexp.MustCompile(`(?i)` + `ROW` + WHITESPACE + `SEPARATOR` + WHITESPACE + `=` + WHITESPACE + QUOTE + namedGroup(ROW_SEPARATOR_PLACEHOLDER, "[a-zA-Z]+") + QUOTE)

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

var fileQueryRegex = regexp.MustCompile(`(?i)` + `FILE` + WHITESPACE + QUOTE + namedGroup(FILE_PLACEHOLDER, `[a-zA-Z0-9:<> \\/._\-~]+`) + QUOTE + ` ?`)

func GetFilePaths(query string) ([]string, error) {
	r := fileQueryRegex.FindAllStringSubmatch(sqlWithoutComments(query), -1)
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

func sqlWithoutComments(query string) string {
	masked := []byte(query)
	for index := 0; index < len(query); index++ {
		if quoteEnd := quotedLiteralEnd(query, index); quoteEnd > index {
			index = quoteEnd - 1
			continue
		}
		if commentEnd := sqlCommentEnd(query, index); commentEnd > index {
			maskComment(masked, index, commentEnd)
			index = commentEnd - 1
		}
	}
	return string(masked)
}

func quotedLiteralEnd(query string, start int) int {
	quote := query[start]
	if quote != '\'' && quote != '"' {
		return start
	}
	for index := start + 1; index < len(query); index++ {
		if query[index] != quote {
			continue
		}
		if index+1 < len(query) && query[index+1] == quote {
			index++
			continue
		}
		return index + 1
	}
	return len(query)
}

func sqlCommentEnd(query string, start int) int {
	if start+1 >= len(query) {
		return start
	}
	switch query[start : start+2] {
	case "--":
		return lineCommentEnd(query, start+2)
	case "/*":
		return blockCommentEnd(query, start+2)
	default:
		return start
	}
}

func lineCommentEnd(query string, start int) int {
	for index := start; index < len(query); index++ {
		if query[index] == '\n' || query[index] == '\r' {
			return index
		}
	}
	return len(query)
}

func blockCommentEnd(query string, start int) int {
	for index := start; index+1 < len(query); index++ {
		if query[index] == '*' && query[index+1] == '/' {
			return index + 2
		}
	}
	return len(query)
}

func maskComment(query []byte, start, end int) {
	for index := start; index < end; index++ {
		if query[index] != '\n' && query[index] != '\r' {
			query[index] = ' '
		}
	}
}

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
	fileName, formatKeyword, urlSuffix := "data.csv", "CSV", ""
	if format == ImportFormatParquet {
		fileName, formatKeyword, urlSuffix = "data.parquet", "PARQUET", ";MaxConcurrentReads=1"
	}
	fileMatches := fileQueryRegex.FindAllStringIndex(sqlWithoutComments(query), -1)
	if len(fileMatches) > 0 {
		var updatedQuery strings.Builder
		lastEnd := 0
		for i, match := range fileMatches {
			updatedQuery.WriteString(query[lastEnd:match[0]])
			if i == 0 {
				fmt.Fprintf(&updatedQuery, "FILE '%s' ", fileName)
			}
			lastEnd = match[1]
		}
		updatedQuery.WriteString(query[lastEnd:])
		query = updatedQuery.String()
	}
	proxyURL := fmt.Sprintf("%s://%s:%d%s", target.scheme(), target.Host, target.Port, urlSuffix)
	updatedImport := fmt.Sprintf("%s AT '%s'%s", formatKeyword, proxyURL, target.publicKeyClause())
	return string(regexp.MustCompile(`(?i)(LOCAL`+WHITESPACE+formatKeyword+`)`).ReplaceAll([]byte(query), []byte(updatedImport)))
}
