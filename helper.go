package exasol

import (
	"database/sql/driver"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

var localImportRegex = regexp.MustCompile(`(?i)(FROM LOCAL CSV )`)
var fileQueryRegex = regexp.MustCompile(`(?i)(FILE\s+(["|'])?(?P<File>[a-zA-Z0-9:<> \\\/._]+)(["|']? ?))`)
var rowSeparatorQueryRegex = regexp.MustCompile(`(?i)(ROW\s+SEPARATOR\s+=\s+(["|'])?(?P<RowSeparator>[a-zA-Z]+)(["|']?))`)

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

func boolToPtr(b bool) *bool {
	return &b
}

func isImportQuery(query string) bool {
	return localImportRegex.MatchString(query)
}

func getRowSeparator(query string) string {
	r := rowSeparatorQueryRegex.FindStringSubmatch(query)
	separator := "LF"
	for i, name := range rowSeparatorQueryRegex.SubexpNames() {
		if name == "RowSeparator" && len(r) >= i {
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

func getFilePaths(query string) ([]string, error) {
	r := fileQueryRegex.FindAllStringSubmatch(query, -1)
	var files []string
	for _, matches := range r {
		for i, name := range fileQueryRegex.SubexpNames() {
			if name == "File" {
				files = append(files, matches[i])
			}
		}
	}
	if len(files) == 0 {
		return nil, ErrInvalidImportQuery
	}
	return files, nil
}

func openFile(path string) (*os.File, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, newFileNotFound(path)
	}
	return file, nil
}

func updateImportQuery(query string, p *proxy) string {

	r := fileQueryRegex.FindAllStringSubmatch(query, -1)
	for i, matches := range r {
		if i == 0 {
			query = strings.Replace(query, matches[0], "FILE 'data.csv' ", 1)
		} else {
			query = strings.Replace(query, matches[0], "", 1)
		}
	}

	proxyURL := fmt.Sprintf("http://%s:%d", p.Host, p.Port)
	updatedImport := fmt.Sprintf("CSV AT '%s'", proxyURL)
	var importQueryRegex = regexp.MustCompile(`(?i)(LOCAL CSV)`)

	return string(importQueryRegex.ReplaceAll([]byte(query), []byte(updatedImport)))
}

func resolveHosts(h string) ([]string, error) {
	var hosts []string
	hostRangeRegex := regexp.MustCompile(`^((.+?)(\d+))\.\.(\d+)$`)

	for _, host := range strings.Split(h, ",") {
		if hostRangeRegex.MatchString(host) {
			parsedHosts, err := parseRange(hostRangeRegex, host)
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

func parseRange(hostRangeRegex *regexp.Regexp, host string) ([]string, error) {
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
		return nil, newInvalidHostRangeLimits(host)
	}

	var hosts []string
	for i := start; i <= stop; i++ {
		hosts = append(hosts, fmt.Sprintf("%s%d", prefix, i))
	}
	return hosts, nil
}
