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

var localImportRegex = regexp.MustCompile(`(?i)(FROM LOCAL CSV )`)
var fileQueryRegex = regexp.MustCompile(`(?i)(FILE\s+(["|'])?(?P<File>[a-zA-Z0-9:<> \\\/._]+)(["|']? ?))`)
var rowSeparatorQueryRegex = regexp.MustCompile(`(?i)(ROW\s+SEPARATOR\s+=\s+(["|'])?(?P<RowSeparator>[a-zA-Z]+)(["|']?))`)

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

func IsImportQuery(query string) bool {
	return localImportRegex.MatchString(query)
}

func GetRowSeparator(query string) string {
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

func GetFilePaths(query string) ([]string, error) {
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

func UpdateImportQuery(query string, host string, port uint32) string {
	r := fileQueryRegex.FindAllStringSubmatch(query, -1)
	for i, matches := range r {
		if i == 0 {
			query = strings.Replace(query, matches[0], "FILE 'data.csv' ", 1)
		} else {
			query = strings.Replace(query, matches[0], "", 1)
		}
	}

	proxyURL := fmt.Sprintf("http://%s:%d", host, port)
	updatedImport := fmt.Sprintf("CSV AT '%s'", proxyURL)
	var importQueryRegex = regexp.MustCompile(`(?i)(LOCAL CSV)`)

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
