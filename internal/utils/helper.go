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
func BoolToPtr(b bool) *bool { return &b }

func OpenFile(path string) (*os.File, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.NewFileNotFound(path)
	}
	return file, nil
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
	r.Shuffle(len(hosts), func(i, j int) { hosts[i], hosts[j] = hosts[j], hosts[i] })
}
