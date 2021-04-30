package exasol

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

func ParseDSN(dsn string) (*Config, error) {
	if !strings.HasPrefix(dsn, "exa:") {
		return nil, fmt.Errorf("invalid connection string, must start with 'exa:'")
	}

	cleanDsn := strings.Replace(dsn, "exa:", "", 1)

	items := strings.SplitN(cleanDsn, ";", 2)
	if len(items) > 2 {
		return nil, fmt.Errorf("invalid connection string format")
	}

	conn := items[0]
	hostPort := strings.Split(conn, ":")

	if len(hostPort) != 2 {
		return nil, fmt.Errorf("invalid host or port, expect host:port format")
	}

	config := &Config{
		Host:       hostPort[0],
		Port:       hostPort[1],
		ApiVersion: 1,
		Autocommit: true,
		Encryption: true,
		ClientName: "Go client",
		Params:     map[string]string{},
		FetchSize:  2000,
	}

	paramsString := ""
	if len(items) > 1 {
		paramsString = items[1]
	}

	if paramsString == "" {
		return config, nil
	}

	reg := regexp.MustCompile("[\\w];")
	params := SplitAfter(paramsString, reg)
	for _, param := range params {
		param = strings.TrimRight(param, ";")
		paramKeyValue := strings.SplitN(param, "=", 2)
		if len(paramKeyValue) != 2 {
			return nil, fmt.Errorf("invalid parameter %s", param)
		}

		switch paramKeyValue[0] {
		case "password":
			config.Password = Unescape(paramKeyValue[1], ";")
		case "user":
			config.User = Unescape(paramKeyValue[1], ";")
		case "autocommit":
			config.Autocommit = paramKeyValue[1] == "1"
		case "encryption":
			config.Encryption = paramKeyValue[1] == "1"
		case "compression":
			config.Encryption = paramKeyValue[1] == "1"
		case "clientname":
			config.ClientName = paramKeyValue[1]
		case "clientversion":
			config.ClientVersion = paramKeyValue[1]
		case "fetchsize":
			value, err := strconv.Atoi(paramKeyValue[1])
			if err != nil {
				return nil, fmt.Errorf("invalid fetch size")
			}
			config.FetchSize = value
		case "resultSetMaxRows":
			value, err := strconv.Atoi(paramKeyValue[1])
			if err != nil {
				return nil, fmt.Errorf("invalid fetch size")
			}
			config.ResultSetMaxRows = value
		default:
			config.Params[paramKeyValue[0]] = Unescape(paramKeyValue[1], ";")
		}
	}

	return config, nil
}

func Unescape(s, char string) string {
	return strings.ReplaceAll(s, `\`+char, char)
}

func SplitAfter(s string, re *regexp.Regexp) []string {
	var (
		r []string
		p int
	)
	is := re.FindAllStringIndex(s, -1)
	if is == nil {
		return append(r, s)
	}
	for _, i := range is {
		r = append(r, s[p:i[1]])
		p = i[1]
	}
	return append(r, s[p:])
}
