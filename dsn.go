package exasol

import (
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
)

func ParseDSN(dsn string) (*Config, error) {
	if !strings.HasPrefix(dsn, "exa:") {
		return nil, fmt.Errorf("invalid connection string, must start with 'exa:'")
	}

	splitDsn := splitIntoConnectionStringAndParameters(dsn)
	hostPort := extractHostAndPort(splitDsn[0])

	if len(hostPort) != 2 {
		return nil, fmt.Errorf("invalid host or port, expected format: <host>:<port>")
	}

	if len(splitDsn) < 2 {
		return getBasicConfig(hostPort), nil
	} else {
		return getConfigWithParameters(hostPort, splitDsn[1])
	}
}

func splitIntoConnectionStringAndParameters(dsn string) []string {
	cleanDsn := strings.Replace(dsn, "exa:", "", 1)
	return strings.SplitN(cleanDsn, ";", 2)
}

func extractHostAndPort(connectionString string) []string {
	return strings.Split(connectionString, ":")
}

func getBasicConfig(hostPort []string) *Config {
	return &Config{
		Host:        hostPort[0],
		Port:        hostPort[1],
		ApiVersion:  2,
		Autocommit:  true,
		Encryption:  true,
		Compression: false,
		ClientName:  "Go client",
		Params:      map[string]string{},
		FetchSize:   128 * 1024,
	}
}

func getConfigWithParameters(hostPort []string, parametersString string) (*Config, error) {
	config := getBasicConfig(hostPort)
	parameters := extractParameters(parametersString)
	for _, parameter := range parameters {
		parameter = strings.TrimRight(parameter, ";")
		keyValuePair := strings.SplitN(parameter, "=", 2)
		if len(keyValuePair) != 2 {
			return nil, fmt.Errorf("invalid parameter %s, expected format <parameter>=<value>", parameter)
		}
		key := keyValuePair[0]
		value := keyValuePair[1]

		switch key {
		case "password":
			config.Password = unescape(value, ";")
		case "user":
			config.User = unescape(value, ";")
		case "autocommit":
			config.Autocommit = value == "1"
		case "encryption":
			config.Encryption = value == "1"
		case "compression":
			config.Compression = value == "1"
		case "clientname":
			config.ClientName = value
		case "clientversion":
			config.ClientVersion = value
		case "schema":
			config.Schema = value
		case "fetchsize":
			value, err := strconv.Atoi(value)
			if err != nil {
				return nil, fmt.Errorf("invalid `fetchsize` value, numeric expected")
			}
			config.FetchSize = value
		case "resultsetmaxrows":
			value, err := strconv.Atoi(value)
			if err != nil {
				return nil, fmt.Errorf("invalid `resultsetmaxrows` value, numeric expected")
			}
			log.Println("Set max row", value)
			config.ResultSetMaxRows = value
		default:
			config.Params[key] = unescape(value, ";")
		}
	}
	return config, nil
}

func extractParameters(parametersString string) []string {
	reg := regexp.MustCompile(`[\w];`)
	return splitAfter(parametersString, reg)
}

func unescape(s, char string) string {
	return strings.ReplaceAll(s, `\`+char, char)
}

func splitAfter(s string, re *regexp.Regexp) []string {
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
