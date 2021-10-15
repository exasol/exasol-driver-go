package exasol

import (
	"fmt"
	"strconv"
	"strings"
)

type DSNConfig struct {
	host                      string
	port                      int
	user                      string
	password                  string
	autocommit                *bool
	encryption                *bool
	compression               *bool
	clientName                string
	clientVersion             string
	fetchSize                 int
	validateServerCertificate *bool
	certificateFingerprint    string
}

func NewConfig(user, password string) *DSNConfig {
	return &DSNConfig{
		host:     "localhost",
		port:     8563,
		user:     user,
		password: password,
	}
}

func (c *DSNConfig) Compression(enabled bool) *DSNConfig {
	c.compression = &enabled
	return c
}
func (c *DSNConfig) Encryption(enabled bool) *DSNConfig {
	c.encryption = &enabled
	return c
}
func (c *DSNConfig) Autocommit(enabled bool) *DSNConfig {
	c.autocommit = &enabled
	return c
}
func (c *DSNConfig) ValidateServerCertificate(validate bool) *DSNConfig {
	c.validateServerCertificate = &validate
	return c
}
func (c *DSNConfig) CertificateFingerprint(fingerprint string) *DSNConfig {
	c.certificateFingerprint = fingerprint
	return c
}
func (c *DSNConfig) FetchSize(size int) *DSNConfig {
	c.fetchSize = size
	return c
}
func (c *DSNConfig) ClientName(name string) *DSNConfig {
	c.clientName = name
	return c
}
func (c *DSNConfig) ClientVersion(version string) *DSNConfig {
	c.clientVersion = version
	return c
}
func (c *DSNConfig) Host(host string) *DSNConfig {
	c.host = host
	return c
}
func (c *DSNConfig) Port(port int) *DSNConfig {
	c.port = port
	return c
}

func (c *DSNConfig) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("exa:%s:%d;user=%s;password=%s;", c.host, c.port, c.user, c.password))
	if c.autocommit != nil {
		sb.WriteString(fmt.Sprintf("autocommit=%d;", boolToInt(*c.autocommit)))
	}
	if c.compression != nil {
		sb.WriteString(fmt.Sprintf("compression=%d;", boolToInt(*c.compression)))
	}
	if c.encryption != nil {
		sb.WriteString(fmt.Sprintf("encryption=%d;", boolToInt(*c.encryption)))
	}
	if c.validateServerCertificate != nil {
		sb.WriteString(fmt.Sprintf("validateservercertificate=%d;", boolToInt(*c.validateServerCertificate)))
	}
	if c.certificateFingerprint != "" {
		sb.WriteString(fmt.Sprintf("certificatefingerprint=%s;", c.certificateFingerprint))
	}
	if c.fetchSize != 0 {
		sb.WriteString(fmt.Sprintf("fetchsize=%d;", c.fetchSize))
	}
	if c.clientName != "" {
		sb.WriteString(fmt.Sprintf("clientname=%s;", c.clientName))
	}
	if c.clientVersion != "" {
		sb.WriteString(fmt.Sprintf("clientversion=%s;", c.clientVersion))
	}
	return strings.TrimRight(sb.String(), ";")
}

func parseDSN(dsn string) (*config, error) {
	if !strings.HasPrefix(dsn, "exa:") {
		return nil, fmt.Errorf("invalid connection string, must start with 'exa:'")
	}

	splitDsn := splitIntoConnectionStringAndParameters(dsn)
	host, port, err := extractHostAndPort(splitDsn[0])
	if err != nil {
		return nil, err
	}

	if len(splitDsn) < 2 {
		return getDefaultConfig(host, port), nil
	} else {
		return getConfigWithParameters(host, port, splitDsn[1])
	}
}

func splitIntoConnectionStringAndParameters(dsn string) []string {
	cleanDsn := strings.Replace(dsn, "exa:", "", 1)
	return strings.SplitN(cleanDsn, ";", 2)
}

func extractHostAndPort(connectionString string) (string, int, error) {
	hostPort := strings.Split(connectionString, ":")
	if len(hostPort) != 2 {
		return "", 0, fmt.Errorf("invalid host or port, expected format: <host>:<port>")
	}
	port, err := strconv.Atoi(hostPort[1])
	if err != nil {
		return "", 0, fmt.Errorf("invalid `port` value, numeric port expected")
	}
	return hostPort[0], port, nil
}

func getDefaultConfig(host string, port int) *config {
	return &config{
		host:                      host,
		port:                      port,
		apiVersion:                2,
		autocommit:                true,
		encryption:                true,
		compression:               false,
		validateServerCertificate: true,
		clientName:                "Go client",
		params:                    map[string]string{},
		fetchSize:                 128 * 1024,
	}
}

func getConfigWithParameters(host string, port int, parametersString string) (*config, error) {
	config := getDefaultConfig(host, port)
	parameters := extractParameters(parametersString)
	for _, parameter := range parameters {
		keyValuePair := strings.SplitN(parameter, "=", 2)
		if len(keyValuePair) != 2 {
			return nil, fmt.Errorf("invalid parameter %s, expected format <parameter>=<value>", parameter)
		}
		key := keyValuePair[0]
		value := keyValuePair[1]

		switch key {
		case "password":
			config.password = unescape(value, ";")
		case "user":
			config.user = unescape(value, ";")
		case "autocommit":
			config.autocommit = value == "1"
		case "encryption":
			config.encryption = value == "1"
		case "validateservercertificate":
			config.validateServerCertificate = value != "0"
		case "certificatefingerprint":
			config.certificateFingerprint = value
		case "compression":
			config.compression = value == "1"
		case "clientname":
			config.clientName = value
		case "clientversion":
			config.clientVersion = value
		case "schema":
			config.schema = value
		case "fetchsize":
			value, err := strconv.Atoi(value)
			if err != nil {
				return nil, fmt.Errorf("invalid `fetchsize` value, numeric expected")
			}
			config.fetchSize = value
		case "resultsetmaxrows":
			value, err := strconv.Atoi(value)
			if err != nil {
				return nil, fmt.Errorf("invalid `resultsetmaxrows` value, numeric expected")
			}
			config.resultSetMaxRows = value
		default:
			config.params[key] = unescape(value, ";")
		}
	}
	return config, nil
}

func extractParameters(parametersString string) []string {
	// Replace escaped separator with placeholder to avoid wrong split
	replaced := strings.ReplaceAll(parametersString, `\;`, "{{,}}")
	splitted := strings.Split(replaced, ";")
	for i := 0; i < len(splitted); i++ {
		// Revert escaped separator placeholder
		splitted[i] = strings.ReplaceAll(splitted[i], "{{,}}", `\;`)
	}
	return splitted
}

func unescape(s, char string) string {
	return strings.ReplaceAll(s, `\`+char, char)
}
