package exasol

import (
	"fmt"
	"strconv"
	"strings"
)

// DSNConfig is a data source name for an Exasol database.
type DSNConfig struct {
	Host                      string            // Hostname
	Port                      int               // Port number
	User                      string            // Username
	Password                  string            // Password
	Autocommit                *bool             // If true, commit() will be executed automatically after each statement. If false, commit() and rollback() must be executed manually. (default: true)
	Encryption                *bool             // Encrypt the database connection via TLS (default: true)
	Compression               *bool             // If true, the WebSocket data frame payload data is compressed. If false, it is not compressed. (default: false)
	ClientName                string            // Client name reported to the database (default: "Go client")
	ClientVersion             string            // Client version reported to the database (default: "")
	FetchSize                 int               // Fetch size for results in KiB (default: 2000 KiB)
	ValidateServerCertificate *bool             // If true, validate the server's TLS certificate (default: true)
	CertificateFingerprint    string            // Expected SHA256 checksum of the server's TLS certificate in Hex format (default: "")
	Schema                    string            // Name of the schema to open during connection (default: "")
	ResultSetMaxRows          int               // Maximum number of result set rows returned (default: 0, means no limit)
	params                    map[string]string // Connection parameters
	AccessToken               string            // Access token (alternative to username/password)
	RefreshToken              string            // Refresh token (alternative to username/password)
}

// DSNConfigBuilder is a builder for DSNConfig objects.
type DSNConfigBuilder struct {
	config *DSNConfig
}

// NewConfig creates a new builder with username/password authentication.
func NewConfig(user, password string) *DSNConfigBuilder {
	return &DSNConfigBuilder{
		config: &DSNConfig{
			Host:     "localhost",
			Port:     8563,
			User:     user,
			Password: password,
		},
	}
}

// NewConfigWithAccessToken creates a new builder with access token authentication.
func NewConfigWithAccessToken(token string) *DSNConfigBuilder {
	return &DSNConfigBuilder{
		config: &DSNConfig{
			Host:        "localhost",
			Port:        8563,
			AccessToken: token,
		},
	}
}

// NewConfigWithRefreshToken creates a new builder with refresh token authentication.
func NewConfigWithRefreshToken(token string) *DSNConfigBuilder {
	return &DSNConfigBuilder{
		config: &DSNConfig{
			Host:         "localhost",
			Port:         8563,
			RefreshToken: token,
		},
	}
}

// Compression sets the compression flag.
// If true, the WebSocket data frame payload data is compressed. If false, it is not compressed (default: false).
func (c *DSNConfigBuilder) Compression(enabled bool) *DSNConfigBuilder {
	c.config.Compression = &enabled
	return c
}

// Encryption defines if the database connection should be encrypted via TLS (default: true).
func (c *DSNConfigBuilder) Encryption(enabled bool) *DSNConfigBuilder {
	c.config.Encryption = &enabled
	return c
}

// Autocommit defines if commit() will be executed automatically after each statement (true)
// or if commit() and rollback() must be executed manually (false). Default: true.
func (c *DSNConfigBuilder) Autocommit(enabled bool) *DSNConfigBuilder {
	c.config.Autocommit = &enabled
	return c
}

// ValidateServerCertificate defines if the driver should validate the server's TLS certificate (default: true).
func (c *DSNConfigBuilder) ValidateServerCertificate(validate bool) *DSNConfigBuilder {
	c.config.ValidateServerCertificate = &validate
	return c
}

// CertificateFingerprint sets the expected SHA256 checksum of the server's TLS certificate in Hex format (default: "").
func (c *DSNConfigBuilder) CertificateFingerprint(fingerprint string) *DSNConfigBuilder {
	c.config.CertificateFingerprint = fingerprint
	return c
}

// FetchSize sets the fetch size for results in KiB (default: 2000 KiB).
func (c *DSNConfigBuilder) FetchSize(size int) *DSNConfigBuilder {
	c.config.FetchSize = size
	return c
}

// ClientName sets the client name reported to the database (default: "Go client")
func (c *DSNConfigBuilder) ClientName(name string) *DSNConfigBuilder {
	c.config.ClientName = name
	return c
}

// ClientVersion sets the client version reported to the database (default: "")
func (c *DSNConfigBuilder) ClientVersion(version string) *DSNConfigBuilder {
	c.config.ClientVersion = version
	return c
}

// Host sets the hostname.
func (c *DSNConfigBuilder) Host(host string) *DSNConfigBuilder {
	c.config.Host = host
	return c
}

// Port sets the port number.
func (c *DSNConfigBuilder) Port(port int) *DSNConfigBuilder {
	c.config.Port = port
	return c
}

// ResultSetMaxRows sets the maximum number of result set rows returned (default: 0, means no limit).
func (c *DSNConfigBuilder) ResultSetMaxRows(maxRows int) *DSNConfigBuilder {
	c.config.ResultSetMaxRows = maxRows
	return c
}

// Schema sets the name of the schema to open during connection (default: "").
func (c *DSNConfigBuilder) Schema(schema string) *DSNConfigBuilder {
	c.config.Schema = schema
	return c
}

// String converts the configuration to a DSN (data source name) that can be used for connecting to an Exasol database.
func (c *DSNConfigBuilder) String() string {
	return c.config.ToDSN()
}

// ToDSN converts the configuration to a DSN (data source name) that can be used for connecting to an Exasol database.
func (c *DSNConfig) ToDSN() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("exa:%s:%d;", c.Host, c.Port))

	if c.AccessToken != "" {
		sb.WriteString(fmt.Sprintf("accesstoken=%s;", c.AccessToken))
	} else if c.RefreshToken != "" {
		sb.WriteString(fmt.Sprintf("refreshtoken=%s;", c.RefreshToken))
	} else {
		sb.WriteString(fmt.Sprintf("user=%s;password=%s;", c.User, c.Password))
	}

	if c.Autocommit != nil {
		sb.WriteString(fmt.Sprintf("autocommit=%d;", boolToInt(*c.Autocommit)))
	}
	if c.Compression != nil {
		sb.WriteString(fmt.Sprintf("compression=%d;", boolToInt(*c.Compression)))
	}
	if c.Encryption != nil {
		sb.WriteString(fmt.Sprintf("encryption=%d;", boolToInt(*c.Encryption)))
	}
	if c.ValidateServerCertificate != nil {
		sb.WriteString(fmt.Sprintf("validateservercertificate=%d;", boolToInt(*c.ValidateServerCertificate)))
	}
	if c.CertificateFingerprint != "" {
		sb.WriteString(fmt.Sprintf("certificatefingerprint=%s;", c.CertificateFingerprint))
	}
	if c.FetchSize != 0 {
		sb.WriteString(fmt.Sprintf("fetchsize=%d;", c.FetchSize))
	}
	if c.ClientName != "" {
		sb.WriteString(fmt.Sprintf("clientname=%s;", c.ClientName))
	}
	if c.ClientVersion != "" {
		sb.WriteString(fmt.Sprintf("clientversion=%s;", c.ClientVersion))
	}
	if c.Schema != "" {
		sb.WriteString(fmt.Sprintf("schema=%s;", c.Schema))
	}
	return strings.TrimRight(sb.String(), ";")
}

// ParseDSN parses the given DSN (data source name).
func ParseDSN(dsn string) (*DSNConfig, error) {
	if !strings.HasPrefix(dsn, "exa:") {
		return nil, newInvalidConnectionString(dsn)
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
		return "", 0, newInvalidConnectionStringHostOrPort(connectionString)
	}
	port, err := strconv.Atoi(hostPort[1])
	if err != nil {
		return "", 0, newInvalidConnectionStringInvalidPort(hostPort[1])
	}
	return hostPort[0], port, nil
}

func getDefaultConfig(host string, port int) *DSNConfig {
	return &DSNConfig{
		Host:                      host,
		Port:                      port,
		Autocommit:                boolToPtr(true),
		Encryption:                boolToPtr(true),
		Compression:               boolToPtr(false),
		ValidateServerCertificate: boolToPtr(true),
		ClientName:                "Go client",
		params:                    map[string]string{},
		FetchSize:                 2000,
	}
}

func getConfigWithParameters(host string, port int, parametersString string) (*DSNConfig, error) {
	config := getDefaultConfig(host, port)
	parameters := extractParameters(parametersString)
	for _, parameter := range parameters {
		keyValuePair := strings.SplitN(parameter, "=", 2)
		if len(keyValuePair) != 2 {
			return nil, newInvalidConnectionStringInvalidParameter(parameter)
		}
		key := keyValuePair[0]
		value := keyValuePair[1]

		switch key {
		case "password":
			config.Password = unescape(value, ";")
		case "accesstoken":
			config.AccessToken = unescape(value, ";")
		case "refreshtoken":
			config.RefreshToken = unescape(value, ";")
		case "user":
			config.User = unescape(value, ";")
		case "autocommit":
			config.Autocommit = boolToPtr(value == "1")
		case "encryption":
			config.Encryption = boolToPtr(value == "1")
		case "validateservercertificate":
			config.ValidateServerCertificate = boolToPtr(value != "0")
		case "certificatefingerprint":
			config.CertificateFingerprint = value
		case "compression":
			config.Compression = boolToPtr(value == "1")
		case "clientname":
			config.ClientName = value
		case "clientversion":
			config.ClientVersion = value
		case "schema":
			config.Schema = value
		case "fetchsize":
			fetchSizeValue, err := strconv.Atoi(value)
			if err != nil {
				return nil, newInvalidConnectionStringInvalidIntParam("fetchsize", value)
			}
			config.FetchSize = fetchSizeValue
		case "resultsetmaxrows":
			maxRowsValue, err := strconv.Atoi(value)
			if err != nil {
				return nil, newInvalidConnectionStringInvalidIntParam("resultsetmaxrows", value)
			}
			config.ResultSetMaxRows = maxRowsValue
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
