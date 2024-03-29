package dsn

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/exasol/exasol-driver-go/internal/utils"
	"github.com/exasol/exasol-driver-go/pkg/errors"
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
	QueryTimeout              int               // QueryTimeout sets the query timeout in seconds. If a query runs longer than the specified time, it will be aborted (default: 0)
	ValidateServerCertificate *bool             // If true, validate the server's TLS certificate (default: true)
	CertificateFingerprint    string            // Expected SHA256 checksum of the server's TLS certificate in Hex format (default: "")
	Schema                    string            // Name of the schema to open during connection (default: "")
	ResultSetMaxRows          int               // Maximum number of result set rows returned (default: 0, means no limit)
	Params                    map[string]string // Connection parameters
	AccessToken               string            // Access token (alternative to username/password)
	RefreshToken              string            // Refresh token (alternative to username/password)
	UrlPath                   string            // If the connection is a Http connection RestApi, this is the path of the query
}

// DSNConfigBuilder is a builder for DSNConfig objects.
type DSNConfigBuilder struct {
	Config *DSNConfig
}

// Compression sets the compression flag.
// If true, the WebSocket data frame payload data is compressed. If false, it is not compressed (default: false).
func (c *DSNConfigBuilder) Compression(enabled bool) *DSNConfigBuilder {
	c.Config.Compression = &enabled
	return c
}

// Encryption defines if the database connection should be encrypted via TLS (default: true).
// Please note that starting with version 8, Exasol does not support unencrypted connections
// and connections will fail with the following error:
//
//	EGOD-11: execution failed with SQL error code '08004' and message 'Connection exception - Only TLS connections are allowed.'
func (c *DSNConfigBuilder) Encryption(enabled bool) *DSNConfigBuilder {
	c.Config.Encryption = &enabled
	return c
}

// Autocommit defines if commit() will be executed automatically after each statement (true)
// or if commit() and rollback() must be executed manually (false). Default: true.
func (c *DSNConfigBuilder) Autocommit(enabled bool) *DSNConfigBuilder {
	c.Config.Autocommit = &enabled
	return c
}

// ValidateServerCertificate defines if the driver should validate the server's TLS certificate (default: true).
func (c *DSNConfigBuilder) ValidateServerCertificate(validate bool) *DSNConfigBuilder {
	c.Config.ValidateServerCertificate = &validate
	return c
}

// CertificateFingerprint sets the expected SHA256 checksum of the server's TLS certificate in Hex format (default: "").
func (c *DSNConfigBuilder) CertificateFingerprint(fingerprint string) *DSNConfigBuilder {
	c.Config.CertificateFingerprint = fingerprint
	return c
}

// FetchSize sets the fetch size for results in KiB (default: 2000 KiB).
func (c *DSNConfigBuilder) FetchSize(size int) *DSNConfigBuilder {
	c.Config.FetchSize = size
	return c
}

// QueryTimeout sets the query timeout in seconds. If a query runs longer than the specified time, it will be aborted (default: 0, i.e. no timeout).
func (c *DSNConfigBuilder) QueryTimeout(timeout int) *DSNConfigBuilder {
	c.Config.QueryTimeout = timeout
	return c
}

// ClientName sets the client name reported to the database (default: "Go client")
func (c *DSNConfigBuilder) ClientName(name string) *DSNConfigBuilder {
	c.Config.ClientName = name
	return c
}

// ClientVersion sets the client version reported to the database (default: "")
func (c *DSNConfigBuilder) ClientVersion(version string) *DSNConfigBuilder {
	c.Config.ClientVersion = version
	return c
}

// Host sets the hostname.
func (c *DSNConfigBuilder) Host(host string) *DSNConfigBuilder {
	c.Config.Host = host
	return c
}

// Port sets the port number.
func (c *DSNConfigBuilder) Port(port int) *DSNConfigBuilder {
	c.Config.Port = port
	return c
}

// UrlPath sets the URL path for the WebSocket connection. Use this only in special cases.
func (c *DSNConfigBuilder) UrlPath(path string) *DSNConfigBuilder {
	c.Config.UrlPath = path
	return c
}

// ResultSetMaxRows sets the maximum number of result set rows returned (default: 0, means no limit).
func (c *DSNConfigBuilder) ResultSetMaxRows(maxRows int) *DSNConfigBuilder {
	c.Config.ResultSetMaxRows = maxRows
	return c
}

// Schema sets the name of the schema to open during connection (default: "").
func (c *DSNConfigBuilder) Schema(schema string) *DSNConfigBuilder {
	c.Config.Schema = schema
	return c
}

// String converts the configuration to a DSN (data source name) that can be used for connecting to an Exasol database.
func (c *DSNConfigBuilder) String() string {
	return c.Config.ToDSN()
}

// ToDSN converts the configuration to a DSN (data source name) that can be used for connecting to an Exasol database.
func (c *DSNConfig) ToDSN() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("exa:%s:%d;", c.Host, c.Port))

	if c.AccessToken != "" {
		sb.WriteString(fmt.Sprintf("accesstoken=%s;", escapeDsnParamValue(c.AccessToken)))
	} else if c.RefreshToken != "" {
		sb.WriteString(fmt.Sprintf("refreshtoken=%s;", escapeDsnParamValue(c.RefreshToken)))
	} else {
		sb.WriteString(fmt.Sprintf("user=%s;password=%s;", escapeDsnParamValue(c.User), escapeDsnParamValue(c.Password)))
	}

	if c.Autocommit != nil {
		sb.WriteString(fmt.Sprintf("autocommit=%d;", utils.BoolToInt(*c.Autocommit)))
	}
	if c.Compression != nil {
		sb.WriteString(fmt.Sprintf("compression=%d;", utils.BoolToInt(*c.Compression)))
	}
	if c.Encryption != nil {
		sb.WriteString(fmt.Sprintf("encryption=%d;", utils.BoolToInt(*c.Encryption)))
	}
	if c.ValidateServerCertificate != nil {
		sb.WriteString(fmt.Sprintf("validateservercertificate=%d;", utils.BoolToInt(*c.ValidateServerCertificate)))
	}
	if c.CertificateFingerprint != "" {
		sb.WriteString(fmt.Sprintf("certificatefingerprint=%s;", escapeDsnParamValue(c.CertificateFingerprint)))
	}
	if c.FetchSize != 0 {
		sb.WriteString(fmt.Sprintf("fetchsize=%d;", c.FetchSize))
	}
	if c.QueryTimeout != 0 {
		sb.WriteString(fmt.Sprintf("querytimeout=%d;", c.QueryTimeout))
	}
	if c.ClientName != "" {
		sb.WriteString(fmt.Sprintf("clientname=%s;", escapeDsnParamValue(c.ClientName)))
	}
	if c.ClientVersion != "" {
		sb.WriteString(fmt.Sprintf("clientversion=%s;", escapeDsnParamValue(c.ClientVersion)))
	}
	if c.Schema != "" {
		sb.WriteString(fmt.Sprintf("schema=%s;", escapeDsnParamValue(c.Schema)))
	}
	if c.UrlPath != "" {
		sb.WriteString(fmt.Sprintf("urlpath=%s;", escapeDsnParamValue(c.UrlPath)))
	}

	return strings.TrimRight(sb.String(), ";")
}

// ParseDSN parses the given DSN (data source name).
func ParseDSN(dsn string) (*DSNConfig, error) {
	if !strings.HasPrefix(dsn, "exa:") {
		return nil, errors.NewInvalidConnectionString(dsn)
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
		return "", 0, errors.NewInvalidConnectionStringHostOrPort(connectionString)
	}
	port, err := strconv.Atoi(hostPort[1])
	if err != nil {
		return "", 0, errors.NewInvalidConnectionStringInvalidPort(hostPort[1])
	}
	return hostPort[0], port, nil
}

func getDefaultConfig(host string, port int) *DSNConfig {
	return &DSNConfig{
		Host:                      host,
		Port:                      port,
		Autocommit:                utils.BoolToPtr(true),
		Encryption:                utils.BoolToPtr(true),
		Compression:               utils.BoolToPtr(false),
		ValidateServerCertificate: utils.BoolToPtr(true),
		ClientName:                "Go client",
		Params:                    map[string]string{},
		FetchSize:                 2000,
		QueryTimeout:              0,
		UrlPath:                   "",
	}
}

func getConfigWithParameters(host string, port int, parametersString string) (*DSNConfig, error) {
	config := getDefaultConfig(host, port)
	parameters := extractParameters(parametersString)
	for _, parameter := range parameters {
		keyValuePair := strings.SplitN(parameter, "=", 2)
		if len(keyValuePair) != 2 {
			return nil, errors.NewInvalidConnectionStringInvalidParameter(parameter)
		}
		key := keyValuePair[0]
		value := keyValuePair[1]

		switch key {
		case "password":
			config.Password = unescapeDsnParamValue(value)
		case "accesstoken":
			config.AccessToken = unescapeDsnParamValue(value)
		case "refreshtoken":
			config.RefreshToken = unescapeDsnParamValue(value)
		case "user":
			config.User = unescapeDsnParamValue(value)
		case "autocommit":
			config.Autocommit = utils.BoolToPtr(value == "1")
		case "encryption":
			config.Encryption = utils.BoolToPtr(value == "1")
		case "validateservercertificate":
			config.ValidateServerCertificate = utils.BoolToPtr(value != "0")
		case "certificatefingerprint":
			config.CertificateFingerprint = unescapeDsnParamValue(value)
		case "compression":
			config.Compression = utils.BoolToPtr(value == "1")
		case "clientname":
			config.ClientName = unescapeDsnParamValue(value)
		case "clientversion":
			config.ClientVersion = unescapeDsnParamValue(value)
		case "schema":
			config.Schema = unescapeDsnParamValue(value)
		case "fetchsize":
			fetchSizeValue, err := strconv.Atoi(value)
			if err != nil {
				return nil, errors.NewInvalidConnectionStringInvalidIntParam("fetchsize", value)
			}
			config.FetchSize = fetchSizeValue
		case "querytimeout":
			queryTimeoutValue, err := strconv.Atoi(value)
			if err != nil {
				return nil, errors.NewInvalidConnectionStringInvalidIntParam("querytimeout", value)
			}
			config.QueryTimeout = queryTimeoutValue
		case "resultsetmaxrows":
			maxRowsValue, err := strconv.Atoi(value)
			if err != nil {
				return nil, errors.NewInvalidConnectionStringInvalidIntParam("resultsetmaxrows", value)
			}
			config.ResultSetMaxRows = maxRowsValue
		case "urlpath":
			config.UrlPath = unescapeDsnParamValue(value)
		default:
			config.Params[key] = unescapeDsnParamValue(value)
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

func unescapeDsnParamValue(value string) string {
	return unescape(value, ";")
}

func escapeDsnParamValue(value string) string {
	return escape(value, ";")
}

func unescape(s, char string) string {
	return strings.ReplaceAll(s, `\`+char, char)
}

func escape(s, char string) string {
	return strings.ReplaceAll(s, char, `\`+char)
}
