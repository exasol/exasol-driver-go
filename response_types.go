package exasol

import "encoding/json"

type baseResponse struct {
	Status       string          `json:"status"`
	ResponseData json.RawMessage `json:"responseData"`
	Exception    *Exception      `json:"exception"`
}

type Exception struct {
	Text    string `json:"text"`
	SQLCode string `json:"sqlCode"`
}

type authResponse struct {
	SessionID             int    `json:"sessionId"`
	ProtocolVersion       int    `json:"protocolVersion"`
	ReleaseVersion        string `json:"releaseVersion"`
	DatabaseName          string `json:"databaseName"`
	ProductName           string `json:"productName"`
	MaxDataMessageSize    int    `json:"maxDataMessageSize"`
	MaxIdentifierLength   int    `json:"maxIdentifierLength"`
	MaxVarcharLength      int    `json:"maxVarcharLength"`
	IdentifierQuoteString string `json:"identifierQuoteString"`
	TimeZone              string `json:"timeZone"`
	TimeZoneBehavior      string `json:"timeZoneBehavior"`
}

type publicKeyResponse struct {
	PublicKeyPem      string `json:"publicKeyPem"`
	PublicKeyModulus  string `json:"publicKeyModulus"`
	PublicKeyExponent string `json:"publicKeyExponent"`
}

type sqlQueriesResponse struct {
	NumResults int               `json:"numResults"`
	Results    []json.RawMessage `json:"results"`
}

type sqlQueryResponseRowCount struct {
	ResultType string `json:"resultType"`
	RowCount   int    `json:"rowCount"`
}

type sqlQueryResponseResultSet struct {
	ResultType string                        `json:"resultType"`
	ResultSet  sqlQueryResponseResultSetData `json:"resultSet"`
}

type sqlQueryResponseResultSetData struct {
	ResultSetHandle  int              `json:"resultSetHandle"`
	NumColumns       int              `json:"numColumns,omitempty"`
	NumRows          int              `json:"numRows"`
	NumRowsInMessage int              `json:"numRowsInMessage"`
	Columns          []sqlQueryColumn `json:"columns,omitempty"`
	Data             [][]interface{}  `json:"data"`
}

type sqlQueryColumn struct {
	Name     string             `json:"name"`
	DataType sqlQueryColumnType `json:"dataType"`
}

type sqlQueryColumnType struct {
	Type              string  `json:"type"`
	Precision         *int64  `json:"precision,omitempty"`
	Scale             *int64  `json:"scale,omitempty"`
	Size              *int64  `json:"size,omitempty"`
	CharacterSet      *string `json:"characterSet,omitempty"`
	WithLocalTimeZone *bool   `json:"withLocalTimeZone,omitempty"`
	Fraction          *int    `json:"fraction,omitempty"`
	SRID              *int    `json:"srid,omitempty"`
}

type createPreparedStatementResponse struct {
	StatementHandle int           `json:"statementHandle"`
	ParameterData   parameterData `json:"parameterData,omitempty"`
}

type parameterData struct {
	NumColumns int              `json:"numColumns"`
	Columns    []sqlQueryColumn `json:"columns"`
	sqlQueriesResponse
}
