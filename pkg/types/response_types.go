package types

import "encoding/json"

type BaseResponse struct {
	Status       string          `json:"status"`
	ResponseData json.RawMessage `json:"responseData"`
	Exception    *Exception      `json:"exception"`
}

type Exception struct {
	Text    string `json:"text"`
	SQLCode string `json:"sqlCode"`
}

type AuthResponse struct {
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

type PublicKeyResponse struct {
	PublicKeyPem      string `json:"publicKeyPem"`
	PublicKeyModulus  string `json:"publicKeyModulus"`
	PublicKeyExponent string `json:"publicKeyExponent"`
}

type SqlQueriesResponse struct {
	NumResults int               `json:"numResults"`
	Results    []json.RawMessage `json:"results"`
}

type SqlQueryResponseRowCount struct {
	ResultType string `json:"resultType"`
	RowCount   int    `json:"rowCount"`
}

type SqlQueryResponseResultSet struct {
	ResultType string                        `json:"resultType"`
	ResultSet  SqlQueryResponseResultSetData `json:"resultSet"`
}

type SqlQueryResponseResultSetData struct {
	ResultSetHandle  int              `json:"resultSetHandle"`
	NumColumns       int              `json:"numColumns,omitempty"`
	NumRows          int              `json:"numRows"`
	NumRowsInMessage int              `json:"numRowsInMessage"`
	Columns          []SqlQueryColumn `json:"columns,omitempty"`
	Data             [][]interface{}  `json:"data"`
}

type SqlQueryColumn struct {
	Name     string             `json:"name"`
	DataType SqlQueryColumnType `json:"dataType"`
}

type SqlQueryColumnType struct {
	Type              string  `json:"type"`
	Precision         *int64  `json:"precision,omitempty"`
	Scale             *int64  `json:"scale,omitempty"`
	Size              *int64  `json:"size,omitempty"`
	CharacterSet      *string `json:"characterSet,omitempty"`
	WithLocalTimeZone *bool   `json:"withLocalTimeZone,omitempty"`
	Fraction          *int    `json:"fraction,omitempty"`
	SRID              *int    `json:"srid,omitempty"`
}

type CreatePreparedStatementResponse struct {
	StatementHandle int           `json:"statementHandle"`
	ParameterData   ParameterData `json:"parameterData,omitempty"`
}

type ParameterData struct {
	NumColumns int              `json:"numColumns"`
	Columns    []SqlQueryColumn `json:"columns"`
	SqlQueriesResponse
}
