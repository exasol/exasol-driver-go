package exasol

type command struct {
	Command string `json:"command"`
}

type loginCommand struct {
	command
	ProtocolVersion int        `json:"protocolVersion"`
	Attributes      attributes `json:"attributes,omitempty"`
}

type loginTokenCommand struct {
	command
	ProtocolVersion int        `json:"protocolVersion"`
	Attributes      attributes `json:"attributes,omitempty"`
}

type closeResultSetCommand struct {
	command
	ResultSetHandles []int      `json:"resultSetHandles"`
	Attributes       attributes `json:"attributes,omitempty"`
}

type closePreparedStatementCommand struct {
	command
	StatementHandle int        `json:"statementHandle"`
	Attributes      attributes `json:"attributes,omitempty"`
}

type createPreparedStatementCommand struct {
	command
	SQLText    string     `json:"sqlText"`
	Attributes attributes `json:"attributes,omitempty"`
}

type fetchCommand struct {
	command
	ResultSetHandle int `json:"resultSetHandle"`
	StartPosition   int `json:"startPosition"`
	NumBytes        int `json:"numBytes"`
}

type executePreparedStatementCommand struct {
	command
	StatementHandle int              `json:"statementHandle"`
	NumColumns      int              `json:"numColumns,omitempty"`
	NumRows         int              `json:"numRows"`
	Columns         []SQLQueryColumn `json:"columns,omitempty"`
	Data            [][]interface{}  `json:"data"`
	Attributes      attributes       `json:"attributes,omitempty"`
}

type attributes struct {
	Autocommit                  *bool  `json:"autocommit,omitempty"`
	CompressionEnabled          *bool  `json:"compressionEnabled,omitempty"`
	CurrentSchema               string `json:"currentSchema,omitempty"`
	DateFormat                  string `json:"dateFormat,omitempty"`
	DateLanguage                string `json:"dateLanguage,omitempty"`
	DatetimeFormat              string `json:"datetimeFormat,omitempty"`
	DefaultLikeEscapeCharacter  string `json:"defaultLikeEscapeCharacter,omitempty"`
	FeedbackInterval            int    `json:"feedbackInterval,omitempty"`
	NumericCharacters           string `json:"numericCharacters,omitempty"`
	OpenTransaction             *bool  `json:"openTransaction,omitempty"`
	QueryTimeout                int    `json:"queryTimeout,omitempty"`
	SnapshotTransactionsEnabled *bool  `json:"snapshotTransactionsEnabled,omitempty"`
	TimestampUtcEnabled         *bool  `json:"timestampUtcEnabled,omitempty"`
	Timezone                    string `json:"timezone,omitempty"`
	TimeZoneBehavior            string `json:"timeZoneBehavior,omitempty"`
	ResultSetMaxRows            int    `json:"resultSetMaxRows,omitempty"`
}

type authCommand struct {
	Username         string     `json:"username,omitempty"`
	Password         string     `json:"password,omitempty"`
	AccessToken      string     `json:"accessToken,omitempty"`
	RefreshToken     string     `json:"refreshToken,omitempty"`
	UseCompression   bool       `json:"useCompression"`
	SessionID        int        `json:"sessionId,omitempty"`
	ClientName       string     `json:"clientName,omitempty"`
	DriverName       string     `json:"driverName,omitempty"`
	ClientOs         string     `json:"clientOs,omitempty"`
	ClientOsUsername string     `json:"clientOsUsername,omitempty"`
	ClientLanguage   string     `json:"clientLanguage,omitempty"`
	ClientVersion    string     `json:"clientVersion,omitempty"`
	ClientRuntime    string     `json:"clientRuntime,omitempty"`
	Attributes       attributes `json:"attributes,omitempty"`
}

type sqlCommand struct {
	command
	SQLText    string     `json:"sqlText"`
	Attributes attributes `json:"attributes,omitempty"`
}
