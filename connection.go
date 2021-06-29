package exasol

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"database/sql/driver"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"log"
	"math/big"
	"os/user"
	"runtime"
	"strconv"

	"github.com/gorilla/websocket"
)

type connection struct {
	config    *config
	websocket *websocket.Conn
	ctx       context.Context
	isClosed  bool
}

func (c *connection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	values, err := namedValuesToValues(args)
	if err != nil {
		return nil, err
	}
	return c.query(ctx, query, values)
}

func (c *connection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	values, err := namedValuesToValues(args)
	if err != nil {
		return nil, err
	}
	return c.exec(ctx, query, values)
}

func (c *connection) Exec(query string, args []driver.Value) (driver.Result, error) {
	return c.exec(context.Background(), query, args)
}

func (c *connection) Query(query string, args []driver.Value) (driver.Rows, error) {
	return c.query(context.Background(), query, args)
}

func (c *connection) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	log.Printf("Prepare")
	if c.isClosed {
		errorLogger.Print(ErrClosed)
		return nil, driver.ErrBadConn
	}

	result := &CreatePreparedStatementResponse{}

	err := c.send(ctx, &CreatePreparedStatementCommand{
		Command: Command{"createPreparedStatement"},
		SQLText: query,
	}, result)
	if err != nil {
		return nil, err
	}

	return &statement{
		connection:      c,
		statementHandle: result.StatementHandle,
		numInput:        result.ParameterData.NumColumns,
		columns:         result.ParameterData.Columns,
	}, nil
}

func (c *connection) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *connection) Close() error {
	return c.close(context.Background())
}

func (c *connection) Begin() (driver.Tx, error) {
	if c.isClosed {
		errorLogger.Print(ErrClosed)
		return nil, driver.ErrBadConn
	}
	if c.config.Autocommit {
		return nil, ErrAutocommitEnabled
	}
	return &transaction{
		connection: c,
	}, nil
}

func (c *connection) query(ctx context.Context, query string, args []driver.Value) (driver.Rows, error) {
	if c.isClosed {
		errorLogger.Print(ErrClosed)
		return nil, driver.ErrBadConn
	}

	// No values provided, simple execute is enough
	if len(args) == 0 {
		result, err := c.simpleExec(ctx, query)
		if err != nil {
			return nil, err
		}
		return toRow(result, c)
	}

	prepResponse := &CreatePreparedStatementResponse{}

	err := c.send(ctx, &CreatePreparedStatementCommand{
		Command: Command{"createPreparedStatement"},
		SQLText: query,
	}, prepResponse)
	if err != nil {
		return nil, err
	}

	result, err := c.executePreparedStatement(ctx, prepResponse, args)
	if err != nil {
		return nil, err
	}
	return toRow(result, c)
}

func (c *connection) executePreparedStatement(ctx context.Context, s *CreatePreparedStatementResponse, args []driver.Value) (*SQLQueriesResponse, error) {
	log.Println("executePreparedStatement")
	columns := s.ParameterData.Columns
	if len(args)%len(columns) != 0 {
		return nil, ErrInvalidValuesCount
	}

	data := make([][]interface{}, len(columns))
	for i, arg := range args {
		if data[i%len(columns)] == nil {
			data[i%len(columns)] = make([]interface{}, 0)
		}
		data[i%len(columns)] = append(data[i%len(columns)], arg)
	}

	command := &ExecutePreparedStatementCommand{
		Command:         Command{"executePreparedStatement"},
		StatementHandle: s.StatementHandle,
		Columns:         columns,
		NumColumns:      len(columns),
		NumRows:         len(data[0]),
		Data:            data,
		Attributes: Attributes{
			ResultSetMaxRows: c.config.ResultSetMaxRows,
		},
	}
	result := &SQLQueriesResponse{}
	err := c.send(ctx, command, result)
	if err != nil {
		return nil, err
	}
	if result.NumResults == 0 {
		return nil, ErrMalformedData
	}

	return result, c.closePreparedStatement(ctx, s)
}

func (c *connection) closePreparedStatement(ctx context.Context, s *CreatePreparedStatementResponse) error {
	return c.send(ctx, &ClosePreparedStatementCommand{
		Command:         Command{"closePreparedStatement"},
		StatementHandle: s.StatementHandle,
	}, nil)
}

func (c *connection) exec(ctx context.Context, query string, args []driver.Value) (driver.Result, error) {
	if c.isClosed {
		errorLogger.Print(ErrClosed)
		return nil, driver.ErrBadConn
	}

	// No values provided, simple execute is enough
	if len(args) == 0 {
		result, err := c.simpleExec(ctx, query)
		if err != nil {
			return nil, err
		}
		return toResult(result)
	}

	prepResponse := &CreatePreparedStatementResponse{}

	err := c.send(ctx, &CreatePreparedStatementCommand{
		Command: Command{"createPreparedStatement"},
		SQLText: query,
	}, prepResponse)
	if err != nil {
		return nil, err
	}

	result, err := c.executePreparedStatement(ctx, prepResponse, args)
	if err != nil {
		return nil, err
	}
	return toResult(result)
}

func (c *connection) simpleExec(ctx context.Context, query string) (*SQLQueriesResponse, error) {
	command := &SQLCommand{
		Command: Command{"execute"},
		SQLText: query,
		Attributes: Attributes{
			ResultSetMaxRows: c.config.ResultSetMaxRows,
		},
	}
	result := &SQLQueriesResponse{}
	err := c.send(ctx, command, result)
	if err != nil {
		return nil, err
	}
	if result.NumResults == 0 {
		return nil, ErrMalformedData
	}
	return result, err
}

func (c *connection) close(ctx context.Context) error {
	c.isClosed = true
	err := c.send(ctx, &Command{Command: "disconnect"}, nil)
	c.websocket.Close()
	c.websocket = nil
	return err
}

func (c *connection) login(ctx context.Context) error {
	hasCompression := c.config.Compression
	c.config.Compression = false
	loginCommand := &LoginCommand{
		Command:         Command{"login"},
		ProtocolVersion: c.config.ApiVersion,
	}
	loginResponse := &PublicKeyResponse{}
	err := c.send(ctx, loginCommand, loginResponse)
	if err != nil {
		return err
	}

	pubKeyMod, _ := hex.DecodeString(loginResponse.PublicKeyModulus)
	var modulus big.Int
	modulus.SetBytes(pubKeyMod)

	pubKeyExp, _ := strconv.ParseUint(loginResponse.PublicKeyExponent, 16, 32)

	pubKey := rsa.PublicKey{
		N: &modulus,
		E: int(pubKeyExp),
	}
	password := []byte(c.config.Password)
	encPass, err := rsa.EncryptPKCS1v15(rand.Reader, &pubKey, password)
	if err != nil {
		errorLogger.Printf("password encryption error: %s", err)
		return driver.ErrBadConn
	}
	b64Pass := base64.StdEncoding.EncodeToString(encPass)

	authRequest := AuthCommand{

		Username:       c.config.User,
		Password:       b64Pass,
		UseCompression: false,
		ClientName:     c.config.ClientName,
		DriverName:     fmt.Sprintf("exasol-driver-go %s", driverVersion),
		ClientOs:       runtime.GOOS,
		ClientVersion:  c.config.ClientName,
		ClientRuntime:  runtime.Version(),
		Attributes: Attributes{
			Autocommit:         c.config.Autocommit,
			CurrentSchema:      c.config.Schema,
			CompressionEnabled: hasCompression,
		},
	}

	if osUser, err := user.Current(); err != nil {
		authRequest.ClientOsUsername = osUser.Username
	}

	authResponse := &AuthResponse{}
	err = c.send(ctx, authRequest, authResponse)
	if err != nil {
		return err
	}
	c.isClosed = false
	c.config.Compression = hasCompression

	return nil
}
