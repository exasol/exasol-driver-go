package exasol

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"database/sql/driver"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math/big"
	mathRand "math/rand"
	"os"
	"os/user"
	"runtime"
	"strconv"
	"time"

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
	if c.isClosed {
		errorLogger.Print(ErrClosed)
		return nil, driver.ErrBadConn
	}
	response := &CreatePreparedStatementResponse{}
	err := c.createPreparedStatement(ctx, query, response)
	if err != nil {
		return nil, err
	}
	return c.createStatement(response), nil
}

func (c *connection) createPreparedStatement(ctx context.Context, query string, response *CreatePreparedStatementResponse) error {
	return c.send(ctx, &CreatePreparedStatementCommand{
		Command: Command{"createPreparedStatement"},
		SQLText: query,
	}, response)
}

func (c *connection) createStatement(result *CreatePreparedStatementResponse) *statement {
	return &statement{
		connection:      c,
		statementHandle: result.StatementHandle,
		numInput:        result.ParameterData.NumColumns,
		columns:         result.ParameterData.Columns,
	}
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
	if c.config.autocommit {
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
		return c.executeSimpleWithRows(ctx, query)
	}

	response := &CreatePreparedStatementResponse{}
	err := c.createPreparedStatement(ctx, query, response)
	if err != nil {
		return nil, err
	}

	result, err := c.executePreparedStatement(ctx, response, args)
	if err != nil {
		return nil, err
	}
	return toRow(result, c)
}

func (c *connection) executeSimpleWithRows(ctx context.Context, query string) (driver.Rows, error) {
	result, err := c.simpleExec(ctx, query)
	if err != nil {
		return nil, err
	}
	return toRow(result, c)
}

func (c *connection) executePreparedStatement(ctx context.Context, s *CreatePreparedStatementResponse, args []driver.Value) (*SQLQueriesResponse, error) {
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
			ResultSetMaxRows: c.config.resultSetMaxRows,
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

	if isImportQuery(query) {
		var err error
		query, err = c.handleImportQuery(query)
		if err != nil {
			return nil, err
		}
	}

	// No values provided, simple execute is enough
	if len(args) == 0 {
		return c.executeSimpleWithResult(ctx, query)
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

func (c *connection) handleImportQuery(query string) (string, error) {
	paths, err := getFilePaths(query)
	if err != nil {
		return "", err
	}

	hosts, err := resolveHosts(c.config.host)
	if err != nil {
		return "", err
	}
	mathRand.Seed(time.Now().UnixNano())
	mathRand.Shuffle(len(hosts), func(i, j int) {
		hosts[i], hosts[j] = hosts[j], hosts[i]
	})

	p, err := newProxy(c.config.host, c.config.port)
	if err != nil {
		return "", err
	}
	defer p.close()

	err = p.startProxy()
	if err != nil {
		return "", err
	}
	query = updateImportQuery(query, p)

	var files []*os.File
	for _, path := range paths {
		f, err := openFile(path)
		if err != nil {
			return "", err
		}
		files = append(files, f)
	}

	err = p.write(files, getRowSeparator(query))
	if err != nil {
		return "", err
	}

	return query, nil
}

func (c *connection) executeSimpleWithResult(ctx context.Context, query string) (driver.Result, error) {
	result, err := c.simpleExec(ctx, query)
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
			ResultSetMaxRows: c.config.resultSetMaxRows,
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
	hasCompression := c.config.compression
	c.config.compression = false

	authRequest, err := c.preLogin(ctx, hasCompression)
	if err != nil {
		return err
	}

	if osUser, err := user.Current(); err == nil && osUser != nil {
		authRequest.ClientOsUsername = osUser.Username
	} else {
		logCouldNotGetOsUser(err)
	}
	authResponse := &AuthResponse{}
	err = c.send(ctx, authRequest, authResponse)
	if err != nil {
		return err
	}
	c.isClosed = false
	c.config.compression = hasCompression

	return nil
}

func (c *connection) preLogin(ctx context.Context, compression bool) (*AuthCommand, error) {
	authRequest := &AuthCommand{
		UseCompression: false,
		ClientName:     c.config.clientName,
		DriverName:     fmt.Sprintf("exasol-driver-go %s", driverVersion),
		ClientOs:       runtime.GOOS,
		ClientVersion:  "(unknown version)",
		ClientRuntime:  runtime.Version(),
		Attributes: Attributes{
			Autocommit:         boolToPtr(c.config.autocommit),
			CurrentSchema:      c.config.schema,
			CompressionEnabled: boolToPtr(compression),
		},
	}
	if c.config.accessToken != "" {
		err := c.prepareLoginViaToken(ctx)
		if err != nil {
			return nil, err
		}
		authRequest.AccessToken = c.config.accessToken
	} else if c.config.refreshToken != "" {
		err := c.prepareLoginViaToken(ctx)
		if err != nil {
			return nil, err
		}
		authRequest.RefreshToken = c.config.refreshToken
	} else {
		password, err := c.prepareLoginViaPassword(ctx)
		if err != nil {
			return nil, err
		}
		authRequest.Username = c.config.user
		authRequest.Password = password
	}
	return authRequest, nil
}

func (c *connection) prepareLoginViaPassword(ctx context.Context) (string, error) {
	loginCommand := &LoginCommand{
		Command:         Command{"login"},
		ProtocolVersion: c.config.apiVersion,
	}
	loginResponse := &PublicKeyResponse{}
	err := c.send(ctx, loginCommand, loginResponse)
	if err != nil {
		return "", err
	}

	pubKeyMod, _ := hex.DecodeString(loginResponse.PublicKeyModulus)
	var modulus big.Int
	modulus.SetBytes(pubKeyMod)

	pubKeyExp, _ := strconv.ParseUint(loginResponse.PublicKeyExponent, 16, 32)

	pubKey := rsa.PublicKey{
		N: &modulus,
		E: int(pubKeyExp),
	}
	password := []byte(c.config.password)
	encPass, err := rsa.EncryptPKCS1v15(rand.Reader, &pubKey, password)
	if err != nil {
		logPasswordEncryptionError(err)
		return "", driver.ErrBadConn
	}
	return base64.StdEncoding.EncodeToString(encPass), nil
}

func (c *connection) prepareLoginViaToken(ctx context.Context) error {
	c.config.compression = false
	loginCommand := &LoginTokenCommand{
		Command:         Command{"loginToken"},
		ProtocolVersion: c.config.apiVersion,
	}
	err := c.send(ctx, loginCommand, nil)
	if err != nil {
		return err
	}
	return nil
}
