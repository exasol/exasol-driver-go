package connection

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"database/sql/driver"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math/big"
	"os/user"
	"runtime"
	"strconv"

	"github.com/exasol/exasol-driver-go/internal/config"
	"github.com/exasol/exasol-driver-go/internal/utils"
	"github.com/exasol/exasol-driver-go/internal/version"
	"github.com/exasol/exasol-driver-go/pkg/connection/wsconn"
	"github.com/exasol/exasol-driver-go/pkg/errors"
	"github.com/exasol/exasol-driver-go/pkg/logger"
	"github.com/exasol/exasol-driver-go/pkg/types"
	"golang.org/x/sync/errgroup"
)

type Connection struct {
	Config    *config.Config
	websocket wsconn.WebsocketConnection
	Ctx       context.Context
	IsClosed  bool
}

func (c *Connection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	values, err := utils.NamedValuesToValues(args)
	if err != nil {
		return nil, err
	}
	return c.query(ctx, query, values)
}

func (c *Connection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	values, err := utils.NamedValuesToValues(args)
	if err != nil {
		return nil, err
	}
	return c.exec(ctx, query, values)
}

func (c *Connection) Exec(query string, args []driver.Value) (driver.Result, error) {
	return c.exec(c.Ctx, query, args)
}

func (c *Connection) Query(query string, args []driver.Value) (driver.Rows, error) {
	return c.query(c.Ctx, query, args)
}

func (c *Connection) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if c.IsClosed {
		logger.ErrorLogger.Print(errors.ErrClosed)
		return nil, driver.ErrBadConn
	}

	response, err := c.createPreparedStatement(ctx, query)
	if err != nil {
		return nil, err
	}
	return c.createStatement(ctx, response), nil
}

func (c *Connection) createPreparedStatement(ctx context.Context, query string) (*types.CreatePreparedStatementResponse, error) {
	response := &types.CreatePreparedStatementResponse{}

	err := c.Send(ctx, &types.CreatePreparedStatementCommand{
		Command: types.Command{Command: "createPreparedStatement"},
		SQLText: query,
	}, response)

	if err != nil {
		return nil, err
	}

	return response, nil
}

func (c *Connection) createStatement(ctx context.Context, result *types.CreatePreparedStatementResponse) *Statement {
	return NewStatement(ctx, c, result)
}

func (c *Connection) Ping(ctx context.Context) error {
	fmt.Printf("Ping\n")
	// FIXME
	return nil
}

func (c *Connection) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(c.Ctx, query)
}

func (c *Connection) Close() error {
	return c.close(c.Ctx)
}

func (c *Connection) Begin() (driver.Tx, error) {
	if c.IsClosed {
		logger.ErrorLogger.Print(errors.ErrClosed)
		return nil, driver.ErrBadConn
	}
	if c.Config.Autocommit {
		return nil, errors.ErrAutocommitEnabled
	}
	return NewTransaction(c.Ctx, c), nil
}

func (c *Connection) query(ctx context.Context, query string, args []driver.Value) (driver.Rows, error) {
	if c.IsClosed {
		logger.ErrorLogger.Print(errors.ErrClosed)
		return nil, driver.ErrBadConn
	}

	// No values provided, simple execute is enough
	if len(args) == 0 {
		return c.executeSimpleWithRows(ctx, query)
	}

	response, err := c.createPreparedStatement(ctx, query)
	if err != nil {
		return nil, err
	}

	result, err := c.executePreparedStatement(ctx, response, args)
	if err != nil {
		return nil, err
	}
	return ToRow(ctx, result, c)
}

func (c *Connection) executeSimpleWithRows(ctx context.Context, query string) (driver.Rows, error) {
	result, err := c.SimpleExec(ctx, query)
	if err != nil {
		return nil, err
	}
	return ToRow(ctx, result, c)
}

func (c *Connection) executePreparedStatement(ctx context.Context, s *types.CreatePreparedStatementResponse, args []driver.Value) (*types.SqlQueriesResponse, error) {
	columns := s.ParameterData.Columns
	if len(args)%len(columns) != 0 {
		return nil, errors.ErrInvalidValuesCount
	}

	data := make([][]interface{}, len(columns))
	for i, arg := range args {
		index := i % len(columns)
		if data[index] == nil {
			data[index] = make([]interface{}, 0)
		}
		data[index] = append(data[index], arg)
	}

	command := &types.ExecutePreparedStatementCommand{
		Command:         types.Command{Command: "executePreparedStatement"},
		StatementHandle: s.StatementHandle,
		Columns:         columns,
		NumColumns:      len(columns),
		NumRows:         len(data[0]),
		Data:            data,
		Attributes: types.Attributes{
			ResultSetMaxRows: c.Config.ResultSetMaxRows,
		},
	}
	result := &types.SqlQueriesResponse{}
	err := c.Send(ctx, command, result)
	if err != nil {
		return nil, err
	}
	if result.NumResults == 0 {
		logger.ErrorLogger.Printf("Got empty result of type %t: %v", result, result)
		return nil, errors.ErrMalformedData
	}
	return result, c.closePreparedStatement(ctx, s)
}

func (c *Connection) closePreparedStatement(ctx context.Context, s *types.CreatePreparedStatementResponse) error {
	return c.Send(ctx, &types.ClosePreparedStatementCommand{
		Command:         types.Command{Command: "closePreparedStatement"},
		StatementHandle: s.StatementHandle,
	}, nil)
}

func (c *Connection) exec(ctx context.Context, query string, args []driver.Value) (driver.Result, error) {
	if c.IsClosed {
		logger.ErrorLogger.Print(errors.ErrClosed)
		return nil, driver.ErrBadConn
	}
	result := make(chan driver.Result, 1)
	errs, errctx := errgroup.WithContext(ctx)

	if utils.IsImportQuery(query) {
		importStatement, err := NewImportStatement(query, c.Config.Host, c.Config.Port)
		if err != nil {
			return nil, err
		}

		defer importStatement.Close()
		query = importStatement.GetUpdatedQuery()
		errs.Go(func() error { return importStatement.UploadFiles(errctx) })
	}
	// No values provided, simple execute is enough
	if len(args) == 0 {
		errs.Go(c.executeSimpleWrapper(errctx, query, result))
	} else {
		errs.Go(c.executePreparedStatementWrapper(errctx, query, args, result))
	}
	err := errs.Wait()
	close(result)

	if err != nil {
		return nil, err
	}

	return <-result, nil
}

func (c *Connection) executeSimpleWrapper(ctx context.Context, query string, result chan driver.Result) func() error {
	return func() error {
		r, err := c.executeSimpleWithResult(ctx, query)
		if err != nil {
			return err
		}
		result <- r
		return nil
	}
}

func (c *Connection) executePreparedStatementWrapper(ctx context.Context, query string, args []driver.Value, result chan driver.Result) func() error {
	return func() error {
		prepResponse := &types.CreatePreparedStatementResponse{}
		err := c.Send(ctx, &types.CreatePreparedStatementCommand{
			Command: types.Command{Command: "createPreparedStatement"},
			SQLText: query,
		}, prepResponse)
		if err != nil {
			return err
		}
		resp, err := c.executePreparedStatement(ctx, prepResponse, args)
		if err != nil {
			return err
		}
		r, err := ToResult(resp)
		if err != nil {
			return err
		}
		result <- r
		return nil
	}
}

func (c *Connection) executeSimpleWithResult(ctx context.Context, query string) (driver.Result, error) {
	result, err := c.SimpleExec(ctx, query)
	if err != nil {
		return nil, err
	}
	return ToResult(result)
}

func (c *Connection) SimpleExec(ctx context.Context, query string) (*types.SqlQueriesResponse, error) {
	command := &types.SqlCommand{
		Command: types.Command{Command: "execute"},
		SQLText: query,
		Attributes: types.Attributes{
			ResultSetMaxRows: c.Config.ResultSetMaxRows,
		},
	}
	result := &types.SqlQueriesResponse{}
	err := c.Send(ctx, command, result)
	if err != nil {
		return nil, err
	}
	if result.NumResults == 0 {
		logger.ErrorLogger.Printf("Got empty result of type %t: %v", result, result)
		return nil, errors.ErrMalformedData
	}
	return result, err
}

func (c *Connection) close(ctx context.Context) error {
	c.IsClosed = true
	err := c.Send(ctx, &types.Command{Command: "disconnect"}, nil)
	closeError := c.websocket.Close()
	c.websocket = nil
	if err != nil {
		return err
	}
	if closeError != nil {
		return fmt.Errorf("failed to close websocket: %w", closeError)
	}
	return nil
}

func (c *Connection) Login(ctx context.Context) error {
	hasCompression := c.Config.Compression
	c.Config.Compression = false

	authRequest, err := c.preLogin(ctx, hasCompression)
	if err != nil {
		return err
	}

	if osUser, err := user.Current(); err == nil && osUser != nil {
		authRequest.ClientOsUsername = osUser.Username
	} else {
		logger.ErrorLogger.Print(errors.NewCouldNotGetOsUser(err))
	}
	authResponse := &types.AuthResponse{}
	err = c.Send(ctx, authRequest, authResponse)
	c.Config.Compression = hasCompression
	if err != nil {
		c.IsClosed = true
		return fmt.Errorf("failed to login: %w", err)
	}
	c.IsClosed = false

	return nil
}

func (c *Connection) preLogin(ctx context.Context, compression bool) (*types.AuthCommand, error) {
	authRequest := &types.AuthCommand{
		UseCompression: false,
		ClientName:     c.Config.ClientName,
		DriverName:     fmt.Sprintf("exasol-driver-go %s", version.DriverVersion),
		ClientOs:       runtime.GOOS,
		ClientVersion:  "(unknown version)",
		ClientRuntime:  runtime.Version(),
		Attributes: types.Attributes{
			Autocommit:         utils.BoolToPtr(c.Config.Autocommit),
			CurrentSchema:      c.Config.Schema,
			CompressionEnabled: utils.BoolToPtr(compression),
			QueryTimeout:       c.Config.QueryTimeout,
		},
	}
	if c.Config.AccessToken != "" {
		err := c.prepareLoginViaToken(ctx)
		if err != nil {
			return nil, fmt.Errorf("access token login failed: %w", err)
		}
		authRequest.AccessToken = c.Config.AccessToken
	} else if c.Config.RefreshToken != "" {
		err := c.prepareLoginViaToken(ctx)
		if err != nil {
			return nil, fmt.Errorf("refresh token login failed: %w", err)
		}
		authRequest.RefreshToken = c.Config.RefreshToken
	} else {
		password, err := c.prepareLoginViaPassword(ctx)
		if err != nil {
			return nil, err
		}
		authRequest.Username = c.Config.User
		authRequest.Password = password
	}
	return authRequest, nil
}

func (c *Connection) prepareLoginViaPassword(ctx context.Context) (string, error) {
	loginCommand := &types.LoginCommand{
		Command:         types.Command{Command: "login"},
		ProtocolVersion: c.Config.ApiVersion,
	}
	loginResponse := &types.PublicKeyResponse{}
	err := c.Send(ctx, loginCommand, loginResponse)
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
	password := []byte(c.Config.Password)
	encPass, err := rsa.EncryptPKCS1v15(rand.Reader, &pubKey, password)
	if err != nil {
		logger.ErrorLogger.Print(errors.NewPasswordEncryptionError(err))
		return "", driver.ErrBadConn
	}
	return base64.StdEncoding.EncodeToString(encPass), nil
}

func (c *Connection) prepareLoginViaToken(ctx context.Context) error {
	c.Config.Compression = false
	loginCommand := &types.LoginTokenCommand{
		Command:         types.Command{Command: "loginToken"},
		ProtocolVersion: c.Config.ApiVersion,
	}
	err := c.Send(ctx, loginCommand, nil)
	if err != nil {
		return err
	}
	return nil
}
