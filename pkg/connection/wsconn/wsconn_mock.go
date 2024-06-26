package wsconn

import (
	"bytes"
	"compress/zlib"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/exasol/exasol-driver-go/pkg/types"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/mock"
)

var LOG = log.New(os.Stderr, "[wsmock] ", log.LstdFlags)

type WebsocketConnectionMock struct {
	mock.Mock
}

func CreateWebsocketConnectionMock() *WebsocketConnectionMock {
	mock := &WebsocketConnectionMock{}
	LOG.Printf("Created new WebsocketConnectionMock: %v", mock)
	return mock
}

func (mock *WebsocketConnectionMock) SimulateSQLQueriesResponse(request interface{}, results interface{}) {
	mock.SimulateResponse(request, baseOKResponse(types.SqlQueriesResponse{NumResults: 1, Results: []json.RawMessage{JsonMarshall(results)}}))
}

func (mock *WebsocketConnectionMock) SimulateOKResponse(request interface{}, response interface{}) {
	mock.SimulateResponse(request, baseOKResponse(response))
}

func (wsMock *WebsocketConnectionMock) SimulateOKResponseOnAnyMessage(response interface{}) {
	wsMock.OnWriteAnyMessage(nil)
	if response != nil {
		wsMock.OnReadTextMessage(JsonMarshall(baseOKResponse(response)), nil)
	}
}

func (mock *WebsocketConnectionMock) SimulateErrorResponse(request interface{}, exception types.Exception) {
	mock.SimulateResponse(request, baseErrorResponse(exception))
}

func (wsMock *WebsocketConnectionMock) SimulateErrorResponseOnAnyMessage(exception types.Exception) {
	wsMock.OnWriteAnyMessage(nil)
	wsMock.OnReadTextMessage(JsonMarshall(baseErrorResponse(exception)), nil)
}

func baseOKResponse(payload interface{}) types.BaseResponse {
	return types.BaseResponse{Status: "ok", ResponseData: JsonMarshall(payload)}
}

func baseErrorResponse(exception types.Exception) types.BaseResponse {
	return types.BaseResponse{Status: "notok", Exception: &exception}
}

func JsonMarshall(payload interface{}) json.RawMessage {
	data, err := json.Marshal(payload)
	if err != nil {
		panic(fmt.Errorf("failed to marshal data %v: %w", payload, err))
	}
	return data
}

func (wsMock *WebsocketConnectionMock) SimulateWriteFails(request interface{}, err error) {
	wsMock.OnWriteTextMessage(JsonMarshall(request), err)
}

func (wsMock *WebsocketConnectionMock) SimulateResponse(request interface{}, response interface{}) {
	wsMock.OnWriteTextMessage(JsonMarshall(request), nil)
	if response != nil {
		wsMock.OnReadTextMessage(JsonMarshall(response), nil)
	}
}

func (wsMock *WebsocketConnectionMock) OnWriteAnyMessage(returnedError error) {
	wsMock.On("WriteMessage", mock.Anything, mock.Anything).Return(returnedError).Once()
}

func (mock *WebsocketConnectionMock) OnWriteTextMessage(data []byte, returnedError error) {
	LOG.Printf("Expect ws.WriteMessage(%d, `%s`) -> return (%v)", websocket.TextMessage, string(data), returnedError)
	mock.On("WriteMessage", websocket.TextMessage, data).Return(returnedError).Once()
}

func (mock *WebsocketConnectionMock) OnWriteCompressedMessage(data []byte, returnedError error) {
	LOG.Printf("Expect compressed ws.WriteMessage(%d, `%s`) -> return (%v)", websocket.BinaryMessage, string(data), returnedError)
	mock.On("WriteMessage", websocket.BinaryMessage, compress(data)).Return(returnedError).Once()
}

func (mock *WebsocketConnectionMock) OnReadTextMessage(data []byte, returnedError error) {
	LOG.Printf("Expect ws.ReadMessage() -> return (%d, `%s`, %v)", websocket.TextMessage, string(data), returnedError)
	mock.On("ReadMessage").Return(websocket.TextMessage, data, returnedError).Once()
}

func (mock *WebsocketConnectionMock) OnReadCompressedMessage(data []byte, returnedError error) {
	LOG.Printf("Expect compressed ws.ReadMessage() -> return (%d, `%s`, %v)", websocket.BinaryMessage, string(data), returnedError)
	mock.On("ReadMessage").Return(websocket.BinaryMessage, compress(data), returnedError).Once()
}

func (mock *WebsocketConnectionMock) OnClose(returnedError error) {
	LOG.Printf("Expect ws.Close() -> return (%v)", returnedError)
	mock.On("Close").Return(returnedError)
}

func (mock *WebsocketConnectionMock) WriteMessage(messageType int, data []byte) error {
	LOG.Printf("Mock call: ws.WriteMessage(%d, `%s`)", messageType, string(data))
	mockArgs := mock.Called(messageType, data)
	err := mockArgs.Error(0)
	LOG.Printf("Mock call: ws.WriteMessage(%d, `%s`) -> return %v", messageType, string(data), err)
	return err
}

func (mock *WebsocketConnectionMock) ReadMessage() (messageType int, response []byte, err error) {
	LOG.Printf("Mock call: ws.ReadMessage()")
	mockArgs := mock.Called()
	responseData := mockArgs.Get(1).([]byte)
	messageType = mockArgs.Int(0)
	err = mockArgs.Error(2)
	LOG.Printf("Mock call: ws.ReadMessage() -> return (%d, %q, %v)", messageType, string(responseData), err)
	return messageType, responseData, err
}

func (mock *WebsocketConnectionMock) Close() error {
	LOG.Printf("Mock call: ws.Close()")
	mockArgs := mock.Called()
	LOG.Printf("Mock call: ws.Close() -> return (%v)", mockArgs.Error(0))
	return mockArgs.Error(0)
}

func compress(data []byte) []byte {
	var buffer bytes.Buffer
	writer := zlib.NewWriter(&buffer)
	_, err := writer.Write(data)
	if err != nil {
		panic(fmt.Errorf("failed to compress data %v", data))
	}
	writer.Close()
	return buffer.Bytes()
}
