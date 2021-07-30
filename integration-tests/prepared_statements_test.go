package integration_tests

import (
	"bytes"
	"fmt"
	client2 "github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/riptano/cloud-gate/integration-tests/client"
	"github.com/riptano/cloud-gate/integration-tests/setup"
	"github.com/riptano/cloud-gate/integration-tests/simulacron"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func TestPreparedIdProxyCacheMiss(t *testing.T) {

	simulacronSetup, err := setup.NewSimulacronTestSetup()
	require.Nil(t, err)
	defer simulacronSetup.Cleanup()

	testClient, err := client.NewTestClient("127.0.0.1:14002")
	require.True(t, err == nil, "testClient setup failed: %s", err)

	defer testClient.Shutdown()

	err = testClient.PerformDefaultHandshake(primitive.ProtocolVersion4, false)
	require.True(t, err == nil, "No-auth handshake failed: %s", err)

	preparedId := []byte{143, 7, 36, 50, 225, 104, 157, 89, 199, 177, 239, 231, 82, 201, 142, 253}

	executeMsg := &message.Execute{
		QueryId:          preparedId,
		ResultMetadataId: nil,
	}
	response, requestStreamId, err := testClient.SendMessage(primitive.ProtocolVersion4, executeMsg)
	require.True(t, err == nil, "execute request send failed: %s", err)
	require.True(t, response != nil, "response received was null")

	errorResponse, ok := response.Body.Message.(message.Error)
	require.True(t, ok, fmt.Sprintf("expected error result but got %02x", response.Body.Message.GetOpCode()))
	require.Equal(t, requestStreamId, response.Header.StreamId, "streamId does not match expected value.")
	require.True(t, err == nil, "Error response could not be parsed: %s", err)
	require.Equal(t, primitive.ErrorCodeUnprepared, errorResponse.GetErrorCode(), "Error code received was not Unprepared.")
	require.Equal(t, "Prepared query with ID 8f072432e1689d59c7b1efe752c98efd not found "+
		"(either the query was not prepared on this host (maybe the host has been restarted?) "+
		"or you have prepared too many queries and it has been evicted from the internal cache)",
		errorResponse.GetErrorMessage(),
		"Unexpected error message.")

	unprepared, ok := errorResponse.(*message.Unprepared)
	require.True(t, ok, fmt.Sprintf("expected unprepared but got %T", errorResponse))
	require.Equal(t, preparedId, unprepared.Id, "Error body did not contain the expected preparedId.")

}

func TestPreparedIdPreparationMismatch(t *testing.T) {

	simulacronSetup, err := setup.NewSimulacronTestSetup()
	require.Nil(t, err)
	defer simulacronSetup.Cleanup()

	testClient, err := client.NewTestClient("127.0.0.1:14002")
	require.True(t, err == nil, "testClient setup failed: %s", err)

	defer testClient.Shutdown()

	err = testClient.PerformDefaultHandshake(primitive.ProtocolVersion4, false)
	require.True(t, err == nil, "No-auth handshake failed: %s", err)

	tests := map[string]*simulacron.Cluster{
		"origin": simulacronSetup.Origin,
		"target": simulacronSetup.Target,
	}

	for name, cluster := range tests {
		t.Run(name, func(t *testing.T) {

			err := simulacronSetup.Origin.ClearPrimes()
			require.True(t, err == nil, "clear primes failed on origin: %s", err)

			err = simulacronSetup.Target.ClearPrimes()
			require.True(t, err == nil, "clear primes failed on target: %s", err)

			prepareMsg := &message.Prepare{
				Query:    "INSERT INTO ks1.table1 (c1, c2) VALUES (1, 2)",
				Keyspace: "",
			}

			response, requestStreamId, err := testClient.SendMessage(primitive.ProtocolVersion4, prepareMsg)
			require.True(t, err == nil, "prepare request send failed: %s", err)

			preparedResponse, ok := response.Body.Message.(*message.PreparedResult)
			require.True(t, ok, "did not receive prepared result, got instead: %v", response.Body.Message)

			// clear primes only on selected cluster
			err = cluster.ClearPrimes()
			require.True(t, err == nil, "clear primes failed: %s", err)

			executeMsg := &message.Execute{
				QueryId:          preparedResponse.PreparedQueryId,
				ResultMetadataId: preparedResponse.ResultMetadataId,
			}

			response, requestStreamId, err = testClient.SendMessage(primitive.ProtocolVersion4, executeMsg)
			require.True(t, err == nil, "execute request send failed: %s", err)

			errorResponse, ok := response.Body.Message.(message.Error)
			require.True(t, ok, fmt.Sprintf("expected error result but got %02x", response.Body.Message.GetOpCode()))
			require.Equal(t, requestStreamId, response.Header.StreamId, "streamId does not match expected value.")
			require.True(t, err == nil, "Error response could not be parsed: %s", err)
			require.Equal(t, primitive.ErrorCodeUnprepared, errorResponse.GetErrorCode(), "Error code received was not Unprepared.")
			require.Equal(t, "Prepared query with ID 05440fe1 not found (either the query was not prepared on " +
				"this host (maybe the host has been restarted?) or you have prepared too many queries and it has been " +
				"evicted from the internal cache)",
				errorResponse.GetErrorMessage(),
				"Unexpected error message.")

			unprepared, ok := errorResponse.(*message.Unprepared)
			require.True(t, ok, fmt.Sprintf("expected unprepared but got %T", errorResponse))
			require.Equal(t, preparedResponse.PreparedQueryId, unprepared.Id, "Error body did not contain the expected preparedId.")
		})
	}
}

func TestPreparedIdReplacement(t *testing.T) {
	type test struct {
		name  string
		query string
		read  bool
	}
	tests := []test{
		{
			"reads",
			"SELECT * FROM ks1.tb1",
			true,
		},
		{
			"writes",
			"INSERT INTO ks1.tb1 (key, value) VALUES ('key', 'value')",
			false,
		}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conf := setup.NewTestConfig("127.0.1.1", "127.0.1.2")
			testSetup, err := setup.NewCqlServerTestSetup(conf, false, false, false)
			require.Nil(t, err)
			defer testSetup.Cleanup()

			originPreparedId := []byte{143, 7, 36, 50, 225, 104, 157, 89, 199, 177, 239, 231, 82, 201, 142, 253}
			targetPreparedId := []byte{142, 8, 36, 51, 225, 104, 157, 89, 199, 177, 239, 231, 82, 201, 142, 253}
			require.NotEqual(t, originPreparedId, targetPreparedId)

			originLock := &sync.Mutex{}
			originExecuteMessages := make([]*message.Execute, 0)
			originPrepareMessages := make([]*message.Prepare, 0)
			originKey := message.Column{0, 1}
			originValue := message.Column{24, 51, 2}

			targetLock := &sync.Mutex{}
			targetExecuteMessages := make([]*message.Execute, 0)
			targetPrepareMessages := make([]*message.Prepare, 0)
			targetKey := message.Column{2, 3, 4}
			targetValue := message.Column{6, 121, 23}

			testSetup.Origin.CqlServer.RequestHandlers = []client2.RequestHandler{
				client2.NewDriverConnectionInitializationHandler("origin", "dc1", func(_ string) {}),
				NewPreparedTestHandler(originLock, &originPrepareMessages, &originExecuteMessages,
					originPreparedId, originKey, originValue, map[string]interface{}{}, false)}
			testSetup.Target.CqlServer.RequestHandlers = []client2.RequestHandler{
				client2.NewDriverConnectionInitializationHandler("target", "dc1", func(_ string) {}),
				NewPreparedTestHandler(targetLock, &targetPrepareMessages, &targetExecuteMessages,
					targetPreparedId, targetKey, targetValue, map[string]interface{}{}, false)}

			err = testSetup.Start(conf, true, primitive.ProtocolVersion4)
			require.Nil(t, err)

			prepareMsg := &message.Prepare{
				Query:    test.query,
				Keyspace: "",
			}

			prepareResp, err := testSetup.Client.CqlConnection.SendAndReceive(
				frame.NewFrame(primitive.ProtocolVersion4, 10, prepareMsg))
			require.Nil(t, err)

			preparedResult, ok := prepareResp.Body.Message.(*message.PreparedResult)
			require.True(t, ok, "prepared result was type %T", preparedResult)

			require.Equal(t, originPreparedId, preparedResult.PreparedQueryId)

			executeMsg := &message.Execute{
				QueryId:          originPreparedId,
				ResultMetadataId: nil,
				Options:          &message.QueryOptions{},
			}

			executeResp, err := testSetup.Client.CqlConnection.SendAndReceive(
				frame.NewFrame(primitive.ProtocolVersion4, 20, executeMsg))
			require.Nil(t, err)

			rowsResult, ok := executeResp.Body.Message.(*message.RowsResult)
			require.True(t, ok, "rows result was type %T", rowsResult)

			require.Equal(t, 1, len(rowsResult.Data))
			require.Equal(t, 2, len(rowsResult.Data[0]))
			require.Equal(t, message.Row{originKey, originValue}, rowsResult.Data[0])
			require.NotEqual(t, message.Row{targetKey, targetValue}, rowsResult.Data[0])

			originLock.Lock()
			defer originLock.Unlock()
			require.Equal(t, 1, len(originExecuteMessages))
			require.Equal(t, 1, len(originPrepareMessages))
			require.Equal(t, originPreparedId, originExecuteMessages[0].QueryId)

			targetLock.Lock()
			defer targetLock.Unlock()
			if !test.read {
				require.Equal(t, 1, len(targetExecuteMessages))
				require.Equal(t, targetPreparedId, targetExecuteMessages[0].QueryId)
				require.NotEqual(t, executeMsg, targetExecuteMessages[0])
			} else {
				require.Equal(t, 0, len(targetExecuteMessages))
			}
			require.Equal(t, 1, len(targetPrepareMessages))

			require.Equal(t, prepareMsg, targetPrepareMessages[0])
			require.Equal(t, prepareMsg, originPrepareMessages[0])
			require.Equal(t, executeMsg, originExecuteMessages[0])
		})
	}
}

func TestUnpreparedIdReplacement(t *testing.T) {
	type test struct {
		name  string
		query string
		read  bool
		originUnprepared bool
		targetUnprepared bool
	}
	tests := []test{
		{
			"reads_origin_unprepared",
			"SELECT * FROM ks1.tb1",
			true,
			true,
			false,
		},
		{
			"writes_origin_unprepared",
			"INSERT INTO ks1.tb1 (key, value) VALUES ('key', 'value')",
			false,
			true,
			false,
		},
		{
			"writes_target_unprepared",
			"INSERT INTO ks1.tb1 (key, value) VALUES ('key', 'value')",
			false,
			false,
			true,
		},
		{
			"writes_both_unprepared",
			"INSERT INTO ks1.tb1 (key, value) VALUES ('key', 'value')",
			false,
			true,
			true,
		}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conf := setup.NewTestConfig("127.0.1.1", "127.0.1.2")
			testSetup, err := setup.NewCqlServerTestSetup(conf, false, false, false)
			require.Nil(t, err)
			defer testSetup.Cleanup()

			originPreparedId := []byte{153, 7, 36, 50, 225, 104, 157, 89, 199, 177, 239, 231, 82, 201, 142, 253}
			targetPreparedId := []byte{162, 8, 36, 51, 225, 104, 157, 89, 199, 177, 239, 231, 82, 201, 142, 253}
			require.NotEqual(t, originPreparedId, targetPreparedId)

			originLock := &sync.Mutex{}
			originExecuteMessages := make([]*message.Execute, 0)
			originPrepareMessages := make([]*message.Prepare, 0)
			originKey := message.Column{0, 1}
			originValue := message.Column{24, 51, 2}

			targetLock := &sync.Mutex{}
			targetExecuteMessages := make([]*message.Execute, 0)
			targetPrepareMessages := make([]*message.Prepare, 0)
			targetKey := message.Column{2, 3, 4}
			targetValue := message.Column{6, 121, 23}
			originCtx := map[string]interface{}{}
			targetCtx := map[string]interface{}{}

			testSetup.Origin.CqlServer.RequestHandlers = []client2.RequestHandler{
				client2.NewDriverConnectionInitializationHandler("origin", "dc1", func(_ string) {}),
				NewPreparedTestHandler(originLock, &originPrepareMessages, &originExecuteMessages,
					originPreparedId, originKey, originValue, originCtx, test.originUnprepared)}
			testSetup.Target.CqlServer.RequestHandlers = []client2.RequestHandler{
				client2.NewDriverConnectionInitializationHandler("target", "dc1", func(_ string) {}),
				NewPreparedTestHandler(targetLock, &targetPrepareMessages, &targetExecuteMessages,
					targetPreparedId, targetKey, targetValue, targetCtx, test.targetUnprepared)}

			err = testSetup.Start(conf, true, primitive.ProtocolVersion4)
			require.Nil(t, err)

			prepareMsg := &message.Prepare{
				Query:    test.query,
				Keyspace: "",
			}

			prepareResp, err := testSetup.Client.CqlConnection.SendAndReceive(
				frame.NewFrame(primitive.ProtocolVersion4, 10, prepareMsg))
			require.Nil(t, err)

			preparedResult, ok := prepareResp.Body.Message.(*message.PreparedResult)
			require.True(t, ok, "prepared result was type %T", preparedResult)

			require.Equal(t, originPreparedId, preparedResult.PreparedQueryId)

			executeMsg := &message.Execute{
				QueryId:          originPreparedId,
				ResultMetadataId: nil,
				Options:          &message.QueryOptions{},
			}

			executeResp, err := testSetup.Client.CqlConnection.SendAndReceive(
				frame.NewFrame(primitive.ProtocolVersion4, 20, executeMsg))
			require.Nil(t, err)

			unPreparedResult, ok := executeResp.Body.Message.(*message.Unprepared)
			require.True(t, ok, "unprepared result was type %T", executeResp.Body.Message)

			require.Equal(t, originPreparedId, unPreparedResult.Id)

			prepareResp, err = testSetup.Client.CqlConnection.SendAndReceive(
				frame.NewFrame(primitive.ProtocolVersion4, 10, prepareMsg))
			require.Nil(t, err)

			preparedResult, ok = prepareResp.Body.Message.(*message.PreparedResult)
			require.True(t, ok, "prepared result was type %T", preparedResult)

			require.Equal(t, originPreparedId, preparedResult.PreparedQueryId)

			executeResp, err = testSetup.Client.CqlConnection.SendAndReceive(
				frame.NewFrame(primitive.ProtocolVersion4, 20, executeMsg))
			require.Nil(t, err)

			rowsResult, ok := executeResp.Body.Message.(*message.RowsResult)
			require.True(t, ok, "rows result was type %T", rowsResult)

			originLock.Lock()
			defer originLock.Unlock()
			targetLock.Lock()
			defer targetLock.Unlock()

			require.Equal(t, 1, len(rowsResult.Data))
			require.Equal(t, 2, len(rowsResult.Data[0]))
			require.Equal(t, message.Row{originKey, originValue}, rowsResult.Data[0])
			require.NotEqual(t, message.Row{targetKey, targetValue}, rowsResult.Data[0])

			require.Equal(t, 2, len(originExecuteMessages))
			require.Equal(t, 2, len(originPrepareMessages))
			require.Equal(t, originPreparedId, originExecuteMessages[0].QueryId)
			require.Equal(t, originPreparedId, originExecuteMessages[1].QueryId)

			if !test.read {
				require.Equal(t, 2, len(targetExecuteMessages))
				require.Equal(t, targetPreparedId, targetExecuteMessages[0].QueryId)
				require.Equal(t, targetPreparedId, targetExecuteMessages[1].QueryId)
				require.NotEqual(t, executeMsg, targetExecuteMessages[0])
				require.NotEqual(t, executeMsg, targetExecuteMessages[1])
			} else {
				require.Equal(t, 0, len(targetExecuteMessages))
			}
			require.Equal(t, 2, len(targetPrepareMessages))

			require.Equal(t, prepareMsg, targetPrepareMessages[0])
			require.Equal(t, prepareMsg, targetPrepareMessages[1])
			require.Equal(t, prepareMsg, originPrepareMessages[0])
			require.Equal(t, prepareMsg, originPrepareMessages[1])
			require.Equal(t, executeMsg, originExecuteMessages[0])
			require.Equal(t, executeMsg, originExecuteMessages[1])

			require.Equal(t, 2, originCtx["EXECUTE_" + string(originPreparedId)])
			if test.originUnprepared {
				require.Equal(t, 1, originCtx["UNPREPARED_" + string(originPreparedId)])
				require.Equal(t, 1, originCtx["ROWS_" + string(originPreparedId)])
			} else {
				require.Equal(t, nil, originCtx["UNPREPARED_" + string(originPreparedId)])
				require.Equal(t, 2, originCtx["ROWS_" + string(originPreparedId)])
			}

			require.Equal(t, nil, originCtx["EXECUTE_" + string(targetPreparedId)])
			require.Equal(t, nil, originCtx["ROWS_" + string(targetPreparedId)])
			require.Equal(t, nil, originCtx["UNPREPARED_" + string(targetPreparedId)])

			if !test.read {
				require.Equal(t, 2, targetCtx["EXECUTE_" + string(targetPreparedId)])
				if test.targetUnprepared {
					require.Equal(t, 1, targetCtx["UNPREPARED_" + string(targetPreparedId)])
					require.Equal(t, 1, targetCtx["ROWS_" + string(targetPreparedId)])
				} else {
					require.Equal(t, nil, targetCtx["UNPREPARED_" + string(targetPreparedId)])
					require.Equal(t, 2, targetCtx["ROWS_" + string(targetPreparedId)])
				}
			} else {
				require.Equal(t, nil, targetCtx["EXECUTE_" + string(targetPreparedId)])
				require.Equal(t, nil, targetCtx["ROWS_" + string(targetPreparedId)])
			}

			require.Equal(t, nil, targetCtx["EXECUTE_" + string(originPreparedId)])
			require.Equal(t, nil, targetCtx["ROWS_" + string(originPreparedId)])
			require.Equal(t, nil, targetCtx["UNPREPARED_" + string(originPreparedId)])
		})
	}
}

func NewPreparedTestHandler(
	lock *sync.Mutex, preparedMessages *[]*message.Prepare, executeMessages *[]*message.Execute,
	preparedId []byte, key message.Column, value message.Column, context map[string]interface{}, unpreparedTest bool) func(
		request *frame.Frame, conn *client2.CqlServerConnection, ctx client2.RequestHandlerContext) *frame.Frame {
	return func(request *frame.Frame, conn *client2.CqlServerConnection, ctx client2.RequestHandlerContext) *frame.Frame {
		rowsMetadata := &message.RowsMetadata{
			ColumnCount: 2,
			Columns: []*message.ColumnMetadata{
				{
					Keyspace: "ks1",
					Table:    "tb1",
					Name:     "key",
					Index:    0,
					Type:     datatype.Varchar,
				},
				{
					Keyspace: "ks1",
					Table:    "tb1",
					Name:     "value",
					Index:    1,
					Type:     datatype.Varchar,
				},
			},
		}
		if request.Header.OpCode == primitive.OpCodePrepare {
			lock.Lock()
			prepareMsg, ok := request.Body.Message.(*message.Prepare)
			if !ok {
				log.Warnf("opcodeprepare expected PrepareMessage but got %T", request.Body.Message)
				lock.Unlock()
				return nil
			}
			*preparedMessages = append(*preparedMessages, prepareMsg)
			counterInterface := context["PREPARE_" + string(preparedId)]
			if counterInterface == nil {
				counterInterface = 0
			}
			counter := counterInterface.(int)
			context["PREPARE_" + string(preparedId)] = counter + 1
			lock.Unlock()
			return frame.NewFrame(request.Header.Version, request.Header.StreamId, &message.PreparedResult{
				PreparedQueryId:   preparedId,
				ResultMetadataId:  nil,
				VariablesMetadata: nil,
				ResultMetadata:    rowsMetadata,
			})
		} else if request.Header.OpCode == primitive.OpCodeExecute {
			lock.Lock()
			executeMsg, ok := request.Body.Message.(*message.Execute)
			if !ok {
				log.Warnf("opcodeexecute expected ExecuteMessage but got %T", request.Body.Message)
				lock.Unlock()
				return nil
			}
			*executeMessages = append(*executeMessages, executeMsg)

			counterInterface := context["EXECUTE_" + string(preparedId)]
			if counterInterface == nil {
				counterInterface = 0
			}
			counter := counterInterface.(int)
			context["EXECUTE_" + string(preparedId)] = counter + 1

			counterInterface = context["PREPARE_" + string(preparedId)]
			if counterInterface == nil {
				counterInterface = 0
			}
			counter = counterInterface.(int)
			lock.Unlock()

			threshold := 1
			if unpreparedTest {
				threshold = 2
			}

			prefix := "UNPREPARED_"
			var msg message.Message
			if counter < threshold || !bytes.Equal(executeMsg.QueryId, preparedId) {
				msg = &message.Unprepared{
					ErrorMessage: "UNPREPARED",
					Id:           executeMsg.QueryId,
				}
			} else {
				prefix = "ROWS_"
				msg = &message.RowsResult{
					Metadata: rowsMetadata,
					Data: message.RowSet{message.Row{key, value}},
				}
			}
			lock.Lock()
			counterInterface = context[prefix + string(executeMsg.QueryId)]
			if counterInterface == nil {
				counterInterface = 0
			}
			counter = counterInterface.(int)
			context[prefix + string(executeMsg.QueryId)] = counter + 1
			lock.Unlock()
			return frame.NewFrame(request.Header.Version, request.Header.StreamId, msg)
		} else {
			return nil
		}
	}
}