package integration_tests

import (
	"bytes"
	"context"
	"fmt"
	client2 "github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/zdm-proxy/integration-tests/client"
	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/integration-tests/simulacron"
	"github.com/datastax/zdm-proxy/proxy/pkg/config"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func TestPreparedIdProxyCacheMiss(t *testing.T) {

	simulacronSetup, err := setup.NewSimulacronTestSetup(t)
	require.Nil(t, err)
	defer simulacronSetup.Cleanup()

	testClient, err := client.NewTestClient(context.Background(), "127.0.0.1:14002")
	require.True(t, err == nil, "testClient setup failed: %s", err)

	defer testClient.Shutdown()

	err = testClient.PerformDefaultHandshake(context.Background(), primitive.ProtocolVersion4, false)
	require.True(t, err == nil, "No-auth handshake failed: %s", err)

	preparedId := []byte{143, 7, 36, 50, 225, 104, 157, 89, 199, 177, 239, 231, 82, 201, 142, 253}

	executeMsg := &message.Execute{
		QueryId:          preparedId,
		ResultMetadataId: nil,
	}
	response, requestStreamId, err := testClient.SendMessage(context.Background(), primitive.ProtocolVersion4, executeMsg)
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

	simulacronSetup, err := setup.NewSimulacronTestSetup(t)
	require.Nil(t, err)
	defer simulacronSetup.Cleanup()

	testClient, err := client.NewTestClient(context.Background(), "127.0.0.1:14002")
	require.True(t, err == nil, "testClient setup failed: %s", err)

	defer testClient.Shutdown()

	err = testClient.PerformDefaultHandshake(context.Background(), primitive.ProtocolVersion4, false)
	require.True(t, err == nil, "No-auth handshake failed: %s", err)

	tests := map[string]struct {
		cluster               *simulacron.Cluster
		dualReadsEnabled      bool
		asyncReadsOnSecondary bool
		query                 string
		read                  bool
		expectedUnprepared    bool
		queryId               string
	}{
		"write_unprepared_origin": {
			cluster:               simulacronSetup.Origin,
			dualReadsEnabled:      false,
			asyncReadsOnSecondary: false,
			query:                 "INSERT INTO ks1.table1 (c1, c2) VALUES (1, 2)",
			read:                  false,
			expectedUnprepared:    true,
			queryId:               "05440fe1",
		},
		"read_unprepared_origin": {
			cluster:               simulacronSetup.Origin,
			dualReadsEnabled:      false,
			asyncReadsOnSecondary: false,
			query:                 "SELECT * FROM ks1.table1",
			read:                  true,
			expectedUnprepared:    true,
			queryId:               "7b442804",
		},
		"write_unprepared_target": {
			cluster:               simulacronSetup.Target,
			dualReadsEnabled:      false,
			asyncReadsOnSecondary: false,
			query:                 "INSERT INTO ks1.table1 (c1, c2) VALUES (1, 2)",
			read:                  false,
			expectedUnprepared:    true,
			queryId:               "05440fe1",
		},
		"read_unprepared_target": {
			cluster:               simulacronSetup.Target,
			dualReadsEnabled:      false,
			asyncReadsOnSecondary: false,
			query:                 "SELECT * FROM ks1.table1",
			read:                  true,
			expectedUnprepared:    false,
			queryId:               "7b442804",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {

			err := simulacronSetup.Origin.ClearPrimes()
			require.True(t, err == nil, "clear primes failed on origin: %s", err)

			err = simulacronSetup.Target.ClearPrimes()
			require.True(t, err == nil, "clear primes failed on target: %s", err)

			prepareMsg := &message.Prepare{
				Query:    test.query,
				Keyspace: "",
			}

			response, requestStreamId, err := testClient.SendMessage(context.Background(), primitive.ProtocolVersion4, prepareMsg)
			require.True(t, err == nil, "prepare request send failed: %s", err)

			preparedResponse, ok := response.Body.Message.(*message.PreparedResult)
			require.True(t, ok, "did not receive prepared result, got instead: %v", response.Body.Message)

			// clear primes only on selected cluster
			err = test.cluster.ClearPrimes()
			require.True(t, err == nil, "clear primes failed: %s", err)

			executeMsg := &message.Execute{
				QueryId:          preparedResponse.PreparedQueryId,
				ResultMetadataId: preparedResponse.ResultMetadataId,
			}

			response, requestStreamId, err = testClient.SendMessage(context.Background(), primitive.ProtocolVersion4, executeMsg)
			require.True(t, err == nil, "execute request send failed: %s", err)

			if test.expectedUnprepared {
				errorResponse, ok := response.Body.Message.(message.Error)
				require.True(t, ok, fmt.Sprintf("expected error result but got %02x", response.Body.Message.GetOpCode()))
				require.Equal(t, requestStreamId, response.Header.StreamId, "streamId does not match expected value.")
				require.True(t, err == nil, "Error response could not be parsed: %s", err)
				require.Equal(t, primitive.ErrorCodeUnprepared, errorResponse.GetErrorCode(), "Error code received was not Unprepared.")
				require.Equal(t, fmt.Sprintf("Prepared query with ID %v not found (either the query was not prepared on "+
					"this host (maybe the host has been restarted?) or you have prepared too many queries and it has been "+
					"evicted from the internal cache)", test.queryId),
					errorResponse.GetErrorMessage(),
					"Unexpected error message.")

				unprepared, ok := errorResponse.(*message.Unprepared)
				require.True(t, ok, fmt.Sprintf("expected unprepared but got %T", errorResponse))
				require.Equal(t, preparedResponse.PreparedQueryId, unprepared.Id, "Error body did not contain the expected preparedId.")
			} else {
				switch response.Body.Message.(type) {
				case *message.RowsResult, *message.VoidResult:
				default:
					require.Fail(t, "expected success response but got %v", response.Body.Message.GetOpCode().String())
				}
			}
		})
	}
}

func TestPreparedIdReplacement(t *testing.T) {
	type test struct {
		name                               string
		query                              string
		expectedQuery                      string
		expectedVariables                  *message.VariablesMetadata
		batchQuery                         string
		expectedBatchQuery                 string
		expectedBatchPreparedStmtVariables *message.VariablesMetadata
		read                               bool
		readMode                           string
		replaceServerSideFunctions         bool
	}
	tests := []test{
		{
			"reads",
			"SELECT * FROM ks1.tb1",
			"SELECT * FROM ks1.tb1",
			nil,
			"",
			"",
			nil,
			true,
			config.ReadModePrimaryOnly,
			false,
		},
		{
			"reads_async",
			"SELECT * FROM ks1.tb1",
			"SELECT * FROM ks1.tb1",
			nil,
			"",
			"",
			nil,
			true,
			config.ReadModeDualAsyncOnSecondary,
			false,
		},
		{
			"writes",
			"INSERT INTO ks1.tb1 (key, value) VALUES ('key', 'value')",
			"INSERT INTO ks1.tb1 (key, value) VALUES ('key', 'value')",
			nil,
			"INSERT INTO ks1.tb1 (key, value) VALUES ('key', 'value2')",
			"INSERT INTO ks1.tb1 (key, value) VALUES ('key', 'value2')",
			nil,
			false,
			config.ReadModePrimaryOnly,
			false,
		},
		{
			"writes_function_call_now_replacement_enabled",
			"INSERT INTO ks1.tb1 (key, value) VALUES ('key', now())",
			"INSERT INTO ks1.tb1 (key, value) VALUES ('key', ?)",
			&message.VariablesMetadata{
				PkIndices: nil,
				Columns: []*message.ColumnMetadata{
					{
						Keyspace: "",
						Table:    "",
						Name:     "value",
						Index:    0,
						Type:     datatype.Timeuuid,
					},
				},
			},
			"INSERT INTO ks1.tb1 (key, value) VALUES ('key2', now())",
			"INSERT INTO ks1.tb1 (key, value) VALUES ('key2', ?)",
			&message.VariablesMetadata{
				PkIndices: nil,
				Columns: []*message.ColumnMetadata{
					{
						Keyspace: "",
						Table:    "",
						Name:     "value",
						Index:    0,
						Type:     datatype.Timeuuid,
					},
				},
			},
			false,
			config.ReadModePrimaryOnly,
			true,
		},
		{
			"writes_function_call_now_replacement_disabled",
			"INSERT INTO ks1.tb1 (key, value) VALUES ('key', now())",
			"INSERT INTO ks1.tb1 (key, value) VALUES ('key', now())",
			nil,
			"INSERT INTO ks1.tb1 (key, value) VALUES ('key2', now())",
			"INSERT INTO ks1.tb1 (key, value) VALUES ('key2', now())",
			nil,
			false,
			config.ReadModePrimaryOnly,
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conf := setup.NewTestConfig("127.0.1.1", "127.0.1.2")
			conf.ReadMode = test.readMode
			dualReadsEnabled := test.readMode == config.ReadModeDualAsyncOnSecondary
			conf.ReplaceCqlFunctions = test.replaceServerSideFunctions
			testSetup, err := setup.NewCqlServerTestSetup(t, conf, false, false, false)
			require.Nil(t, err)
			defer testSetup.Cleanup()

			originPreparedId := []byte{143, 7, 36, 50, 225, 104, 157, 89, 199, 177, 239, 231, 82, 201, 142, 253}
			targetPreparedId := []byte{142, 8, 36, 51, 225, 104, 157, 89, 199, 177, 239, 231, 82, 201, 142, 253}
			originBatchPreparedId := []byte{141, 7, 36, 50, 225, 104, 157, 89, 199, 177, 239, 231, 82, 201, 142, 253}
			targetBatchPreparedId := []byte{140, 8, 36, 51, 225, 104, 157, 89, 199, 177, 239, 231, 82, 201, 142, 253}
			require.NotEqual(t, originPreparedId, targetPreparedId)
			require.NotEqual(t, originBatchPreparedId, targetBatchPreparedId)

			originLock := &sync.Mutex{}
			originBatchMessages := make([]*message.Batch, 0)
			originExecuteMessages := make([]*message.Execute, 0)
			originPrepareMessages := make([]*message.Prepare, 0)
			originKey := message.Column{0, 1}
			originValue := message.Column{24, 51, 2}

			targetLock := &sync.Mutex{}
			targetBatchMessages := make([]*message.Batch, 0)
			targetExecuteMessages := make([]*message.Execute, 0)
			targetPrepareMessages := make([]*message.Prepare, 0)
			targetKey := message.Column{2, 3, 4}
			targetValue := message.Column{6, 121, 23}

			testSetup.Origin.CqlServer.RequestHandlers = []client2.RequestHandler{
				client2.NewDriverConnectionInitializationHandler("origin", "dc1", func(_ string) {}),
				NewPreparedTestHandler(originLock, &originPrepareMessages, &originExecuteMessages, &originBatchMessages,
					test.expectedBatchQuery, originPreparedId, originBatchPreparedId, originKey, originValue, map[string]interface{}{}, false,
					test.expectedVariables, test.expectedBatchPreparedStmtVariables, false)}
			testSetup.Target.CqlServer.RequestHandlers = []client2.RequestHandler{
				client2.NewDriverConnectionInitializationHandler("target", "dc1", func(_ string) {}),
				NewPreparedTestHandler(targetLock, &targetPrepareMessages, &targetExecuteMessages, &targetBatchMessages,
					test.expectedBatchQuery, targetPreparedId, targetBatchPreparedId, targetKey, targetValue, map[string]interface{}{}, false,
					test.expectedVariables, test.expectedBatchPreparedStmtVariables, test.readMode == config.ReadModeDualAsyncOnSecondary && test.read)}

			err = testSetup.Start(conf, true, primitive.ProtocolVersion4)
			require.Nil(t, err)

			prepareMsg := &message.Prepare{
				Query:    test.query,
				Keyspace: "",
			}
			expectedPrepareMsg := &message.Prepare{
				Query:    test.expectedQuery,
				Keyspace: "",
			}

			prepareResp, err := testSetup.Client.CqlConnection.SendAndReceive(
				frame.NewFrame(primitive.ProtocolVersion4, 10, prepareMsg))
			require.Nil(t, err)

			preparedResult, ok := prepareResp.Body.Message.(*message.PreparedResult)
			require.True(t, ok, "prepared result was type %T", preparedResult)

			require.Equal(t, originPreparedId, preparedResult.PreparedQueryId)

			var batchPrepareMsg *message.Prepare
			var expectedBatchPrepareMsg *message.Prepare
			if test.batchQuery != "" {
				batchPrepareMsg = prepareMsg.Clone().(*message.Prepare)
				batchPrepareMsg.Query = test.batchQuery
				expectedBatchPrepareMsg = batchPrepareMsg.Clone().(*message.Prepare)
				expectedBatchPrepareMsg.Query = test.expectedBatchQuery
				prepareResp, err = testSetup.Client.CqlConnection.SendAndReceive(
					frame.NewFrame(primitive.ProtocolVersion4, 10, batchPrepareMsg))
				require.Nil(t, err)

				preparedResult, ok = prepareResp.Body.Message.(*message.PreparedResult)
				require.True(t, ok, "prepared result was type %T", preparedResult)

				require.Equal(t, originBatchPreparedId, preparedResult.PreparedQueryId)
			}

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

			var batchMsg *message.Batch
			if test.batchQuery != "" {
				batchMsg = &message.Batch{
					Type: primitive.BatchTypeLogged,
					Children: []*message.BatchChild{
						{
							QueryOrId: test.query,

							// the decoder uses empty slices instead of nil so this has to be initialized this way
							// so that the equality assertions work later in this test
							Values: make([]*primitive.Value, 0),
						},
						{
							QueryOrId: originBatchPreparedId,
							Values:    make([]*primitive.Value, 0),
						},
					},
					Consistency:       primitive.ConsistencyLevelLocalQuorum,
					SerialConsistency: nil,
					DefaultTimestamp:  nil,
					Keyspace:          "",
					NowInSeconds:      nil,
				}

				batchResp, err := testSetup.Client.CqlConnection.SendAndReceive(
					frame.NewFrame(primitive.ProtocolVersion4, 30, batchMsg))
				require.Nil(t, err)

				batchResult, ok := batchResp.Body.Message.(*message.VoidResult)
				require.True(t, ok, "batch result was type %T", batchResult)
			}

			originLock.Lock()
			defer originLock.Unlock()
			require.Equal(t, 1, len(originExecuteMessages))
			if test.batchQuery != "" {
				require.Equal(t, 2, len(originPrepareMessages))
			} else {
				require.Equal(t, 1, len(originPrepareMessages))
			}
			require.Equal(t, originPreparedId, originExecuteMessages[0].QueryId)
			if test.batchQuery != "" {
				require.Equal(t, 1, len(originBatchMessages))
				require.Equal(t, 2, len(originBatchMessages[0].Children))
				require.Equal(t, originBatchPreparedId, originBatchMessages[0].Children[1].QueryOrId)
			}

			targetLock.Lock()
			defer targetLock.Unlock()

			expectedTargetPrepares := 1
			expectedTargetExecutes := 0
			expectedTargetBatches := 0
			if !test.read || dualReadsEnabled {
				expectedTargetExecutes += 1
				if test.batchQuery != "" {
					expectedTargetBatches += 1
				}
			}
			if dualReadsEnabled {
				expectedTargetPrepares += 1
			}
			if test.batchQuery != "" {
				expectedTargetPrepares += 1
			}

			require.Equal(t, expectedTargetExecutes, len(targetExecuteMessages))
			for _, targetExecute := range targetExecuteMessages {
				require.Equal(t, targetPreparedId, targetExecute.QueryId)
				require.NotEqual(t, executeMsg, targetExecute)
			}
			require.Equal(t, expectedTargetBatches, len(targetBatchMessages))
			if expectedTargetBatches > 0 {
				require.Equal(t, 2, len(targetBatchMessages[0].Children))
				require.Equal(t, targetBatchPreparedId, targetBatchMessages[0].Children[1].QueryOrId)
				require.NotEqual(t, batchMsg, targetBatchMessages[0])
			}
			require.Equal(t, expectedTargetPrepares, len(targetPrepareMessages))

			require.Equal(t, expectedPrepareMsg, targetPrepareMessages[0])
			if dualReadsEnabled {
				require.Equal(t, expectedPrepareMsg, targetPrepareMessages[1])
			}
			require.Equal(t, expectedPrepareMsg, originPrepareMessages[0])
			if test.expectedVariables != nil {
				require.Equal(t, executeMsg.QueryId, originExecuteMessages[0].QueryId)
				require.Equal(t, executeMsg.ResultMetadataId, originExecuteMessages[0].ResultMetadataId)
				require.NotEqual(t, executeMsg.Options, originExecuteMessages[0].Options)
				require.Equal(t, len(test.expectedVariables.Columns), len(originExecuteMessages[0].Options.PositionalValues))
				require.NotEqual(t, len(executeMsg.Options.PositionalValues), len(originExecuteMessages[0].Options.PositionalValues))

				// check if only the positional values are different, we test the parameter replacement in depth on other tests
				modifiedOriginExecuteMsg := originExecuteMessages[0].Clone()
				modifiedOriginExecuteMsg.(*message.Execute).Options.PositionalValues = executeMsg.Options.PositionalValues
				require.Equal(t, executeMsg, modifiedOriginExecuteMsg)
				require.Equal(t, originExecuteMessages[0].Options, targetExecuteMessages[0].Options)
			} else {
				require.Equal(t, executeMsg, originExecuteMessages[0])
			}
			if test.batchQuery != "" {
				if dualReadsEnabled {
					require.Equal(t, expectedBatchPrepareMsg, targetPrepareMessages[2])
				} else {
					require.Equal(t, expectedBatchPrepareMsg, targetPrepareMessages[1])
				}
				require.Equal(t, expectedBatchPrepareMsg, originPrepareMessages[1])

				if test.expectedBatchPreparedStmtVariables != nil {
					require.NotEqual(t, batchMsg.Children[0].QueryOrId, originBatchMessages[0].Children[0].QueryOrId)
					require.NotEqual(t, batchMsg.Children[0].QueryOrId, targetBatchMessages[0].Children[0].QueryOrId)
					require.Equal(t, originBatchMessages[0].Children[0].QueryOrId, targetBatchMessages[0].Children[0].QueryOrId)
					require.Equal(t, 0, len(targetBatchMessages[0].Children[0].Values))
					require.Equal(t, 0, len(originBatchMessages[0].Children[0].Values))
					require.Equal(t, 0, len(batchMsg.Children[0].Values))

					require.Equal(t, batchMsg.Children[1].QueryOrId, originBatchMessages[0].Children[1].QueryOrId)
					require.NotEqual(t, batchMsg.Children[1].QueryOrId, targetBatchMessages[0].Children[1].QueryOrId)
					require.NotEqual(t, originBatchMessages[0].Children[1].QueryOrId, targetBatchMessages[0].Children[1].QueryOrId)
					require.Equal(t, targetBatchPreparedId, targetBatchMessages[0].Children[1].QueryOrId)
					require.Equal(t, originBatchPreparedId, originBatchMessages[0].Children[1].QueryOrId)
					require.Equal(t, originBatchPreparedId, batchMsg.Children[1].QueryOrId)
					require.Equal(t, len(test.expectedBatchPreparedStmtVariables.Columns), len(targetBatchMessages[0].Children[1].Values))
					require.Equal(t, len(test.expectedBatchPreparedStmtVariables.Columns), len(originBatchMessages[0].Children[1].Values))
					require.Equal(t, 0, len(batchMsg.Children[1].Values))
					require.NotEqual(t, batchMsg.Children[1].Values, originBatchMessages[0].Children[1].Values)
					require.NotEqual(t, batchMsg.Children[1].Values, targetBatchMessages[0].Children[1].Values)
					require.Equal(t, originBatchMessages[0].Children[1].Values, targetBatchMessages[0].Children[1].Values)
				} else {
					require.Equal(t, batchMsg, originBatchMessages[0])
					require.NotEqual(t, batchMsg, targetBatchMessages[0])
					clonedBatchMsg := targetBatchMessages[0].Clone().(*message.Batch)
					clonedBatchMsg.Children[1].QueryOrId = originBatchPreparedId
					require.Equal(t, batchMsg, clonedBatchMsg)
				}
			}
		})
	}
}

func TestUnpreparedIdReplacement(t *testing.T) {
	type test struct {
		name             string
		query            string
		batchQuery       string
		read             bool
		originUnprepared bool
		targetUnprepared bool
		readMode         string
	}
	tests := []test{
		{
			"reads_origin_unprepared",
			"SELECT * FROM ks1.tb1",
			"",
			true,
			true,
			false,
			config.ReadModePrimaryOnly,
		},
		{
			"reads_both_unprepared_async_reads_on_target",
			"SELECT * FROM ks1.tb1",
			"",
			true,
			true,
			true,
			config.ReadModeDualAsyncOnSecondary,
		},
		{
			"writes_origin_unprepared",
			"INSERT INTO ks1.tb1 (key, value) VALUES ('key', 'value')",
			"INSERT INTO ks1.tb1 (key, value) VALUES ('key', 'value2')",
			false,
			true,
			false,
			config.ReadModePrimaryOnly,
		},
		{
			"writes_target_unprepared",
			"INSERT INTO ks1.tb1 (key, value) VALUES ('key', 'value')",
			"INSERT INTO ks1.tb1 (key, value) VALUES ('key', 'value2')",
			false,
			false,
			true,
			config.ReadModePrimaryOnly,
		},
		{
			"writes_both_unprepared",
			"INSERT INTO ks1.tb1 (key, value) VALUES ('key', 'value')",
			"INSERT INTO ks1.tb1 (key, value) VALUES ('key', 'value2')",
			false,
			true,
			true,
			config.ReadModePrimaryOnly,
		}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conf := setup.NewTestConfig("127.0.1.1", "127.0.1.2")
			conf.ReadMode = test.readMode
			dualReadsEnabled := test.readMode == config.ReadModeDualAsyncOnSecondary
			testSetup, err := setup.NewCqlServerTestSetup(t, conf, false, false, false)
			require.Nil(t, err)
			defer testSetup.Cleanup()

			originPreparedId := []byte{153, 7, 36, 50, 225, 104, 157, 89, 199, 177, 239, 231, 82, 201, 142, 253}
			targetPreparedId := []byte{162, 8, 36, 51, 225, 104, 157, 89, 199, 177, 239, 231, 82, 201, 142, 253}
			originBatchPreparedId := []byte{141, 7, 36, 50, 225, 104, 157, 89, 199, 177, 239, 231, 82, 201, 142, 253}
			targetBatchPreparedId := []byte{140, 8, 36, 51, 225, 104, 157, 89, 199, 177, 239, 231, 82, 201, 142, 253}
			require.NotEqual(t, originPreparedId, targetPreparedId)
			require.NotEqual(t, originBatchPreparedId, targetBatchPreparedId)

			originLock := &sync.Mutex{}
			originBatchMessages := make([]*message.Batch, 0)
			originExecuteMessages := make([]*message.Execute, 0)
			originPrepareMessages := make([]*message.Prepare, 0)
			originKey := message.Column{0, 1}
			originValue := message.Column{24, 51, 2}

			targetLock := &sync.Mutex{}
			targetBatchMessages := make([]*message.Batch, 0)
			targetExecuteMessages := make([]*message.Execute, 0)
			targetPrepareMessages := make([]*message.Prepare, 0)
			targetKey := message.Column{2, 3, 4}
			targetValue := message.Column{6, 121, 23}
			originCtx := map[string]interface{}{}
			targetCtx := map[string]interface{}{}

			testSetup.Origin.CqlServer.RequestHandlers = []client2.RequestHandler{
				client2.NewDriverConnectionInitializationHandler("origin", "dc1", func(_ string) {}),
				NewPreparedTestHandler(originLock, &originPrepareMessages, &originExecuteMessages, &originBatchMessages,
					test.batchQuery, originPreparedId, originBatchPreparedId, originKey, originValue, originCtx, test.originUnprepared,
					nil, nil, false)}
			testSetup.Target.CqlServer.RequestHandlers = []client2.RequestHandler{
				client2.NewDriverConnectionInitializationHandler("target", "dc1", func(_ string) {}),
				NewPreparedTestHandler(targetLock, &targetPrepareMessages, &targetExecuteMessages, &targetBatchMessages,
					test.batchQuery, targetPreparedId, targetBatchPreparedId, targetKey, targetValue, targetCtx, test.targetUnprepared,
					nil, nil, dualReadsEnabled && test.read)}

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

			var batchMsg *message.Batch
			var batchPrepareMsg *message.Prepare
			if test.batchQuery != "" {
				batchPrepareMsg = prepareMsg.Clone().(*message.Prepare)
				batchPrepareMsg.Query = test.batchQuery
				prepareResp, err = testSetup.Client.CqlConnection.SendAndReceive(
					frame.NewFrame(primitive.ProtocolVersion4, 10, batchPrepareMsg))
				require.Nil(t, err)

				preparedResult, ok = prepareResp.Body.Message.(*message.PreparedResult)
				require.True(t, ok, "prepared result was type %T", preparedResult)

				require.Equal(t, originBatchPreparedId, preparedResult.PreparedQueryId)

				batchMsg = &message.Batch{
					Type: primitive.BatchTypeLogged,
					Children: []*message.BatchChild{
						{
							QueryOrId: test.query,
							// the decoder uses empty slices instead of nil so this has to be initialized this way
							// so that the equality assertions work later in this test
							Values: make([]*primitive.Value, 0),
						},
						{
							QueryOrId: originBatchPreparedId,
							Values:    make([]*primitive.Value, 0),
						},
					},
					Consistency:       primitive.ConsistencyLevelLocalQuorum,
					SerialConsistency: nil,
					DefaultTimestamp:  nil,
					Keyspace:          "",
					NowInSeconds:      nil,
				}

				batchResp, err := testSetup.Client.CqlConnection.SendAndReceive(
					frame.NewFrame(primitive.ProtocolVersion4, 30, batchMsg))
				require.Nil(t, err)

				unPreparedResult, ok := batchResp.Body.Message.(*message.Unprepared)
				require.True(t, ok, "unprepared result was type %T", batchResp.Body.Message)

				require.Equal(t, originBatchPreparedId, unPreparedResult.Id)

				prepareResp, err = testSetup.Client.CqlConnection.SendAndReceive(
					frame.NewFrame(primitive.ProtocolVersion4, 10, batchPrepareMsg))
				require.Nil(t, err)

				preparedResult, ok = prepareResp.Body.Message.(*message.PreparedResult)
				require.True(t, ok, "prepared result was type %T", preparedResult)

				require.Equal(t, originBatchPreparedId, preparedResult.PreparedQueryId)

				batchResp, err = testSetup.Client.CqlConnection.SendAndReceive(
					frame.NewFrame(primitive.ProtocolVersion4, 30, batchMsg))
				require.Nil(t, err)

				batchResult, ok := batchResp.Body.Message.(*message.VoidResult)
				require.True(t, ok, "batch result was type %T", batchResult)
			}

			originLock.Lock()
			defer originLock.Unlock()
			targetLock.Lock()
			defer targetLock.Unlock()

			require.Equal(t, 1, len(rowsResult.Data))
			require.Equal(t, 2, len(rowsResult.Data[0]))
			require.Equal(t, message.Row{originKey, originValue}, rowsResult.Data[0])
			require.NotEqual(t, message.Row{targetKey, targetValue}, rowsResult.Data[0])

			require.Equal(t, 2, len(originExecuteMessages))
			if test.batchQuery != "" {
				require.Equal(t, 4, len(originPrepareMessages))
			} else {
				require.Equal(t, 2, len(originPrepareMessages))
			}
			require.Equal(t, originPreparedId, originExecuteMessages[0].QueryId)
			require.Equal(t, originPreparedId, originExecuteMessages[1].QueryId)

			expectedTargetPrepares := 2
			expectedTargetExecutes := 0
			expectedTargetBatches := 0
			if !test.read || dualReadsEnabled {
				expectedTargetExecutes = 2
				if test.batchQuery != "" {
					expectedTargetBatches += 2
				}
			}
			if dualReadsEnabled {
				expectedTargetPrepares += 3 // cluster connector sends a PREPARE on its own
			}
			if test.batchQuery != "" {
				expectedTargetPrepares += 2
			}
			require.Equal(t, expectedTargetExecutes, len(targetExecuteMessages))
			for _, execute := range targetExecuteMessages {
				require.Equal(t, targetPreparedId, execute.QueryId)
				require.NotEqual(t, executeMsg, execute)
			}
			require.Equal(t, expectedTargetBatches, len(targetBatchMessages))
			if expectedTargetBatches > 0 {
				for _, batch := range targetBatchMessages {
					require.Equal(t, 2, len(batch.Children))
					require.Equal(t, targetBatchPreparedId, batch.Children[1].QueryOrId)
					require.NotEqual(t, batchMsg, batch)
				}
			}
			require.Equal(t, expectedTargetPrepares, len(targetPrepareMessages))
			require.Equal(t, prepareMsg, targetPrepareMessages[0])
			require.Equal(t, prepareMsg, targetPrepareMessages[1])
			if dualReadsEnabled {
				require.Equal(t, prepareMsg, targetPrepareMessages[2])
				require.Equal(t, prepareMsg, targetPrepareMessages[3])
			}
			require.Equal(t, prepareMsg, originPrepareMessages[0])
			require.Equal(t, prepareMsg, originPrepareMessages[1])

			require.Equal(t, executeMsg, originExecuteMessages[0])
			require.Equal(t, executeMsg, originExecuteMessages[1])

			if test.batchQuery != "" {
				if dualReadsEnabled {
					require.Equal(t, batchPrepareMsg, targetPrepareMessages[4])
					require.Equal(t, batchPrepareMsg, targetPrepareMessages[5])
				} else {
					require.Equal(t, batchPrepareMsg, targetPrepareMessages[2])
					require.Equal(t, batchPrepareMsg, targetPrepareMessages[3])
				}
				require.Equal(t, batchPrepareMsg, originPrepareMessages[2])
				require.Equal(t, batchPrepareMsg, originPrepareMessages[3])
				require.Equal(t, batchMsg, originBatchMessages[0])
				require.Equal(t, batchMsg, originBatchMessages[1])
			}

			require.Equal(t, 2, originCtx["EXECUTE_"+string(originPreparedId)])
			if test.originUnprepared {
				require.Equal(t, 1, originCtx["UNPREPARED_"+string(originPreparedId)])
				require.Equal(t, 1, originCtx["ROWS_"+string(originPreparedId)])
				if test.batchQuery != "" {
					require.Equal(t, 1, originCtx["UNPREPARED_"+string(originBatchPreparedId)])
					require.Equal(t, 1, originCtx["VOID_"+string(originBatchPreparedId)])
				}
			} else {
				require.Equal(t, nil, originCtx["UNPREPARED_"+string(originPreparedId)])
				require.Equal(t, 2, originCtx["ROWS_"+string(originPreparedId)])
				if test.batchQuery != "" {
					require.Equal(t, nil, originCtx["UNPREPARED_"+string(originBatchPreparedId)])
					require.Equal(t, 2, originCtx["VOID_"+string(originBatchPreparedId)])
				}
			}

			require.Equal(t, nil, originCtx["EXECUTE_"+string(targetPreparedId)])
			require.Equal(t, nil, originCtx["ROWS_"+string(targetPreparedId)])
			require.Equal(t, nil, originCtx["UNPREPARED_"+string(targetPreparedId)])
			require.Equal(t, nil, originCtx["BATCH_"+string(targetBatchPreparedId)])
			require.Equal(t, nil, originCtx["VOID_"+string(targetBatchPreparedId)])
			require.Equal(t, nil, originCtx["UNPREPARED_"+string(targetBatchPreparedId)])

			if !test.read || dualReadsEnabled {
				require.Equal(t, 2, targetCtx["EXECUTE_"+string(targetPreparedId)])
				if test.targetUnprepared {
					require.Equal(t, 1, targetCtx["UNPREPARED_"+string(targetPreparedId)])
					require.Equal(t, 1, targetCtx["ROWS_"+string(targetPreparedId)])
				} else {
					require.Equal(t, nil, targetCtx["UNPREPARED_"+string(targetPreparedId)])
					require.Equal(t, 2, targetCtx["ROWS_"+string(targetPreparedId)])
				}
				if test.batchQuery != "" {
					require.Equal(t, 2, targetCtx["BATCH_"+string(targetBatchPreparedId)])
					if test.targetUnprepared {
						require.Equal(t, 1, targetCtx["UNPREPARED_"+string(targetBatchPreparedId)])
						require.Equal(t, 1, targetCtx["VOID_"+string(targetBatchPreparedId)])
					} else {
						require.Equal(t, nil, targetCtx["UNPREPARED_"+string(targetBatchPreparedId)])
						require.Equal(t, 2, targetCtx["VOID_"+string(targetBatchPreparedId)])
					}
				}
			} else {
				require.Equal(t, nil, targetCtx["EXECUTE_"+string(targetPreparedId)])
				require.Equal(t, nil, targetCtx["ROWS_"+string(targetPreparedId)])
				require.Equal(t, nil, targetCtx["BATCH_"+string(targetBatchPreparedId)])
				require.Equal(t, nil, targetCtx["VOID_"+string(targetBatchPreparedId)])
			}

			require.Equal(t, nil, targetCtx["EXECUTE_"+string(originPreparedId)])
			require.Equal(t, nil, targetCtx["ROWS_"+string(originPreparedId)])
			require.Equal(t, nil, targetCtx["UNPREPARED_"+string(originPreparedId)])
			if test.batchQuery != "" {
				require.Equal(t, nil, targetCtx["BATCH_"+string(originBatchPreparedId)])
				require.Equal(t, nil, targetCtx["VOID_"+string(originBatchPreparedId)])
				require.Equal(t, nil, targetCtx["UNPREPARED_"+string(originBatchPreparedId)])
			}
		})
	}
}

func NewPreparedTestHandler(
	lock *sync.Mutex, preparedMessages *[]*message.Prepare, executeMessages *[]*message.Execute, batchMessages *[]*message.Batch,
	batchQuery string, preparedId []byte, batchPreparedId []byte, key message.Column, value message.Column, context map[string]interface{}, unpreparedTest bool,
	variableMetadata *message.VariablesMetadata, batchVariableMetadata *message.VariablesMetadata, dualReads bool) func(
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
			prepId := preparedId
			variablesMetadata := variableMetadata
			if prepareMsg.Query == batchQuery {
				prepId = batchPreparedId
				variablesMetadata = batchVariableMetadata
			}
			counterInterface := context["PREPARE_"+string(prepId)]
			if counterInterface == nil {
				counterInterface = 0
			}
			counter := counterInterface.(int)
			context["PREPARE_"+string(prepId)] = counter + 1
			lock.Unlock()
			return frame.NewFrame(request.Header.Version, request.Header.StreamId, &message.PreparedResult{
				PreparedQueryId:   prepId,
				ResultMetadataId:  nil,
				VariablesMetadata: variablesMetadata,
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

			counterInterface := context["EXECUTE_"+string(preparedId)]
			if counterInterface == nil {
				counterInterface = 0
			}
			counter := counterInterface.(int)
			context["EXECUTE_"+string(preparedId)] = counter + 1

			counterInterface = context["PREPARE_"+string(preparedId)]
			if counterInterface == nil {
				counterInterface = 0
			}
			counter = counterInterface.(int)
			lock.Unlock()

			threshold := 1
			if dualReads {
				threshold += 1
			}
			if unpreparedTest {
				threshold += 1
				if dualReads {
					threshold += 1
				}
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
					Data:     message.RowSet{message.Row{key, value}},
				}
			}
			lock.Lock()
			counterInterface = context[prefix+string(executeMsg.QueryId)]
			if counterInterface == nil {
				counterInterface = 0
			}
			counter = counterInterface.(int)
			context[prefix+string(executeMsg.QueryId)] = counter + 1
			lock.Unlock()
			return frame.NewFrame(request.Header.Version, request.Header.StreamId, msg)

		} else if request.Header.OpCode == primitive.OpCodeBatch {
			lock.Lock()
			batchMsg, ok := request.Body.Message.(*message.Batch)
			if !ok {
				log.Warnf("opcodebatch expected BatchMessage but got %T", request.Body.Message)
				lock.Unlock()
				return nil
			}
			*batchMessages = append(*batchMessages, batchMsg)

			batchCounterInterface := context["BATCH_"+string(batchPreparedId)]
			if batchCounterInterface == nil {
				batchCounterInterface = 0
			}
			batchCounter := batchCounterInterface.(int)
			context["BATCH_"+string(batchPreparedId)] = batchCounter + 1

			prepareCounterInterface := context["PREPARE_"+string(batchPreparedId)]
			if prepareCounterInterface == nil {
				prepareCounterInterface = 0
			}
			prepareCounter := prepareCounterInterface.(int)
			lock.Unlock()

			prepareThreshold := 1 // until this threshold is hit (number of prepare requests), this handler returns UNPREPARED
			if unpreparedTest {
				prepareThreshold = 2
			}

			prefix := "UNPREPARED_"
			var msg message.Message
			preparedIdMatches, unpreparedId := checkIfPreparedIdMatches(batchMsg, batchPreparedId)
			if (prepareCounter < prepareThreshold && unpreparedId != nil) || !preparedIdMatches {
				msg = &message.Unprepared{
					ErrorMessage: "UNPREPARED",
					Id:           unpreparedId,
				}
			} else {
				prefix = "VOID_"
				msg = &message.VoidResult{}
			}

			lock.Lock()
			counterInterface := context[prefix+string(batchPreparedId)]
			if counterInterface == nil {
				counterInterface = 0
			}
			counter := counterInterface.(int)
			context[prefix+string(batchPreparedId)] = counter + 1
			lock.Unlock()
			return frame.NewFrame(request.Header.Version, request.Header.StreamId, msg)
		} else {
			return nil
		}
	}
}

func checkIfPreparedIdMatches(batchMsg *message.Batch, preparedId []byte) (bool, []byte) {
	var batchPreparedId []byte
	for _, child := range batchMsg.Children {
		switch queryOrId := child.QueryOrId.(type) {
		case []byte:
			batchPreparedId = queryOrId
			if !bytes.Equal(queryOrId, preparedId) {
				return false, batchPreparedId
			}
		default:
		}
	}

	return true, batchPreparedId
}
