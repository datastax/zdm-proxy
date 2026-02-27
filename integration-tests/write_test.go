package integration_tests

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"github.com/apache/cassandra-gocql-driver/v2"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/integration-tests/simulacron"
	"github.com/datastax/zdm-proxy/integration-tests/utils"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestBothWriteTimeout(t *testing.T) {
	testSetup, err := setup.NewSimulacronTestSetup(t)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	// Connect to proxy as a "client"
	proxy, err := utils.ConnectToCluster("127.0.0.1", "", "", 14002)

	if err != nil {
		t.Log("Unable to connect to proxy session.")
		t.Fatal(err)
	}
	defer proxy.Close()

	originReceived := 1
	targetReceived := 2

	queryPrimeOrigin :=
		simulacron.WhenQuery(
			"INSERT INTO myks.users (name) VALUES (?)",
			simulacron.
				NewWhenQueryOptions().
				WithPositionalParameter(simulacron.DataTypeText, "john")).
			ThenWriteTimeout(gocql.One, originReceived, 1, simulacron.Simple)

	queryPrimeTarget :=
		simulacron.WhenQuery(
			"INSERT INTO myks.users (name) VALUES (?)",
			simulacron.
				NewWhenQueryOptions().
				WithPositionalParameter(simulacron.DataTypeText, "john")).
			ThenWriteTimeout(gocql.Two, targetReceived, 0, simulacron.Simple)

	err = testSetup.Origin.Prime(queryPrimeOrigin)
	if err != nil {
		t.Fatal("prime on origin error: ", err.Error())
	}
	err = testSetup.Target.Prime(queryPrimeTarget)
	if err != nil {
		t.Fatal("prime on target error: ", err.Error())
	}

	err = proxy.Query("INSERT INTO myks.users (name) VALUES (?)", "john").Exec()

	require.True(t, err != nil, "query should have failed but it didn't")

	errTimeOut, ok := err.(*gocql.RequestErrWriteTimeout)
	require.True(t, ok, "error is not Write Timeout: ", err.Error())

	// assert that the error returned by the proxy matches the origin cluster error not the target cluster one
	require.Equal(t, originReceived, errTimeOut.Received, "timeout error received field doesn't match the origin cluster error")
	require.Equal(t, gocql.One, errTimeOut.Consistency, "timeout error consistency field doesn't match the origin cluster error")
}

func TestOriginWriteTimeout(t *testing.T) {
	testSetup, err := setup.NewSimulacronTestSetup(t)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	// Connect to proxy as a "client"
	proxy, err := utils.ConnectToCluster("127.0.0.1", "", "", 14002)

	if err != nil {
		t.Log("Unable to connect to proxy session.")
		t.Fatal(err)
	}
	defer proxy.Close()

	originReceived := 1

	queryPrimeOrigin :=
		simulacron.WhenQuery(
			"INSERT INTO myks.users (name) VALUES (?)",
			simulacron.
				NewWhenQueryOptions().
				WithPositionalParameter(simulacron.DataTypeText, "john")).
			ThenWriteTimeout(gocql.Two, originReceived, 1, simulacron.Simple)

	queryPrimeTarget :=
		simulacron.WhenQuery(
			"INSERT INTO myks.users (name) VALUES (?)",
			simulacron.
				NewWhenQueryOptions().
				WithPositionalParameter(simulacron.DataTypeText, "john")).
			ThenRowsSuccess(simulacron.NewRowsResult(map[string]simulacron.DataType{"name": simulacron.DataTypeText}).
				WithRow(map[string]interface{}{"name": "john"}))

	err = testSetup.Origin.Prime(queryPrimeOrigin)
	if err != nil {
		t.Fatal("prime on origin error: ", err.Error())
	}
	err = testSetup.Target.Prime(queryPrimeTarget)
	if err != nil {
		t.Fatal("prime on target error: ", err.Error())
	}

	err = proxy.Query("INSERT INTO myks.users (name) VALUES (?)", "john").Exec()

	require.True(t, err != nil, "query should have failed but it didn't")

	errTimeOut, ok := err.(*gocql.RequestErrWriteTimeout)
	require.True(t, ok, "error is not Write Timeout: ", err.Error())

	// assert that the error returned by the proxy matches the origin cluster error not the target cluster one
	require.Equal(t, originReceived, errTimeOut.Received, "timeout error received field doesn't match the origin cluster error")
	require.Equal(t, gocql.Two, errTimeOut.Consistency, "timeout error consistency field doesn't match the origin cluster error")
}

func TestTargetWriteTimeout(t *testing.T) {
	testSetup, err := setup.NewSimulacronTestSetup(t)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	// Connect to proxy as a "client"
	proxy, err := utils.ConnectToCluster("127.0.0.1", "", "", 14002)

	if err != nil {
		t.Log("Unable to connect to proxy session.")
		t.Fatal(err)
	}
	defer proxy.Close()

	targetReceived := 2

	queryPrimeOrigin :=
		simulacron.WhenQuery(
			"INSERT INTO myks.users (name) VALUES (?)",
			simulacron.
				NewWhenQueryOptions().
				WithPositionalParameter(simulacron.DataTypeText, "john")).
			ThenRowsSuccess(simulacron.NewRowsResult(map[string]simulacron.DataType{"name": simulacron.DataTypeText}).
				WithRow(map[string]interface{}{"name": "john"}))

	queryPrimeTarget :=
		simulacron.WhenQuery(
			"INSERT INTO myks.users (name) VALUES (?)",
			simulacron.
				NewWhenQueryOptions().
				WithPositionalParameter(simulacron.DataTypeText, "john")).
			ThenWriteTimeout(gocql.Two, targetReceived, 1, simulacron.Simple)

	err = testSetup.Origin.Prime(queryPrimeOrigin)
	if err != nil {
		t.Fatal("prime on origin error: ", err.Error())
	}
	err = testSetup.Target.Prime(queryPrimeTarget)
	if err != nil {
		t.Fatal("prime on target error: ", err.Error())
	}

	err = proxy.Query("INSERT INTO myks.users (name) VALUES (?)", "john").Exec()

	require.True(t, err != nil, "query should have failed but it didn't")

	errTimeOut, ok := err.(*gocql.RequestErrWriteTimeout)
	require.True(t, ok, "error is not Write Timeout: ", err.Error())

	// assert that the error returned by the proxy matches the origin cluster error not the target cluster one
	require.Equal(t, targetReceived, errTimeOut.Received, "timeout error received field doesn't match the target cluster error")
	require.Equal(t, gocql.Two, errTimeOut.Consistency, "timeout error consistency field doesn't match the target cluster error")
}

func TestWriteSuccessful(t *testing.T) {
	testSetup, err := setup.NewSimulacronTestSetup(t)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	// Connect to proxy as a "client"
	proxy, err := utils.ConnectToCluster("127.0.0.1", "", "", 14002)

	if err != nil {
		t.Log("Unable to connect to proxy session.")
		t.Fatal(err)
	}
	defer proxy.Close()

	queryPrime :=
		simulacron.WhenQuery(
			"INSERT INTO myks.users (name) VALUES (?)",
			simulacron.
				NewWhenQueryOptions().
				WithPositionalParameter(simulacron.DataTypeText, "john")).
			ThenSuccess()

	err = testSetup.Origin.Prime(queryPrime)
	if err != nil {
		t.Fatal("prime on origin error: ", err.Error())
	}
	err = testSetup.Target.Prime(queryPrime)
	if err != nil {
		t.Fatal("prime on target error: ", err.Error())
	}

	err = proxy.Query("INSERT INTO myks.users (name) VALUES (?)", "john").Exec()
	if err != nil {
		t.Fatal("query failed: ", err.Error())
	}
}

func TestRequestIdTracingSkipped(t *testing.T) {
	tests := []struct {
		name        string
		initFunc    func(t *testing.T) (*setup.SimulacronTestSetup, error)
		clientReqId []byte
	}{
		{
			name: "config_disabled",
			initFunc: func(t *testing.T) (*setup.SimulacronTestSetup, error) {
				return setup.NewSimulacronTestSetup(t)
			},
		},
		{
			name: "config_disabled_client_id",
			initFunc: func(t *testing.T) (*setup.SimulacronTestSetup, error) {
				return setup.NewSimulacronTestSetup(t)
			},
			clientReqId: []byte("client"),
		},
		{
			name: "proto_ver_before_4",
			initFunc: func(t *testing.T) (*setup.SimulacronTestSetup, error) {
				c := setup.NewTestConfig("", "")
				c.TracingEnabled = true
				c.ControlConnMaxProtocolVersion = "3"
				return setup.NewSimulacronTestSetupWithSessionAndNodesAndConfig(t, true, false, 1, c,
					&simulacron.ClusterVersion{"3.0", "3.0"})
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testSetup, err := tt.initFunc(t)
			require.Nil(t, err)
			defer testSetup.Cleanup()

			// Connect to proxy as a "client"
			proxy, err := utils.ConnectToCluster("127.0.0.1", "", "", 14002)

			if err != nil {
				t.Log("Unable to connect to proxy session.")
				t.Fatal(err)
			}
			defer proxy.Close()

			queryPrime :=
				simulacron.WhenQuery(
					"INSERT INTO myks.users (name) VALUES (?)",
					simulacron.
						NewWhenQueryOptions().
						WithPositionalParameter(simulacron.DataTypeText, "john")).
					ThenSuccess()

			err = testSetup.Origin.Prime(queryPrime)
			require.Nil(t, err)
			err = testSetup.Target.Prime(queryPrime)
			require.Nil(t, err)

			qry := proxy.Query("INSERT INTO myks.users (name) VALUES (?)", "john")
			if tt.clientReqId != nil {
				customPayload := make(map[string][]byte)
				customPayload["request-id"] = tt.clientReqId
				qry.CustomPayload(customPayload)
			}
			err = qry.Exec()
			require.Nil(t, err)

			// EXECUTE message shall not contain request ID
			originQueryLog, _ := testSetup.Origin.GetLogsByType(simulacron.QueryTypeExecute)
			queries := originQueryLog.Datacenters[0].Nodes[0].Queries
			require.NotContains(t, queries[len(queries)-1].Frame.CustomPayload, "request-id")
		})
	}
}

func TestRequestIdTracing(t *testing.T) {
	var buff bytes.Buffer
	buffWriter := bufio.NewWriter(&buff)
	log.SetOutput(buffWriter)

	c := setup.NewTestConfig("", "")
	c.TracingEnabled = true
	testSetup, err := setup.NewSimulacronTestSetupWithSessionAndNodesAndConfig(t, true, false, 1, c, nil)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	// Connect to proxy as a "client"
	proxy, err := utils.ConnectToCluster("127.0.0.1", "", "", 14002)

	if err != nil {
		t.Log("Unable to connect to proxy session.")
		t.Fatal(err)
	}
	defer proxy.Close()

	tests := []struct {
		name                      string
		mockQuery                 simulacron.Then
		execFunc                  func(session *gocql.Session) ([]byte, error)
		expectedLogOps            []primitive.OpCode
		expectedSimulacronQueries []simulacron.QueryType
		expectAtTarget            bool
	}{
		{
			name: "prepare_and_execute",
			mockQuery: simulacron.WhenQuery(
				"INSERT INTO myks.users (name) VALUES (?)",
				simulacron.
					NewWhenQueryOptions().
					WithPositionalParameter(simulacron.DataTypeText, "john")).
				ThenSuccess(),
			execFunc: func(session *gocql.Session) ([]byte, error) {
				err = proxy.Query("INSERT INTO myks.users (name) VALUES (?)", "john").Exec()
				return nil, err
			},
			expectedLogOps: []primitive.OpCode{primitive.OpCodePrepare, primitive.OpCodeExecute},
			expectAtTarget: true,
		},
		{
			name: "query",
			mockQuery: simulacron.WhenQuery(
				" /* trick to skip prepare */ SELECT user, pass FROM users",
				simulacron.NewWhenQueryOptions()).
				ThenSuccess(),
			execFunc: func(session *gocql.Session) ([]byte, error) {
				_, err = proxy.Query(" /* trick to skip prepare */ SELECT user, pass FROM users", "john").Iter().SliceMap()
				return nil, err
			},
			expectedLogOps:            []primitive.OpCode{primitive.OpCodeQuery},
			expectedSimulacronQueries: []simulacron.QueryType{simulacron.QueryTypeQuery},
			expectAtTarget:            false,
		},
		{
			name: "batch_with_client_id",
			mockQuery: simulacron.WhenBatch(
				simulacron.NewWhenBatchOptions().
					WithQueries(
						simulacron.NewBatchQuery("INSERT INTO myks.users (name) VALUES (?)").
							WithPositionalParameter(simulacron.DataTypeText, "Alice"),
						simulacron.NewBatchQuery("INSERT INTO myks.users (name) VALUES (?)").
							WithPositionalParameter(simulacron.DataTypeText, "Bob")).
					WithAllowedConsistencyLevels(gocql.LocalQuorum)).
				ThenSuccess(),
			execFunc: func(session *gocql.Session) ([]byte, error) {
				batch := proxy.NewBatch(gocql.UnloggedBatch)
				batch.Query("INSERT INTO myks.users (name) VALUES (?)", "Alice")
				batch.Query("INSERT INTO myks.users (name) VALUES (?)", "Bob")
				batch.SetConsistency(gocql.LocalQuorum) // must match batch options above

				reqId := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}
				require.Nil(t, err)
				batch.CustomPayload = make(map[string][]byte)
				batch.CustomPayload["request-id"] = reqId // client assigned request ID

				err = proxy.ExecuteBatch(batch)
				return reqId, err
			},
			expectedLogOps:            []primitive.OpCode{primitive.OpCodeBatch},
			expectedSimulacronQueries: []simulacron.QueryType{simulacron.QueryTypeBatch},
			expectAtTarget:            true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err = testSetup.Origin.Prime(tt.mockQuery)
			require.Nil(t, err)
			err = testSetup.Target.Prime(tt.mockQuery)
			require.Nil(t, err)

			clientReqId, err := tt.execFunc(proxy)
			require.Nil(t, err)

			reqIdRegExp := "(.){32}"
			if clientReqId != nil {
				reqIdRegExp = hex.EncodeToString(clientReqId)
			}

			err = buffWriter.Flush()
			require.Nil(t, err)
			allLogs := buff.String()

			// assert request logs
			for _, opCode := range tt.expectedLogOps {
				op := opCode.String()[:len(opCode.String())-7]
				reqRegExp := fmt.Sprintf("(.*)Request id %s \\(hex\\) for query with stream id (.){1,3} and %s(.*)", reqIdRegExp, op)
				require.Regexp(t, reqRegExp, allLogs)
			}
			// assert response logs
			respOriginRegExp := fmt.Sprintf("(.*)Received response from ORIGIN-CONNECTOR for query with stream id (.){1,3} and request id %s \\(hex\\)", reqIdRegExp)
			require.Regexp(t, respOriginRegExp, allLogs)
			respTargetRegExp := fmt.Sprintf("(.*)Received response from TARGET-CONNECTOR for query with stream id (.){1,3} and request id %s \\(hex\\)", reqIdRegExp)
			if tt.expectAtTarget {
				require.Regexp(t, respTargetRegExp, allLogs)
			} else {
				require.NotRegexp(t, respTargetRegExp, allLogs)
			}

			r := regexp.MustCompile(fmt.Sprintf("(.*)\"Received response from ORIGIN-CONNECTOR for query with stream id (.){1,3} and request id %s", reqIdRegExp))
			idLogHex := r.FindAllStringSubmatch(allLogs, -1)[0][0]
			idLogHex = idLogHex[strings.LastIndex(idLogHex, " ")+1:]
			idLog, err := hex.DecodeString(idLogHex)
			require.Nil(t, err)

			// assert ID logged is the same as sent to the C* cluster
			for _, qt := range tt.expectedSimulacronQueries {
				originQueryLog, _ := testSetup.Origin.GetLogsByType(qt)
				queries := originQueryLog.Datacenters[0].Nodes[0].Queries
				require.Equal(t, base64.StdEncoding.EncodeToString(idLog), queries[len(queries)-1].Frame.CustomPayload["request-id"])
			}

			err = testSetup.Origin.DeleteLogs()
			require.Nil(t, err)
			buff.Reset()
		})
	}
}

func TestRequestIdTracingRateLimited(t *testing.T) {
	var buff bytes.Buffer
	buffWriter := bufio.NewWriter(&buff)
	log.SetOutput(buffWriter)

	c := setup.NewTestConfig("", "")
	c.TracingEnabled = true
	c.TracingRateLimit = 5
	testSetup, err := setup.NewSimulacronTestSetupWithSessionAndNodesAndConfig(t, true, false, 1, c, nil)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	// Connect to proxy as a "client"
	proxy, err := utils.ConnectToCluster("127.0.0.1", "", "", 14002)

	if err != nil {
		t.Log("Unable to connect to proxy session.")
		t.Fatal(err)
	}
	defer proxy.Close()

	queryPrime :=
		simulacron.WhenQuery(
			"INSERT INTO myks.users (name) VALUES (?)",
			simulacron.
				NewWhenQueryOptions().
				WithPositionalParameter(simulacron.DataTypeText, "john")).
			ThenSuccess()

	err = testSetup.Origin.Prime(queryPrime)
	require.Nil(t, err)
	err = testSetup.Target.Prime(queryPrime)
	require.Nil(t, err)

	time.Sleep(1 * time.Second) // wait to have a new rate-limit bucket
	for range c.TracingRateLimit * 3 {
		err = proxy.Query("INSERT INTO myks.users (name) VALUES (?)", "john").Exec()
		require.Nil(t, err)
	}

	err = buffWriter.Flush()
	require.Nil(t, err)
	allLogs := buff.String()

	require.Equal(t, c.TracingRateLimit, strings.Count(allLogs, "Request id"))
}
