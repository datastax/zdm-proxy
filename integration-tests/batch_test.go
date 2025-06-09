package integration_tests

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/integration-tests/simulacron"
	"github.com/datastax/zdm-proxy/integration-tests/utils"
	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBatchBothWriteTimeout(t *testing.T) {
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

	options := newBatchOptions()

	queryPrimeOrigin := simulacron.WhenBatch(options).
		ThenWriteTimeout(gocql.One, 1, 1, simulacron.Batch)

	queryPrimeTarget := simulacron.WhenBatch(options).
		ThenWriteTimeout(gocql.Two, 2, 2, simulacron.BatchLog)

	doPrime(t, testSetup.Origin, queryPrimeOrigin)
	doPrime(t, testSetup.Target, queryPrimeTarget)

	batch := newBatch(proxy)
	err = proxy.ExecuteBatch(batch)

	require.True(t, err != nil, "batch should have failed but it didn't")

	errTimeOut, ok := err.(*gocql.RequestErrWriteTimeout)
	require.True(t, ok, "error is not Write Timeout: ", err.Error())

	// assert that the error returned by the proxy matches the origin cluster error not the target cluster one
	require.Equal(t, 1, errTimeOut.Received, "errTimeOut.Received: expected 1, got ", errTimeOut.Received)
	require.Equal(t, 1, errTimeOut.BlockFor, "errTimeOut.BlockFor: expected 1, got ", errTimeOut.BlockFor)
	require.Equal(t, string(simulacron.Batch), errTimeOut.WriteType, "errTimeOut.WriteType: expected BATCH, got ", errTimeOut.WriteType)
	require.Equal(t, gocql.One, errTimeOut.Consistency, "errTimeOut.Consistency: expected ONE, got ", errTimeOut.Consistency)
}

func TestBatchOriginWriteTimeout(t *testing.T) {
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

	options := newBatchOptions()

	queryPrimeOrigin := simulacron.WhenBatch(options).
		ThenWriteTimeout(gocql.One, 1, 1, simulacron.Batch)

	queryPrimeTarget := simulacron.WhenBatch(options).ThenSuccess()

	doPrime(t, testSetup.Origin, queryPrimeOrigin)
	doPrime(t, testSetup.Target, queryPrimeTarget)

	batch := newBatch(proxy)
	err = proxy.ExecuteBatch(batch)

	require.True(t, err != nil, "batch should have failed but it didn't")

	errTimeOut, ok := err.(*gocql.RequestErrWriteTimeout)
	require.True(t, ok, "error is not Write Timeout: ", err.Error())

	// assert that the error returned by the proxy matches the origin cluster error
	require.Equal(t, 1, errTimeOut.Received, "errTimeOut.Received: expected 1, got ", errTimeOut.Received)
	require.Equal(t, 1, errTimeOut.BlockFor, "errTimeOut.BlockFor: expected 1, got ", errTimeOut.BlockFor)
	require.Equal(t, string(simulacron.Batch), errTimeOut.WriteType, "errTimeOut.WriteType: expected BATCH, got ", errTimeOut.WriteType)
	require.Equal(t, gocql.One, errTimeOut.Consistency, "errTimeOut.Consistency: expected ONE, got ", errTimeOut.Consistency)
}

func TestBatchTargetWriteTimeout(t *testing.T) {
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

	options := newBatchOptions()

	queryPrimeOrigin := simulacron.WhenBatch(options).ThenSuccess()

	queryPrimeTarget := simulacron.WhenBatch(options).
		ThenWriteTimeout(gocql.One, 1, 1, simulacron.Batch)

	doPrime(t, testSetup.Origin, queryPrimeOrigin)
	doPrime(t, testSetup.Target, queryPrimeTarget)

	batch := newBatch(proxy)
	err = proxy.ExecuteBatch(batch)

	require.True(t, err != nil, "batch should have failed but it didn't")

	errTimeOut, ok := err.(*gocql.RequestErrWriteTimeout)
	require.True(t, ok, "error is not Write Timeout: ", err.Error())

	// assert that the error returned by the proxy matches the target cluster error
	require.Equal(t, 1, errTimeOut.Received, "errTimeOut.Received: expected 1, got ", errTimeOut.Received)
	require.Equal(t, 1, errTimeOut.BlockFor, "errTimeOut.BlockFor: expected 1, got ", errTimeOut.BlockFor)
	require.Equal(t, string(simulacron.Batch), errTimeOut.WriteType, "errTimeOut.WriteType: expected BATCH, got ", errTimeOut.WriteType)
	require.Equal(t, gocql.One, errTimeOut.Consistency, "errTimeOut.Consistency: expected ONE, got ", errTimeOut.Consistency)
}

func TestBatchWriteSuccessful(t *testing.T) {
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

	options := newBatchOptions()

	queryPrimeOrigin := simulacron.WhenBatch(options).ThenSuccess()
	queryPrimeTarget := simulacron.WhenBatch(options).ThenSuccess()

	doPrime(t, testSetup.Origin, queryPrimeOrigin)
	doPrime(t, testSetup.Target, queryPrimeTarget)

	batch := newBatch(proxy)
	err = proxy.ExecuteBatch(batch)

	require.True(t, err == nil, "batch shouldn't have failed but it did")
}

func TestBatchTracingWithClientId(t *testing.T) {
	var buff bytes.Buffer
	buffWriter := bufio.NewWriter(&buff)
	log.SetOutput(buffWriter)

	c := setup.NewTestConfig("", "")
	c.EnableTracing = true
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

	options := newBatchOptions()

	queryPrimeOrigin := simulacron.WhenBatch(options).ThenSuccess()
	queryPrimeTarget := simulacron.WhenBatch(options).ThenSuccess()

	doPrime(t, testSetup.Origin, queryPrimeOrigin)
	doPrime(t, testSetup.Target, queryPrimeTarget)

	batch := newBatch(proxy)
	reqId := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}
	require.Nil(t, err)
	batch.CustomPayload = make(map[string][]byte)
	batch.CustomPayload["request-id"] = reqId // client assigned request ID
	reqIdHex := hex.EncodeToString(reqId)
	err = proxy.ExecuteBatch(batch)
	require.True(t, err == nil, "batch shouldn't have failed but it did")

	err = buffWriter.Flush()
	require.Nil(t, err)
	allLogs := buff.String()
	require.Regexp(t, fmt.Sprintf("(.*)Request id %s \\(hex\\) for query with stream id (.){1,3} and OpCode BATCH \\[0x0D\\](.*)", reqIdHex), allLogs)
	require.Regexp(t, fmt.Sprintf("(.*)Received response from ORIGIN-CONNECTOR for query with stream id (.){1,3} and request id %s \\(hex\\)", reqIdHex), allLogs)
	require.Regexp(t, fmt.Sprintf("(.*)Received response from TARGET-CONNECTOR for query with stream id (.){1,3} and request id %s \\(hex\\)", reqIdHex), allLogs)

	targetQueryLog, _ := testSetup.Origin.GetLogsByType(simulacron.QueryTypeBatch)
	require.Equal(t, base64.StdEncoding.EncodeToString(reqId), targetQueryLog.Datacenters[0].Nodes[0].Queries[0].Frame.CustomPayload["request-id"])
}

func newBatchOptions() *simulacron.WhenBatchOptions {
	return simulacron.
		NewWhenBatchOptions().
		WithQueries(
			simulacron.NewBatchQuery("INSERT INTO myks.users (name) VALUES (?)").
				WithPositionalParameter(simulacron.DataTypeText, "Alice"),
			simulacron.NewBatchQuery("INSERT INTO myks.users (name) VALUES (?)").
				WithPositionalParameter(simulacron.DataTypeText, "Bob")).
		WithAllowedConsistencyLevels(gocql.LocalQuorum)
}

func newBatch(proxy *gocql.Session) *gocql.Batch {
	batch := proxy.NewBatch(gocql.UnloggedBatch)
	batch.Query("INSERT INTO myks.users (name) VALUES (?)", "Alice")
	batch.Query("INSERT INTO myks.users (name) VALUES (?)", "Bob")
	batch.SetConsistency(gocql.LocalQuorum) // must match batch options above
	return batch
}

func doPrime(t *testing.T, cluster *simulacron.Cluster, prime simulacron.Then) {
	err := cluster.Prime(prime)
	if err != nil {
		t.Fatal("prime error: ", err.Error())
	}
}
