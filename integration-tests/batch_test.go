package integration_tests

import (
	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/integration-tests/simulacron"
	"github.com/datastax/zdm-proxy/integration-tests/utils"
	"github.com/gocql/gocql"
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
