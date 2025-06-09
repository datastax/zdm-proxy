package integration_tests

import (
	"bufio"
	"bytes"
	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/integration-tests/simulacron"
	"github.com/datastax/zdm-proxy/integration-tests/utils"
	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"testing"
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

func TestPrepareAndExecuteTracing(t *testing.T) {
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

	err = proxy.Query("INSERT INTO myks.users (name) VALUES (?)", "john").Exec()
	require.Nil(t, err)

	err = buffWriter.Flush()
	require.Nil(t, err)
	allLogs := buff.String()
	require.Regexp(t, "(.*)Request id (.){32} \\(hex\\) for query with stream id (.){1,3} and OpCode PREPARE \\[0x09\\](.*)", allLogs)
	require.Regexp(t, "(.*)Request id (.){32} \\(hex\\) for query with stream id (.){1,3} and OpCode EXECUTE \\[0x0A\\](.*)", allLogs)
	require.Regexp(t, "(.*)Received response from ORIGIN-CONNECTOR for query with stream id (.){1,3} and request id (.){32} \\(hex\\)", allLogs)
	require.Regexp(t, "(.*)Received response from TARGET-CONNECTOR for query with stream id (.){1,3} and request id (.){32} \\(hex\\)", allLogs)
}
