package integration_tests

import (
	"context"
	"github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/gocql/gocql"
	"github.com/riptano/cloud-gate/integration-tests/setup"
	"github.com/riptano/cloud-gate/integration-tests/simulacron"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestAsyncReadError(t *testing.T) {
	c := setup.NewTestConfig("", "")
	c.DualReadsEnabled = true
	c.AsyncReadsOnSecondary = true
	testSetup, err := setup.NewSimulacronTestSetupWithConfig(c)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	query := "SELECT * FROM test"
	expectedRows := simulacron.NewRowsResult(
		map[string]simulacron.DataType{
			"abcd": simulacron.DataTypeText,
		}).WithRow(map[string]interface{}{
		"abcd": "123",
	})

	err = testSetup.Origin.Prime(simulacron.WhenQuery(
		query,
		simulacron.NewWhenQueryOptions()).
		ThenRowsSuccess(expectedRows))
	require.Nil(t, err)

	err = testSetup.Target.Prime(simulacron.WhenQuery(
		query,
		simulacron.NewWhenQueryOptions()).
		ThenReadTimeout(gocql.LocalOne, 0, 0, false))
	require.Nil(t, err)

	client := client.NewCqlClient("127.0.0.1:14002", nil)
	cqlClientConn, err := client.ConnectAndInit(context.Background(), primitive.ProtocolVersion4, 0)
	require.Nil(t, err)
	defer cqlClientConn.Close()

	queryMsg := &message.Query{
		Query:   query,
		Options: nil,
	}

	rsp, err := cqlClientConn.SendAndReceive(frame.NewFrame(primitive.ProtocolVersion4, 0, queryMsg))
	require.Nil(t, err)
	require.Equal(t, primitive.OpCodeResult, rsp.Header.OpCode)
	rowsMsg, ok := rsp.Body.Message.(*message.RowsResult)
	require.True(t, ok)
	require.Equal(t, 1, len(rowsMsg.Metadata.Columns))
	require.Equal(t, "abcd", rowsMsg.Metadata.Columns[0].Name)
}

func TestAsyncReadHighLatency(t *testing.T) {
	c := setup.NewTestConfig("", "")
	c.DualReadsEnabled = true
	c.AsyncReadsOnSecondary = true
	testSetup, err := setup.NewSimulacronTestSetupWithConfig(c)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	query := "SELECT * FROM test"
	expectedRows := simulacron.NewRowsResult(
		map[string]simulacron.DataType{
			"abcd": simulacron.DataTypeText,
		}).WithRow(map[string]interface{}{
		"abcd": "123",
	})

	err = testSetup.Origin.Prime(simulacron.WhenQuery(
		query,
		simulacron.NewWhenQueryOptions()).
		ThenRowsSuccess(expectedRows))
	require.Nil(t, err)

	err = testSetup.Target.Prime(simulacron.WhenQuery(
		query,
		simulacron.NewWhenQueryOptions()).
		ThenReadTimeout(gocql.LocalOne, 0, 0, false).WithDelay(1 * time.Second))
	require.Nil(t, err)

	client := client.NewCqlClient("127.0.0.1:14002", nil)
	cqlClientConn, err := client.ConnectAndInit(context.Background(), primitive.ProtocolVersion4, 0)
	require.Nil(t, err)
	defer cqlClientConn.Close()

	queryMsg := &message.Query{
		Query:   query,
		Options: nil,
	}

	now := time.Now()
	rsp, err := cqlClientConn.SendAndReceive(frame.NewFrame(primitive.ProtocolVersion4, 0, queryMsg))
	require.Less(t, time.Now().Sub(now).Milliseconds(), int64(500))
	require.Nil(t, err)
	require.Equal(t, primitive.OpCodeResult, rsp.Header.OpCode)
	rowsMsg, ok := rsp.Body.Message.(*message.RowsResult)
	require.True(t, ok)
	require.Equal(t, 1, len(rowsMsg.Metadata.Columns))
	require.Equal(t, "abcd", rowsMsg.Metadata.Columns[0].Name)
}