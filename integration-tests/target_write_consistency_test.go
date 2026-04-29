package integration_tests

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/zdm-proxy/integration-tests/env"
	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/integration-tests/simulacron"
	"github.com/stretchr/testify/require"
)

// getWriteQueries returns QUERY-type log entries for a given cluster.
func getWriteQueries(t *testing.T, cluster *simulacron.Cluster) []*simulacron.RequestLogEntry {
	logs, err := cluster.GetLogsByType(simulacron.QueryTypeQuery)
	require.NoError(t, err)
	var queries []*simulacron.RequestLogEntry
	for _, dc := range logs.Datacenters {
		for _, node := range dc.Nodes {
			queries = append(queries, node.Queries...)
		}
	}
	return queries
}

// TestTargetConsistencyOverride_Disabled verifies that when the override config is NOT set,
// both origin and target receive the client-requested consistency level unchanged.
func TestTargetConsistencyOverride_Disabled(t *testing.T) {
	// Default config — no override
	testSetup, err := setup.NewSimulacronTestSetup(t)
	require.NoError(t, err)
	defer testSetup.Cleanup()

	queryPrime :=
		simulacron.WhenQuery(
			"INSERT INTO myks.users (name) VALUES ('alice')",
			simulacron.NewWhenQueryOptions()).
			ThenSuccess()

	err = testSetup.Origin.Prime(queryPrime)
	require.NoError(t, err)
	err = testSetup.Target.Prime(queryPrime)
	require.NoError(t, err)

	// Clear logs before test
	err = testSetup.Origin.DeleteLogs()
	require.NoError(t, err)
	err = testSetup.Target.DeleteLogs()
	require.NoError(t, err)

	// Send a write with LOCAL_QUORUM using low-level client
	cqlClient := client.NewCqlClient("127.0.0.1:14002", nil)
	cqlConn, err := cqlClient.ConnectAndInit(context.Background(), env.DefaultProtocolVersionSimulacron, 0)
	require.NoError(t, err)
	defer cqlConn.Close()

	queryMsg := &message.Query{
		Query: "INSERT INTO myks.users (name) VALUES ('alice')",
		Options: &message.QueryOptions{
			Consistency: primitive.ConsistencyLevelLocalQuorum,
		},
	}

	rsp, err := cqlConn.SendAndReceive(frame.NewFrame(env.DefaultProtocolVersionSimulacron, 0, queryMsg))
	require.NoError(t, err)
	require.Equal(t, primitive.OpCodeResult, rsp.Header.OpCode)

	// Verify origin received LOCAL_QUORUM
	originQueries := getWriteQueries(t, testSetup.Origin)
	require.GreaterOrEqual(t, len(originQueries), 1, "expected at least 1 query on origin")
	lastOriginQuery := originQueries[len(originQueries)-1]
	require.Equal(t, "LOCAL_QUORUM", lastOriginQuery.ConsistencyLevel,
		"origin should receive client-requested LOCAL_QUORUM")

	// Verify target also received LOCAL_QUORUM (no override)
	targetQueries := getWriteQueries(t, testSetup.Target)
	require.GreaterOrEqual(t, len(targetQueries), 1, "expected at least 1 query on target")
	lastTargetQuery := targetQueries[len(targetQueries)-1]
	require.Equal(t, "LOCAL_QUORUM", lastTargetQuery.ConsistencyLevel,
		"target should receive client-requested LOCAL_QUORUM when override is disabled")
}

// TestTargetConsistencyOverride_Enabled verifies that when the override is set to LOCAL_ONE,
// origin receives the original CL but target receives LOCAL_ONE.
func TestTargetConsistencyOverride_Enabled(t *testing.T) {
	c := setup.NewTestConfig("", "")
	c.TargetConsistencyLevel = "LOCAL_ONE"

	testSetup, err := setup.NewSimulacronTestSetupWithConfig(t, c)
	require.NoError(t, err)
	defer testSetup.Cleanup()

	queryPrime :=
		simulacron.WhenQuery(
			"INSERT INTO myks.users (name) VALUES ('bob')",
			simulacron.NewWhenQueryOptions()).
			ThenSuccess()

	err = testSetup.Origin.Prime(queryPrime)
	require.NoError(t, err)
	err = testSetup.Target.Prime(queryPrime)
	require.NoError(t, err)

	// Clear logs before test
	err = testSetup.Origin.DeleteLogs()
	require.NoError(t, err)
	err = testSetup.Target.DeleteLogs()
	require.NoError(t, err)

	// Send a write with LOCAL_QUORUM
	cqlClient := client.NewCqlClient("127.0.0.1:14002", nil)
	cqlConn, err := cqlClient.ConnectAndInit(context.Background(), env.DefaultProtocolVersionSimulacron, 0)
	require.NoError(t, err)
	defer cqlConn.Close()

	queryMsg := &message.Query{
		Query: "INSERT INTO myks.users (name) VALUES ('bob')",
		Options: &message.QueryOptions{
			Consistency: primitive.ConsistencyLevelLocalQuorum,
		},
	}

	rsp, err := cqlConn.SendAndReceive(frame.NewFrame(env.DefaultProtocolVersionSimulacron, 0, queryMsg))
	require.NoError(t, err)
	require.Equal(t, primitive.OpCodeResult, rsp.Header.OpCode)

	// Verify origin still receives LOCAL_QUORUM (unchanged)
	originQueries := getWriteQueries(t, testSetup.Origin)
	require.GreaterOrEqual(t, len(originQueries), 1, "expected at least 1 query on origin")
	lastOriginQuery := originQueries[len(originQueries)-1]
	require.Equal(t, "LOCAL_QUORUM", lastOriginQuery.ConsistencyLevel,
		"origin should always receive client-requested LOCAL_QUORUM")

	// Verify target receives LOCAL_ONE (overridden)
	targetQueries := getWriteQueries(t, testSetup.Target)
	require.GreaterOrEqual(t, len(targetQueries), 1, "expected at least 1 query on target")
	lastTargetQuery := targetQueries[len(targetQueries)-1]
	require.Equal(t, "LOCAL_ONE", lastTargetQuery.ConsistencyLevel,
		"target should receive overridden LOCAL_ONE")
}

// TestTargetConsistencyOverride_ReadAlsoAffected verifies that read requests
// routed to the target cluster are also affected by the consistency override.
// With default config (PrimaryCluster=ORIGIN), reads go to origin only, so
// the target does not receive them. This test uses PrimaryCluster=TARGET to
// route reads to target and verify the override applies.
func TestTargetConsistencyOverride_ReadAlsoAffected(t *testing.T) {
	c := setup.NewTestConfig("", "")
	c.TargetConsistencyLevel = "LOCAL_ONE"
	c.PrimaryCluster = "TARGET"

	testSetup, err := setup.NewSimulacronTestSetupWithConfig(t, c)
	require.NoError(t, err)
	defer testSetup.Cleanup()

	expectedRows := simulacron.NewRowsResult(
		map[string]simulacron.DataType{"name": simulacron.DataTypeText}).
		WithRow(map[string]interface{}{"name": "alice"})

	queryPrime :=
		simulacron.WhenQuery(
			"SELECT name FROM myks.users",
			simulacron.NewWhenQueryOptions()).
			ThenRowsSuccess(expectedRows)

	err = testSetup.Target.Prime(queryPrime)
	require.NoError(t, err)

	// Clear logs before test
	err = testSetup.Target.DeleteLogs()
	require.NoError(t, err)

	// Send a read with LOCAL_QUORUM
	cqlClient := client.NewCqlClient("127.0.0.1:14002", nil)
	cqlConn, err := cqlClient.ConnectAndInit(context.Background(), env.DefaultProtocolVersionSimulacron, 0)
	require.NoError(t, err)
	defer cqlConn.Close()

	queryMsg := &message.Query{
		Query: "SELECT name FROM myks.users",
		Options: &message.QueryOptions{
			Consistency: primitive.ConsistencyLevelLocalQuorum,
		},
	}

	rsp, err := cqlConn.SendAndReceive(frame.NewFrame(env.DefaultProtocolVersionSimulacron, 0, queryMsg))
	require.NoError(t, err)
	require.Equal(t, primitive.OpCodeResult, rsp.Header.OpCode)

	// Verify target received LOCAL_ONE (overridden), not LOCAL_QUORUM
	targetQueries := getWriteQueries(t, testSetup.Target)
	require.GreaterOrEqual(t, len(targetQueries), 1, "expected at least 1 query on target")
	lastTargetQuery := targetQueries[len(targetQueries)-1]
	require.Equal(t, "LOCAL_ONE", lastTargetQuery.ConsistencyLevel,
		"read queries to target should also be affected by consistency override")
}

// TestTargetConsistencyOverride_Enabled_ONE verifies override with a different CL value (ONE).
func TestTargetConsistencyOverride_Enabled_ONE(t *testing.T) {
	c := setup.NewTestConfig("", "")
	c.TargetConsistencyLevel = "ONE"

	testSetup, err := setup.NewSimulacronTestSetupWithConfig(t, c)
	require.NoError(t, err)
	defer testSetup.Cleanup()

	queryPrime :=
		simulacron.WhenQuery(
			"INSERT INTO myks.users (name) VALUES ('charlie')",
			simulacron.NewWhenQueryOptions()).
			ThenSuccess()

	err = testSetup.Origin.Prime(queryPrime)
	require.NoError(t, err)
	err = testSetup.Target.Prime(queryPrime)
	require.NoError(t, err)

	err = testSetup.Origin.DeleteLogs()
	require.NoError(t, err)
	err = testSetup.Target.DeleteLogs()
	require.NoError(t, err)

	cqlClient := client.NewCqlClient("127.0.0.1:14002", nil)
	cqlConn, err := cqlClient.ConnectAndInit(context.Background(), env.DefaultProtocolVersionSimulacron, 0)
	require.NoError(t, err)
	defer cqlConn.Close()

	queryMsg := &message.Query{
		Query: "INSERT INTO myks.users (name) VALUES ('charlie')",
		Options: &message.QueryOptions{
			Consistency: primitive.ConsistencyLevelAll,
		},
	}

	rsp, err := cqlConn.SendAndReceive(frame.NewFrame(env.DefaultProtocolVersionSimulacron, 0, queryMsg))
	require.NoError(t, err)
	require.Equal(t, primitive.OpCodeResult, rsp.Header.OpCode)

	// Origin gets ALL (client-requested)
	originQueries := getWriteQueries(t, testSetup.Origin)
	require.GreaterOrEqual(t, len(originQueries), 1)
	require.Equal(t, "ALL", originQueries[len(originQueries)-1].ConsistencyLevel)

	// Target gets ONE (overridden)
	targetQueries := getWriteQueries(t, testSetup.Target)
	require.GreaterOrEqual(t, len(targetQueries), 1)
	require.Equal(t, "ONE", targetQueries[len(targetQueries)-1].ConsistencyLevel)
}

// TestTargetConsistencyOverride_PreparedStatement verifies that the override applies
// to EXECUTE messages (prepared statement execution), not just inline Query writes.
func TestTargetConsistencyOverride_PreparedStatement(t *testing.T) {
	c := setup.NewTestConfig("", "")
	c.TargetConsistencyLevel = "LOCAL_ONE"

	testSetup, err := setup.NewSimulacronTestSetupWithConfig(t, c)
	require.NoError(t, err)
	defer testSetup.Cleanup()

	// Prime the query for both clusters (simulacron needs this for PREPARE + EXECUTE)
	queryPrime :=
		simulacron.WhenQuery(
			"INSERT INTO myks.users (name) VALUES (?)",
			simulacron.NewWhenQueryOptions().
				WithPositionalParameter(simulacron.DataTypeText, "dave")).
			ThenSuccess()

	err = testSetup.Origin.Prime(queryPrime)
	require.NoError(t, err)
	err = testSetup.Target.Prime(queryPrime)
	require.NoError(t, err)

	// Connect with low-level client
	cqlClient := client.NewCqlClient("127.0.0.1:14002", nil)
	cqlConn, err := cqlClient.ConnectAndInit(context.Background(), env.DefaultProtocolVersionSimulacron, 0)
	require.NoError(t, err)
	defer cqlConn.Close()

	// Step 1: PREPARE
	prepareMsg := &message.Prepare{
		Query: "INSERT INTO myks.users (name) VALUES (?)",
	}
	prepareResp, err := cqlConn.SendAndReceive(frame.NewFrame(env.DefaultProtocolVersionSimulacron, 0, prepareMsg))
	require.NoError(t, err)

	prepared, ok := prepareResp.Body.Message.(*message.PreparedResult)
	require.True(t, ok, "expected PreparedResult but got %T", prepareResp.Body.Message)

	// Clear logs between PREPARE and EXECUTE
	err = testSetup.Origin.DeleteLogs()
	require.NoError(t, err)
	err = testSetup.Target.DeleteLogs()
	require.NoError(t, err)

	// Step 2: EXECUTE with LOCAL_QUORUM
	executeMsg := &message.Execute{
		QueryId:          prepared.PreparedQueryId,
		ResultMetadataId: prepared.ResultMetadataId,
		Options: &message.QueryOptions{
			Consistency:      primitive.ConsistencyLevelLocalQuorum,
			PositionalValues: []*primitive.Value{primitive.NewValue([]byte("dave"))},
		},
	}
	execResp, err := cqlConn.SendAndReceive(frame.NewFrame(env.DefaultProtocolVersionSimulacron, 0, executeMsg))
	require.NoError(t, err)
	require.Equal(t, primitive.OpCodeResult, execResp.Header.OpCode)

	// Check origin EXECUTE logs — should have LOCAL_QUORUM
	originExecLogs, err := testSetup.Origin.GetLogsByType(simulacron.QueryTypeExecute)
	require.NoError(t, err)
	originExecQueries := originExecLogs.Datacenters[0].Nodes[0].Queries
	require.GreaterOrEqual(t, len(originExecQueries), 1, "expected at least 1 EXECUTE on origin")

	lastOriginExec := originExecQueries[len(originExecQueries)-1]
	var originExecMsg simulacron.ExecuteMessage
	err = json.Unmarshal(lastOriginExec.Frame.Message, &originExecMsg)
	require.NoError(t, err)
	require.NotNil(t, originExecMsg.Options)
	require.Equal(t, "LOCAL_QUORUM", originExecMsg.Options.Consistency,
		"origin EXECUTE should retain client-requested LOCAL_QUORUM")

	// Check target EXECUTE logs — should have LOCAL_ONE (overridden)
	targetExecLogs, err := testSetup.Target.GetLogsByType(simulacron.QueryTypeExecute)
	require.NoError(t, err)
	targetExecQueries := targetExecLogs.Datacenters[0].Nodes[0].Queries
	require.GreaterOrEqual(t, len(targetExecQueries), 1, "expected at least 1 EXECUTE on target")

	lastTargetExec := targetExecQueries[len(targetExecQueries)-1]
	var targetExecMsg simulacron.ExecuteMessage
	err = json.Unmarshal(lastTargetExec.Frame.Message, &targetExecMsg)
	require.NoError(t, err)
	require.NotNil(t, targetExecMsg.Options)
	require.Equal(t, "LOCAL_ONE", targetExecMsg.Options.Consistency,
		"target EXECUTE should have overridden LOCAL_ONE")
}

// TestTargetConsistencyOverride_Batch verifies that the override applies to BATCH messages.
func TestTargetConsistencyOverride_Batch(t *testing.T) {
	c := setup.NewTestConfig("", "")
	c.TargetConsistencyLevel = "LOCAL_ONE"

	testSetup, err := setup.NewSimulacronTestSetupWithConfig(t, c)
	require.NoError(t, err)
	defer testSetup.Cleanup()

	// Prime individual queries that will be part of the batch
	queryPrime1 :=
		simulacron.WhenQuery(
			"INSERT INTO myks.users (name) VALUES ('eve')",
			simulacron.NewWhenQueryOptions()).
			ThenSuccess()
	queryPrime2 :=
		simulacron.WhenQuery(
			"INSERT INTO myks.users (name) VALUES ('frank')",
			simulacron.NewWhenQueryOptions()).
			ThenSuccess()

	for _, prime := range []simulacron.Then{queryPrime1, queryPrime2} {
		err = testSetup.Origin.Prime(prime)
		require.NoError(t, err)
		err = testSetup.Target.Prime(prime)
		require.NoError(t, err)
	}

	cqlClient := client.NewCqlClient("127.0.0.1:14002", nil)
	cqlConn, err := cqlClient.ConnectAndInit(context.Background(), env.DefaultProtocolVersionSimulacron, 0)
	require.NoError(t, err)
	defer cqlConn.Close()

	// Clear logs
	err = testSetup.Origin.DeleteLogs()
	require.NoError(t, err)
	err = testSetup.Target.DeleteLogs()
	require.NoError(t, err)

	// Send a BATCH with LOCAL_QUORUM
	batchMsg := &message.Batch{
		Type: primitive.BatchTypeLogged,
		Children: []*message.BatchChild{
			{
				Query: "INSERT INTO myks.users (name) VALUES ('eve')",
			},
			{
				Query: "INSERT INTO myks.users (name) VALUES ('frank')",
			},
		},
		Consistency: primitive.ConsistencyLevelLocalQuorum,
	}

	batchResp, err := cqlConn.SendAndReceive(frame.NewFrame(env.DefaultProtocolVersionSimulacron, 0, batchMsg))
	require.NoError(t, err)
	require.Equal(t, primitive.OpCodeResult, batchResp.Header.OpCode)

	// Helper to extract batch messages from logs
	getBatchMessages := func(cluster *simulacron.Cluster) []*simulacron.BatchMessage {
		logs, err := cluster.GetLogsByType(simulacron.QueryTypeBatch)
		require.NoError(t, err)
		var batches []*simulacron.BatchMessage
		for _, dc := range logs.Datacenters {
			for _, node := range dc.Nodes {
				for _, entry := range node.Queries {
					var bm simulacron.BatchMessage
					err := json.Unmarshal(entry.Frame.Message, &bm)
					if err == nil {
						batches = append(batches, &bm)
					}
				}
			}
		}
		return batches
	}

	// Check origin BATCH — should have LOCAL_QUORUM
	originBatches := getBatchMessages(testSetup.Origin)
	require.GreaterOrEqual(t, len(originBatches), 1, "expected at least 1 BATCH on origin")
	require.Equal(t, "LOCAL_QUORUM", originBatches[len(originBatches)-1].Consistency,
		"origin BATCH should retain client-requested LOCAL_QUORUM")

	// Check target BATCH — should have LOCAL_ONE (overridden)
	targetBatches := getBatchMessages(testSetup.Target)
	require.GreaterOrEqual(t, len(targetBatches), 1, "expected at least 1 BATCH on target")
	require.Equal(t, "LOCAL_ONE", targetBatches[len(targetBatches)-1].Consistency,
		"target BATCH should have overridden LOCAL_ONE")
}
