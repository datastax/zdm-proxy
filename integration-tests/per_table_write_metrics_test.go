package integration_tests

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/datastax/zdm-proxy/integration-tests/env"
	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/integration-tests/utils"
	"github.com/datastax/zdm-proxy/proxy/pkg/config"
	"github.com/datastax/zdm-proxy/proxy/pkg/httpzdmproxy"
)

// handleWritesAllTypes handles QUERY (INSERT/UPDATE/DELETE), PREPARE, EXECUTE, and BATCH messages.
func handleWritesAllTypes(request *frame.Frame, _ *client.CqlServerConnection, _ client.RequestHandlerContext) (response *frame.Frame) {
	time.Sleep(10 * time.Millisecond)
	version := request.Header.Version
	id := request.Header.StreamId

	switch msg := request.Body.Message.(type) {
	case *message.Query:
		query := msg.Query
		if strings.HasPrefix(query, "INSERT") || strings.HasPrefix(query, "UPDATE") || strings.HasPrefix(query, "DELETE") {
			return frame.NewFrame(version, id, &message.VoidResult{})
		}
	case *message.Prepare:
		result := &message.PreparedResult{
			PreparedQueryId:   []byte(msg.Query),
			VariablesMetadata: &message.VariablesMetadata{},
			ResultMetadata:    &message.RowsMetadata{ColumnCount: 0},
		}
		return frame.NewFrame(version, id, result)
	case *message.Execute:
		return frame.NewFrame(version, id, &message.VoidResult{})
	case *message.Batch:
		return frame.NewFrame(version, id, &message.VoidResult{})
	}
	return
}

func testPerTableWriteMetrics(t *testing.T, metricsHandler *httpzdmproxy.HandlerWithFallback) {
	originHost := "127.0.1.1"
	targetHost := "127.0.1.2"
	conf := setup.NewTestConfig(originHost, targetHost)

	testSetup, err := setup.NewCqlServerTestSetup(t, conf, false, false, false)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	testSetup.Origin.CqlServer.RequestHandlers = []client.RequestHandler{
		client.RegisterHandler, client.HeartbeatHandler, client.HandshakeHandler,
		client.NewSystemTablesHandler("cluster1", "dc1"),
		handleReads, handleWritesAllTypes,
	}
	testSetup.Target.CqlServer.RequestHandlers = []client.RequestHandler{
		client.RegisterHandler, client.HeartbeatHandler, client.HandshakeHandler,
		client.NewSystemTablesHandler("cluster2", "dc2"),
		handleReads, handleWritesAllTypes,
	}

	err = testSetup.Start(conf, false, env.DefaultProtocolVersion)
	require.Nil(t, err)

	wg := &sync.WaitGroup{}
	defer wg.Wait()
	srv := startMetricsHandler(t, conf, wg, metricsHandler)
	defer func() {
		if err := srv.Close(); err != nil {
			log.Warnf("error cleaning http server: %v", err)
		}
	}()

	EnsureMetricsServerListening(t, conf)

	err = testSetup.Client.Connect(env.DefaultProtocolVersion)
	require.Nil(t, err)
	clientConn := testSetup.Client.CqlConnection

	// ================================================================
	// TEST 1: Inline INSERT
	// ================================================================
	t.Run("inline_insert", func(t *testing.T) {
		f := frame.NewFrame(env.DefaultProtocolVersion, client.ManagedStreamId,
			&message.Query{Query: "INSERT INTO ks1.t1 (id, name) VALUES (1, 'alice')"})
		_, err = clientConn.SendAndReceive(f)
		require.Nil(t, err)

		lines := gatherMetricLines(t, conf)
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="origin",keyspace="ks1",table="t1"}`, 1)
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="target",keyspace="ks1",table="t1"}`, 1)
	})

	// ================================================================
	// TEST 2: Inline UPDATE
	// ================================================================
	t.Run("inline_update", func(t *testing.T) {
		f := frame.NewFrame(env.DefaultProtocolVersion, client.ManagedStreamId,
			&message.Query{Query: "UPDATE ks1.t2 SET val = 'updated' WHERE id = 1"})
		_, err = clientConn.SendAndReceive(f)
		require.Nil(t, err)

		lines := gatherMetricLines(t, conf)
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="origin",keyspace="ks1",table="t2"}`, 1)
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="target",keyspace="ks1",table="t2"}`, 1)
	})

	// ================================================================
	// TEST 3: Inline DELETE
	// ================================================================
	t.Run("inline_delete", func(t *testing.T) {
		f := frame.NewFrame(env.DefaultProtocolVersion, client.ManagedStreamId,
			&message.Query{Query: "DELETE FROM ks1.t1 WHERE id = 1"})
		_, err = clientConn.SendAndReceive(f)
		require.Nil(t, err)

		lines := gatherMetricLines(t, conf)
		// t1 had 1 from insert + 1 from delete = 2
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="origin",keyspace="ks1",table="t1"}`, 2)
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="target",keyspace="ks1",table="t1"}`, 2)
	})

	// ================================================================
	// TEST 4: Counter UPDATE
	// ================================================================
	t.Run("counter_update", func(t *testing.T) {
		f := frame.NewFrame(env.DefaultProtocolVersion, client.ManagedStreamId,
			&message.Query{Query: "UPDATE ks1.counters SET count = count + 1 WHERE id = 1"})
		_, err = clientConn.SendAndReceive(f)
		require.Nil(t, err)

		lines := gatherMetricLines(t, conf)
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="origin",keyspace="ks1",table="counters"}`, 1)
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="target",keyspace="ks1",table="counters"}`, 1)
	})

	// ================================================================
	// TEST 5: Prepared INSERT (PREPARE + EXECUTE)
	// ================================================================
	t.Run("prepared_insert", func(t *testing.T) {
		prepareFrame := frame.NewFrame(env.DefaultProtocolVersion, client.ManagedStreamId,
			&message.Prepare{Query: "INSERT INTO ks1.t3 (id, name) VALUES (?, ?)"})
		prepResp, err := clientConn.SendAndReceive(prepareFrame)
		require.Nil(t, err)
		prepared, ok := prepResp.Body.Message.(*message.PreparedResult)
		require.True(t, ok, "expected PreparedResult but got %T", prepResp.Body.Message)

		executeFrame := frame.NewFrame(env.DefaultProtocolVersion, client.ManagedStreamId,
			&message.Execute{
				QueryId: prepared.PreparedQueryId,
				Options: &message.QueryOptions{
					PositionalValues: []*primitive.Value{
						primitive.NewValue([]byte{0, 0, 0, 1}),
						primitive.NewValue([]byte("bob")),
					},
				},
			})
		_, err = clientConn.SendAndReceive(executeFrame)
		require.Nil(t, err)

		lines := gatherMetricLines(t, conf)
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="origin",keyspace="ks1",table="t3"}`, 1)
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="target",keyspace="ks1",table="t3"}`, 1)
	})

	// ================================================================
	// TEST 6: Prepared UPDATE (PREPARE + EXECUTE)
	// ================================================================
	t.Run("prepared_update", func(t *testing.T) {
		prepareFrame := frame.NewFrame(env.DefaultProtocolVersion, client.ManagedStreamId,
			&message.Prepare{Query: "UPDATE ks1.t3 SET name = ? WHERE id = ?"})
		prepResp, err := clientConn.SendAndReceive(prepareFrame)
		require.Nil(t, err)
		prepared, ok := prepResp.Body.Message.(*message.PreparedResult)
		require.True(t, ok)

		executeFrame := frame.NewFrame(env.DefaultProtocolVersion, client.ManagedStreamId,
			&message.Execute{
				QueryId: prepared.PreparedQueryId,
				Options: &message.QueryOptions{
					PositionalValues: []*primitive.Value{
						primitive.NewValue([]byte("updated")),
						primitive.NewValue([]byte{0, 0, 0, 1}),
					},
				},
			})
		_, err = clientConn.SendAndReceive(executeFrame)
		require.Nil(t, err)

		lines := gatherMetricLines(t, conf)
		// t3 had 1 from insert + 1 from update = 2
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="origin",keyspace="ks1",table="t3"}`, 2)
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="target",keyspace="ks1",table="t3"}`, 2)
	})

	// ================================================================
	// TEST 7: Prepared counter UPDATE (PREPARE + EXECUTE)
	// ================================================================
	t.Run("prepared_counter_update", func(t *testing.T) {
		prepareFrame := frame.NewFrame(env.DefaultProtocolVersion, client.ManagedStreamId,
			&message.Prepare{Query: "UPDATE ks1.counters SET count = count + ? WHERE id = ?"})
		prepResp, err := clientConn.SendAndReceive(prepareFrame)
		require.Nil(t, err)
		prepared, ok := prepResp.Body.Message.(*message.PreparedResult)
		require.True(t, ok)

		executeFrame := frame.NewFrame(env.DefaultProtocolVersion, client.ManagedStreamId,
			&message.Execute{
				QueryId: prepared.PreparedQueryId,
				Options: &message.QueryOptions{
					PositionalValues: []*primitive.Value{
						primitive.NewValue([]byte{0, 0, 0, 0, 0, 0, 0, 5}), // counter value (bigint)
						primitive.NewValue([]byte{0, 0, 0, 1}),
					},
				},
			})
		_, err = clientConn.SendAndReceive(executeFrame)
		require.Nil(t, err)

		lines := gatherMetricLines(t, conf)
		// counters: 1 from inline + 1 from prepared = 2
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="origin",keyspace="ks1",table="counters"}`, 2)
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="target",keyspace="ks1",table="counters"}`, 2)
	})

	// ================================================================
	// TEST 8: Prepared DELETE (PREPARE + EXECUTE)
	// ================================================================
	t.Run("prepared_delete", func(t *testing.T) {
		prepareFrame := frame.NewFrame(env.DefaultProtocolVersion, client.ManagedStreamId,
			&message.Prepare{Query: "DELETE FROM ks1.t3 WHERE id = ?"})
		prepResp, err := clientConn.SendAndReceive(prepareFrame)
		require.Nil(t, err)
		prepared, ok := prepResp.Body.Message.(*message.PreparedResult)
		require.True(t, ok)

		executeFrame := frame.NewFrame(env.DefaultProtocolVersion, client.ManagedStreamId,
			&message.Execute{
				QueryId: prepared.PreparedQueryId,
				Options: &message.QueryOptions{
					PositionalValues: []*primitive.Value{primitive.NewValue([]byte{0, 0, 0, 1})},
				},
			})
		_, err = clientConn.SendAndReceive(executeFrame)
		require.Nil(t, err)

		lines := gatherMetricLines(t, conf)
		// t3: insert(1) + update(1) + delete(1) = 3
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="origin",keyspace="ks1",table="t3"}`, 3)
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="target",keyspace="ks1",table="t3"}`, 3)
	})

	// ================================================================
	// TEST 7: BATCH with inline children targeting different tables
	// ================================================================
	t.Run("batch_inline_multi_table", func(t *testing.T) {
		batchFrame := frame.NewFrame(env.DefaultProtocolVersion, client.ManagedStreamId,
			&message.Batch{
				Type: primitive.BatchTypeLogged,
				Children: []*message.BatchChild{
					{Query: "INSERT INTO ks1.t4 (id) VALUES (1)"},
					{Query: "INSERT INTO ks1.t5 (id) VALUES (2)"},
				},
				Consistency: primitive.ConsistencyLevelOne,
			})
		_, err = clientConn.SendAndReceive(batchFrame)
		require.Nil(t, err)

		lines := gatherMetricLines(t, conf)
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="origin",keyspace="ks1",table="t4"}`, 1)
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="origin",keyspace="ks1",table="t5"}`, 1)
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="target",keyspace="ks1",table="t4"}`, 1)
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="target",keyspace="ks1",table="t5"}`, 1)
	})

	// ================================================================
	// TEST 8: BATCH with prepared children
	// ================================================================
	t.Run("batch_prepared", func(t *testing.T) {
		// First prepare two statements
		prep1Frame := frame.NewFrame(env.DefaultProtocolVersion, client.ManagedStreamId,
			&message.Prepare{Query: "INSERT INTO ks1.t6 (id) VALUES (?)"})
		prep1Resp, err := clientConn.SendAndReceive(prep1Frame)
		require.Nil(t, err)
		prepared1 := prep1Resp.Body.Message.(*message.PreparedResult)

		prep2Frame := frame.NewFrame(env.DefaultProtocolVersion, client.ManagedStreamId,
			&message.Prepare{Query: "INSERT INTO ks1.t7 (id) VALUES (?)"})
		prep2Resp, err := clientConn.SendAndReceive(prep2Frame)
		require.Nil(t, err)
		prepared2 := prep2Resp.Body.Message.(*message.PreparedResult)

		// Now batch with prepared children
		batchFrame := frame.NewFrame(env.DefaultProtocolVersion, client.ManagedStreamId,
			&message.Batch{
				Type: primitive.BatchTypeLogged,
				Children: []*message.BatchChild{
					{Id: prepared1.PreparedQueryId, Values: []*primitive.Value{primitive.NewValue([]byte{0, 0, 0, 1})}},
					{Id: prepared2.PreparedQueryId, Values: []*primitive.Value{primitive.NewValue([]byte{0, 0, 0, 2})}},
				},
				Consistency: primitive.ConsistencyLevelOne,
			})
		_, err = clientConn.SendAndReceive(batchFrame)
		require.Nil(t, err)

		lines := gatherMetricLines(t, conf)
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="origin",keyspace="ks1",table="t6"}`, 1)
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="origin",keyspace="ks1",table="t7"}`, 1)
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="target",keyspace="ks1",table="t6"}`, 1)
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="target",keyspace="ks1",table="t7"}`, 1)
	})

	// ================================================================
	// TEST 9: BATCH with mixed inline and prepared children
	// ================================================================
	t.Run("batch_mixed", func(t *testing.T) {
		// Prepare one statement
		prepFrame := frame.NewFrame(env.DefaultProtocolVersion, client.ManagedStreamId,
			&message.Prepare{Query: "INSERT INTO ks1.t8 (id) VALUES (?)"})
		prepResp, err := clientConn.SendAndReceive(prepFrame)
		require.Nil(t, err)
		prepared := prepResp.Body.Message.(*message.PreparedResult)

		// Batch with one inline and one prepared
		batchFrame := frame.NewFrame(env.DefaultProtocolVersion, client.ManagedStreamId,
			&message.Batch{
				Type: primitive.BatchTypeLogged,
				Children: []*message.BatchChild{
					{Query: "INSERT INTO ks1.t9 (id) VALUES (1)"},
					{Id: prepared.PreparedQueryId, Values: []*primitive.Value{primitive.NewValue([]byte{0, 0, 0, 2})}},
				},
				Consistency: primitive.ConsistencyLevelOne,
			})
		_, err = clientConn.SendAndReceive(batchFrame)
		require.Nil(t, err)

		lines := gatherMetricLines(t, conf)
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="origin",keyspace="ks1",table="t8"}`, 1)
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="origin",keyspace="ks1",table="t9"}`, 1)
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="target",keyspace="ks1",table="t8"}`, 1)
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="target",keyspace="ks1",table="t9"}`, 1)
	})

	// ================================================================
	// TEST 10: Batch inline with UPDATE and DELETE
	// ================================================================
	t.Run("batch_inline_update_and_delete", func(t *testing.T) {
		batchFrame := frame.NewFrame(env.DefaultProtocolVersion, client.ManagedStreamId,
			&message.Batch{
				Type: primitive.BatchTypeLogged,
				Children: []*message.BatchChild{
					{Query: "UPDATE ks1.t4 SET name = 'updated' WHERE id = 1"},
					{Query: "DELETE FROM ks1.t5 WHERE id = 2"},
				},
				Consistency: primitive.ConsistencyLevelOne,
			})
		_, err = clientConn.SendAndReceive(batchFrame)
		require.Nil(t, err)

		lines := gatherMetricLines(t, conf)
		// t4: 1 (insert batch) + 1 (update batch) = 2
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="origin",keyspace="ks1",table="t4"}`, 2)
		// t5: 1 (insert batch) + 1 (delete batch) = 2
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="origin",keyspace="ks1",table="t5"}`, 2)
	})

	// ================================================================
	// TEST 11: Batch prepared with UPDATE and DELETE
	// ================================================================
	t.Run("batch_prepared_update_and_delete", func(t *testing.T) {
		prepUpdateFrame := frame.NewFrame(env.DefaultProtocolVersion, client.ManagedStreamId,
			&message.Prepare{Query: "UPDATE ks1.t6 SET name = ? WHERE id = ?"})
		prepUpdateResp, err := clientConn.SendAndReceive(prepUpdateFrame)
		require.Nil(t, err)
		preparedUpdate := prepUpdateResp.Body.Message.(*message.PreparedResult)

		prepDeleteFrame := frame.NewFrame(env.DefaultProtocolVersion, client.ManagedStreamId,
			&message.Prepare{Query: "DELETE FROM ks1.t7 WHERE id = ?"})
		prepDeleteResp, err := clientConn.SendAndReceive(prepDeleteFrame)
		require.Nil(t, err)
		preparedDelete := prepDeleteResp.Body.Message.(*message.PreparedResult)

		batchFrame := frame.NewFrame(env.DefaultProtocolVersion, client.ManagedStreamId,
			&message.Batch{
				Type: primitive.BatchTypeLogged,
				Children: []*message.BatchChild{
					{Id: preparedUpdate.PreparedQueryId, Values: []*primitive.Value{primitive.NewValue([]byte("updated")), primitive.NewValue([]byte{0, 0, 0, 1})}},
					{Id: preparedDelete.PreparedQueryId, Values: []*primitive.Value{primitive.NewValue([]byte{0, 0, 0, 2})}},
				},
				Consistency: primitive.ConsistencyLevelOne,
			})
		_, err = clientConn.SendAndReceive(batchFrame)
		require.Nil(t, err)

		lines := gatherMetricLines(t, conf)
		// t6: 1 (insert batch) + 1 (update batch) = 2
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="origin",keyspace="ks1",table="t6"}`, 2)
		// t7: 1 (insert batch) + 1 (delete batch) = 2
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="origin",keyspace="ks1",table="t7"}`, 2)
	})

	// ================================================================
	// TEST 12: Accumulation across multiple writes
	// ================================================================
	t.Run("accumulation", func(t *testing.T) {
		for i := 0; i < 3; i++ {
			f := frame.NewFrame(env.DefaultProtocolVersion, client.ManagedStreamId,
				&message.Query{Query: "INSERT INTO ks1.t1 (id, name) VALUES (1, 'repeat')"})
			_, err = clientConn.SendAndReceive(f)
			require.Nil(t, err)
		}

		lines := gatherMetricLines(t, conf)
		// t1: 1 (insert) + 1 (delete) + 3 (accumulation) = 5
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="origin",keyspace="ks1",table="t1"}`, 5)
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="target",keyspace="ks1",table="t1"}`, 5)
	})
}

// gatherMetricLines scrapes the Prometheus endpoint and returns non-comment lines.
func gatherMetricLines(t *testing.T, conf *config.Config) []string {
	httpAddr := fmt.Sprintf("%s:%d", conf.MetricsAddress, conf.MetricsPort)
	statusCode, rspStr, err := utils.GetMetrics(httpAddr)
	require.Nil(t, err)
	require.Equal(t, http.StatusOK, statusCode)

	var result []string
	for _, line := range strings.Split(rspStr, "\n") {
		if !strings.HasPrefix(line, "#") && strings.TrimSpace(line) != "" {
			result = append(result, line)
		}
	}
	return result
}

// requireMetricLine asserts that a specific metric line with the expected value exists.
func requireMetricLine(t *testing.T, lines []string, metricPrefix string, expectedValue int) {
	t.Helper()
	expected := fmt.Sprintf("%s %d", metricPrefix, expectedValue)
	for _, line := range lines {
		if line == expected {
			return
		}
	}
	var matching []string
	for _, line := range lines {
		if strings.Contains(line, "write_success") {
			matching = append(matching, line)
		}
	}
	t.Errorf("metric line not found: %q\nAll write_success lines: %v", expected, matching)
}
