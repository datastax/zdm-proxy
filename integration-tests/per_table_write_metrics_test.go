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
// Returns VoidResult for writes and PreparedResult for PREPARE requests.
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
		// Return a PreparedResult using the query string as the prepared ID
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
		insertFrame := frame.NewFrame(env.DefaultProtocolVersion, client.ManagedStreamId,
			&message.Query{Query: "INSERT INTO ks1.t1 (id, name) VALUES (1, 'alice')"})
		_, err = clientConn.SendAndReceive(insertFrame)
		require.Nil(t, err)

		lines := gatherMetricLines(t, conf)
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="origin",keyspace="ks1",table="t1"}`, 1)
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="target",keyspace="ks1",table="t1"}`, 1)
	})

	// ================================================================
	// TEST 2: Inline UPDATE (including counter-style)
	// ================================================================
	t.Run("inline_update", func(t *testing.T) {
		updateFrame := frame.NewFrame(env.DefaultProtocolVersion, client.ManagedStreamId,
			&message.Query{Query: "UPDATE ks1.t2 SET val = 'updated' WHERE id = 1"})
		_, err = clientConn.SendAndReceive(updateFrame)
		require.Nil(t, err)

		lines := gatherMetricLines(t, conf)
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="origin",keyspace="ks1",table="t2"}`, 1)
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="target",keyspace="ks1",table="t2"}`, 1)
	})

	// ================================================================
	// TEST 3: Prepared statement (PREPARE + EXECUTE)
	// ================================================================
	t.Run("prepared_statement", func(t *testing.T) {
		// PREPARE
		prepareFrame := frame.NewFrame(env.DefaultProtocolVersion, client.ManagedStreamId,
			&message.Prepare{Query: "INSERT INTO ks1.t3 (id, name) VALUES (?, ?)"})
		prepResp, err := clientConn.SendAndReceive(prepareFrame)
		require.Nil(t, err)
		prepared, ok := prepResp.Body.Message.(*message.PreparedResult)
		require.True(t, ok, "expected PreparedResult but got %T", prepResp.Body.Message)

		// EXECUTE
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
	// TEST 4: BATCH with inline children targeting different tables
	// ================================================================
	t.Run("batch_inline", func(t *testing.T) {
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
	// TEST 5: Multiple writes accumulate correctly
	// ================================================================
	t.Run("accumulation", func(t *testing.T) {
		for i := 0; i < 3; i++ {
			insertFrame := frame.NewFrame(env.DefaultProtocolVersion, client.ManagedStreamId,
				&message.Query{Query: "INSERT INTO ks1.t1 (id, name) VALUES (1, 'repeat')"})
			_, err = clientConn.SendAndReceive(insertFrame)
			require.Nil(t, err)
		}

		lines := gatherMetricLines(t, conf)
		// t1 had 1 from test 1 + 3 from this test = 4
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="origin",keyspace="ks1",table="t1"}`, 4)
		requireMetricLine(t, lines, `zdm_proxy_write_success_total{cluster="target",keyspace="ks1",table="t1"}`, 4)
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
	// Print all matching lines for debugging
	var matching []string
	for _, line := range lines {
		if strings.Contains(line, "write_success") {
			matching = append(matching, line)
		}
	}
	t.Errorf("metric line not found: %q\nAll write_success lines: %v", expected, matching)
}
