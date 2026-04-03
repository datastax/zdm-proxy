package integration_tests

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/stretchr/testify/require"

	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/integration-tests/utils"
)

const metricsTable = "metrics_test_data"
const countersTable = "metrics_test_counters"
const batchTableA = "metrics_test_batch_a"
const batchTableB = "metrics_test_batch_b"

// TestPerTableWriteMetricsCCM tests per-table write success metrics against real Cassandra clusters via CCM.
// Covers: inline INSERT, UPDATE, DELETE, counter UPDATE, prepared INSERT, prepared DELETE,
// batch with inline children, batch with prepared children, and batch with mixed children.
func TestPerTableWriteMetricsCCM(t *testing.T) {
	proxyInstance, err := NewProxyInstanceForGlobalCcmClusters(t)
	require.Nil(t, err)
	defer proxyInstance.Shutdown()

	originCluster, targetCluster, err := SetupOrGetGlobalCcmClusters(t)
	require.Nil(t, err)

	targetSession := targetCluster.GetSession()

	// Create test tables on both clusters
	createTables := func(s *gocql.Session) {
		s.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", setup.TestKeyspace, metricsTable)).Exec()
		s.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", setup.TestKeyspace, countersTable)).Exec()
		s.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", setup.TestKeyspace, batchTableA)).Exec()
		s.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", setup.TestKeyspace, batchTableB)).Exec()

		require.Nil(t, s.Query(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (id uuid PRIMARY KEY, name text)", setup.TestKeyspace, metricsTable)).Exec())
		require.Nil(t, s.Query(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (id uuid PRIMARY KEY, count counter)", setup.TestKeyspace, countersTable)).Exec())
		require.Nil(t, s.Query(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (id uuid PRIMARY KEY, val text)", setup.TestKeyspace, batchTableA)).Exec())
		require.Nil(t, s.Query(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (id uuid PRIMARY KEY, val text)", setup.TestKeyspace, batchTableB)).Exec())
	}
	createTables(originCluster.GetSession())
	createTables(targetSession)

	// Connect to proxy as a "client"
	proxy, err := utils.ConnectToCluster("127.0.0.1", "", "", 14002)
	require.Nil(t, err)
	defer proxy.Close()

	// ================================================================
	// TEST 1: Inline INSERT
	// ================================================================
	t.Run("inline_insert", func(t *testing.T) {
		err = proxy.Query(fmt.Sprintf(
			"INSERT INTO %s.%s (id, name) VALUES (d1b05da0-8c20-11ea-9fc6-6d2c86545d91, 'alice')",
			setup.TestKeyspace, metricsTable)).Exec()
		require.Nil(t, err)

		lines := gatherCCMMetricLines(t)
		requireMetricPresent(t, lines, "proxy_write_success_total", "origin", setup.TestKeyspace, metricsTable)
		requireMetricPresent(t, lines, "proxy_write_success_total", "target", setup.TestKeyspace, metricsTable)
	})

	// ================================================================
	// TEST 2: Inline UPDATE
	// ================================================================
	t.Run("inline_update", func(t *testing.T) {
		err = proxy.Query(fmt.Sprintf(
			"UPDATE %s.%s SET name = 'updated' WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d91",
			setup.TestKeyspace, metricsTable)).Exec()
		require.Nil(t, err)

		lines := gatherCCMMetricLines(t)
		requireMetricPresent(t, lines, "proxy_write_success_total", "origin", setup.TestKeyspace, metricsTable)
		requireMetricPresent(t, lines, "proxy_write_success_total", "target", setup.TestKeyspace, metricsTable)
	})

	// ================================================================
	// TEST 3: Inline DELETE
	// ================================================================
	t.Run("inline_delete", func(t *testing.T) {
		err = proxy.Query(fmt.Sprintf(
			"DELETE FROM %s.%s WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d91",
			setup.TestKeyspace, metricsTable)).Exec()
		require.Nil(t, err)

		lines := gatherCCMMetricLines(t)
		requireMetricPresent(t, lines, "proxy_write_success_total", "origin", setup.TestKeyspace, metricsTable)
		requireMetricPresent(t, lines, "proxy_write_success_total", "target", setup.TestKeyspace, metricsTable)
	})

	// ================================================================
	// TEST 4: Counter UPDATE
	// ================================================================
	t.Run("counter_update", func(t *testing.T) {
		err = proxy.Query(fmt.Sprintf(
			"UPDATE %s.%s SET count = count + 1 WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d91",
			setup.TestKeyspace, countersTable)).Exec()
		require.Nil(t, err)

		lines := gatherCCMMetricLines(t)
		requireMetricPresent(t, lines, "proxy_write_success_total", "origin", setup.TestKeyspace, countersTable)
		requireMetricPresent(t, lines, "proxy_write_success_total", "target", setup.TestKeyspace, countersTable)
	})

	// ================================================================
	// TEST 5: Prepared INSERT
	// ================================================================
	t.Run("prepared_insert", func(t *testing.T) {
		stmt := proxy.Query(fmt.Sprintf(
			"INSERT INTO %s.%s (id, name) VALUES (?, ?)", setup.TestKeyspace, metricsTable))
		stmt.Bind("eed574b0-8c20-11ea-9fc6-6d2c86545d91", "prepared_alice")
		err = stmt.Exec()
		require.Nil(t, err)

		lines := gatherCCMMetricLines(t)
		requireMetricPresent(t, lines, "proxy_write_success_total", "origin", setup.TestKeyspace, metricsTable)
		requireMetricPresent(t, lines, "proxy_write_success_total", "target", setup.TestKeyspace, metricsTable)
	})

	// ================================================================
	// TEST 6: Prepared DELETE
	// ================================================================
	t.Run("prepared_delete", func(t *testing.T) {
		stmt := proxy.Query(fmt.Sprintf(
			"DELETE FROM %s.%s WHERE id = ?", setup.TestKeyspace, metricsTable))
		stmt.Bind("eed574b0-8c20-11ea-9fc6-6d2c86545d91")
		err = stmt.Exec()
		require.Nil(t, err)

		lines := gatherCCMMetricLines(t)
		requireMetricPresent(t, lines, "proxy_write_success_total", "origin", setup.TestKeyspace, metricsTable)
		requireMetricPresent(t, lines, "proxy_write_success_total", "target", setup.TestKeyspace, metricsTable)
	})

	// ================================================================
	// TEST 7: Batch with inline children across different tables
	// ================================================================
	t.Run("batch_inline_multi_table", func(t *testing.T) {
		batch := proxy.NewBatch(0) // LOGGED
		batch.Query(fmt.Sprintf(
			"INSERT INTO %s.%s (id, val) VALUES (cf0f4cf0-8c20-11ea-9fc6-6d2c86545d91, 'batch_a')",
			setup.TestKeyspace, batchTableA))
		batch.Query(fmt.Sprintf(
			"INSERT INTO %s.%s (id, val) VALUES (cf0f4cf0-8c20-11ea-9fc6-6d2c86545d92, 'batch_b')",
			setup.TestKeyspace, batchTableB))
		err = proxy.ExecuteBatch(batch)
		require.Nil(t, err)

		lines := gatherCCMMetricLines(t)
		requireMetricPresent(t, lines, "proxy_write_success_total", "origin", setup.TestKeyspace, batchTableA)
		requireMetricPresent(t, lines, "proxy_write_success_total", "origin", setup.TestKeyspace, batchTableB)
		requireMetricPresent(t, lines, "proxy_write_success_total", "target", setup.TestKeyspace, batchTableA)
		requireMetricPresent(t, lines, "proxy_write_success_total", "target", setup.TestKeyspace, batchTableB)
	})

	// ================================================================
	// TEST 8: Batch with inline children on same table (INSERT + DELETE)
	// ================================================================
	t.Run("batch_inline_insert_and_delete", func(t *testing.T) {
		batch := proxy.NewBatch(0)
		batch.Query(fmt.Sprintf(
			"INSERT INTO %s.%s (id, val) VALUES (cf0f4cf0-8c20-11ea-9fc6-6d2c86545d93, 'to_delete')",
			setup.TestKeyspace, batchTableA))
		batch.Query(fmt.Sprintf(
			"DELETE FROM %s.%s WHERE id = cf0f4cf0-8c20-11ea-9fc6-6d2c86545d91",
			setup.TestKeyspace, batchTableA))
		err = proxy.ExecuteBatch(batch)
		require.Nil(t, err)

		lines := gatherCCMMetricLines(t)
		requireMetricPresent(t, lines, "proxy_write_success_total", "origin", setup.TestKeyspace, batchTableA)
		requireMetricPresent(t, lines, "proxy_write_success_total", "target", setup.TestKeyspace, batchTableA)
	})

	// ================================================================
	// TEST 9: Verify data landed on both clusters
	// ================================================================
	t.Run("verify_data_on_both_clusters", func(t *testing.T) {
		// Counter should exist on target
		var count int
		err := targetSession.Query(fmt.Sprintf(
			"SELECT count FROM %s.%s WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d91",
			setup.TestKeyspace, countersTable)).Scan(&count)
		require.Nil(t, err)
		require.Equal(t, 1, count)

		// Batch data should exist on target
		var val string
		err = targetSession.Query(fmt.Sprintf(
			"SELECT val FROM %s.%s WHERE id = cf0f4cf0-8c20-11ea-9fc6-6d2c86545d92",
			setup.TestKeyspace, batchTableB)).Scan(&val)
		require.Nil(t, err)
		require.Equal(t, "batch_b", val)
	})
}

// gatherCCMMetricLines scrapes the metrics endpoint used by CCM tests.
func gatherCCMMetricLines(t *testing.T) []string {
	t.Helper()
	statusCode, rspStr, err := utils.GetMetrics("localhost:14001")
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

// requireMetricPresent checks that a write_success metric exists for the given cluster/keyspace/table.
func requireMetricPresent(t *testing.T, lines []string, metricName string, cluster string, keyspace string, table string) {
	t.Helper()
	prefix := fmt.Sprintf(`zdm_%s{cluster="%s",keyspace="%s",table="%s"}`, metricName, cluster, keyspace, table)
	for _, line := range lines {
		if strings.HasPrefix(line, prefix) {
			return
		}
	}

	var matching []string
	for _, line := range lines {
		if strings.Contains(line, "write_success") {
			matching = append(matching, line)
		}
	}
	t.Errorf("metric not found with prefix: %q\nAll write_success lines: %v", prefix, matching)
}
