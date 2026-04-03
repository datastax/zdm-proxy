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
const countersTable2 = "metrics_test_counters2"
const batchTableA = "metrics_test_batch_a"
const batchTableB = "metrics_test_batch_b"

// TestPerTableWriteMetricsCCM tests per-table write success metrics against real Cassandra clusters via CCM.
// Full permutation matrix:
//   - Statement types: INSERT, UPDATE, DELETE, counter UPDATE
//   - Execution modes: inline (Query), prepared (Prepare+Execute)
//   - Batch modes: batch with inline children, batch with prepared children, batch with mixed children
//
// Each test verifies that the Prometheus metric is tracked independently for origin and target.
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
		s.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", setup.TestKeyspace, countersTable2)).Exec()
		s.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", setup.TestKeyspace, batchTableA)).Exec()
		s.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", setup.TestKeyspace, batchTableB)).Exec()

		require.Nil(t, s.Query(fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s.%s (id uuid PRIMARY KEY, name text)", setup.TestKeyspace, metricsTable)).Exec())
		require.Nil(t, s.Query(fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s.%s (id uuid PRIMARY KEY, count counter)", setup.TestKeyspace, countersTable)).Exec())
		require.Nil(t, s.Query(fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s.%s (id uuid PRIMARY KEY, count counter)", setup.TestKeyspace, countersTable2)).Exec())
		require.Nil(t, s.Query(fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s.%s (id uuid PRIMARY KEY, val text)", setup.TestKeyspace, batchTableA)).Exec())
		require.Nil(t, s.Query(fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s.%s (id uuid PRIMARY KEY, val text)", setup.TestKeyspace, batchTableB)).Exec())
	}
	createTables(originCluster.GetSession())
	createTables(targetSession)

	// Connect to proxy
	proxy, err := utils.ConnectToCluster("127.0.0.1", "", "", 14002)
	require.Nil(t, err)
	defer proxy.Close()

	ks := setup.TestKeyspace

	// ================================================================
	// INLINE STATEMENTS
	// ================================================================

	t.Run("inline_insert", func(t *testing.T) {
		err = proxy.Query(fmt.Sprintf(
			"INSERT INTO %s.%s (id, name) VALUES (d1b05da0-8c20-11ea-9fc6-6d2c86545d91, 'alice')", ks, metricsTable)).Exec()
		require.Nil(t, err)
		assertMetricOnBothClusters(t, ks, metricsTable)
	})

	t.Run("inline_update", func(t *testing.T) {
		err = proxy.Query(fmt.Sprintf(
			"UPDATE %s.%s SET name = 'updated' WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d91", ks, metricsTable)).Exec()
		require.Nil(t, err)
		assertMetricOnBothClusters(t, ks, metricsTable)
	})

	t.Run("inline_delete", func(t *testing.T) {
		err = proxy.Query(fmt.Sprintf(
			"DELETE FROM %s.%s WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d91", ks, metricsTable)).Exec()
		require.Nil(t, err)
		assertMetricOnBothClusters(t, ks, metricsTable)
	})

	t.Run("inline_counter_update", func(t *testing.T) {
		err = proxy.Query(fmt.Sprintf(
			"UPDATE %s.%s SET count = count + 1 WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d91", ks, countersTable)).Exec()
		require.Nil(t, err)
		assertMetricOnBothClusters(t, ks, countersTable)
	})

	// ================================================================
	// PREPARED STATEMENTS
	// ================================================================

	t.Run("prepared_insert", func(t *testing.T) {
		q := proxy.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (?, ?)", ks, metricsTable))
		q.Bind("eed574b0-8c20-11ea-9fc6-6d2c86545d91", "prepared_alice")
		require.Nil(t, q.Exec())
		assertMetricOnBothClusters(t, ks, metricsTable)
	})

	t.Run("prepared_update", func(t *testing.T) {
		q := proxy.Query(fmt.Sprintf("UPDATE %s.%s SET name = ? WHERE id = ?", ks, metricsTable))
		q.Bind("prepared_updated", "eed574b0-8c20-11ea-9fc6-6d2c86545d91")
		require.Nil(t, q.Exec())
		assertMetricOnBothClusters(t, ks, metricsTable)
	})

	t.Run("prepared_delete", func(t *testing.T) {
		q := proxy.Query(fmt.Sprintf("DELETE FROM %s.%s WHERE id = ?", ks, metricsTable))
		q.Bind("eed574b0-8c20-11ea-9fc6-6d2c86545d91")
		require.Nil(t, q.Exec())
		assertMetricOnBothClusters(t, ks, metricsTable)
	})

	t.Run("prepared_counter_update", func(t *testing.T) {
		q := proxy.Query(fmt.Sprintf("UPDATE %s.%s SET count = count + ? WHERE id = ?", ks, countersTable))
		q.Bind(int64(5), "eed574b0-8c20-11ea-9fc6-6d2c86545d91")
		require.Nil(t, q.Exec())
		assertMetricOnBothClusters(t, ks, countersTable)
	})

	// ================================================================
	// BATCH WITH INLINE CHILDREN
	// ================================================================

	t.Run("batch_inline_inserts_multi_table", func(t *testing.T) {
		batch := proxy.NewBatch(gocql.LoggedBatch)
		batch.Query(fmt.Sprintf("INSERT INTO %s.%s (id, val) VALUES (cf0f4cf0-8c20-11ea-9fc6-6d2c86545d91, 'a')", ks, batchTableA))
		batch.Query(fmt.Sprintf("INSERT INTO %s.%s (id, val) VALUES (cf0f4cf0-8c20-11ea-9fc6-6d2c86545d92, 'b')", ks, batchTableB))
		require.Nil(t, proxy.ExecuteBatch(batch))
		assertMetricOnBothClusters(t, ks, batchTableA)
		assertMetricOnBothClusters(t, ks, batchTableB)
	})

	t.Run("batch_inline_update_and_delete", func(t *testing.T) {
		batch := proxy.NewBatch(gocql.LoggedBatch)
		batch.Query(fmt.Sprintf("UPDATE %s.%s SET val = 'updated' WHERE id = cf0f4cf0-8c20-11ea-9fc6-6d2c86545d91", ks, batchTableA))
		batch.Query(fmt.Sprintf("DELETE FROM %s.%s WHERE id = cf0f4cf0-8c20-11ea-9fc6-6d2c86545d92", ks, batchTableB))
		require.Nil(t, proxy.ExecuteBatch(batch))
		assertMetricOnBothClusters(t, ks, batchTableA)
		assertMetricOnBothClusters(t, ks, batchTableB)
	})

	// ================================================================
	// BATCH WITH PREPARED CHILDREN
	// gocql automatically prepares statements when batch.Query() is
	// called with bind parameters — the batch children become prepared
	// statement IDs, not inline query strings.
	// ================================================================

	t.Run("batch_prepared_inserts_multi_table", func(t *testing.T) {
		batch := proxy.NewBatch(gocql.LoggedBatch)
		batch.Query(fmt.Sprintf("INSERT INTO %s.%s (id, val) VALUES (?, ?)", ks, batchTableA), "cf0f4cf0-8c20-11ea-9fc6-6d2c86545da1", "prep_a")
		batch.Query(fmt.Sprintf("INSERT INTO %s.%s (id, val) VALUES (?, ?)", ks, batchTableB), "cf0f4cf0-8c20-11ea-9fc6-6d2c86545da2", "prep_b")
		require.Nil(t, proxy.ExecuteBatch(batch))
		assertMetricOnBothClusters(t, ks, batchTableA)
		assertMetricOnBothClusters(t, ks, batchTableB)
	})

	t.Run("batch_prepared_update_and_delete", func(t *testing.T) {
		batch := proxy.NewBatch(gocql.LoggedBatch)
		batch.Query(fmt.Sprintf("UPDATE %s.%s SET val = ? WHERE id = ?", ks, batchTableA), "batch_updated", "cf0f4cf0-8c20-11ea-9fc6-6d2c86545da1")
		batch.Query(fmt.Sprintf("DELETE FROM %s.%s WHERE id = ?", ks, batchTableB), "cf0f4cf0-8c20-11ea-9fc6-6d2c86545da2")
		require.Nil(t, proxy.ExecuteBatch(batch))
		assertMetricOnBothClusters(t, ks, batchTableA)
		assertMetricOnBothClusters(t, ks, batchTableB)
	})

	// ================================================================
	// BATCH WITH MIXED INLINE AND PREPARED CHILDREN
	// ================================================================

	t.Run("batch_mixed_inline_and_prepared", func(t *testing.T) {
		batch := proxy.NewBatch(gocql.LoggedBatch)
		// Inline child (no bind params)
		batch.Query(fmt.Sprintf("INSERT INTO %s.%s (id, val) VALUES (cf0f4cf0-8c20-11ea-9fc6-6d2c86545db1, 'inline')", ks, batchTableA))
		// Prepared child (with bind params — gocql will prepare this)
		batch.Query(fmt.Sprintf("INSERT INTO %s.%s (id, val) VALUES (?, ?)", ks, batchTableB), "cf0f4cf0-8c20-11ea-9fc6-6d2c86545db2", "prepared")
		require.Nil(t, proxy.ExecuteBatch(batch))
		assertMetricOnBothClusters(t, ks, batchTableA)
		assertMetricOnBothClusters(t, ks, batchTableB)
	})

	// ================================================================
	// COUNTER BATCH (inline)
	// ================================================================

	t.Run("batch_counter_inline", func(t *testing.T) {
		batch := proxy.NewBatch(gocql.CounterBatch)
		batch.Query(fmt.Sprintf("UPDATE %s.%s SET count = count + 1 WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d91", ks, countersTable))
		batch.Query(fmt.Sprintf("UPDATE %s.%s SET count = count + 1 WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d91", ks, countersTable2))
		require.Nil(t, proxy.ExecuteBatch(batch))
		assertMetricOnBothClusters(t, ks, countersTable)
		assertMetricOnBothClusters(t, ks, countersTable2)
	})

	// ================================================================
	// COUNTER BATCH (prepared — gocql prepares when bind params are used)
	// ================================================================

	t.Run("batch_counter_prepared", func(t *testing.T) {
		batch := proxy.NewBatch(gocql.CounterBatch)
		batch.Query(fmt.Sprintf("UPDATE %s.%s SET count = count + ? WHERE id = ?", ks, countersTable), int64(3), "eed574b0-8c20-11ea-9fc6-6d2c86545d91")
		batch.Query(fmt.Sprintf("UPDATE %s.%s SET count = count + ? WHERE id = ?", ks, countersTable2), int64(3), "eed574b0-8c20-11ea-9fc6-6d2c86545d91")
		require.Nil(t, proxy.ExecuteBatch(batch))
		assertMetricOnBothClusters(t, ks, countersTable)
		assertMetricOnBothClusters(t, ks, countersTable2)
	})

	// ================================================================
	// DATA VERIFICATION ON BOTH CLUSTERS
	// ================================================================

	t.Run("verify_counter_on_target", func(t *testing.T) {
		var count int64
		err := targetSession.Query(fmt.Sprintf(
			"SELECT count FROM %s.%s WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d91", ks, countersTable)).Scan(&count)
		require.Nil(t, err)
		require.True(t, count >= 1, "counter should be at least 1, got %d", count)
	})

	t.Run("verify_batch_data_on_target", func(t *testing.T) {
		var val string
		err := targetSession.Query(fmt.Sprintf(
			"SELECT val FROM %s.%s WHERE id = cf0f4cf0-8c20-11ea-9fc6-6d2c86545da1", ks, batchTableA)).Scan(&val)
		require.Nil(t, err)
		require.Equal(t, "batch_updated", val)
	})
}

// assertMetricOnBothClusters verifies that the write success metric exists for both origin and target.
func assertMetricOnBothClusters(t *testing.T, keyspace string, table string) {
	t.Helper()
	lines := gatherCCMMetricLines(t)
	requireMetricPresent(t, lines, "proxy_write_success_total", "origin", keyspace, table)
	requireMetricPresent(t, lines, "proxy_write_success_total", "target", keyspace, table)
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
