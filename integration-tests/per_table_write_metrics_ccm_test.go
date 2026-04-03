package integration_tests

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/integration-tests/utils"
)

// TestPerTableWriteMetricsCCM tests per-table write success metrics against real Cassandra clusters via CCM.
// This test verifies that successful writes to both origin and target are tracked per keyspace and table.
func TestPerTableWriteMetricsCCM(t *testing.T) {
	proxyInstance, err := NewProxyInstanceForGlobalCcmClusters(t)
	require.Nil(t, err)
	defer proxyInstance.Shutdown()

	originCluster, targetCluster, err := SetupOrGetGlobalCcmClusters(t)
	require.Nil(t, err)

	// Seed test data
	data := [][]string{
		{"cf0f4cf0-8c20-11ea-9fc6-6d2c86545d91", "seed_data"},
	}
	setup.SeedData(originCluster.GetSession(), targetCluster.GetSession(), setup.TasksModel, data)

	// Connect to proxy as a "client"
	proxy, err := utils.ConnectToCluster("127.0.0.1", "", "", 14002)
	require.Nil(t, err)
	defer proxy.Close()

	// ================================================================
	// TEST 1: Inline INSERT
	// ================================================================
	t.Run("inline_insert", func(t *testing.T) {
		err = proxy.Query(fmt.Sprintf("INSERT INTO %s.%s (id, task) VALUES (d1b05da0-8c20-11ea-9fc6-6d2c86545d91, 'inline_test')", setup.TestKeyspace, setup.TasksModel)).Exec()
		require.Nil(t, err)

		lines := gatherCCMMetricLines(t)
		requireMetricPresent(t, lines, "proxy_write_success_total", "origin", setup.TestKeyspace, setup.TasksModel)
		requireMetricPresent(t, lines, "proxy_write_success_total", "target", setup.TestKeyspace, setup.TasksModel)
	})

	// ================================================================
	// TEST 2: Inline UPDATE
	// ================================================================
	t.Run("inline_update", func(t *testing.T) {
		err = proxy.Query(fmt.Sprintf("UPDATE %s.%s SET task = 'updated' WHERE id = cf0f4cf0-8c20-11ea-9fc6-6d2c86545d91", setup.TestKeyspace, setup.TasksModel)).Exec()
		require.Nil(t, err)

		lines := gatherCCMMetricLines(t)
		// Should have at least 2 now (1 from insert + 1 from update)
		requireMetricPresent(t, lines, "proxy_write_success_total", "origin", setup.TestKeyspace, setup.TasksModel)
	})

	// ================================================================
	// TEST 3: Prepared statement
	// ================================================================
	t.Run("prepared_statement", func(t *testing.T) {
		stmt := proxy.Query(fmt.Sprintf("INSERT INTO %s.%s (id, task) VALUES (?, ?)", setup.TestKeyspace, setup.TasksModel))
		stmt.Bind("eed574b0-8c20-11ea-9fc6-6d2c86545d91", "prepared_test")
		err = stmt.Exec()
		require.Nil(t, err)

		lines := gatherCCMMetricLines(t)
		requireMetricPresent(t, lines, "proxy_write_success_total", "origin", setup.TestKeyspace, setup.TasksModel)
		requireMetricPresent(t, lines, "proxy_write_success_total", "target", setup.TestKeyspace, setup.TasksModel)
	})

	// ================================================================
	// TEST 4: Batch statement
	// ================================================================
	t.Run("batch_statement", func(t *testing.T) {
		batch := proxy.NewBatch(0) // LOGGED batch
		batch.Query(fmt.Sprintf("INSERT INTO %s.%s (id, task) VALUES (cf0f4cf0-8c20-11ea-9fc6-6d2c86545d92, 'batch1')", setup.TestKeyspace, setup.TasksModel))
		batch.Query(fmt.Sprintf("INSERT INTO %s.%s (id, task) VALUES (cf0f4cf0-8c20-11ea-9fc6-6d2c86545d93, 'batch2')", setup.TestKeyspace, setup.TasksModel))
		err = proxy.ExecuteBatch(batch)
		require.Nil(t, err)

		lines := gatherCCMMetricLines(t)
		requireMetricPresent(t, lines, "proxy_write_success_total", "origin", setup.TestKeyspace, setup.TasksModel)
		requireMetricPresent(t, lines, "proxy_write_success_total", "target", setup.TestKeyspace, setup.TasksModel)
	})

	// ================================================================
	// TEST 5: Verify data actually landed on both clusters
	// ================================================================
	t.Run("verify_data_on_both", func(t *testing.T) {
		// Check origin has the data
		var task string
		err := originCluster.GetSession().Query(
			fmt.Sprintf("SELECT task FROM %s.%s WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d91", setup.TestKeyspace, setup.TasksModel)).Scan(&task)
		require.Nil(t, err)
		require.Equal(t, "inline_test", task)

		// Check target has the data
		err = targetCluster.GetSession().Query(
			fmt.Sprintf("SELECT task FROM %s.%s WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d91", setup.TestKeyspace, setup.TasksModel)).Scan(&task)
		require.Nil(t, err)
		require.Equal(t, "inline_test", task)
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

// requireMetricPresent checks that a write_success metric exists for the given cluster/keyspace/table with a value > 0.
func requireMetricPresent(t *testing.T, lines []string, metricName string, cluster string, keyspace string, table string) {
	t.Helper()
	prefix := fmt.Sprintf(`zdm_%s{cluster="%s",keyspace="%s",table="%s"}`, metricName, cluster, keyspace, table)
	for _, line := range lines {
		if strings.HasPrefix(line, prefix) {
			return // found it
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
