package integration_tests

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/stretchr/testify/require"

	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/integration-tests/utils"
	"github.com/datastax/zdm-proxy/proxy/pkg/httpzdmproxy"
	log "github.com/sirupsen/logrus"
)

const toggleTable = "toggle_test_data"
const toggleCounterTable = "toggle_test_counters"
const toggleBatchA = "toggle_test_batch_a"
const toggleBatchB = "toggle_test_batch_b"
const togglePreparedTable = "toggle_test_prepared"

const toggleHTTPAddr = "localhost:14098"

// TestTargetToggleCCM tests the full lifecycle of disabling and re-enabling the target cluster
// at runtime via the REST API.
//
// Phases:
//  1. Write broad set (inline, prepared, batch, counter) with target enabled → verify metrics on both clusters.
//  2. Disable target via API → write same broad set → verify only origin metrics increment.
//  3. While disabled, create NEW prepared statements (these only go to origin).
//  4. Re-enable target via API → use those new prepared statements → verify they work
//     (UNPREPARED recovery re-prepares them on target) → verify both metrics resume.
func TestTargetToggleCCM(t *testing.T) {
	proxyInstance, err := NewProxyInstanceForGlobalCcmClusters(t)
	require.Nil(t, err)
	defer proxyInstance.Shutdown()

	// Start a dedicated HTTP server for metrics and target toggle API
	mux := http.NewServeMux()
	mux.Handle("/metrics", proxyInstance.GetMetricHandler().GetHttpHandler())
	mux.Handle("/api/v1/target", httpzdmproxy.TargetHandler(proxyInstance))
	mux.Handle("/api/v1/target/enable", httpzdmproxy.TargetHandler(proxyInstance))
	mux.Handle("/api/v1/target/disable", httpzdmproxy.TargetHandler(proxyInstance))
	srv := &http.Server{Addr: toggleHTTPAddr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Warnf("toggle test http server error: %v", err)
		}
	}()
	defer srv.Close()

	// Wait for HTTP server to be ready
	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/target", toggleHTTPAddr))
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 5*time.Second, 100*time.Millisecond, "HTTP server did not start")

	originCluster, targetCluster, err := SetupOrGetGlobalCcmClusters(t)
	require.Nil(t, err)

	// Create test tables on both clusters
	createToggleTables := func(s *gocql.Session) {
		tables := []string{toggleTable, toggleBatchA, toggleBatchB, togglePreparedTable}
		for _, tbl := range tables {
			s.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", setup.TestKeyspace, tbl)).Exec()
			require.Nil(t, s.Query(fmt.Sprintf(
				"CREATE TABLE IF NOT EXISTS %s.%s (id uuid PRIMARY KEY, name text)", setup.TestKeyspace, tbl)).Exec())
		}
		s.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", setup.TestKeyspace, toggleCounterTable)).Exec()
		require.Nil(t, s.Query(fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s.%s (id uuid PRIMARY KEY, count counter)", setup.TestKeyspace, toggleCounterTable)).Exec())
	}
	createToggleTables(originCluster.GetSession())
	createToggleTables(targetCluster.GetSession())

	// Connect to proxy
	proxy, err := utils.ConnectToCluster("127.0.0.1", "", "", 14002)
	require.Nil(t, err)
	defer proxy.Close()

	ks := setup.TestKeyspace

	// ================================================================
	// PHASE 1: Target enabled — writes go to both clusters
	// ================================================================
	t.Run("phase1_target_enabled", func(t *testing.T) {
		requireTargetStatus(t, true)

		// Inline INSERT
		require.Nil(t, proxy.Query(fmt.Sprintf(
			"INSERT INTO %s.%s (id, name) VALUES (a0000001-0000-0000-0000-000000000001, 'p1_inline')", ks, toggleTable)).Exec())

		// Inline counter
		require.Nil(t, proxy.Query(fmt.Sprintf(
			"UPDATE %s.%s SET count = count + 1 WHERE id = a0000001-0000-0000-0000-000000000001", ks, toggleCounterTable)).Exec())

		// Prepared INSERT
		q := proxy.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (?, ?)", ks, toggleTable))
		q.Bind("a0000001-0000-0000-0000-000000000002", "p1_prepared")
		require.Nil(t, q.Exec())

		// Batch with inline children
		batch := proxy.NewBatch(gocql.LoggedBatch)
		batch.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (a0000001-0000-0000-0000-000000000003, 'p1_batch_a')", ks, toggleBatchA))
		batch.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (a0000001-0000-0000-0000-000000000003, 'p1_batch_b')", ks, toggleBatchB))
		require.Nil(t, proxy.ExecuteBatch(batch))

		// Batch with prepared children
		batch2 := proxy.NewBatch(gocql.LoggedBatch)
		batch2.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (?, ?)", ks, toggleBatchA), "a0000001-0000-0000-0000-000000000004", "p1_batch_prep_a")
		batch2.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (?, ?)", ks, toggleBatchB), "a0000001-0000-0000-0000-000000000004", "p1_batch_prep_b")
		require.Nil(t, proxy.ExecuteBatch(batch2))

		// Verify metrics on both clusters
		lines := gatherToggleMetricLines(t)
		requireMetricPresent(t, lines, "proxy_write_success_total", "origin", ks, toggleTable)
		requireMetricPresent(t, lines, "proxy_write_success_total", "target", ks, toggleTable)
		requireMetricPresent(t, lines, "proxy_write_success_total", "origin", ks, toggleBatchA)
		requireMetricPresent(t, lines, "proxy_write_success_total", "target", ks, toggleBatchA)

		// Verify data exists on target
		var name string
		err := targetCluster.GetSession().Query(fmt.Sprintf(
			"SELECT name FROM %s.%s WHERE id = a0000001-0000-0000-0000-000000000001", ks, toggleTable)).Scan(&name)
		require.Nil(t, err)
		require.Equal(t, "p1_inline", name)
	})

	// Capture metric values before disabling
	originCountBefore := getMetricValue(t, ks, toggleTable, "origin")
	targetCountBefore := getMetricValue(t, ks, toggleTable, "target")
	require.True(t, originCountBefore > 0, "origin count should be > 0 before disable")
	require.True(t, targetCountBefore > 0, "target count should be > 0 before disable")

	// ================================================================
	// PHASE 2: Disable target — writes go to origin only
	// ================================================================
	t.Run("phase2_target_disabled", func(t *testing.T) {
		// Disable target via API
		postTargetToggle(t, "disable")
		requireTargetStatus(t, false)

		// Inline INSERT — should succeed (origin only)
		require.Nil(t, proxy.Query(fmt.Sprintf(
			"INSERT INTO %s.%s (id, name) VALUES (b0000001-0000-0000-0000-000000000001, 'p2_inline')", ks, toggleTable)).Exec())

		// Inline counter
		require.Nil(t, proxy.Query(fmt.Sprintf(
			"UPDATE %s.%s SET count = count + 1 WHERE id = b0000001-0000-0000-0000-000000000001", ks, toggleCounterTable)).Exec())

		// Prepared INSERT (re-using existing prepared statement)
		q := proxy.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (?, ?)", ks, toggleTable))
		q.Bind("b0000001-0000-0000-0000-000000000002", "p2_prepared")
		require.Nil(t, q.Exec())

		// Batch with inline children
		batch := proxy.NewBatch(gocql.LoggedBatch)
		batch.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (b0000001-0000-0000-0000-000000000003, 'p2_batch_a')", ks, toggleBatchA))
		batch.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (b0000001-0000-0000-0000-000000000003, 'p2_batch_b')", ks, toggleBatchB))
		require.Nil(t, proxy.ExecuteBatch(batch))

		// Batch with prepared children
		batch2 := proxy.NewBatch(gocql.LoggedBatch)
		batch2.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (?, ?)", ks, toggleBatchA), "b0000001-0000-0000-0000-000000000004", "p2_batch_prep_a")
		batch2.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (?, ?)", ks, toggleBatchB), "b0000001-0000-0000-0000-000000000004", "p2_batch_prep_b")
		require.Nil(t, proxy.ExecuteBatch(batch2))

		// Verify: origin metrics increased, target metrics did NOT
		originCountAfter := getMetricValue(t, ks, toggleTable, "origin")
		targetCountAfter := getMetricValue(t, ks, toggleTable, "target")
		require.True(t, originCountAfter > originCountBefore,
			"origin write count should have increased: before=%v after=%v", originCountBefore, originCountAfter)
		require.Equal(t, targetCountBefore, targetCountAfter,
			"target write count should NOT have changed: before=%v after=%v", targetCountBefore, targetCountAfter)

		// Verify data NOT on target (written during disabled period)
		var count int
		iter := targetCluster.GetSession().Query(fmt.Sprintf(
			"SELECT name FROM %s.%s WHERE id = b0000001-0000-0000-0000-000000000001", ks, toggleTable)).Iter()
		count = iter.NumRows()
		iter.Close()
		require.Equal(t, 0, count, "data written during disabled period should NOT be on target")

		// Verify data IS on origin
		var name string
		err := originCluster.GetSession().Query(fmt.Sprintf(
			"SELECT name FROM %s.%s WHERE id = b0000001-0000-0000-0000-000000000001", ks, toggleTable)).Scan(&name)
		require.Nil(t, err)
		require.Equal(t, "p2_inline", name)
	})

	// ================================================================
	// PHASE 3: While disabled, create NEW prepared statements
	// These will only be prepared on origin, NOT on target.
	// ================================================================
	t.Run("phase3_new_prepared_while_disabled", func(t *testing.T) {
		requireTargetStatus(t, false)

		// Prepare and execute a NEW statement on a different table
		// gocql auto-prepares when bind params are used
		q := proxy.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (?, ?)", ks, togglePreparedTable))
		q.Bind("c0000001-0000-0000-0000-000000000001", "p3_new_prepared")
		require.Nil(t, q.Exec())

		// Also create a prepared UPDATE
		q2 := proxy.Query(fmt.Sprintf("UPDATE %s.%s SET name = ? WHERE id = ?", ks, togglePreparedTable))
		q2.Bind("p3_updated", "c0000001-0000-0000-0000-000000000001")
		require.Nil(t, q2.Exec())

		// Verify data on origin only
		var name string
		err := originCluster.GetSession().Query(fmt.Sprintf(
			"SELECT name FROM %s.%s WHERE id = c0000001-0000-0000-0000-000000000001", ks, togglePreparedTable)).Scan(&name)
		require.Nil(t, err)
		require.Equal(t, "p3_updated", name)

		// Verify NOT on target
		var count int
		iter := targetCluster.GetSession().Query(fmt.Sprintf(
			"SELECT name FROM %s.%s WHERE id = c0000001-0000-0000-0000-000000000001", ks, togglePreparedTable)).Iter()
		count = iter.NumRows()
		iter.Close()
		require.Equal(t, 0, count, "data from phase 3 should NOT be on target")
	})

	// ================================================================
	// PHASE 4: Re-enable target — use the NEW prepared statements
	// Target should receive UNPREPARED, client re-prepares, data flows to both.
	// ================================================================
	t.Run("phase4_reenable_target", func(t *testing.T) {
		// Re-enable
		postTargetToggle(t, "enable")
		requireTargetStatus(t, true)

		// Use the same prepared statements created during disabled period.
		// gocql will receive UNPREPARED from target, automatically re-prepare, and retry.
		q := proxy.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (?, ?)", ks, togglePreparedTable))
		q.Bind("d0000001-0000-0000-0000-000000000001", "p4_after_reenable")
		require.Nil(t, q.Exec())

		// Use the prepared UPDATE from phase 3
		q2 := proxy.Query(fmt.Sprintf("UPDATE %s.%s SET name = ? WHERE id = ?", ks, togglePreparedTable))
		q2.Bind("p4_updated_reenable", "d0000001-0000-0000-0000-000000000001")
		require.Nil(t, q2.Exec())

		// Inline insert to verify basic flow restored
		require.Nil(t, proxy.Query(fmt.Sprintf(
			"INSERT INTO %s.%s (id, name) VALUES (d0000001-0000-0000-0000-000000000002, 'p4_inline')", ks, toggleTable)).Exec())

		// Counter update
		require.Nil(t, proxy.Query(fmt.Sprintf(
			"UPDATE %s.%s SET count = count + 1 WHERE id = d0000001-0000-0000-0000-000000000001", ks, toggleCounterTable)).Exec())

		// Batch
		batch := proxy.NewBatch(gocql.LoggedBatch)
		batch.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (?, ?)", ks, toggleBatchA), "d0000001-0000-0000-0000-000000000003", "p4_batch_a")
		batch.Query(fmt.Sprintf("INSERT INTO %s.%s (id, name) VALUES (?, ?)", ks, toggleBatchB), "d0000001-0000-0000-0000-000000000003", "p4_batch_b")
		require.Nil(t, proxy.ExecuteBatch(batch))

		// Verify metrics: both origin and target should now be incrementing
		originCountFinal := getMetricValue(t, ks, toggleTable, "origin")
		targetCountFinal := getMetricValue(t, ks, toggleTable, "target")
		require.True(t, originCountFinal > originCountBefore,
			"origin count should be higher than initial: initial=%v final=%v", originCountBefore, originCountFinal)
		require.True(t, targetCountFinal > targetCountBefore,
			"target count should be higher than initial (writes resumed): initial=%v final=%v", targetCountBefore, targetCountFinal)

		// Verify data on TARGET for phase 4 writes
		var name string
		err := targetCluster.GetSession().Query(fmt.Sprintf(
			"SELECT name FROM %s.%s WHERE id = d0000001-0000-0000-0000-000000000001", ks, togglePreparedTable)).Scan(&name)
		require.Nil(t, err)
		require.Equal(t, "p4_updated_reenable", name, "prepared statement data should be on target after re-enable")

		// Verify inline data on target
		err = targetCluster.GetSession().Query(fmt.Sprintf(
			"SELECT name FROM %s.%s WHERE id = d0000001-0000-0000-0000-000000000002", ks, toggleTable)).Scan(&name)
		require.Nil(t, err)
		require.Equal(t, "p4_inline", name, "inline write should be on target after re-enable")

		// Verify batch data on target
		err = targetCluster.GetSession().Query(fmt.Sprintf(
			"SELECT name FROM %s.%s WHERE id = d0000001-0000-0000-0000-000000000003", ks, toggleBatchA)).Scan(&name)
		require.Nil(t, err)
		require.Equal(t, "p4_batch_a", name, "batch write should be on target after re-enable")

		// Verify the Prometheus gauge shows enabled (1)
		lines := gatherToggleMetricLines(t)
		requireTargetEnabledGauge(t, lines, 1)
	})
}

// gatherToggleMetricLines scrapes the metrics endpoint for the toggle test.
func gatherToggleMetricLines(t *testing.T) []string {
	t.Helper()
	statusCode, rspStr, err := utils.GetMetrics(toggleHTTPAddr)
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

// getMetricValue returns the numeric value of the write success metric for the given cluster/keyspace/table.
func getMetricValue(t *testing.T, keyspace, table, cluster string) float64 {
	t.Helper()
	lines := gatherToggleMetricLines(t)
	prefix := fmt.Sprintf(`zdm_proxy_write_success_total{cluster="%s",keyspace="%s",table="%s"}`, cluster, keyspace, table)
	for _, line := range lines {
		if strings.HasPrefix(line, prefix) {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				val, err := strconv.ParseFloat(parts[len(parts)-1], 64)
				require.Nil(t, err, "failed to parse metric value from: %s", line)
				return val
			}
		}
	}
	t.Fatalf("metric not found: %s", prefix)
	return 0
}

// postTargetToggle sends a POST to /api/v1/target/enable or /api/v1/target/disable.
func postTargetToggle(t *testing.T, action string) {
	t.Helper()
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/target/%s", toggleHTTPAddr, action), "application/json", nil)
	require.Nil(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// requireTargetEnabledGauge checks the Prometheus gauge for target_enabled.
func requireTargetEnabledGauge(t *testing.T, lines []string, expectedValue float64) {
	t.Helper()
	for _, line := range lines {
		if strings.HasPrefix(line, "zdm_target_enabled") {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				val, err := strconv.ParseFloat(parts[len(parts)-1], 64)
				require.Nil(t, err)
				require.Equal(t, expectedValue, val, "target_enabled gauge mismatch")
				return
			}
		}
	}
	t.Errorf("zdm_target_enabled gauge not found in metrics output")
}

// requireTargetStatus verifies the target status via the API.
func requireTargetStatus(t *testing.T, expectedEnabled bool) {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/target", toggleHTTPAddr))
	require.Nil(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.Nil(t, err)

	var status struct {
		Enabled bool `json:"enabled"`
	}
	require.Nil(t, json.Unmarshal(body, &status))
	require.Equal(t, expectedEnabled, status.Enabled, "target enabled status mismatch")
}
