package integration_tests

import (
	"fmt"
	"strings"
	"testing"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/stretchr/testify/require"

	"github.com/datastax/zdm-proxy/integration-tests/env"
	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/integration-tests/utils"
)

// TestTargetConsistencyOverrideCCM verifies that the ZDM_TARGET_CONSISTENCY_LEVEL config
// overrides the consistency level on the target cluster while preserving the original
// client-requested CL on origin. Verified via Cassandra system_traces.
//
// Test matrix:
//   - Inline INSERT at QUORUM → origin should see QUORUM, target should see LOCAL_ONE
//   - Prepared INSERT at QUORUM → same verification via EXECUTE trace
//   - Batch INSERT at QUORUM → same verification via BATCH trace
func TestTargetConsistencyOverrideCCM(t *testing.T) {
	if env.CompareServerVersion("3.0.0") < 0 {
		t.Skip("Skipping consistency override trace test: system_traces.sessions parameters map not available before Cassandra 3.0")
	}

	originCluster, targetCluster, err := SetupOrGetGlobalCcmClusters(t)
	require.Nil(t, err)

	originSession := originCluster.GetSession()
	targetSession := targetCluster.GetSession()

	// Create a proxy with target consistency override set to LOCAL_ONE
	conf := setup.NewTestConfig(originCluster.GetInitialContactPoint(), targetCluster.GetInitialContactPoint())
	conf.TargetConsistencyLevel = "LOCAL_ONE"

	proxyInstance, err := setup.NewProxyInstanceWithConfig(conf)
	require.Nil(t, err)
	defer proxyInstance.Shutdown()

	// Ensure system_traces has RF=1 on both single-node CCM clusters
	// and create the test table
	for _, s := range []*gocql.Session{originSession, targetSession} {
		s.Query("ALTER KEYSPACE system_traces WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}").Exec()
		s.Query(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.cl_test (id uuid PRIMARY KEY, val text)", setup.TestKeyspace)).Exec()
	}

	// Connect through the proxy
	proxy, err := utils.ConnectToCluster("127.0.0.1", "", "", conf.ProxyListenPort)
	require.Nil(t, err)
	defer proxy.Close()

	// ================================================================
	// TEST 1: Inline INSERT
	// Client sends QUORUM, origin should see QUORUM, target should see LOCAL_ONE
	// ================================================================
	t.Run("inline_insert", func(t *testing.T) {
		clearTraces(originSession, targetSession)

		q := proxy.Query(fmt.Sprintf(
			"INSERT INTO %s.cl_test (id, val) VALUES (d1b05da0-8c20-11ea-9fc6-6d2c86545d91, 'inline_cl_test')",
			setup.TestKeyspace))
		q.Consistency(gocql.Quorum)
		q.Trace(noopTracer{})
		err = q.Exec()
		require.Nil(t, err, "inline INSERT through proxy failed")

		originCL := findTraceCL(t, originSession, "inline_cl_test")
		require.Equal(t, "QUORUM", originCL, "origin should receive client-requested QUORUM")

		targetCL := findTraceCL(t, targetSession, "inline_cl_test")
		require.Equal(t, "LOCAL_ONE", targetCL, "target should receive overridden LOCAL_ONE")
	})

	// ================================================================
	// TEST 2: Prepared INSERT
	// gocql auto-prepares when bind params are used. The EXECUTE trace
	// contains the original query string (e.g. "INSERT INTO ks.cl_test (id, val) VALUES (?, ?)")
	// so we search for the table name as the marker.
	// ================================================================
	t.Run("prepared_insert", func(t *testing.T) {
		clearTraces(originSession, targetSession)

		q := proxy.Query(fmt.Sprintf(
			"INSERT INTO %s.cl_test (id, val) VALUES (?, ?)", setup.TestKeyspace))
		q.Bind("eed574b0-8c20-11ea-9fc6-6d2c86545d91", "prepared_cl_test")
		q.Consistency(gocql.Quorum)
		q.Trace(noopTracer{})
		err = q.Exec()
		require.Nil(t, err, "prepared INSERT through proxy failed")

		// For prepared statements, the trace query field contains the CQL with ? markers,
		// not the bound values. Search for the table name instead.
		originCL := findTraceCL(t, originSession, "cl_test")
		require.Equal(t, "QUORUM", originCL, "origin should receive client-requested QUORUM for prepared statement")

		targetCL := findTraceCL(t, targetSession, "cl_test")
		require.Equal(t, "LOCAL_ONE", targetCL, "target should receive overridden LOCAL_ONE for prepared statement")
	})

	// ================================================================
	// TEST 3: Batch INSERT
	// Batch traces don't include the query text in the parameters map,
	// so we check the first trace found after clearing.
	// ================================================================
	t.Run("batch_insert", func(t *testing.T) {
		clearTraces(originSession, targetSession)

		batch := proxy.NewBatch(gocql.LoggedBatch)
		batch.Query(fmt.Sprintf(
			"INSERT INTO %s.cl_test (id, val) VALUES (cf0f4cf0-8c20-11ea-9fc6-6d2c86545d91, 'batch_cl_test')",
			setup.TestKeyspace))
		batch.SetConsistency(gocql.Quorum)
		batch.Trace(noopTracer{})
		err = proxy.ExecuteBatch(batch)
		require.Nil(t, err, "batch INSERT through proxy failed")

		originCL := findAnyTraceCL(t, originSession)
		require.Equal(t, "QUORUM", originCL, "origin should receive client-requested QUORUM for batch")

		targetCL := findAnyTraceCL(t, targetSession)
		require.Equal(t, "LOCAL_ONE", targetCL, "target should receive overridden LOCAL_ONE for batch")
	})
}

// clearTraces truncates system_traces.sessions on both clusters.
func clearTraces(origin *gocql.Session, target *gocql.Session) {
	origin.Query("TRUNCATE system_traces.sessions").Consistency(gocql.One).Exec()
	origin.Query("TRUNCATE system_traces.events").Consistency(gocql.One).Exec()
	target.Query("TRUNCATE system_traces.sessions").Consistency(gocql.One).Exec()
	target.Query("TRUNCATE system_traces.events").Consistency(gocql.One).Exec()
}

// findTraceCL searches system_traces.sessions for a trace whose query parameter contains
// the given marker, and returns the consistency_level. Retries for up to 10 seconds.
func findTraceCL(t *testing.T, session *gocql.Session, marker string) string {
	t.Helper()
	for attempt := 0; attempt < 20; attempt++ {
		q := session.Query("SELECT parameters FROM system_traces.sessions")
		q.Consistency(gocql.One)
		iter := q.Iter()
		var params map[string]string
		for iter.Scan(&params) {
			if query, ok := params["query"]; ok && strings.Contains(query, marker) {
				if cl, ok := params["consistency_level"]; ok {
					iter.Close()
					return cl
				}
			}
		}
		iter.Close()
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("no trace found containing marker %q after 10s of retries", marker)
	return ""
}

// findAnyTraceCL returns the consistency_level from the first trace session found.
// Retries for up to 10 seconds.
func findAnyTraceCL(t *testing.T, session *gocql.Session) string {
	t.Helper()
	for attempt := 0; attempt < 20; attempt++ {
		q := session.Query("SELECT parameters FROM system_traces.sessions")
		q.Consistency(gocql.One)
		iter := q.Iter()
		var params map[string]string
		for iter.Scan(&params) {
			if cl, ok := params["consistency_level"]; ok {
				iter.Close()
				return cl
			}
		}
		iter.Close()
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("no trace sessions found after 10s of retries")
	return ""
}

// noopTracer enables the tracing flag on the CQL protocol frame without
// fetching trace results. This avoids trace-fetch queries going through
// the proxy and interfering with the test.
type noopTracer struct{}

func (noopTracer) Trace(_ []byte) {}
