package integration_tests

import (
	"fmt"
	"testing"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/stretchr/testify/require"

	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/integration-tests/utils"
)

// TestTargetConsistencyOverrideCCM verifies that the ZDM_TARGET_CONSISTENCY_LEVEL config
// overrides the consistency level on the target cluster while preserving the original
// client-requested CL on origin. Verified via Cassandra system_traces.
func TestTargetConsistencyOverrideCCM(t *testing.T) {
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

	// Ensure test table exists on both clusters
	for _, s := range []*gocql.Session{originSession, targetSession} {
		s.Query(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.cl_test (id uuid PRIMARY KEY, val text)", setup.TestKeyspace)).Exec()
	}

	// Clear traces on both clusters
	originSession.Query("TRUNCATE system_traces.sessions").Exec()
	originSession.Query("TRUNCATE system_traces.events").Exec()
	targetSession.Query("TRUNCATE system_traces.sessions").Exec()
	targetSession.Query("TRUNCATE system_traces.events").Exec()

	// Connect to proxy and send a traced write at LOCAL_QUORUM
	proxy, err := utils.ConnectToCluster("127.0.0.1", "", "", conf.ProxyListenPort)
	require.Nil(t, err)
	defer proxy.Close()

	t.Run("inline_insert_cl_override", func(t *testing.T) {
		q := proxy.Query(fmt.Sprintf(
			"INSERT INTO %s.cl_test (id, val) VALUES (d1b05da0-8c20-11ea-9fc6-6d2c86545d91, 'cl_test')", setup.TestKeyspace))
		q.Consistency(gocql.LocalQuorum)
		q.Trace(gocql.NewTraceWriter(proxy, nil))
		err = q.Exec()
		require.Nil(t, err)

		// Wait for traces to persist
		time.Sleep(5 * time.Second)

		// Check origin trace — should show LOCAL_QUORUM (client-requested, unchanged)
		originCL := getTracedConsistencyLevel(t, originSession, "cl_test")
		require.Equal(t, "LOCAL_QUORUM", originCL,
			"origin should receive client-requested LOCAL_QUORUM")

		// Check target trace — should show LOCAL_ONE (overridden by proxy)
		targetCL := getTracedConsistencyLevel(t, targetSession, "cl_test")
		require.Equal(t, "LOCAL_ONE", targetCL,
			"target should receive overridden LOCAL_ONE")
	})

	t.Run("prepared_insert_cl_override", func(t *testing.T) {
		// Clear traces
		originSession.Query("TRUNCATE system_traces.sessions").Exec()
		targetSession.Query("TRUNCATE system_traces.sessions").Exec()

		q := proxy.Query(fmt.Sprintf(
			"INSERT INTO %s.cl_test (id, val) VALUES (?, ?)", setup.TestKeyspace))
		q.Bind("eed574b0-8c20-11ea-9fc6-6d2c86545d91", "cl_prepared_test")
		q.Consistency(gocql.LocalQuorum)
		q.Trace(gocql.NewTraceWriter(proxy, nil))
		err = q.Exec()
		require.Nil(t, err)

		time.Sleep(5 * time.Second)

		originCL := getTracedConsistencyLevel(t, originSession, "cl_prepared_test")
		require.Equal(t, "LOCAL_QUORUM", originCL,
			"origin should receive client-requested LOCAL_QUORUM for prepared statement")

		targetCL := getTracedConsistencyLevel(t, targetSession, "cl_prepared_test")
		require.Equal(t, "LOCAL_ONE", targetCL,
			"target should receive overridden LOCAL_ONE for prepared statement")
	})

	t.Run("batch_cl_override", func(t *testing.T) {
		// Clear traces
		originSession.Query("TRUNCATE system_traces.sessions").Exec()
		targetSession.Query("TRUNCATE system_traces.sessions").Exec()

		batch := proxy.NewBatch(gocql.LoggedBatch)
		batch.Query(fmt.Sprintf(
			"INSERT INTO %s.cl_test (id, val) VALUES (cf0f4cf0-8c20-11ea-9fc6-6d2c86545d91, 'cl_batch_test')", setup.TestKeyspace))
		batch.SetConsistency(gocql.LocalQuorum)
		err = proxy.ExecuteBatch(batch)
		require.Nil(t, err)

		time.Sleep(5 * time.Second)

		// Batches show CL in traces without the query text, so search for any trace with CL
		originCL := getAnyTracedConsistencyLevel(t, originSession)
		require.Equal(t, "LOCAL_QUORUM", originCL,
			"origin batch should receive client-requested LOCAL_QUORUM")

		targetCL := getAnyTracedConsistencyLevel(t, targetSession)
		require.Equal(t, "LOCAL_ONE", targetCL,
			"target batch should receive overridden LOCAL_ONE")
	})
}

// getTracedConsistencyLevel finds a trace session containing the given marker text
// and returns its consistency_level from the parameters map.
func getTracedConsistencyLevel(t *testing.T, session *gocql.Session, marker string) string {
	t.Helper()
	iter := session.Query("SELECT parameters FROM system_traces.sessions").Iter()
	var params map[string]string
	for iter.Scan(&params) {
		if query, ok := params["query"]; ok && containsString(query, marker) {
			if cl, ok := params["consistency_level"]; ok {
				return cl
			}
		}
	}
	if err := iter.Close(); err != nil {
		t.Fatalf("error querying traces: %v", err)
	}
	t.Fatalf("no trace found containing marker %q", marker)
	return ""
}

// getAnyTracedConsistencyLevel returns the consistency_level from the first trace session found.
func getAnyTracedConsistencyLevel(t *testing.T, session *gocql.Session) string {
	t.Helper()
	iter := session.Query("SELECT parameters FROM system_traces.sessions").Iter()
	var params map[string]string
	for iter.Scan(&params) {
		if cl, ok := params["consistency_level"]; ok {
			return cl
		}
	}
	if err := iter.Close(); err != nil {
		t.Fatalf("error querying traces: %v", err)
	}
	t.Fatalf("no trace sessions found")
	return ""
}

func containsString(s string, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && searchString(s, substr)))
}

func searchString(s string, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
