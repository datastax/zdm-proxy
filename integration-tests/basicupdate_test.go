package integration_tests

import (
	"fmt"
	"github.com/datastax/zdm-proxy/integration-tests/env"
	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/integration-tests/utils"
	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"testing"
)

// BasicUpdate tests if update queries run correctly
// Unloads the originCluster database,
// performs an update where through the proxy
// then loads the unloaded data into the destination
func TestBasicUpdate(t *testing.T) {
	if !env.RunCcmTests {
		t.Skip("Test requires CCM, set RUN_CCMTESTS env variable to TRUE")
	}

	proxyInstance, err := NewProxyInstanceForGlobalCcmClusters()
	require.Nil(t, err)
	defer proxyInstance.Shutdown()

	originCluster, targetCluster, err := SetupOrGetGlobalCcmClusters()
	require.Nil(t, err)

	// Initialize test data
	data := [][]string{
		{"cf0f4cf0-8c20-11ea-9fc6-6d2c86545d91", "MSzZMTWA9hw6tkYWPTxT0XfGL9nGQUpy"},
		{"d1b05da0-8c20-11ea-9fc6-6d2c86545d91", "IH0FC3aWM4ynriOFvtr5TfiKxziR5aB1"},
		{"eed574b0-8c20-11ea-9fc6-6d2c86545d91", "FgQfJesbNcxAebzFPRRcW2p1bBtoz1P1"},
	}

	// Seed originCluster and targetCluster w/ schema and data
	setup.SeedData(originCluster.GetSession(), targetCluster.GetSession(), setup.TasksModel, data)

	// Connect to proxy as a "client"
	proxy, err := utils.ConnectToCluster("127.0.0.1", "", "", 14002)

	if err != nil {
		t.Log("Unable to connect to proxy session.")
		t.Fatal(err)
	}
	defer proxy.Close()

	// Run query on proxied connection
	err = proxy.Query(fmt.Sprintf("UPDATE %s.%s SET task = 'terrance' WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d91;", setup.TestKeyspace, setup.TasksModel)).Exec()
	if err != nil {
		t.Log("Mid-migration update failed.")
		t.Fatal(err)
	}

	// Assertions!
	itr := targetCluster.GetSession().Query(fmt.Sprintf("SELECT * FROM %s.%s WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d91;", setup.TestKeyspace, setup.TasksModel)).Iter()
	row := make(map[string]interface{})

	require.True(t, itr.MapScan(row))
	task := setup.MapToTask(row)

	setup.AssertEqual(t, "terrance", task.Task)
}

func TestCompression(t *testing.T) {
	if !env.RunCcmTests {
		t.Skip("Test requires CCM, set RUN_CCMTESTS env variable to TRUE")
	}

	log.SetLevel(log.TraceLevel)
	defer log.SetLevel(log.InfoLevel)

	proxyInstance, err := NewProxyInstanceForGlobalCcmClusters()
	require.Nil(t, err)
	defer proxyInstance.Shutdown()

	originCluster, targetCluster, err := SetupOrGetGlobalCcmClusters()
	require.Nil(t, err)

	// Initialize test data
	data := [][]string{
		{"cf0f4cf0-8c20-11ea-9fc6-6d2c86545d91", "MSzZMTWA9hw6tkYWPTxT0XfGL9nGQUpy"},
	}

	// Seed originCluster and targetCluster w/ schema and data
	setup.SeedData(originCluster.GetSession(), targetCluster.GetSession(), setup.TasksModel, data)

	// Connect to proxy as a "client"
	cluster := utils.NewCluster("127.0.0.1", "", "", 14002)
	cluster.Compressor = gocql.SnappyCompressor{}
	proxy, err := cluster.CreateSession()

	if err != nil {
		t.Log("Unable to connect to proxy session.")
		t.Fatal(err)
	}
	defer proxy.Close()

	// Run query on proxied connection
	err = proxy.Query(fmt.Sprintf("UPDATE %s.%s SET task = 'terrance' WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d91;", setup.TestKeyspace, setup.TasksModel)).Exec()
	if err != nil {
		t.Log("Mid-migration update failed.")
		t.Fatal(err)
	}

	// Assertions!
	itr := targetCluster.GetSession().Query(fmt.Sprintf("SELECT * FROM %s.%s WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d91;", setup.TestKeyspace, setup.TasksModel)).Iter()
	row := make(map[string]interface{})

	require.True(t, itr.MapScan(row))
	task := setup.MapToTask(row)

	setup.AssertEqual(t, "terrance", task.Task)
}
