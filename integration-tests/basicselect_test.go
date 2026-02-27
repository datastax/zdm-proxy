package integration_tests

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/datastax/zdm-proxy/integration-tests/env"
	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/integration-tests/utils"
)

func TestSaiSelect(t *testing.T) {
	if !(env.IsDse && env.CompareServerVersion("6.9") >= 0) {
		t.Skip("Test requires DSE 6.9 cluster")
	}

	proxyInstance, err := NewProxyInstanceForGlobalCcmClusters(t)
	require.Nil(t, err)
	defer proxyInstance.Shutdown()

	originCluster, targetCluster, err := SetupOrGetGlobalCcmClusters(t)
	require.Nil(t, err)

	// Initialize test data
	data := [][]string{
		{"43bfd159-dede-47bf-a4da-6d446065e618", "Mile"},
		{"17a424d1-e611-47e2-8bcd-2cec885edf28", "Mission"},
	}

	// Seed originCluster and targetCluster w/ schema and data
	setup.SeedData(originCluster.GetSession(), targetCluster.GetSession(), setup.EmployeeModel, data)

	// Connect to proxy as a "client"
	proxy, err := utils.ConnectToCluster("127.0.0.1", "", "", 14002)

	if err != nil {
		t.Log("Unable to connect to proxy session.")
		t.Fatal(err)
	}
	defer proxy.Close()

	// run query on proxied connection
	// use lowercased version of the actual value
	itr := proxy.Query(fmt.Sprintf("SELECT * FROM %s.%s where task : 'mile';", setup.TestKeyspace, setup.EmployeeModel)).Iter()
	row := make(map[string]interface{})

	// Assertions!
	require.True(t, itr.MapScan(row))
	task := setup.MapToTask(row)

	setup.AssertEqual(t, "Mile", task.Task)
}
