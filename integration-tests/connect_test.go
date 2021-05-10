package integration_tests

import (
	"github.com/riptano/cloud-gate/integration-tests/setup"
	"github.com/riptano/cloud-gate/utils"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGoCqlConnect(t *testing.T) {
	testSetup, err := setup.NewSimulacronTestSetup()
	require.Nil(t, err)
	defer testSetup.Cleanup()

	// Connect to proxy as a "client"
	proxy, err := utils.ConnectToCluster("127.0.0.1", "", "", 14002)

	if err != nil {
		t.Log("Unable to connect to proxy session.")
		t.Fatal(err)
	}
	defer proxy.Close()

	iter := proxy.Query("SELECT * FROM fakeks.faketb").Iter()
	result, err := iter.SliceMap()

	if err != nil {
		t.Fatal("query failed:", err)
	}

	require.Equal(t, 0, len(result))

	// simulacron generates fake response metadata when queries aren't primed
	require.Equal(t, "fake", iter.Columns()[0].Name)
}

func TestMaxClientsThreshold(t *testing.T) {
	maxClients := 10
	goCqlConnectionsPerHost := 1
	maxSessions := 5 // each session spawns 2 connections (1 control connection)

	testSetup, err := setup.NewSimulacronTestSetupWithSession(false, false)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	config := setup.NewTestConfig(testSetup.Origin.GetInitialContactPoint(), testSetup.Target.GetInitialContactPoint())
	config.MaxClientsThreshold = maxClients
	proxyInstance, err := setup.NewProxyInstanceWithConfig(config)
	require.Nil(t, err)
	defer proxyInstance.Shutdown()

	for i := 0; i < maxSessions + 1; i++ {
		// Connect to proxy as a "client"
		cluster := utils.NewCluster("127.0.0.1", "", "", 14002)
		cluster.NumConns = goCqlConnectionsPerHost
		session, err := cluster.CreateSession()

		if err != nil {
			if i == maxSessions {
				return
			}
			t.Log("Unable to connect to proxy.")
			t.Fatal(err)
		}
		defer session.Close()
	}

	t.Fatal("Expected failure in last session connection but it was successful.")
}
