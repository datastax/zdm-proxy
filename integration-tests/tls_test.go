package integration_tests

import (
	"context"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/riptano/cloud-gate/integration-tests/env"
	"github.com/riptano/cloud-gate/integration-tests/setup"
	"github.com/riptano/cloud-gate/proxy/pkg/config"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"strings"
	"testing"
)

type testClusterTlsConfiguration struct {
	name              string
	enableOnOrigin    bool
	mutualTlsOnOrigin bool
	enableOnTarget    bool
	mutualTlsOnTarget bool
}

type testProxyTlsConfiguration struct {
	name              string
	enableOnOrigin    bool
	mutualTlsOnOrigin bool
	enableOnTarget    bool
	mutualTlsOnTarget bool
	errExpected       bool
	errMsgExpected    string
}

const (
	ClientCertRelPath       = "../integration-tests/resources/client.crt"
	ClientKeyRelPath        = "../integration-tests/resources/client.key"
	ServerCaCertRelPath     = "../integration-tests/resources/node1_ca.crt"
	ServerKeystoreRelPath   = "../integration-tests/resources/server.keystore"
	ServerTruststoreRelPath = "../integration-tests/resources/server.truststore"
)

// Runs only when the full test suite is executed
func TestTls_OneWayOrigin_OneWayTarget(t *testing.T) {

	essentialTest := false
	skipNonEssentialTests(essentialTest, t)

	clusterTlsConfiguration := testClusterTlsConfiguration{
		name:              "Clusters: One-way TLS on Origin and Target",
		enableOnOrigin:    true,
		mutualTlsOnOrigin: false,
		enableOnTarget:    true,
		mutualTlsOnTarget: false,
	}

	proxyTlsConfigurations := []testProxyTlsConfiguration{
		{
			name:              "Proxy: One-way TLS for Origin and Target",
			enableOnOrigin:    true,
			mutualTlsOnOrigin: false,
			enableOnTarget:    true,
			mutualTlsOnTarget: false,
			errExpected:       false,
			errMsgExpected:    "",
		},
		{
			name:              "Proxy: No TLS for Origin, one-way TLS for Target",
			enableOnOrigin:    false,
			mutualTlsOnOrigin: false,
			enableOnTarget:    true,
			mutualTlsOnTarget: false,
			errExpected:       true,
			errMsgExpected:    "failed to initialize origin control connection: could not open control connection to ORIGIN",
		},
		{
			name:              "Proxy: mutual TLS for Origin, one-way TLS for Target",
			enableOnOrigin:    true,
			mutualTlsOnOrigin: true,
			enableOnTarget:    true,
			mutualTlsOnTarget: false,
			errExpected:       false,
			errMsgExpected:    "",
		},
		{
			name:              "Proxy: one-way TLS for Origin, no TLS for Target",
			enableOnOrigin:    true,
			mutualTlsOnOrigin: false,
			enableOnTarget:    false,
			mutualTlsOnTarget: false,
			errExpected:       true,
			errMsgExpected:    "failed to initialize target control connection: could not open control connection to TARGET",
		},
		{
			name:              "Proxy: one-way TLS for Origin, mutual TLS for Target",
			enableOnOrigin:    true,
			mutualTlsOnOrigin: false,
			enableOnTarget:    true,
			mutualTlsOnTarget: true,
			errExpected:       false,
			errMsgExpected:    "",
		},
	}

	ccmSetup, err := setupOriginAndTargetClusters(clusterTlsConfiguration, t)
	defer func() {
		if ccmSetup != nil {
			ccmSetup.Cleanup()
		}
	}()
	require.Nil(t, err)

	for _, proxyTlsConfig := range proxyTlsConfigurations {
		t.Run(proxyTlsConfig.name, func(t *testing.T) {
			executeTest(ccmSetup, proxyTlsConfig, t)
		})
	}
}

// Runs only when the full test suite is executed
func TestTls_MutualOrigin_MutualTarget(t *testing.T) {

	essentialTest := false
	skipNonEssentialTests(essentialTest, t)

	clusterTlsConfiguration := testClusterTlsConfiguration{
		name:              "Clusters: Mutual TLS on Origin and Target",
		enableOnOrigin:    true,
		mutualTlsOnOrigin: true,
		enableOnTarget:    true,
		mutualTlsOnTarget: true,
	}

	proxyTlsConfigurations := []testProxyTlsConfiguration{
		{
			name:              "Proxy: Mutual TLS for Origin and Target",
			enableOnOrigin:    true,
			mutualTlsOnOrigin: true,
			enableOnTarget:    true,
			mutualTlsOnTarget: true,
			errExpected:       false,
			errMsgExpected:    "",
		},
		{
			name:              "Proxy: No TLS for Origin, mutual TLS for Target",
			enableOnOrigin:    false,
			mutualTlsOnOrigin: false,
			enableOnTarget:    true,
			mutualTlsOnTarget: true,
			errExpected:       true,
			errMsgExpected:    "failed to initialize origin control connection: could not open control connection to ORIGIN",
		},
		{
			name:              "Proxy: one-way TLS for Origin, mutual TLS for Target",
			enableOnOrigin:    true,
			mutualTlsOnOrigin: false,
			enableOnTarget:    true,
			mutualTlsOnTarget: true,
			errExpected:       true,
			errMsgExpected:    "failed to initialize origin control connection: could not open control connection to ORIGIN",
		},
		{
			name:              "Proxy: mutual TLS for Origin, no TLS for Target",
			enableOnOrigin:    true,
			mutualTlsOnOrigin: true,
			enableOnTarget:    false,
			mutualTlsOnTarget: false,
			errExpected:       true,
			errMsgExpected:    "failed to initialize target control connection: could not open control connection to TARGET",
		},
		{
			name:              "Proxy: mutual TLS for Origin, one-way TLS for Target",
			enableOnOrigin:    true,
			mutualTlsOnOrigin: true,
			enableOnTarget:    true,
			mutualTlsOnTarget: false,
			errExpected:       true,
			errMsgExpected:    "failed to initialize target control connection: could not open control connection to TARGET",
		},
	}

	ccmSetup, err := setupOriginAndTargetClusters(clusterTlsConfiguration, t)
	defer func() {
		if ccmSetup != nil {
			ccmSetup.Cleanup()
		}
	}()
	require.Nil(t, err)

	for _, proxyTlsConfig := range proxyTlsConfigurations {
		t.Run(proxyTlsConfig.name, func(t *testing.T) {
			executeTest(ccmSetup, proxyTlsConfig, t)
		})
	}
}

// Always runs
func TestTls_OneWayOrigin_MutualTarget(t *testing.T) {

	essentialTest := true
	skipNonEssentialTests(essentialTest, t)

	clusterTlsConfiguration := testClusterTlsConfiguration{
		name:              "Clusters: One-way TLS on Origin and Mutual TLS on Target",
		enableOnOrigin:    true,
		mutualTlsOnOrigin: false,
		enableOnTarget:    true,
		mutualTlsOnTarget: true,
	}

	proxyTlsConfigurations := []testProxyTlsConfiguration{
		{
			name:              "Proxy: One-way TLS on Origin, mutual TLS on Target",
			enableOnOrigin:    true,
			mutualTlsOnOrigin: false,
			enableOnTarget:    true,
			mutualTlsOnTarget: true,
			errExpected:       false,
			errMsgExpected:    "",
		},
		{
			name:              "Proxy: No TLS for Origin, mutual TLS for Target",
			enableOnOrigin:    false,
			mutualTlsOnOrigin: false,
			enableOnTarget:    true,
			mutualTlsOnTarget: true,
			errExpected:       true,
			errMsgExpected:    "failed to initialize origin control connection: could not open control connection to ORIGIN",
		},
		{
			name:              "Proxy: mutual TLS for Origin, mutual TLS for Target",
			enableOnOrigin:    true,
			mutualTlsOnOrigin: true,
			enableOnTarget:    true,
			mutualTlsOnTarget: true,
			errExpected:       false,
			errMsgExpected:    "",
		},
		{
			name:              "Proxy: one-way TLS for Origin, no TLS for Target",
			enableOnOrigin:    true,
			mutualTlsOnOrigin: false,
			enableOnTarget:    false,
			mutualTlsOnTarget: false,
			errExpected:       true,
			errMsgExpected:    "failed to initialize target control connection: could not open control connection to TARGET",
		},
		{
			name:              "Proxy: one-way TLS for Origin, one-way TLS for Target",
			enableOnOrigin:    true,
			mutualTlsOnOrigin: false,
			enableOnTarget:    true,
			mutualTlsOnTarget: false,
			errExpected:       true,
			errMsgExpected:    "failed to initialize target control connection: could not open control connection to TARGET",
		},
	}

	ccmSetup, err := setupOriginAndTargetClusters(clusterTlsConfiguration, t)
	defer func() {
		if ccmSetup != nil {
			ccmSetup.Cleanup()
		}
	}()
	require.Nil(t, err)

	for _, proxyTlsConfig := range proxyTlsConfigurations {
		t.Run(proxyTlsConfig.name, func(t *testing.T) {
			executeTest(ccmSetup, proxyTlsConfig, t)
		})
	}
}

// Runs only when the full test suite is executed
func TestTls_MutualOrigin_OneWayTarget(t *testing.T) {

	essentialTest := false
	skipNonEssentialTests(essentialTest, t)

	clusterTlsConfiguration := testClusterTlsConfiguration{
		name:              "Clusters: Mutual TLS on Origin and one-way TLS on Target",
		enableOnOrigin:    true,
		mutualTlsOnOrigin: true,
		enableOnTarget:    true,
		mutualTlsOnTarget: false,
	}

	proxyTlsConfigurations := []testProxyTlsConfiguration{
		{
			name:              "Proxy: Mutual TLS for Origin, one-way TLS for Target",
			enableOnOrigin:    true,
			mutualTlsOnOrigin: true,
			enableOnTarget:    true,
			mutualTlsOnTarget: false,
			errExpected:       false,
			errMsgExpected:    "",
		},
		{
			name:              "Proxy: No TLS for Origin, one-way TLS for Target",
			enableOnOrigin:    false,
			mutualTlsOnOrigin: false,
			enableOnTarget:    true,
			mutualTlsOnTarget: false,
			errExpected:       true,
			errMsgExpected:    "failed to initialize origin control connection: could not open control connection to ORIGIN",
		},
		{
			name:              "Proxy: One-way TLS for Origin, one-way TLS for Target",
			enableOnOrigin:    true,
			mutualTlsOnOrigin: false,
			enableOnTarget:    true,
			mutualTlsOnTarget: false,
			errExpected:       true,
			errMsgExpected:    "failed to initialize origin control connection: could not open control connection to ORIGIN",
		},
		{
			name:              "Proxy: Mutual TLS for Origin, no TLS for Target",
			enableOnOrigin:    true,
			mutualTlsOnOrigin: true,
			enableOnTarget:    false,
			mutualTlsOnTarget: false,
			errExpected:       true,
			errMsgExpected:    "failed to initialize target control connection: could not open control connection to TARGET",
		},
		{
			name:              "Proxy: Mutual TLS for Origin, mutual TLS for Target",
			enableOnOrigin:    true,
			mutualTlsOnOrigin: true,
			enableOnTarget:    true,
			mutualTlsOnTarget: false,
			errExpected:       false,
			errMsgExpected:    "",
		},
	}

	ccmSetup, err := setupOriginAndTargetClusters(clusterTlsConfiguration, t)
	defer func() {
		if ccmSetup != nil {
			ccmSetup.Cleanup()
		}
	}()
	require.Nil(t, err)

	for _, proxyTlsConfig := range proxyTlsConfigurations {
		t.Run(proxyTlsConfig.name, func(t *testing.T) {
			executeTest(ccmSetup, proxyTlsConfig, t)
		})
	}
}

// Runs only when the full test suite is executed
func TestTls_NoOrigin_OneWayTarget(t *testing.T) {

	essentialTest := false
	skipNonEssentialTests(essentialTest, t)

	clusterTlsConfiguration := testClusterTlsConfiguration{
		name:              "Clusters: No TLS on Origin and one-way TLS on Target",
		enableOnOrigin:    false,
		mutualTlsOnOrigin: false,
		enableOnTarget:    true,
		mutualTlsOnTarget: false,
	}

	proxyTlsConfigurations := []testProxyTlsConfiguration{
		{
			name:              "Proxy: No TLS for Origin, one-way TLS for Target",
			enableOnOrigin:    false,
			mutualTlsOnOrigin: false,
			enableOnTarget:    true,
			mutualTlsOnTarget: false,
			errExpected:       false,
			errMsgExpected:    "",
		},
		{
			name:              "Proxy: One-way TLS for Origin, one-way TLS for Target",
			enableOnOrigin:    true,
			mutualTlsOnOrigin: false,
			enableOnTarget:    true,
			mutualTlsOnTarget: false,
			errExpected:       true,
			errMsgExpected:    "failed to initialize origin control connection: could not open control connection to ORIGIN",
		},
		{
			name:              "Proxy: Mutual TLS for Origin, one-way TLS for Target",
			enableOnOrigin:    true,
			mutualTlsOnOrigin: true,
			enableOnTarget:    true,
			mutualTlsOnTarget: false,
			errExpected:       true,
			errMsgExpected:    "failed to initialize origin control connection: could not open control connection to ORIGIN",
		},
		{
			name:              "Proxy: No TLS for Origin, no TLS for Target",
			enableOnOrigin:    false,
			mutualTlsOnOrigin: false,
			enableOnTarget:    false,
			mutualTlsOnTarget: false,
			errExpected:       true,
			errMsgExpected:    "failed to initialize target control connection: could not open control connection to TARGET",
		},
		{
			name:              "Proxy: No TLS for Origin, mutual TLS for Target",
			enableOnOrigin:    false,
			mutualTlsOnOrigin: false,
			enableOnTarget:    true,
			mutualTlsOnTarget: true,
			errExpected:       false,
			errMsgExpected:    "",
		},
	}

	ccmSetup, err := setupOriginAndTargetClusters(clusterTlsConfiguration, t)
	defer func() {
		if ccmSetup != nil {
			ccmSetup.Cleanup()
		}
	}()
	require.Nil(t, err)

	for _, proxyTlsConfig := range proxyTlsConfigurations {
		t.Run(proxyTlsConfig.name, func(t *testing.T) {
			executeTest(ccmSetup, proxyTlsConfig, t)
		})
	}
}

// Always runs
func TestTls_NoOrigin_MutualTarget(t *testing.T) {

	essentialTest := true
	skipNonEssentialTests(essentialTest, t)

	clusterTlsConfiguration := testClusterTlsConfiguration{
		name:              "Clusters: No TLS on Origin and mutual TLS on Target",
		enableOnOrigin:    false,
		mutualTlsOnOrigin: false,
		enableOnTarget:    true,
		mutualTlsOnTarget: true,
	}

	proxyTlsConfigurations := []testProxyTlsConfiguration{
		{
			name:              "Proxy: No TLS for Origin, mutual TLS for Target",
			enableOnOrigin:    false,
			mutualTlsOnOrigin: false,
			enableOnTarget:    true,
			mutualTlsOnTarget: true,
			errExpected:       false,
			errMsgExpected:    "",
		},
		{
			name:              "Proxy: One-way TLS for Origin, mutual TLS for Target",
			enableOnOrigin:    true,
			mutualTlsOnOrigin: false,
			enableOnTarget:    true,
			mutualTlsOnTarget: true,
			errExpected:       true,
			errMsgExpected:    "failed to initialize origin control connection: could not open control connection to ORIGIN",
		},
		{
			name:              "Proxy: Mutual TLS for Origin, mutual TLS for Target",
			enableOnOrigin:    true,
			mutualTlsOnOrigin: true,
			enableOnTarget:    true,
			mutualTlsOnTarget: true,
			errExpected:       true,
			errMsgExpected:    "failed to initialize origin control connection: could not open control connection to ORIGIN",
		},
		{
			name:              "Proxy: No TLS for Origin, no TLS for Target",
			enableOnOrigin:    false,
			mutualTlsOnOrigin: false,
			enableOnTarget:    false,
			mutualTlsOnTarget: false,
			errExpected:       true,
			errMsgExpected:    "failed to initialize target control connection: could not open control connection to TARGET",
		},
		{
			name:              "Proxy: No TLS for Origin, one-way TLS for Target",
			enableOnOrigin:    false,
			mutualTlsOnOrigin: false,
			enableOnTarget:    true,
			mutualTlsOnTarget: false,
			errExpected:       true,
			errMsgExpected:    "failed to initialize target control connection: could not open control connection to TARGET",
		},
	}

	ccmSetup, err := setupOriginAndTargetClusters(clusterTlsConfiguration, t)
	defer func() {
		if ccmSetup != nil {
			ccmSetup.Cleanup()
		}
	}()
	require.Nil(t, err)

	for _, proxyTlsConfig := range proxyTlsConfigurations {
		t.Run(proxyTlsConfig.name, func(t *testing.T) {
			executeTest(ccmSetup, proxyTlsConfig, t)
		})
	}
}

// Runs only when the full test suite is executed
func TestTls_OneWayOrigin_NoTarget(t *testing.T) {

	essentialTest := false
	skipNonEssentialTests(essentialTest, t)

	clusterTlsConfiguration := testClusterTlsConfiguration{
		name:              "Clusters: One-way TLS on Origin and no TLS on Target",
		enableOnOrigin:    true,
		mutualTlsOnOrigin: false,
		enableOnTarget:    false,
		mutualTlsOnTarget: false,
	}

	proxyTlsConfigurations := []testProxyTlsConfiguration{
		{
			name:              "Proxy: One-way TLS for Origin, no TLS for Target",
			enableOnOrigin:    true,
			mutualTlsOnOrigin: false,
			enableOnTarget:    false,
			mutualTlsOnTarget: false,
			errExpected:       false,
			errMsgExpected:    "",
		},
		{
			name:              "Proxy: No TLS for Origin, no TLS for Target",
			enableOnOrigin:    false,
			mutualTlsOnOrigin: false,
			enableOnTarget:    false,
			mutualTlsOnTarget: false,
			errExpected:       true,
			errMsgExpected:    "failed to initialize origin control connection: could not open control connection to ORIGIN",
		},
		{
			name:              "Proxy: Mutual TLS for Origin, no TLS for Target",
			enableOnOrigin:    true,
			mutualTlsOnOrigin: true,
			enableOnTarget:    false,
			mutualTlsOnTarget: false,
			errExpected:       false,
			errMsgExpected:    "",
		},
		{
			name:              "Proxy: One-way TLS for Origin, one-way TLS for Target",
			enableOnOrigin:    true,
			mutualTlsOnOrigin: false,
			enableOnTarget:    true,
			mutualTlsOnTarget: false,
			errExpected:       true,
			errMsgExpected:    "failed to initialize target control connection: could not open control connection to TARGET",
		},
		{
			name:              "Proxy: One-way TLS for Origin, mutual TLS for Target",
			enableOnOrigin:    true,
			mutualTlsOnOrigin: false,
			enableOnTarget:    true,
			mutualTlsOnTarget: true,
			errExpected:       true,
			errMsgExpected:    "failed to initialize target control connection: could not open control connection to TARGET",
		},
	}

	ccmSetup, err := setupOriginAndTargetClusters(clusterTlsConfiguration, t)
	defer func() {
		if ccmSetup != nil {
			ccmSetup.Cleanup()
		}
	}()
	require.Nil(t, err)

	for _, proxyTlsConfig := range proxyTlsConfigurations {
		t.Run(proxyTlsConfig.name, func(t *testing.T) {
			executeTest(ccmSetup, proxyTlsConfig, t)
		})
	}
}

// Runs only when the full test suite is executed
func TestTls_MutualOrigin_NoTarget(t *testing.T) {

	essentialTest := false
	skipNonEssentialTests(essentialTest, t)

	clusterTlsConfiguration := testClusterTlsConfiguration{
		name:              "Clusters: Mutual TLS on Origin and no TLS on Target",
		enableOnOrigin:    true,
		mutualTlsOnOrigin: true,
		enableOnTarget:    false,
		mutualTlsOnTarget: false,
	}

	proxyTlsConfigurations := []testProxyTlsConfiguration{
		{
			name:              "Proxy: Mutual TLS for Origin, no TLS for Target",
			enableOnOrigin:    true,
			mutualTlsOnOrigin: true,
			enableOnTarget:    false,
			mutualTlsOnTarget: false,
			errExpected:       false,
			errMsgExpected:    "",
		},
		{
			name:              "Proxy: No TLS for Origin, no TLS for Target",
			enableOnOrigin:    false,
			mutualTlsOnOrigin: false,
			enableOnTarget:    false,
			mutualTlsOnTarget: false,
			errExpected:       true,
			errMsgExpected:    "failed to initialize origin control connection: could not open control connection to ORIGIN",
		},
		{
			name:              "Proxy: One-way TLS for Origin, no TLS for Target",
			enableOnOrigin:    true,
			mutualTlsOnOrigin: false,
			enableOnTarget:    false,
			mutualTlsOnTarget: false,
			errExpected:       true,
			errMsgExpected:    "failed to initialize origin control connection: could not open control connection to ORIGIN",
		},
		{
			name:              "Proxy: Mutual TLS for Origin, one-way TLS for Target",
			enableOnOrigin:    true,
			mutualTlsOnOrigin: true,
			enableOnTarget:    true,
			mutualTlsOnTarget: false,
			errExpected:       true,
			errMsgExpected:    "failed to initialize target control connection: could not open control connection to TARGET",
		},
		{
			name:              "Proxy: Mutual TLS for Origin, mutual TLS for Target",
			enableOnOrigin:    true,
			mutualTlsOnOrigin: true,
			enableOnTarget:    true,
			mutualTlsOnTarget: true,
			errExpected:       true,
			errMsgExpected:    "failed to initialize target control connection: could not open control connection to TARGET",
		},
	}

	ccmSetup, err := setupOriginAndTargetClusters(clusterTlsConfiguration, t)
	defer func() {
		if ccmSetup != nil {
			ccmSetup.Cleanup()
		}
	}()
	require.Nil(t, err)

	for _, proxyTlsConfig := range proxyTlsConfigurations {
		t.Run(proxyTlsConfig.name, func(t *testing.T) {
			executeTest(ccmSetup, proxyTlsConfig, t)
		})
	}
}

func skipNonEssentialTests(essentialTest bool, t *testing.T)  {
	if !essentialTest && !env.RunAllTlsTests {
		t.Skip("Skipping this test. To run it, set the env var RUN_ALL_TLS_TESTS to true")
	}
}

func setupOriginAndTargetClusters(clusterConf testClusterTlsConfiguration, t *testing.T) (*setup.CcmTestSetup, error) {
	if !env.UseCcm {
		t.Skip("Test requires CCM, set USE_CCM env variable to TRUE")
	}

	ccmSetup, err := setup.NewTemporaryCcmTestSetup(false, false)
	if ccmSetup == nil {
		return nil, fmt.Errorf("ccm setup could not be created and is nil")
	}
	if err != nil {
		return ccmSetup, fmt.Errorf("Error while instantiating the ccm setup: %s", err)
	}

	if clusterConf.enableOnOrigin {
		yamlChanges := getTlsYamlChanges(clusterConf.mutualTlsOnOrigin, t)
		err = ccmSetup.Origin.UpdateConf(yamlChanges...)
		if err != nil {
			return ccmSetup, fmt.Errorf("Origin cluster could not be started after changing its configuration to enable TLS. Error: %s", err)
		}
	}

	if clusterConf.enableOnTarget {
		yamlChanges := getTlsYamlChanges(clusterConf.mutualTlsOnTarget, t)
		err = ccmSetup.Target.UpdateConf(yamlChanges...)
		if err != nil {
			return ccmSetup, fmt.Errorf("Target cluster could not be started after changing its configuration to enable TLS. Error: %s", err)
		}
	}

	err = ccmSetup.Start(nil)
	if err != nil {
		return ccmSetup, fmt.Errorf("Start CCM setup failed: %s", err)
	}

	return ccmSetup, nil
}

func executeTest(ccmSetup *setup.CcmTestSetup, proxyTlsConfig testProxyTlsConfiguration, t *testing.T) {

	proxyConfig := setup.NewTestConfig(ccmSetup.Origin.GetInitialContactPoint(), ccmSetup.Target.GetInitialContactPoint())

	if proxyTlsConfig.enableOnOrigin {
		proxyConfig = getProxyTlsConfiguration(proxyTlsConfig.mutualTlsOnOrigin, true, proxyConfig, t)
	}
	if proxyTlsConfig.enableOnTarget {
		proxyConfig = getProxyTlsConfiguration(proxyTlsConfig.mutualTlsOnTarget, false, proxyConfig, t)
	}

	proxy, err := setup.NewProxyInstanceWithConfig(proxyConfig)
	defer func() {
		if proxy != nil {
			proxy.Shutdown()
		}
	}()

	if proxyTlsConfig.errExpected {
		require.NotNil(t, err, "Did not get expected error %s", proxyTlsConfig.errMsgExpected)
		require.True(t, strings.Contains(err.Error(), proxyTlsConfig.errMsgExpected))
		require.Nil(t, proxy)
	} else {
		require.Nil(t, err, "Error while instantiating the proxy with the required configuration", err)
		require.NotNil(t, proxy)

		cqlConn := createTestClientConnection("127.0.0.1:14002", t)
		defer cqlConn.Close()

		// create schema on clusters through the proxy
		sendRequest(cqlConn, "CREATE KEYSPACE IF NOT EXISTS testks "+
			"WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1};", true, t)
		sendRequest(cqlConn, "CREATE TABLE IF NOT EXISTS testks.testtable "+
			"(key text primary key, value text);", true, t)

		// write some data and read it out again (all through the proxy)
		sendRequest(cqlConn, "INSERT INTO testks.testtable (key, value) values ('key1', 'value1');", false, t)
		sendRequest(cqlConn, "SELECT * FROM testks.testtable;", false, t)
	}

}

func loadAbsoluteFilePath(relativeFilePath string, t *testing.T) string {
	absoluteFilePath, err := filepath.Abs(relativeFilePath)
	require.Nil(t, err, "Error while retrieving the absolute path of the file %s", relativeFilePath, err)
	require.NotEmptyf(t, absoluteFilePath, "The absolute path of the given relative path is empty.")
	return absoluteFilePath
}

func getTlsYamlChanges(isMutualTls bool, t *testing.T) []string {
	serverKeystoreAbsPath := loadAbsoluteFilePath(ServerKeystoreRelPath, t)

	// common
	yamlChanges := []string{
		"client_encryption_options.enabled: true",
		"client_encryption_options.optional: false",
		"client_encryption_options.keystore: " + serverKeystoreAbsPath,
		"client_encryption_options.keystore_password: fakePasswordForTests",
	}

	if isMutualTls {
		serverTruststoreAbsPath := loadAbsoluteFilePath(ServerTruststoreRelPath, t)
		yamlChanges = append(yamlChanges, "client_encryption_options.require_client_auth: true")
		yamlChanges = append(yamlChanges, "client_encryption_options.truststore: "+serverTruststoreAbsPath)
		yamlChanges = append(yamlChanges, "client_encryption_options.truststore_password: fakePasswordForTests")
	} else {
		yamlChanges = append(yamlChanges, "client_encryption_options.require_client_auth: false")
	}
	return yamlChanges
}

func getProxyTlsConfiguration(isMutualTls bool, isOrigin bool, conf *config.Config, t *testing.T) *config.Config {
	serverCACertAbsPath := loadAbsoluteFilePath(ServerCaCertRelPath, t)

	if isOrigin {
		conf.OriginTlsServerCaPath = serverCACertAbsPath
		if isMutualTls {
			clientCertAbsPath := loadAbsoluteFilePath(ClientCertRelPath, t)
			conf.OriginTlsClientCertPath = clientCertAbsPath
			clientKeyAbsPath := loadAbsoluteFilePath(ClientKeyRelPath, t)
			conf.OriginTlsClientKeyPath = clientKeyAbsPath
		}
	} else {
		conf.TargetTlsServerCaPath = serverCACertAbsPath
		if isMutualTls {
			clientCertAbsPath := loadAbsoluteFilePath(ClientCertRelPath, t)
			conf.TargetTlsClientCertPath = clientCertAbsPath
			clientKeyAbsPath := loadAbsoluteFilePath(ClientKeyRelPath, t)
			conf.TargetTlsClientKeyPath = clientKeyAbsPath
		}
	}

	return conf
}

func createTestClientConnection(endpoint string, t *testing.T) *client.CqlClientConnection {
	testClient := client.NewCqlClient(endpoint, nil)
	cqlConn, err := testClient.ConnectAndInit(context.Background(), primitive.ProtocolVersion4, 1)
	require.Nil(t, err, "testClient setup failed: %v", err)
	return cqlConn
}

func sendRequest(cqlConn *client.CqlClientConnection, cqlRequest string, isSchemaChange bool, t *testing.T) {
	requestMsg := &message.Query{
		Query: fmt.Sprintf(cqlRequest),
		Options: &message.QueryOptions{
			Consistency: primitive.ConsistencyLevelOne,
		},
	}

	queryFrame := frame.NewFrame(primitive.ProtocolVersion4, 0, requestMsg)

	response, err := cqlConn.SendAndReceive(queryFrame)
	require.Nil(t, err)

	if isSchemaChange {
		switch response.Body.Message.(type) {
		case *message.SchemaChangeResult, *message.VoidResult:
		default:
			require.Fail(t, "expected schema change result or void, but got %v", response.Body.Message)
		}
	} else {
		switch response.Body.Message.(type) {
		case *message.RowsResult, *message.VoidResult:
		default:
			require.Fail(t, "expected request result but got %v", response.Body.Message)
		}
	}

}
