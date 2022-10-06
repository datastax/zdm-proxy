package integration_tests

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/zdm-proxy/integration-tests/env"
	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/proxy/pkg/config"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"
)

type clusterTlsConfiguration struct {
	name              string
	enableOnOrigin    bool
	mutualTlsOnOrigin bool
	enableOnTarget    bool
	mutualTlsOnTarget bool
	expiredCaOnOrigin bool
	expiredCaOnTarget bool
}

type proxyClusterTlsConfiguration struct {
	name                string
	enableOnOrigin      bool
	mutualTlsOnOrigin   bool
	enableOnTarget      bool
	mutualTlsOnTarget   bool
	errExpected         bool
	errWarningsExpected []string
	errMsgExpected      string
}

type proxyClientTlsConfiguration struct {
	name                string
	enableOnProxy       bool
	enableOnTest        bool
	mutualTlsOnProxy    bool
	mutualTlsOnTest     bool
	serverName          string
	errExpected         bool
	errWarningsExpected []string
	errMsgExpected      string
}

type proxyClusterIncorrectTlsConfiguration struct {
	name                string
	expiredOriginCa     bool
	expiredTargetCa     bool
	incorrectOriginCa   bool
	incorrectTargetCa   bool
	errWarningsExpected []string
	errMsgExpected      string
}

const (
	IncorrectCaCertRelPath = "../integration-tests/resources/myCA.pem"

	ExpiredCaCertRelPath     = "../integration-tests/resources/node1_ca.crt"
	ExpiredKeystoreRelPath   = "../integration-tests/resources/server.keystore"
	ExpiredTruststoreRelPath = "../integration-tests/resources/server.truststore"
	ExpiredClientCertRelPath = "../integration-tests/resources/client.crt"
	ExpiredClientKeyRelPath  = "../integration-tests/resources/client.key"

	OriginClientCertRelPath = "../integration-tests/resources/client1-zdm.crt"
	OriginClientKeyRelPath  = "../integration-tests/resources/client1-zdm.key"

	TargetClientCertRelPath = "../integration-tests/resources/client2-zdm.crt"
	TargetClientKeyRelPath  = "../integration-tests/resources/client2-zdm.key"

	AppClientCertRelPath = "../integration-tests/resources/appclient.crt"
	AppClientKeyRelPath  = "../integration-tests/resources/appclient.key"

	OriginCaCertRelPath = "../integration-tests/resources/rootcazdm.crt"
	TargetCaCertRelPath = "../integration-tests/resources/rootcazdm2.crt"
	ProxyCaCertRelPath  = "../integration-tests/resources/rootcaproxy.crt"

	OriginKeystoreRelPath   = "../integration-tests/resources/node1-zdm-keystore.jks"
	OriginTruststoreRelPath = "../integration-tests/resources/node1-truststore.jks"

	TargetKeystoreRelPath   = "../integration-tests/resources/node2-zdm-keystore.jks"
	TargetTruststoreRelPath = "../integration-tests/resources/node2-truststore.jks"

	ProxyServerCertRelPath = "../integration-tests/resources/proxycert.crt"
	ProxyServerKeyRelPath  = "../integration-tests/resources/proxycert.key"

	ExpiredKeystorePassword   = "fakePasswordForTests"
	ExpiredTruststorePassword = "fakePasswordForTests"
	OriginKeystorePassword    = "zdmzdm"
	TargetKeystorePassword    = "zdmzdm"
	OriginTruststorePassword  = "zdmzdm"
	TargetTruststorePassword  = "zdmzdm"
)

// Runs only when the full test suite is executed
func TestTls_OneWayOrigin_OneWayTarget(t *testing.T) {

	essentialTest := false
	skipNonEssentialTests(essentialTest, t)

	clusterTlsConfiguration := clusterTlsConfiguration{
		name:              "Clusters: One-way TLS on Origin and Target",
		enableOnOrigin:    true,
		mutualTlsOnOrigin: false,
		enableOnTarget:    true,
		mutualTlsOnTarget: false,
	}

	proxyTlsConfigurations := []*proxyClusterTlsConfiguration{
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
			testProxyClusterTls(ccmSetup, proxyTlsConfig, t)
		})
	}
}

// Runs only when the full test suite is executed
func TestTls_MutualOrigin_MutualTarget(t *testing.T) {

	essentialTest := false
	skipNonEssentialTests(essentialTest, t)

	clusterTlsConfiguration := clusterTlsConfiguration{
		name:              "Clusters: Mutual TLS on Origin and Target",
		enableOnOrigin:    true,
		mutualTlsOnOrigin: true,
		enableOnTarget:    true,
		mutualTlsOnTarget: true,
	}

	proxyTlsConfigurations := []*proxyClusterTlsConfiguration{
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
			testProxyClusterTls(ccmSetup, proxyTlsConfig, t)
		})
	}
}

// Always runs
func TestTls_OneWayOrigin_MutualTarget(t *testing.T) {

	essentialTest := true
	skipNonEssentialTests(essentialTest, t)

	clusterConfig := clusterTlsConfiguration{
		name:              "Clusters: One-way TLS on Origin and Mutual TLS on Target",
		enableOnOrigin:    true,
		mutualTlsOnOrigin: false,
		enableOnTarget:    true,
		mutualTlsOnTarget: true,
	}

	proxyClusterTlsConfigurations := []*proxyClusterTlsConfiguration{
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

	incorrectCertProxyConfigurations := []*proxyClusterIncorrectTlsConfiguration{
		{
			name:              "Proxy: mutual TLS for Origin (INCORRECT CA), mutual TLS for Target",
			expiredOriginCa:   false,
			expiredTargetCa:   false,
			incorrectOriginCa: true,
			incorrectTargetCa: false,
			errWarningsExpected: []string{
				"Failed to open control connection to ORIGIN",
				"certificate signed by unknown authority",
			},
			errMsgExpected: "failed to initialize origin control connection: could not open control connection to ORIGIN",
		},
		{
			name:              "Proxy: mutual TLS for Origin, mutual TLS for Target (INCORRECT CA)",
			expiredOriginCa:   false,
			expiredTargetCa:   false,
			incorrectOriginCa: false,
			incorrectTargetCa: true,
			errWarningsExpected: []string{
				"Failed to open control connection to TARGET",
				"certificate signed by unknown authority",
			},
			errMsgExpected: "failed to initialize target control connection: could not open control connection to TARGET",
		},
	}

	proxyClientTlsConfigurations := []*proxyClientTlsConfiguration{
		{
			name:                "Proxy: No TLS on Client, One-way TLS on Listener, One-way TLS on Origin, mutual TLS on Target",
			enableOnProxy:       true,
			enableOnTest:        false,
			mutualTlsOnProxy:    false,
			mutualTlsOnTest:     false,
			serverName:          "",
			errExpected:         true,
			errWarningsExpected: []string{"tls: first record does not look like a TLS handshake"},
			errMsgExpected:      "",
		},
		{
			name:                "Proxy: One-Way TLS and SNI on Client, One-way TLS on Listener, One-way TLS on Origin, mutual TLS on Target",
			enableOnProxy:       true,
			enableOnTest:        true,
			mutualTlsOnProxy:    false,
			mutualTlsOnTest:     false,
			serverName:          "zdmproxy",
			errExpected:         false,
			errWarningsExpected: nil,
			errMsgExpected:      "",
		},
		{
			name:                "Proxy: One-Way TLS on Client, One-way TLS on Listener, One-way TLS on Origin, mutual TLS on Target",
			enableOnProxy:       true,
			enableOnTest:        true,
			mutualTlsOnProxy:    false,
			mutualTlsOnTest:     false,
			serverName:          "",
			errExpected:         false,
			errWarningsExpected: nil,
			errMsgExpected:      "",
		},
		{
			name:                "Proxy: Mutual TLS on Client, One-way TLS on Listener, One-way TLS on Origin, mutual TLS on Target",
			enableOnProxy:       true,
			enableOnTest:        true,
			mutualTlsOnProxy:    false,
			mutualTlsOnTest:     true,
			serverName:          "",
			errExpected:         false,
			errWarningsExpected: nil,
			errMsgExpected:      "",
		},
		{
			name:                "Proxy: No TLS on Client, Mutual TLS on Listener, One-way TLS on Origin, mutual TLS on Target",
			enableOnProxy:       true,
			enableOnTest:        false,
			mutualTlsOnProxy:    true,
			mutualTlsOnTest:     false,
			serverName:          "",
			errExpected:         true,
			errWarningsExpected: []string{"tls: first record does not look like a TLS handshake"},
			errMsgExpected:      "",
		},
		{
			name:                "Proxy: One-Way TLS on Client, Mutual TLS on Listener, One-way TLS on Origin, mutual TLS on Target",
			enableOnProxy:       true,
			enableOnTest:        true,
			mutualTlsOnProxy:    true,
			mutualTlsOnTest:     false,
			serverName:          "",
			errExpected:         true,
			errWarningsExpected: []string{"tls: client didn't provide a certificate"},
			errMsgExpected:      "",
		},
		{
			name:                "Proxy: Mutual TLS and SNI on Client, Mutual TLS on Listener, One-way TLS on Origin, mutual TLS on Target",
			enableOnProxy:       true,
			enableOnTest:        true,
			mutualTlsOnProxy:    true,
			mutualTlsOnTest:     true,
			serverName:          "zdmproxy",
			errExpected:         false,
			errWarningsExpected: nil,
			errMsgExpected:      "",
		},
		{
			name:                "Proxy: Mutual TLS on Client, Mutual TLS on Listener, One-way TLS on Origin, mutual TLS on Target",
			enableOnProxy:       true,
			enableOnTest:        true,
			mutualTlsOnProxy:    true,
			mutualTlsOnTest:     true,
			serverName:          "",
			errExpected:         false,
			errWarningsExpected: nil,
			errMsgExpected:      "",
		},
	}

	ccmSetup, err := setupOriginAndTargetClusters(clusterConfig, t)
	defer func() {
		if ccmSetup != nil {
			ccmSetup.Cleanup()
		}
	}()
	require.Nil(t, err)

	for _, proxyTlsConfig := range proxyClusterTlsConfigurations {
		t.Run(proxyTlsConfig.name, func(t *testing.T) {
			testProxyClusterTls(ccmSetup, proxyTlsConfig, t)
		})
	}

	for _, proxyTlsConfig := range incorrectCertProxyConfigurations {
		t.Run(proxyTlsConfig.name, func(t *testing.T) {
			testProxyClusterTlsInvalidCertificate(t, ccmSetup, proxyTlsConfig)
		})
	}

	for _, proxyTlsConfig := range proxyClientTlsConfigurations {
		t.Run(proxyTlsConfig.name, func(t *testing.T) {
			testProxyClientTls(t, ccmSetup, clusterConfig, proxyTlsConfig)
		})
	}
}

// Runs only when the full test suite is executed
func TestTls_ExpiredCA(t *testing.T) {
	essentialTest := false
	skipNonEssentialTests(essentialTest, t)

	clusterTlsConfiguration := clusterTlsConfiguration{
		name:              "Clusters: Mutual TLS on Origin and Mutual TLS on Target",
		enableOnOrigin:    true,
		mutualTlsOnOrigin: true,
		enableOnTarget:    true,
		mutualTlsOnTarget: true,
		expiredCaOnOrigin: true,
		expiredCaOnTarget: true,
	}

	proxyTlsConfigurations := []*proxyClusterIncorrectTlsConfiguration{
		{
			name:              "Proxy: mutual TLS for Origin, mutual TLS for Target (Expired CAs on both)",
			expiredOriginCa:   true,
			expiredTargetCa:   true,
			incorrectOriginCa: false,
			incorrectTargetCa: false,
			errWarningsExpected: []string{
				"Failed to open control connection to ORIGIN",
				"x509: certificate has expired or is not yet valid",
			},
			errMsgExpected: "failed to initialize origin control connection: could not open control connection to ORIGIN",
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
			testProxyClusterTlsInvalidCertificate(t, ccmSetup, proxyTlsConfig)
		})
	}
}

// Runs only when the full test suite is executed
func TestTls_MutualOrigin_OneWayTarget(t *testing.T) {

	essentialTest := false
	skipNonEssentialTests(essentialTest, t)

	clusterTlsConfiguration := clusterTlsConfiguration{
		name:              "Clusters: Mutual TLS on Origin and one-way TLS on Target",
		enableOnOrigin:    true,
		mutualTlsOnOrigin: true,
		enableOnTarget:    true,
		mutualTlsOnTarget: false,
	}

	proxyTlsConfigurations := []*proxyClusterTlsConfiguration{
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
			testProxyClusterTls(ccmSetup, proxyTlsConfig, t)
		})
	}
}

// Runs only when the full test suite is executed
func TestTls_NoOrigin_OneWayTarget(t *testing.T) {

	essentialTest := false
	skipNonEssentialTests(essentialTest, t)

	clusterTlsConfiguration := clusterTlsConfiguration{
		name:              "Clusters: No TLS on Origin and one-way TLS on Target",
		enableOnOrigin:    false,
		mutualTlsOnOrigin: false,
		enableOnTarget:    true,
		mutualTlsOnTarget: false,
	}

	proxyTlsConfigurations := []*proxyClusterTlsConfiguration{
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
			testProxyClusterTls(ccmSetup, proxyTlsConfig, t)
		})
	}
}

// Always runs
func TestTls_NoOrigin_MutualTarget(t *testing.T) {

	essentialTest := true
	skipNonEssentialTests(essentialTest, t)

	clusterTlsConfiguration := clusterTlsConfiguration{
		name:              "Clusters: No TLS on Origin and mutual TLS on Target",
		enableOnOrigin:    false,
		mutualTlsOnOrigin: false,
		enableOnTarget:    true,
		mutualTlsOnTarget: true,
	}

	proxyTlsConfigurations := []*proxyClusterTlsConfiguration{
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
			testProxyClusterTls(ccmSetup, proxyTlsConfig, t)
		})
	}
}

// Runs only when the full test suite is executed
func TestTls_OneWayOrigin_NoTarget(t *testing.T) {

	essentialTest := false
	skipNonEssentialTests(essentialTest, t)

	clusterTlsConfiguration := clusterTlsConfiguration{
		name:              "Clusters: One-way TLS on Origin and no TLS on Target",
		enableOnOrigin:    true,
		mutualTlsOnOrigin: false,
		enableOnTarget:    false,
		mutualTlsOnTarget: false,
	}

	proxyTlsConfigurations := []*proxyClusterTlsConfiguration{
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
			testProxyClusterTls(ccmSetup, proxyTlsConfig, t)
		})
	}
}

// Runs only when the full test suite is executed
func TestTls_MutualOrigin_NoTarget(t *testing.T) {

	essentialTest := false
	skipNonEssentialTests(essentialTest, t)

	clusterTlsConfiguration := clusterTlsConfiguration{
		name:              "Clusters: Mutual TLS on Origin and no TLS on Target",
		enableOnOrigin:    true,
		mutualTlsOnOrigin: true,
		enableOnTarget:    false,
		mutualTlsOnTarget: false,
	}

	proxyTlsConfigurations := []*proxyClusterTlsConfiguration{
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
			testProxyClusterTls(ccmSetup, proxyTlsConfig, t)
		})
	}
}

func skipNonEssentialTests(essentialTest bool, t *testing.T) {
	if !essentialTest && !env.RunAllTlsTests {
		t.Skip("Skipping this test. To run it, set the env var RUN_ALL_TLS_TESTS to true")
	}
}

func setupOriginAndTargetClusters(clusterConf clusterTlsConfiguration, t *testing.T) (*setup.CcmTestSetup, error) {
	if !env.RunCcmTests {
		t.Skip("Test requires CCM, set RUN_CCMTESTS env variable to TRUE")
	}

	ccmSetup, err := setup.NewTemporaryCcmTestSetup(false, false)
	if ccmSetup == nil {
		return nil, fmt.Errorf("ccm setup could not be created and is nil")
	}
	if err != nil {
		return ccmSetup, fmt.Errorf("Error while instantiating the ccm setup: %s", err)
	}

	if clusterConf.enableOnOrigin {
		yamlChanges := getOriginTlsYamlChanges(t, clusterConf.mutualTlsOnOrigin, clusterConf.expiredCaOnOrigin)
		err = ccmSetup.Origin.UpdateConf(yamlChanges...)
		if err != nil {
			return ccmSetup, fmt.Errorf("Origin cluster could not be started after changing its configuration to enable TLS. Error: %s", err)
		}
	}

	if clusterConf.enableOnTarget {
		yamlChanges := getTargetTlsYamlChanges(t, clusterConf.mutualTlsOnTarget, clusterConf.expiredCaOnTarget)
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

func testProxyClusterTls(ccmSetup *setup.CcmTestSetup, proxyTlsConfig *proxyClusterTlsConfiguration, t *testing.T) {

	proxyConfig := setup.NewTestConfig(ccmSetup.Origin.GetInitialContactPoint(), ccmSetup.Target.GetInitialContactPoint())

	if proxyTlsConfig.enableOnOrigin {
		proxyConfig = applyProxyTlsConfiguration(
			false, false, proxyTlsConfig.mutualTlsOnOrigin, true, proxyConfig, t)
	}
	if proxyTlsConfig.enableOnTarget {
		proxyConfig = applyProxyTlsConfiguration(
			false, false, proxyTlsConfig.mutualTlsOnTarget, false, proxyConfig, t)
	}

	proxy, err := setup.NewProxyInstanceWithConfig(proxyConfig)
	defer func() {
		if proxy != nil {
			proxy.Shutdown()
		}
	}()

	if proxyTlsConfig.errExpected {
		require.NotNil(t, err, "Did not get expected error %s", proxyTlsConfig.errMsgExpected)
		require.True(t, strings.Contains(err.Error(), proxyTlsConfig.errMsgExpected), err.Error())
		require.Nil(t, proxy)
	} else {
		require.Nil(t, err, "Error while instantiating the proxy with the required configuration", err)
		require.NotNil(t, proxy)

		cqlConn, err := createTestClientConnection("127.0.0.1:14002", nil)
		require.Nil(t, err, "testClient setup failed: %v", err)
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

func testProxyClusterTlsInvalidCertificate(t *testing.T, ccmSetup *setup.CcmTestSetup, proxyTlsConfig *proxyClusterIncorrectTlsConfiguration) {
	proxyConfig := setup.NewTestConfig(ccmSetup.Origin.GetInitialContactPoint(), ccmSetup.Target.GetInitialContactPoint())

	proxyConfig = applyProxyTlsConfiguration(
		proxyTlsConfig.expiredOriginCa, proxyTlsConfig.incorrectOriginCa, true, true, proxyConfig, t)
	proxyConfig = applyProxyTlsConfiguration(
		proxyTlsConfig.expiredTargetCa, proxyTlsConfig.incorrectTargetCa, true, false, proxyConfig, t)

	buffer := createLogHooks(log.WarnLevel)
	defer log.StandardLogger().ReplaceHooks(make(log.LevelHooks))

	proxy, err := setup.NewProxyInstanceWithConfig(proxyConfig)
	defer func() {
		if proxy != nil {
			proxy.Shutdown()
		}
	}()

	logMessages := buffer.String()

	require.NotEqual(t, "", proxyTlsConfig.errMsgExpected)
	require.True(t, strings.Contains(err.Error(), proxyTlsConfig.errMsgExpected), err.Error())
	require.NotEqual(t, 0, len(proxyTlsConfig.errWarningsExpected))
	for _, errMsgExpected := range proxyTlsConfig.errWarningsExpected {
		require.NotEqual(t, "", errMsgExpected)
		require.True(t, strings.Contains(logMessages, errMsgExpected))
	}
	require.Nil(t, proxy)
}

func testProxyClientTls(t *testing.T, ccmSetup *setup.CcmTestSetup,
	clusterConfig clusterTlsConfiguration, proxyTlsConfig *proxyClientTlsConfiguration) {

	proxyConfig := setup.NewTestConfig(ccmSetup.Origin.GetInitialContactPoint(), ccmSetup.Target.GetInitialContactPoint())

	if clusterConfig.enableOnOrigin {
		proxyConfig = applyProxyTlsConfiguration(
			false, false, clusterConfig.mutualTlsOnOrigin, true, proxyConfig, t)
	}
	if clusterConfig.enableOnTarget {
		proxyConfig = applyProxyTlsConfiguration(
			false, false, clusterConfig.mutualTlsOnTarget, false, proxyConfig, t)
	}
	if proxyTlsConfig.enableOnProxy {
		proxyConfig = applyProxyClientTlsConfiguration(false, false, proxyTlsConfig.mutualTlsOnProxy, proxyConfig, t)
	}
	var tlsCfg *tls.Config
	var err error
	if proxyTlsConfig.enableOnTest {
		tlsCfg, err = getClientSideTlsConfigForTest(proxyTlsConfig.mutualTlsOnTest, proxyTlsConfig.serverName)
	}
	require.Nil(t, err)

	proxy, err := setup.NewProxyInstanceWithConfig(proxyConfig)
	defer func() {
		if proxy != nil {
			proxy.Shutdown()
		}
	}()

	require.Nil(t, err, "Error while instantiating the proxy with the required configuration", err)
	require.NotNil(t, proxy)

	buffer := createLogHooks(log.WarnLevel, log.ErrorLevel)
	defer log.StandardLogger().ReplaceHooks(make(log.LevelHooks))

	cqlConn, err := createTestClientConnection("127.0.0.1:14002", tlsCfg)
	defer func() {
		if cqlConn != nil {
			_ = cqlConn.Close()
		}
	}()

	logMessages := buffer.String()

	for _, errWarnExpected := range proxyTlsConfig.errWarningsExpected {
		require.True(t, strings.Contains(logMessages, errWarnExpected), "%v not found", errWarnExpected)
	}

	if proxyTlsConfig.errExpected {
		require.NotNil(t, err, "Did not get expected error %s", proxyTlsConfig.errMsgExpected)
		if proxyTlsConfig.errMsgExpected != "" {
			require.True(t, strings.Contains(err.Error(), proxyTlsConfig.errMsgExpected), err.Error())
		}
	} else {
		require.Nil(t, err, "testClient setup failed: %v", err)
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

func getOriginTlsYamlChanges(t *testing.T, isMutualTls bool, expiredCa bool) []string {
	if !expiredCa {
		return getTlsYamlChanges(isMutualTls, t,
			loadAbsoluteFilePath(OriginKeystoreRelPath, t),
			loadAbsoluteFilePath(OriginTruststoreRelPath, t),
			OriginKeystorePassword, OriginTruststorePassword)
	}
	return getTlsYamlChanges(isMutualTls, t,
		loadAbsoluteFilePath(ExpiredKeystoreRelPath, t),
		loadAbsoluteFilePath(ExpiredTruststoreRelPath, t),
		ExpiredKeystorePassword, ExpiredTruststorePassword)
}

func getTargetTlsYamlChanges(t *testing.T, isMutualTls bool, expiredCa bool) []string {
	if !expiredCa {
		return getTlsYamlChanges(isMutualTls, t,
			loadAbsoluteFilePath(TargetKeystoreRelPath, t),
			loadAbsoluteFilePath(TargetTruststoreRelPath, t),
			TargetKeystorePassword, TargetTruststorePassword)
	}
	return getTlsYamlChanges(isMutualTls, t,
		loadAbsoluteFilePath(ExpiredKeystoreRelPath, t),
		loadAbsoluteFilePath(ExpiredTruststoreRelPath, t),
		ExpiredKeystorePassword, ExpiredTruststorePassword)
}

func getTlsYamlChanges(isMutualTls bool, t *testing.T,
	serverKeystorePath string, serverTruststorePath string, keystorePassword string, truststorePassword string) []string {
	serverKeystoreAbsPath := loadAbsoluteFilePath(serverKeystorePath, t)

	// common
	yamlChanges := []string{
		"client_encryption_options.enabled: true",
		"client_encryption_options.optional: false",
		"client_encryption_options.keystore: " + serverKeystoreAbsPath,
		"client_encryption_options.keystore_password: " + keystorePassword,
	}

	if isMutualTls {
		serverTruststoreAbsPath := loadAbsoluteFilePath(serverTruststorePath, t)
		yamlChanges = append(yamlChanges, "client_encryption_options.require_client_auth: true")
		yamlChanges = append(yamlChanges, "client_encryption_options.truststore: "+serverTruststoreAbsPath)
		yamlChanges = append(yamlChanges, "client_encryption_options.truststore_password: "+truststorePassword)
	} else {
		yamlChanges = append(yamlChanges, "client_encryption_options.require_client_auth: false")
	}
	return yamlChanges
}

func applyProxyTlsConfiguration(expiredCa bool, incorrectCa bool, isMutualTls bool, isOrigin bool, conf *config.Config, t *testing.T) *config.Config {
	incorrectCACertAbsPath := loadAbsoluteFilePath(IncorrectCaCertRelPath, t)
	expiredCACertAbsPath := loadAbsoluteFilePath(ExpiredCaCertRelPath, t)

	if isOrigin {
		if incorrectCa {
			conf.OriginTlsServerCaPath = incorrectCACertAbsPath
		} else if expiredCa {
			conf.OriginTlsServerCaPath = expiredCACertAbsPath
		} else {
			conf.OriginTlsServerCaPath = loadAbsoluteFilePath(OriginCaCertRelPath, t)
		}
		if isMutualTls {
			if expiredCa {
				clientCertAbsPath := loadAbsoluteFilePath(ExpiredClientCertRelPath, t)
				conf.OriginTlsClientCertPath = clientCertAbsPath
				clientKeyAbsPath := loadAbsoluteFilePath(ExpiredClientKeyRelPath, t)
				conf.OriginTlsClientKeyPath = clientKeyAbsPath
			} else {
				clientCertAbsPath := loadAbsoluteFilePath(OriginClientCertRelPath, t)
				conf.OriginTlsClientCertPath = clientCertAbsPath
				clientKeyAbsPath := loadAbsoluteFilePath(OriginClientKeyRelPath, t)
				conf.OriginTlsClientKeyPath = clientKeyAbsPath
			}
		}
	} else {
		if incorrectCa {
			conf.TargetTlsServerCaPath = incorrectCACertAbsPath
		} else if expiredCa {
			conf.TargetTlsServerCaPath = expiredCACertAbsPath
		} else {
			conf.TargetTlsServerCaPath = loadAbsoluteFilePath(TargetCaCertRelPath, t)
		}
		if isMutualTls {
			if expiredCa {
				clientCertAbsPath := loadAbsoluteFilePath(ExpiredClientCertRelPath, t)
				conf.TargetTlsClientCertPath = clientCertAbsPath
				clientKeyAbsPath := loadAbsoluteFilePath(ExpiredClientKeyRelPath, t)
				conf.TargetTlsClientKeyPath = clientKeyAbsPath
			} else {
				clientCertAbsPath := loadAbsoluteFilePath(TargetClientCertRelPath, t)
				conf.TargetTlsClientCertPath = clientCertAbsPath
				clientKeyAbsPath := loadAbsoluteFilePath(TargetClientKeyRelPath, t)
				conf.TargetTlsClientKeyPath = clientKeyAbsPath
			}
		}
	}

	return conf
}

func applyProxyClientTlsConfiguration(expiredCa bool, incorrectCa bool, isMutualTls bool, conf *config.Config, t *testing.T) *config.Config {
	incorrectCACertAbsPath := loadAbsoluteFilePath(IncorrectCaCertRelPath, t)
	expiredCACertAbsPath := loadAbsoluteFilePath(ExpiredCaCertRelPath, t)

	if incorrectCa {
		conf.ProxyTlsCaPath = incorrectCACertAbsPath
	} else if expiredCa {
		conf.ProxyTlsCaPath = expiredCACertAbsPath
	} else {
		conf.ProxyTlsCaPath = loadAbsoluteFilePath(ProxyCaCertRelPath, t)
	}

	conf.ProxyTlsCertPath = loadAbsoluteFilePath(ProxyServerCertRelPath, t)
	conf.ProxyTlsKeyPath = loadAbsoluteFilePath(ProxyServerKeyRelPath, t)

	if isMutualTls {
		conf.ProxyTlsRequireClientAuth = true
	} else {
		conf.ProxyTlsRequireClientAuth = false
	}

	return conf
}

func createTestClientConnection(endpoint string, tlsCfg *tls.Config) (*client.CqlClientConnection, error) {
	testClient := client.NewCqlClient(endpoint, nil)
	testClient.TLSConfig = tlsCfg
	return testClient.ConnectAndInit(context.Background(), primitive.ProtocolVersion4, 1)
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

func loadTlsFile(filePath string) ([]byte, error) {
	var file []byte
	var err error
	if filePath != "" {
		file, err = ioutil.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("could not load file with path %s due to: %v", filePath, err)
		}
	}
	return file, err
}

func getClientSideTlsConfigForTest(mutualTls bool, serverName string) (*tls.Config, error) {
	// create tls config object using the values provided in the cluster security config
	serverCAFile, err := loadTlsFile(ProxyCaCertRelPath)
	if err != nil {
		return nil, err
	}
	clientCertFile, err := loadTlsFile(AppClientCertRelPath)
	if err != nil {
		return nil, err
	}
	clientKeyFile, err := loadTlsFile(AppClientKeyRelPath)
	if err != nil {
		return nil, err
	}
	// currently not supporting server hostname verification for non-Astra clusters
	return getClientSideTlsConfigFromLoadedFiles(serverCAFile, clientCertFile, clientKeyFile, mutualTls, serverName)
}

func getClientSideTlsConfigFromLoadedFiles(
	caCert []byte, cert []byte, key []byte, mutualTls bool, serverName string) (*tls.Config, error) {

	rootCAs := x509.NewCertPool()

	// if TLS is used, server CA must always be specified
	if caCert != nil {
		ok := rootCAs.AppendCertsFromPEM(caCert)
		if !ok {
			return nil, fmt.Errorf("the provided CA cert could not be added to the rootCAs")
		}
	} else {
		return nil, fmt.Errorf("no CA provided")
	}

	var clientCerts []tls.Certificate
	if mutualTls {
		if cert == nil && key == nil {
			return nil, fmt.Errorf("mutual tls enabled but no key or cert")
		} else {
			// if using mTLS, both client cert and client key have to be specified
			if cert == nil {
				return nil, fmt.Errorf("mutual tls enabled but no cert")
			}
			if key == nil {
				return nil, fmt.Errorf("mutual tls enabled but no key")
			}
			clientCert, err := tls.X509KeyPair(cert, key)
			if err != nil {
				return nil, err
			}
			clientCerts = []tls.Certificate{clientCert}
		}
	}

	return getClientSideTlsConfigFromParsedCerts(rootCAs, clientCerts, serverName), nil
}

func getClientSideTlsConfigFromParsedCerts(rootCAs *x509.CertPool, clientCerts []tls.Certificate, serverName string) *tls.Config {
	verifyConnectionCallback := getClientSideVerifyConnectionCallback(rootCAs)
	tlsConfig := &tls.Config{
		RootCAs:            rootCAs,
		Certificates:       clientCerts,
		ServerName:         serverName,
		InsecureSkipVerify: verifyConnectionCallback != nil,
		VerifyConnection:   verifyConnectionCallback,
	}

	return tlsConfig
}

func getClientSideVerifyConnectionCallback(rootCAs *x509.CertPool) func(cs tls.ConnectionState) error {
	return func(cs tls.ConnectionState) error {
		dnsName := cs.ServerName
		opts := x509.VerifyOptions{
			DNSName: dnsName,
			Roots:   rootCAs,
		}
		if len(cs.PeerCertificates) > 0 {
			opts.Intermediates = x509.NewCertPool()
			for _, cert := range cs.PeerCertificates[1:] {
				opts.Intermediates.AddCert(cert)
			}
		}
		_, err := cs.PeerCertificates[0].Verify(opts)
		return err
	}
}
