package config

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTargetConfig_WithBundleOnly(t *testing.T) {
	defer clearAllEnvVars()

	// general setup
	clearAllEnvVars()
	setOriginCredentialsEnvVars()
	setTargetCredentialsEnvVars()
	setOriginContactPointsAndPortEnvVars()

	// test-specific setup
	setEnvVar("ZDM_TARGET_SECURE_CONNECT_BUNDLE_PATH", "/path/to/target/bundle")

	conf, err := New().LoadConfig("")
	require.Nil(t, err)
	require.Equal(t, conf.TargetSecureConnectBundlePath, "/path/to/target/bundle")
	require.Empty(t, conf.TargetContactPoints)
	require.Equal(t, conf.TargetPort, 9042)
}

func TestTargetConfig_WithHostnameAndPortOnly(t *testing.T) {
	defer clearAllEnvVars()

	// general setup
	clearAllEnvVars()
	setOriginCredentialsEnvVars()
	setTargetCredentialsEnvVars()
	setOriginContactPointsAndPortEnvVars()

	// test-specific setup
	setTargetContactPointsAndPortEnvVars()

	conf, err := New().LoadConfig("")
	require.Nil(t, err)
	require.Equal(t, conf.TargetContactPoints, "target.hostname.com")
	require.Equal(t, conf.TargetPort, 5647)
	require.Empty(t, conf.TargetSecureConnectBundlePath)
}

func TestTargetConfig_WithBundleAndHostname(t *testing.T) {
	defer clearAllEnvVars()

	// general setup
	clearAllEnvVars()
	setOriginCredentialsEnvVars()
	setTargetCredentialsEnvVars()
	setOriginContactPointsAndPortEnvVars()

	// test-specific setup
	setTargetContactPointsAndPortEnvVars()
	setTargetSecureConnectBundleEnvVar()

	_, err := New().LoadConfig("")
	require.Error(t, err, "TargetSecureConnectBundlePath and TargetContactPoints are "+
		"mutually exclusive. Please specify only one of them.")
}

func TestTargetConfig_WithoutBundleAndHostname(t *testing.T) {
	defer clearAllEnvVars()

	// general setup
	clearAllEnvVars()
	setOriginCredentialsEnvVars()
	setTargetCredentialsEnvVars()
	setOriginContactPointsAndPortEnvVars()

	// no test-specific setup in this case

	_, err := New().LoadConfig("")
	require.Error(t, err, "Both TargetSecureConnectBundlePath and TargetContactPoints are "+
		"empty. Please specify either one of them.")
}

func TestTargetConfig_WithHostnameButWithoutPort(t *testing.T) {
	defer clearAllEnvVars()

	// general setup
	clearAllEnvVars()
	setOriginCredentialsEnvVars()
	setTargetCredentialsEnvVars()
	setOriginContactPointsAndPortEnvVars()

	//test-specific setup
	setEnvVar("ZDM_TARGET_CONTACT_POINTS", "target.hostname.com")

	c, err := New().LoadConfig("")
	require.Nil(t, err)
	require.Equal(t, 9042, c.TargetPort)
}

func TestConfig_LoadNotExistingFile(t *testing.T) {
	defer clearAllEnvVars()
	clearAllEnvVars()

	_, err := New().LoadConfig("/not/existing/file")
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "could not read configuration file /not/existing/file")
}

func TestConfig_LoadConfigFromFile(t *testing.T) {
	defer clearAllEnvVars()
	clearAllEnvVars()

	f, err := createConfigFile(`
primary_cluster: ORIGIN

origin_username: foo1
origin_password: bar1
target_username: foo2
target_password: bar2

origin_contact_points: 192.168.100.101
origin_port: 19042
target_contact_points: 192.168.100.102
target_port: 29042
proxy_listen_port: 39042
`)
	defer removeConfigFile(f)
	require.Nil(t, err)

	c, err := New().LoadConfig(f.Name())
	require.Nil(t, err)
	require.Equal(t, "ORIGIN", c.PrimaryCluster)
	require.Equal(t, "foo1", c.OriginUsername)
	require.Equal(t, "bar1", c.OriginPassword)
	require.Equal(t, "foo2", c.TargetUsername)
	require.Equal(t, "bar2", c.TargetPassword)
	require.Equal(t, "192.168.100.101", c.OriginContactPoints)
	require.Equal(t, 19042, c.OriginPort)
	require.Equal(t, "192.168.100.102", c.TargetContactPoints)
	require.Equal(t, 29042, c.TargetPort)
	require.Equal(t, 39042, c.ProxyListenPort)
	require.Equal(t, 4000, c.AsyncHandshakeTimeoutMs) // verify that defaults were applied
}
