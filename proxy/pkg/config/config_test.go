package config

import (
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func setupRequiredEnvVars() {
	clearAllEnvVars()
	setEnvVar("ORIGIN_CASSANDRA_USERNAME", "originUser")
	setEnvVar("ORIGIN_CASSANDRA_PASSWORD", "originPassword")
	setEnvVar("ORIGIN_CASSANDRA_HOSTNAME", "origin.hostname.com")
	setEnvVar("ORIGIN_CASSANDRA_PORT", "7890")
	setEnvVar("PROXY_METRICS_PORT", "1234")
	setEnvVar("TARGET_CASSANDRA_USERNAME", "targetUser")
	setEnvVar("TARGET_CASSANDRA_PASSWORD", "targetPassword")
}

func TestTargetConfig_WithBundleOnly(t *testing.T) {
	setupRequiredEnvVars()

	setEnvVar("TARGET_CASSANDRA_SECURE_CONNECT_BUNDLE_PATH", "/path/to/bundle")

	conf, err := New().ParseEnvVars()
	require.Equal(t, conf.TargetCassandraSecureConnectBundlePath, "/path/to/bundle")
	require.Empty(t, conf.TargetCassandraContactPoints)
	require.Equal(t, conf.TargetCassandraPort, 9042)
	require.Nil(t, err)
	
	os.Clearenv()
}

func TestTargetConfig_WithHostnameAndPortOnly(t *testing.T) {
	setupRequiredEnvVars()

	setEnvVar("TARGET_CASSANDRA_HOSTNAME", "target.hostname.com")
	setEnvVar("TARGET_CASSANDRA_PORT", "5647")

	conf, err := New().ParseEnvVars()
	require.Equal(t, conf.TargetCassandraContactPoints, "target.hostname.com")
	require.Equal(t, conf.TargetCassandraPort, 5647)
	require.Empty(t, conf.TargetCassandraSecureConnectBundlePath)
	require.Nil(t, err)

	os.Clearenv()
}

func TestTargetConfig_WithBundleAndHostname(t *testing.T) {
	setupRequiredEnvVars()

	setEnvVar("TARGET_CASSANDRA_SECURE_CONNECT_BUNDLE_PATH", "/path/to/bundle")
	setEnvVar("TARGET_CASSANDRA_HOSTNAME", "target.hostname.com")

	_, err := New().ParseEnvVars()
	require.Error(t, err, "TargetCassandraSecureConnectBundlePath and TargetCassandraContactPoints are " +
		"mutually exclusive. Please specify only one of them.")

	os.Clearenv()
}

func TestTargetConfig_WithoutBundleAndHostname(t *testing.T) {
	setupRequiredEnvVars()

	_, err := New().ParseEnvVars()
	require.Error(t, err, "Both TargetCassandraSecureConnectBundlePath and TargetCassandraContactPoints are " +
		"empty. Please specify either one of them.")
}

func TestTargetConfig_WithHostnameButWithoutPort(t *testing.T) {
	setupRequiredEnvVars()

	setEnvVar("TARGET_CASSANDRA_HOSTNAME", "target.hostname.com")

	c, err := New().ParseEnvVars()
	require.Nil(t, err)
	require.Equal(t, 9042, c.TargetCassandraPort)
}

func setEnvVar(key string, value string) {
	os.Setenv(key, value)
}

func clearAllEnvVars() {
	os.Clearenv()
}