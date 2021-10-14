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
	setEnvVar("TARGET_CASSANDRA_SECURE_CONNECT_BUNDLE_PATH", "/path/to/target/bundle")

	conf, err := New().ParseEnvVars()
	require.Nil(t, err)
	require.Equal(t, conf.TargetCassandraSecureConnectBundlePath, "/path/to/target/bundle")
	require.Empty(t, conf.TargetCassandraContactPoints)
	require.Equal(t, conf.TargetCassandraPort, 9042)
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

	conf, err := New().ParseEnvVars()
	require.Nil(t, err)
	require.Equal(t, conf.TargetCassandraContactPoints, "target.hostname.com")
	require.Equal(t, conf.TargetCassandraPort, 5647)
	require.Empty(t, conf.TargetCassandraSecureConnectBundlePath)
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

	_, err := New().ParseEnvVars()
	require.Error(t, err, "TargetCassandraSecureConnectBundlePath and TargetCassandraContactPoints are "+
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

	_, err := New().ParseEnvVars()
	require.Error(t, err, "Both TargetCassandraSecureConnectBundlePath and TargetCassandraContactPoints are "+
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
	setEnvVar("TARGET_CASSANDRA_CONTACT_POINTS", "target.hostname.com")

	c, err := New().ParseEnvVars()
	require.Nil(t, err)
	require.Equal(t, 9042, c.TargetCassandraPort)
}
