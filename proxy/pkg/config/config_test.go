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

	conf, err := New().ParseEnvVars()
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

	conf, err := New().ParseEnvVars()
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

	_, err := New().ParseEnvVars()
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

	_, err := New().ParseEnvVars()
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

	c, err := New().ParseEnvVars()
	require.Nil(t, err)
	require.Equal(t, 9042, c.TargetPort)
}