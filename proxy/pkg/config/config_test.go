package config

import (
	"github.com/datastax/go-cassandra-native-protocol/primitive"
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

func TestTargetConfig_ParsingControlConnMaxProtocolVersion(t *testing.T) {
	defer clearAllEnvVars()

	// general setup
	clearAllEnvVars()
	setOriginCredentialsEnvVars()
	setTargetCredentialsEnvVars()
	setOriginContactPointsAndPortEnvVars()

	// test-specific setup
	setTargetContactPointsAndPortEnvVars()

	conf, _ := New().ParseEnvVars()

	tests := []struct {
		name                          string
		controlConnMaxProtocolVersion string
		parsedProtocolVersion         primitive.ProtocolVersion
		errorMessage                  string
	}{
		{
			name:                          "ParsedV2",
			controlConnMaxProtocolVersion: "2",
			parsedProtocolVersion:         primitive.ProtocolVersion2,
			errorMessage:                  "",
		},
		{
			name:                          "ParsedV3",
			controlConnMaxProtocolVersion: "3",
			parsedProtocolVersion:         primitive.ProtocolVersion3,
			errorMessage:                  "",
		},
		{
			name:                          "ParsedV4",
			controlConnMaxProtocolVersion: "4",
			parsedProtocolVersion:         primitive.ProtocolVersion4,
			errorMessage:                  "",
		},
		{
			name:                          "ParsedDse1",
			controlConnMaxProtocolVersion: "DseV1",
			parsedProtocolVersion:         primitive.ProtocolVersionDse1,
			errorMessage:                  "",
		},
		{
			name:                          "ParsedDse2",
			controlConnMaxProtocolVersion: "DseV2",
			parsedProtocolVersion:         primitive.ProtocolVersionDse2,
			errorMessage:                  "",
		},
		{
			name:                          "ParsedDse2CaseInsensitive",
			controlConnMaxProtocolVersion: "dsev2",
			parsedProtocolVersion:         primitive.ProtocolVersionDse2,
			errorMessage:                  "",
		},
		{
			name:                          "UnsupportedCassandraV5",
			controlConnMaxProtocolVersion: "5",
			parsedProtocolVersion:         0,
			errorMessage:                  "invalid control connection max protocol version, valid values are 2, 3, 4, DseV1, DseV2",
		},
		{
			name:                          "UnsupportedCassandraV1",
			controlConnMaxProtocolVersion: "1",
			parsedProtocolVersion:         0,
			errorMessage:                  "invalid control connection max protocol version, valid values are 2, 3, 4, DseV1, DseV2",
		},
		{
			name:                          "InvalidValue",
			controlConnMaxProtocolVersion: "Dsev123",
			parsedProtocolVersion:         0,
			errorMessage:                  "could not parse control connection max protocol version, valid values are 2, 3, 4, DseV1, DseV2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf.ControlConnMaxProtocolVersion = tt.controlConnMaxProtocolVersion
			ver, err := conf.ParseControlConnMaxProtocolVersion()
			if ver == 0 {
				require.NotNil(t, err)
				require.Contains(t, err.Error(), tt.errorMessage)
			} else {
				require.Equal(t, tt.parsedProtocolVersion, ver)
			}
		})
	}
}
