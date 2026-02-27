package config

import (
	"testing"

	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/require"
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

func TestTargetConfig_ParsingControlConnMaxProtocolVersion(t *testing.T) {
	defer clearAllEnvVars()

	// general setup
	clearAllEnvVars()
	setOriginCredentialsEnvVars()
	setTargetCredentialsEnvVars()
	setOriginContactPointsAndPortEnvVars()

	// test-specific setup
	setTargetContactPointsAndPortEnvVars()

	conf := New()
	err := conf.parseEnvVars()
	require.Nil(t, err)

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
			name:                          "ParsedV5",
			controlConnMaxProtocolVersion: "5",
			parsedProtocolVersion:         primitive.ProtocolVersion5,
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
			name:                          "UnsupportedCassandraV1",
			controlConnMaxProtocolVersion: "1",
			parsedProtocolVersion:         0,
			errorMessage:                  "invalid control connection max protocol version, valid values are 2, 3, 4, 5, DseV1, DseV2",
		},
		{
			name:                          "InvalidValue",
			controlConnMaxProtocolVersion: "Dsev123",
			parsedProtocolVersion:         0,
			errorMessage:                  "could not parse control connection max protocol version, valid values are 2, 3, 4, 5, DseV1, DseV2",
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
