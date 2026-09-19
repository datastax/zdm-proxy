package config

import (
	"testing"

	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/require"
)

func TestParseTargetConsistencyLevel_Empty(t *testing.T) {
	c := &Config{TargetConsistencyLevel: ""}
	cl, err := c.ParseTargetConsistencyLevel()
	require.NoError(t, err)
	require.Nil(t, cl, "empty value should return nil (disabled)")
}

func TestParseTargetConsistencyLevel_Whitespace(t *testing.T) {
	c := &Config{TargetConsistencyLevel: "   "}
	cl, err := c.ParseTargetConsistencyLevel()
	require.NoError(t, err)
	require.Nil(t, cl, "whitespace-only value should return nil (disabled)")
}

func TestParseTargetConsistencyLevel_ValidValues(t *testing.T) {
	tests := []struct {
		input    string
		expected primitive.ConsistencyLevel
	}{
		{"ANY", primitive.ConsistencyLevelAny},
		{"ONE", primitive.ConsistencyLevelOne},
		{"TWO", primitive.ConsistencyLevelTwo},
		{"THREE", primitive.ConsistencyLevelThree},
		{"QUORUM", primitive.ConsistencyLevelQuorum},
		{"ALL", primitive.ConsistencyLevelAll},
		{"LOCAL_QUORUM", primitive.ConsistencyLevelLocalQuorum},
		{"EACH_QUORUM", primitive.ConsistencyLevelEachQuorum},
		{"LOCAL_ONE", primitive.ConsistencyLevelLocalOne},
		// case-insensitive
		{"local_one", primitive.ConsistencyLevelLocalOne},
		{"Local_Quorum", primitive.ConsistencyLevelLocalQuorum},
		{"one", primitive.ConsistencyLevelOne},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			c := &Config{TargetConsistencyLevel: tt.input}
			cl, err := c.ParseTargetConsistencyLevel()
			require.NoError(t, err)
			require.NotNil(t, cl)
			require.Equal(t, tt.expected, *cl)
		})
	}
}

func TestParseTargetConsistencyLevel_InvalidValues(t *testing.T) {
	tests := []string{
		"SERIAL",
		"LOCAL_SERIAL",
		"INVALID",
		"local_serial",
		"QUOROM",
		"12345",
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			c := &Config{TargetConsistencyLevel: input}
			cl, err := c.ParseTargetConsistencyLevel()
			require.Error(t, err)
			require.Nil(t, cl)
			require.Contains(t, err.Error(), "ZDM_TARGET_CONSISTENCY_LEVEL")
		})
	}
}

func TestParseTargetConsistencyLevel_WithWhitespacePadding(t *testing.T) {
	c := &Config{TargetConsistencyLevel: "  LOCAL_ONE  "}
	cl, err := c.ParseTargetConsistencyLevel()
	require.NoError(t, err)
	require.NotNil(t, cl)
	require.Equal(t, primitive.ConsistencyLevelLocalOne, *cl)
}

func TestValidate_RejectsInvalidTargetConsistencyLevel(t *testing.T) {
	defer clearAllEnvVars()

	clearAllEnvVars()
	setOriginCredentialsEnvVars()
	setTargetCredentialsEnvVars()
	setOriginContactPointsAndPortEnvVars()
	setTargetContactPointsAndPortEnvVars()

	setEnvVar("ZDM_TARGET_CONSISTENCY_LEVEL", "SERIAL")

	_, err := New().LoadConfig("")
	require.Error(t, err)
	require.Contains(t, err.Error(), "ZDM_TARGET_CONSISTENCY_LEVEL")
}

func TestValidate_AcceptsEmptyTargetConsistencyLevel(t *testing.T) {
	defer clearAllEnvVars()

	clearAllEnvVars()
	setOriginCredentialsEnvVars()
	setTargetCredentialsEnvVars()
	setOriginContactPointsAndPortEnvVars()
	setTargetContactPointsAndPortEnvVars()

	// do NOT set ZDM_TARGET_CONSISTENCY_LEVEL

	conf, err := New().LoadConfig("")
	require.NoError(t, err)
	require.Empty(t, conf.TargetConsistencyLevel)
}

func TestValidate_AcceptsValidTargetConsistencyLevel(t *testing.T) {
	defer clearAllEnvVars()

	clearAllEnvVars()
	setOriginCredentialsEnvVars()
	setTargetCredentialsEnvVars()
	setOriginContactPointsAndPortEnvVars()
	setTargetContactPointsAndPortEnvVars()

	setEnvVar("ZDM_TARGET_CONSISTENCY_LEVEL", "LOCAL_ONE")

	conf, err := New().LoadConfig("")
	require.NoError(t, err)
	require.Equal(t, "LOCAL_ONE", conf.TargetConsistencyLevel)
}

func TestTargetConsistencyLevel_YamlConfig(t *testing.T) {
	yamlContent := `
origin_contact_points: "origin.hostname.com"
origin_port: 9042
origin_username: "user"
origin_password: "pass"
target_contact_points: "target.hostname.com"
target_port: 9042
target_username: "user"
target_password: "pass"
target_consistency_level: "LOCAL_ONE"
`

	f, err := createConfigFile(yamlContent)
	require.NoError(t, err)
	defer removeConfigFile(f)

	conf, err := New().LoadConfig(f.Name())
	require.NoError(t, err)
	require.Equal(t, "LOCAL_ONE", conf.TargetConsistencyLevel)

	cl, err := conf.ParseTargetConsistencyLevel()
	require.NoError(t, err)
	require.NotNil(t, cl)
	require.Equal(t, primitive.ConsistencyLevelLocalOne, *cl)
}
