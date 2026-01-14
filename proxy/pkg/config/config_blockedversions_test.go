package config

import (
	"testing"

	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/require"
)

func TestConfig_ParseBlockedProtocolVersions(t *testing.T) {

	type test struct {
		name                    string
		envVars                 []envVar
		expectedBlockedVersions []primitive.ProtocolVersion
		errExpected             bool
		errMsg                  string
	}

	tests := []test{
		{
			name:                    "Valid: no versions blocked",
			envVars:                 []envVar{{"ZDM_BLOCKED_PROTOCOL_VERSIONS", ""}},
			expectedBlockedVersions: []primitive.ProtocolVersion{},
			errExpected:             false,
			errMsg:                  "",
		},
		{
			name:                    "Valid: no versions blocked (with spaces)",
			envVars:                 []envVar{{"ZDM_BLOCKED_PROTOCOL_VERSIONS", ", ,  ,"}},
			expectedBlockedVersions: []primitive.ProtocolVersion{},
			errExpected:             false,
			errMsg:                  "",
		},
		{
			name:                    "Valid: v5 blocked",
			envVars:                 []envVar{{"ZDM_BLOCKED_PROTOCOL_VERSIONS", "v5"}},
			expectedBlockedVersions: []primitive.ProtocolVersion{primitive.ProtocolVersion5},
			errExpected:             false,
			errMsg:                  "",
		},
		{
			name:                    "Valid: v2, v3, v5 blocked",
			envVars:                 []envVar{{"ZDM_BLOCKED_PROTOCOL_VERSIONS", "v2,v3,v5"}},
			expectedBlockedVersions: []primitive.ProtocolVersion{0x2, 0x3, 0x5},
			errExpected:             false,
			errMsg:                  "",
		},
		{
			name:                    "Valid: 2, 3, 5 blocked",
			envVars:                 []envVar{{"ZDM_BLOCKED_PROTOCOL_VERSIONS", "2,3,5"}},
			expectedBlockedVersions: []primitive.ProtocolVersion{0x2, 0x3, 0x5},
			errExpected:             false,
			errMsg:                  "",
		},
		{
			name:                    "Valid: 2, V3, 5 blocked",
			envVars:                 []envVar{{"ZDM_BLOCKED_PROTOCOL_VERSIONS", "2,V3,5"}},
			expectedBlockedVersions: []primitive.ProtocolVersion{0x2, 0x3, 0x5},
			errExpected:             false,
			errMsg:                  "",
		},
		{
			name:    "Valid: 2,v2,3,v3,4,v4,5,v5,dsev1,dse_v1,dsev2,dse_v2 blocked",
			envVars: []envVar{{"ZDM_BLOCKED_PROTOCOL_VERSIONS", "2,v2,3,v3,4,v4,5,v5,DSEv1,DSE_V1,DSEV2,DSE_V2"}},
			expectedBlockedVersions: []primitive.ProtocolVersion{0x2, 0x2, 0x3, 0x3, 0x4, 0x4, 0x5, 0x5,
				primitive.ProtocolVersionDse1, primitive.ProtocolVersionDse1, primitive.ProtocolVersionDse2, primitive.ProtocolVersionDse2},
			errExpected: false,
			errMsg:      "",
		},
		{
			name:                    "Invalid: unrecognized v1",
			envVars:                 []envVar{{"ZDM_BLOCKED_PROTOCOL_VERSIONS", "v1"}},
			expectedBlockedVersions: nil,
			errExpected:             true,
			errMsg:                  "invalid value for ZDM_BLOCKED_PROTOCOL_VERSIONS (v1); possible values are: [2 3 4 5 DseV1 DseV2 Dse_V1 Dse_V2 v2 v3 v4 v5] (case insensitive)",
		},
		{
			name:                    "Invalid: unrecognized sdasd",
			envVars:                 []envVar{{"ZDM_BLOCKED_PROTOCOL_VERSIONS", "v2, sdasd"}},
			expectedBlockedVersions: nil,
			errExpected:             true,
			errMsg:                  "invalid value for ZDM_BLOCKED_PROTOCOL_VERSIONS (sdasd); possible values are: [2 3 4 5 DseV1 DseV2 Dse_V1 Dse_V2 v2 v3 v4 v5] (case insensitive)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearAllEnvVars()

			// set test-specific env vars
			for _, envVar := range tt.envVars {
				setEnvVar(envVar.vName, envVar.vValue)
			}

			// set other general env vars
			setOriginCredentialsEnvVars()
			setTargetCredentialsEnvVars()
			setOriginContactPointsAndPortEnvVars()
			setTargetContactPointsAndPortEnvVars()

			conf, err := New().LoadConfig("")
			if err != nil {
				if tt.errExpected {
					require.Equal(t, tt.errMsg, err.Error())
					return
				} else {
					t.Fatal("Unexpected configuration validation error, stopping test here")
				}
			}

			if conf == nil {
				t.Fatal("No configuration validation error was thrown but the parsed configuration is null, stopping test here")
			} else {
				blockedVersions, err := conf.ParseBlockedProtocolVersions()
				require.Nil(t, err) // validate should have failed before if err is expected
				require.Equal(t, tt.expectedBlockedVersions, blockedVersions)
			}
		})
	}

}
