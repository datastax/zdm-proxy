package config

import (
	"github.com/datastax/zdm-proxy/proxy/pkg/common"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConfig_ParseReadMode(t *testing.T) {

	type test struct {
		name             string
		envVars          []envVar
		expectedReadMode common.ReadMode
		errExpected      bool
		errMsg           string
	}

	tests := []test{
		{
			name:             "Valid: Async reads on secondary disabled",
			envVars:          []envVar{{"ZDM_READ_MODE", "PRIMARY_ONLY"}},
			expectedReadMode: common.ReadModePrimaryOnly,
			errExpected:      false,
			errMsg:           "",
		},
		{
			name:             "Valid: Async reads on secondary enabled",
			envVars:          []envVar{{"ZDM_READ_MODE", "DUAL_ASYNC_ON_SECONDARY"}},
			expectedReadMode: common.ReadModeDualAsyncOnSecondary,
			errExpected:      false,
			errMsg:           "",
		},
		{
			name:             "Invalid: Dual reads enabled but async reads on secondary disabled",
			envVars:          []envVar{{"ZDM_READ_MODE", "DUAL_SYNC"}},
			expectedReadMode: common.ReadModeUndefined,
			errExpected:      true,
			errMsg:           "invalid value for ZDM_READ_MODE; possible values are: PRIMARY_ONLY and DUAL_ASYNC_ON_SECONDARY",
		},
		{
			name:             "Valid: Read mode unset",
			envVars:          []envVar{},
			expectedReadMode: common.ReadModePrimaryOnly,
			errExpected:      false,
			errMsg:           "",
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

			conf, err := New().ParseEnvVars()
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
				actualReadMode, _ := conf.ParseReadMode()
				require.Equal(t, tt.expectedReadMode, actualReadMode)
			}
		})
	}

}
