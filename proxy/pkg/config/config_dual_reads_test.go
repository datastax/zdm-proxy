package config

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConfig_ParseReadMode(t *testing.T) {

	type test struct {
		name             string
		envVars          []envVar
		expectedReadMode ReadMode
		errExpected      bool
		errMsg           string
	}

	tests := []test{
		{
			name:           "Valid: Dual reads disabled, async reads on secondary disabled",
			envVars:        []envVar{
				{"DUAL_READS_ENABLED", "false"},
				{"ASYNC_READS_ON_SECONDARY", "false"},
			},
			expectedReadMode: ReadModePrimaryOnly,
			errExpected:      false,
			errMsg:           "",
		},
		{
			name:           "Valid: Dual reads enabled, async reads on secondary enabled",
			envVars:        []envVar{
				{"DUAL_READS_ENABLED", "true"},
				{"ASYNC_READS_ON_SECONDARY", "true"},
			},
			expectedReadMode: ReadModeSecondaryAsync,
			errExpected:      false,
			errMsg:           "",
		},
		{
			name:           "Invalid: Dual reads enabled but async reads on secondary disabled",
			envVars:        []envVar{
				{"DUAL_READS_ENABLED", "true"},
				{"ASYNC_READS_ON_SECONDARY", "false"},
			},
			expectedReadMode: ReadModeUndefined,
			errExpected:      true,
			errMsg:           "combination of DUAL_READS_ENABLED (true) and ASYNC_READS_ON_SECONDARY (false) not yet implemented",
		},
		{
			name:           "Invalid: Dual reads disabled but async reads on secondary enabled",
			envVars:        []envVar{
				{"DUAL_READS_ENABLED", "false"},
				{"ASYNC_READS_ON_SECONDARY", "true"},
			},
			expectedReadMode: ReadModeUndefined,
			errExpected:      true,
			errMsg:           "invalid combination of DUAL_READS_ENABLED (false) and ASYNC_READS_ON_SECONDARY (true)",
		},
		{
			name:           "Valid: Dual reads unset, async reads on secondary unset",
			envVars:        []envVar{},
			expectedReadMode: ReadModePrimaryOnly,
			errExpected:      false,
			errMsg:           "",
		},
		{
			name:           "Invalid: Dual reads enabled but async reads on secondary unset",
			envVars:        []envVar{
				{"DUAL_READS_ENABLED", "true"},
			},
			expectedReadMode: ReadModeUndefined,
			errExpected:      true,
			errMsg:           "combination of DUAL_READS_ENABLED (true) and ASYNC_READS_ON_SECONDARY (false) not yet implemented",
		},
		{
			name:           "Invalid: Dual reads unset but async reads on secondary enabled",
			envVars:        []envVar{
				{"ASYNC_READS_ON_SECONDARY", "true"},
			},
			expectedReadMode: ReadModeUndefined,
			errExpected:      true,
			errMsg:           "invalid combination of DUAL_READS_ENABLED (false) and ASYNC_READS_ON_SECONDARY (true)",
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
