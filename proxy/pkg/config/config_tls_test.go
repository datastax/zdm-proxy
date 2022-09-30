package config

import (
	"github.com/datastax/zdm-proxy/proxy/pkg/common"
	"github.com/stretchr/testify/require"
	"testing"
)

type tlsTest struct {
	name               string
	envVars            []envVar
	needsContactPoints bool
	tlsEnabled         bool
	serverCaPath       string
	clientCertPath     string
	clientKeyPath      string
	scbPath            string
	errExpected        bool
	errMsg             string
}

func TestOriginConfig_ClusterTlsConfig(t *testing.T) {

	tests := []tlsTest{
		{name: "No TLS at all",
			envVars:            []envVar{{}},
			needsContactPoints: true,
			tlsEnabled:         false,
			serverCaPath:       "",
			clientCertPath:     "",
			clientKeyPath:      "",
			scbPath:            "",
			errExpected:        false,
			errMsg:             "",
		},
		{name: "SCB only",
			envVars:            []envVar{{"ZDM_ORIGIN_SECURE_CONNECT_BUNDLE_PATH", "/path/to/origin/bundle"}},
			needsContactPoints: false,
			tlsEnabled:         true,
			serverCaPath:       "",
			clientCertPath:     "",
			clientKeyPath:      "",
			scbPath:            "/path/to/origin/bundle",
			errExpected:        false,
			errMsg:             "",
		},
		{name: "Custom TLS config only for one-way TLS",
			needsContactPoints: true,
			envVars: []envVar{
				{"ZDM_ORIGIN_TLS_SERVER_CA_PATH", "/path/to/origin/server/ca"},
			},
			tlsEnabled:     true,
			serverCaPath:   "/path/to/origin/server/ca",
			clientCertPath: "",
			clientKeyPath:  "",
			scbPath:        "",
			errExpected:    false,
			errMsg:         "",
		},
		{name: "Custom TLS config only for mutual TLS",
			needsContactPoints: true,
			envVars: []envVar{
				{"ZDM_ORIGIN_TLS_SERVER_CA_PATH", "/path/to/origin/server/ca"},
				{"ZDM_ORIGIN_TLS_CLIENT_CERT_PATH", "/path/to/origin/client/cert"},
				{"ZDM_ORIGIN_TLS_CLIENT_KEY_PATH", "/path/to/origin/client/key"},
			},
			tlsEnabled:     true,
			serverCaPath:   "/path/to/origin/server/ca",
			clientCertPath: "/path/to/origin/client/cert",
			clientKeyPath:  "/path/to/origin/client/key",
			scbPath:        "",
			errExpected:    false,
			errMsg:         "",
		},
		{name: "SCB and one-way TLS config",
			needsContactPoints: false,
			envVars: []envVar{
				{"ZDM_ORIGIN_SECURE_CONNECT_BUNDLE_PATH", "/path/to/origin/bundle"},
				{"ZDM_ORIGIN_TLS_SERVER_CA_PATH", "/path/to/origin/server/ca"},
			},
			tlsEnabled:     false,
			serverCaPath:   "",
			clientCertPath: "",
			clientKeyPath:  "",
			scbPath:        "",
			errExpected:    true,
			errMsg:         "Incorrect TLS configuration for Origin: Secure Connect Bundle and custom TLS parameters cannot be specified at the same time.",
		},
		{name: "SCB and mutual TLS config",
			needsContactPoints: false,
			envVars: []envVar{
				{"ZDM_ORIGIN_SECURE_CONNECT_BUNDLE_PATH", "/path/toorigin//bundle"},
				{"ZDM_ORIGIN_TLS_SERVER_CA_PATH", "/path/to/origin/server/ca"},
				{"ZDM_ORIGIN_TLS_CLIENT_CERT_PATH", "/path/to/origin/client/cert"},
				{"ZDM_ORIGIN_TLS_CLIENT_KEY_PATH", "/path/to/origin/client/key"},
			},
			tlsEnabled:     false,
			serverCaPath:   "",
			clientCertPath: "",
			clientKeyPath:  "",
			scbPath:        "",
			errExpected:    true,
			errMsg:         "Incorrect TLS configuration for Origin: Secure Connect Bundle and custom TLS parameters cannot be specified at the same time.",
		},

		{name: "Incomplete custom TLS config - mutual TLS with Client Cert path only",
			needsContactPoints: true,
			envVars: []envVar{
				{"ZDM_ORIGIN_TLS_CLIENT_CERT_PATH", "/path/to/origin/client/cert"},
			},
			tlsEnabled:     false,
			serverCaPath:   "",
			clientCertPath: "",
			clientKeyPath:  "",
			scbPath:        "",
			errExpected:    true,
			errMsg:         "incomplete TLS configuration for Origin: when using mutual TLS, please specify Server CA path, Client Cert path and Client Key path",
		},
		{name: "Incomplete custom TLS config - Client Key path only",
			needsContactPoints: true,
			envVars: []envVar{
				{"ZDM_ORIGIN_TLS_CLIENT_KEY_PATH", "/path/to/origin/client/key"},
			},
			tlsEnabled:     false,
			serverCaPath:   "",
			clientCertPath: "",
			clientKeyPath:  "",
			scbPath:        "",
			errExpected:    true,
			errMsg:         "incomplete TLS configuration for Origin: when using mutual TLS, please specify Server CA path, Client Cert path and Client Key path",
		},
		{name: "Incomplete custom TLS config - Server CA and Client Cert only",
			needsContactPoints: true,
			envVars: []envVar{
				{"ZDM_ORIGIN_TLS_SERVER_CA_PATH", "/path/to/origin/server/ca"},
				{"ZDM_ORIGIN_TLS_CLIENT_CERT_PATH", "/path/to/origin/client/cert"},
			},
			tlsEnabled:     false,
			serverCaPath:   "",
			clientCertPath: "",
			clientKeyPath:  "",
			scbPath:        "",
			errExpected:    true,
			errMsg:         "incomplete TLS configuration for Origin: when using mutual TLS, please specify Server CA path, Client Cert path and Client Key path",
		},
		{name: "Incomplete custom TLS config - Server CA and Client Key only",
			needsContactPoints: true,
			envVars: []envVar{
				{"ZDM_ORIGIN_TLS_SERVER_CA_PATH", "/path/to/origin/server/ca"},
				{"ZDM_ORIGIN_TLS_CLIENT_KEY_PATH", "/path/to/origin/client/key"},
			},
			tlsEnabled:     false,
			serverCaPath:   "",
			clientCertPath: "",
			clientKeyPath:  "",
			scbPath:        "",
			errExpected:    true,
			errMsg:         "incomplete TLS configuration for Origin: when using mutual TLS, please specify Server CA path, Client Cert path and Client Key path",
		},
		{name: "Incomplete custom TLS config - Client Cert and Client Key only",
			needsContactPoints: true,
			envVars: []envVar{
				{"ZDM_ORIGIN_TLS_CLIENT_CERT_PATH", "/path/to/origin/client/cert"},
				{"ZDM_ORIGIN_TLS_CLIENT_KEY_PATH", "/path/to/origin/client/key"},
			},
			tlsEnabled:     false,
			serverCaPath:   "",
			clientCertPath: "",
			clientKeyPath:  "",
			scbPath:        "",
			errExpected:    true,
			errMsg:         "incomplete TLS configuration for Origin: when using mutual TLS, please specify Server CA path, Client Cert path and Client Key path",
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
			if tt.needsContactPoints {
				setOriginContactPointsAndPortEnvVars()
			}
			setTargetContactPointsAndPortEnvVars()

			var tlsConf *common.ClusterTlsConfig
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

				tlsConf, err = conf.ParseOriginTlsConfig(true)

				require.Equal(t, tt.tlsEnabled, tlsConf.TlsEnabled)
				require.Equal(t, tt.serverCaPath, tlsConf.ServerCaPath)
				require.Equal(t, tt.clientCertPath, tlsConf.ClientCertPath)
				require.Equal(t, tt.clientKeyPath, tlsConf.ClientKeyPath)
				require.Equal(t, tt.scbPath, tlsConf.SecureConnectBundlePath)
			}
		})
	}
}

func TestTargetConfig_ClusterTlsConfig(t *testing.T) {

	tests := []tlsTest{
		{name: "No TLS at all",
			envVars:            []envVar{{}},
			needsContactPoints: true,
			tlsEnabled:         false,
			serverCaPath:       "",
			clientCertPath:     "",
			clientKeyPath:      "",
			scbPath:            "",
			errExpected:        false,
			errMsg:             "",
		},
		{name: "SCB only",
			envVars:            []envVar{{"ZDM_TARGET_SECURE_CONNECT_BUNDLE_PATH", "/path/to/target/bundle"}},
			needsContactPoints: false,
			tlsEnabled:         true,
			serverCaPath:       "",
			clientCertPath:     "",
			clientKeyPath:      "",
			scbPath:            "/path/to/target/bundle",
			errExpected:        false,
			errMsg:             "",
		},
		{name: "Custom TLS config only for one-way TLS",
			needsContactPoints: true,
			envVars: []envVar{
				{"ZDM_TARGET_TLS_SERVER_CA_PATH", "/path/to/target/server/ca"},
			},
			tlsEnabled:     true,
			serverCaPath:   "/path/to/target/server/ca",
			clientCertPath: "",
			clientKeyPath:  "",
			scbPath:        "",
			errExpected:    false,
			errMsg:         "",
		},
		{name: "Custom TLS config only for mutual TLS",
			needsContactPoints: true,
			envVars: []envVar{
				{"ZDM_TARGET_TLS_SERVER_CA_PATH", "/path/to/target/server/ca"},
				{"ZDM_TARGET_TLS_CLIENT_CERT_PATH", "/path/to/target/client/cert"},
				{"ZDM_TARGET_TLS_CLIENT_KEY_PATH", "/path/to/target/client/key"},
			},
			tlsEnabled:     true,
			serverCaPath:   "/path/to/target/server/ca",
			clientCertPath: "/path/to/target/client/cert",
			clientKeyPath:  "/path/to/target/client/key",
			scbPath:        "",
			errExpected:    false,
			errMsg:         "",
		},
		{name: "SCB and one-way TLS config",
			needsContactPoints: false,
			envVars: []envVar{
				{"ZDM_TARGET_SECURE_CONNECT_BUNDLE_PATH", "/path/to/target/bundle"},
				{"ZDM_TARGET_TLS_SERVER_CA_PATH", "/path/to/target/server/ca"},
			},
			tlsEnabled:     false,
			serverCaPath:   "",
			clientCertPath: "",
			clientKeyPath:  "",
			scbPath:        "",
			errExpected:    true,
			errMsg:         "Incorrect TLS configuration for Target: Secure Connect Bundle and custom TLS parameters cannot be specified at the same time.",
		},
		{name: "SCB and mutual TLS config",
			needsContactPoints: false,
			envVars: []envVar{
				{"ZDM_TARGET_SECURE_CONNECT_BUNDLE_PATH", "/path/to/target/bundle"},
				{"ZDM_TARGET_TLS_SERVER_CA_PATH", "/path/to/target/server/ca"},
				{"ZDM_TARGET_TLS_CLIENT_CERT_PATH", "/path/to/target/client/cert"},
				{"ZDM_TARGET_TLS_CLIENT_KEY_PATH", "/path/to/target/client/key"},
			},
			tlsEnabled:     false,
			serverCaPath:   "",
			clientCertPath: "",
			clientKeyPath:  "",
			scbPath:        "",
			errExpected:    true,
			errMsg:         "Incorrect TLS configuration for Target: Secure Connect Bundle and custom TLS parameters cannot be specified at the same time.",
		},

		{name: "Incomplete custom TLS config - mutual TLS with Client Cert path only",
			needsContactPoints: true,
			envVars: []envVar{
				{"ZDM_TARGET_TLS_CLIENT_CERT_PATH", "/path/to/target/client/cert"},
			},
			tlsEnabled:     false,
			serverCaPath:   "",
			clientCertPath: "",
			clientKeyPath:  "",
			scbPath:        "",
			errExpected:    true,
			errMsg:         "incomplete TLS configuration for Target: when using mutual TLS, please specify Server CA path, Client Cert path and Client Key path",
		},
		{name: "Incomplete custom TLS config - Client Key path only",
			needsContactPoints: true,
			envVars: []envVar{
				{"ZDM_TARGET_TLS_CLIENT_KEY_PATH", "/path/to/target/client/key"},
			},
			tlsEnabled:     false,
			serverCaPath:   "",
			clientCertPath: "",
			clientKeyPath:  "",
			scbPath:        "",
			errExpected:    true,
			errMsg:         "incomplete TLS configuration for Target: when using mutual TLS, please specify Server CA path, Client Cert path and Client Key path",
		},
		{name: "Incomplete custom TLS config - Server CA and Client Cert only",
			needsContactPoints: true,
			envVars: []envVar{
				{"ZDM_TARGET_TLS_SERVER_CA_PATH", "/path/to/target/server/ca"},
				{"ZDM_TARGET_TLS_CLIENT_CERT_PATH", "/path/to/target/client/cert"},
			},
			tlsEnabled:     false,
			serverCaPath:   "",
			clientCertPath: "",
			clientKeyPath:  "",
			scbPath:        "",
			errExpected:    true,
			errMsg:         "incomplete TLS configuration for Target: when using mutual TLS, please specify Server CA path, Client Cert path and Client Key path",
		},
		{name: "Incomplete custom TLS config - Server CA and Client Key only",
			needsContactPoints: true,
			envVars: []envVar{
				{"ZDM_TARGET_TLS_SERVER_CA_PATH", "/path/to/target/server/ca"},
				{"ZDM_TARGET_TLS_CLIENT_KEY_PATH", "/path/to/target/client/key"},
			},
			tlsEnabled:     false,
			serverCaPath:   "",
			clientCertPath: "",
			clientKeyPath:  "",
			scbPath:        "",
			errExpected:    true,
			errMsg:         "incomplete TLS configuration for Target: when using mutual TLS, please specify Server CA path, Client Cert path and Client Key path",
		},
		{name: "Incomplete custom TLS config - Client Cert and Client Key only",
			needsContactPoints: true,
			envVars: []envVar{
				{"ZDM_TARGET_TLS_CLIENT_CERT_PATH", "/path/to/target/client/cert"},
				{"ZDM_TARGET_TLS_CLIENT_KEY_PATH", "/path/to/target/client/key"},
			},
			tlsEnabled:     false,
			serverCaPath:   "",
			clientCertPath: "",
			clientKeyPath:  "",
			scbPath:        "",
			errExpected:    true,
			errMsg:         "incomplete TLS configuration for Target: when using mutual TLS, please specify Server CA path, Client Cert path and Client Key path",
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
			if tt.needsContactPoints {
				setTargetContactPointsAndPortEnvVars()
			}
			setOriginContactPointsAndPortEnvVars()

			var tlsConf *common.ClusterTlsConfig
			conf, err := New().ParseEnvVars()
			if err != nil {
				if tt.errExpected {
					// Expected configuration validation error
					require.Equal(t, tt.errMsg, err.Error())
					return
				} else {
					t.Fatal("Unexpected configuration validation error, stopping test here")
				}
			}

			if conf == nil {
				t.Fatal("No configuration validation error was thrown but the parsed configuration is null, stopping test here")
			} else {

				tlsConf, err = conf.ParseTargetTlsConfig(true)

				require.Equal(t, tt.tlsEnabled, tlsConf.TlsEnabled)
				require.Equal(t, tt.serverCaPath, tlsConf.ServerCaPath)
				require.Equal(t, tt.clientCertPath, tlsConf.ClientCertPath)
				require.Equal(t, tt.clientKeyPath, tlsConf.ClientKeyPath)
				require.Equal(t, tt.scbPath, tlsConf.SecureConnectBundlePath)
			}
		})
	}
}
