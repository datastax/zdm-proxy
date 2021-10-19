package integration_tests

import (
	"context"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/riptano/cloud-gate/integration-tests/setup"
	"github.com/riptano/cloud-gate/proxy/pkg/health"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

func TestAuth(t *testing.T) {
	originUsername := "origin_username"
	originPassword := "originPassword"
	targetUsername := "target_username"
	targetPassword := "targetPassword"

	true := true
	false := false

	tests := []struct {
		name                       string
		originUsername             string
		originPassword             string
		targetUsername             string
		targetPassword             string
		proxyTargetUsername        string
		proxyTargetPassword        string
		proxyOriginUsername        string
		proxyOriginPassword        string
		clientUsername             string
		clientPassword             string
		success                    bool
		initError                  bool
		forwardClientCredsToOrigin *bool
	}{
		{
			name:                       "ClientTargetCreds_AuthOnBoth_ProxyHasBothCreds_DefaultForwardClientCreds",
			originUsername:             originUsername,
			originPassword:             originPassword,
			targetUsername:             targetUsername,
			targetPassword:             targetPassword,
			proxyTargetUsername:        targetUsername,
			proxyTargetPassword:        targetPassword,
			proxyOriginUsername:        originUsername,
			proxyOriginPassword:        originPassword,
			clientUsername:             targetUsername,
			clientPassword:             targetPassword,
			success:                    true,
			initError:                  false,
			forwardClientCredsToOrigin: nil,
		},
		{
			name:                       "ClientOriginCreds_AuthOnBoth_ProxyHasBothCreds_DefaultForwardClientCreds",
			originUsername:             originUsername,
			originPassword:             originPassword,
			targetUsername:             targetUsername,
			targetPassword:             targetPassword,
			proxyTargetUsername:        targetUsername,
			proxyTargetPassword:        targetPassword,
			proxyOriginUsername:        originUsername,
			proxyOriginPassword:        originPassword,
			clientUsername:             originUsername,
			clientPassword:             originPassword,
			success:                    false,
			initError:                  false,
			forwardClientCredsToOrigin: nil,
		},

		{
			name:                       "ClientTargetCreds_AuthOnBoth_ProxyHasBothCreds_ForwardClientCredsToTarget",
			originUsername:             originUsername,
			originPassword:             originPassword,
			targetUsername:             targetUsername,
			targetPassword:             targetPassword,
			proxyTargetUsername:        targetUsername,
			proxyTargetPassword:        targetPassword,
			proxyOriginUsername:        originUsername,
			proxyOriginPassword:        originPassword,
			clientUsername:             targetUsername,
			clientPassword:             targetPassword,
			success:                    true,
			initError:                  false,
			forwardClientCredsToOrigin: &false,
		},
		{
			name:                       "ClientOriginCreds_AuthOnBoth_ProxyHasBothCreds_ForwardClientCredsToTarget",
			originUsername:             originUsername,
			originPassword:             originPassword,
			targetUsername:             targetUsername,
			targetPassword:             targetPassword,
			proxyTargetUsername:        targetUsername,
			proxyTargetPassword:        targetPassword,
			proxyOriginUsername:        originUsername,
			proxyOriginPassword:        originPassword,
			clientUsername:             originUsername,
			clientPassword:             originPassword,
			success:                    false,
			initError:                  false,
			forwardClientCredsToOrigin: &false,
		},
		{
			name:                       "ClientTargetCreds_AuthOnBoth_ProxyHasBothCreds_ForwardClientCredsToOrigin",
			originUsername:             originUsername,
			originPassword:             originPassword,
			targetUsername:             targetUsername,
			targetPassword:             targetPassword,
			proxyTargetUsername:        targetUsername,
			proxyTargetPassword:        targetPassword,
			proxyOriginUsername:        originUsername,
			proxyOriginPassword:        originPassword,
			clientUsername:             targetUsername,
			clientPassword:             targetPassword,
			success:                    false,
			initError:                  false,
			forwardClientCredsToOrigin: &true,
		},
		{
			name:                       "ClientOriginCreds_AuthOnBoth_ProxyHasBothCreds_ForwardClientCredsToOrigin",
			originUsername:             originUsername,
			originPassword:             originPassword,
			targetUsername:             targetUsername,
			targetPassword:             targetPassword,
			proxyTargetUsername:        targetUsername,
			proxyTargetPassword:        targetPassword,
			proxyOriginUsername:        originUsername,
			proxyOriginPassword:        originPassword,
			clientUsername:             originUsername,
			clientPassword:             originPassword,
			success:                    true,
			initError:                  false,
			forwardClientCredsToOrigin: &true,
		},
		{
			name:                       "ClientNoCreds_AuthOnBoth_ProxyHasBothCreds_ForwardClientCredsToTarget",
			originUsername:             originUsername,
			originPassword:             originPassword,
			targetUsername:             targetUsername,
			targetPassword:             targetPassword,
			proxyTargetUsername:        targetUsername,
			proxyTargetPassword:        targetPassword,
			proxyOriginUsername:        originUsername,
			proxyOriginPassword:        originPassword,
			clientUsername:             "",
			clientPassword:             "",
			success:                    false,
			initError:                  false,
			forwardClientCredsToOrigin: &false,
		},
		{
			name:                       "ClientNoCreds_AuthOnBoth_ProxyHasBothCreds_ForwardClientCredsToOrigin",
			originUsername:             originUsername,
			originPassword:             originPassword,
			targetUsername:             targetUsername,
			targetPassword:             targetPassword,
			proxyTargetUsername:        targetUsername,
			proxyTargetPassword:        targetPassword,
			proxyOriginUsername:        originUsername,
			proxyOriginPassword:        originPassword,
			clientUsername:             "",
			clientPassword:             "",
			success:                    false,
			initError:                  false,
			forwardClientCredsToOrigin: &true,
		},


		{
			name:                       "ClientTargetCreds_NoAuthOnOrigin_ProxyHasTargetCreds_ForwardClientCredsToTarget",
			originUsername:             "",
			originPassword:             "",
			targetUsername:             targetUsername,
			targetPassword:             targetPassword,
			proxyTargetUsername:        targetUsername,
			proxyTargetPassword:        targetPassword,
			proxyOriginUsername:        "",
			proxyOriginPassword:        "",
			clientUsername:             targetUsername,
			clientPassword:             targetPassword,
			success:                    true,
			initError:                  false,
			forwardClientCredsToOrigin: &false,
		},
		{
			name:                       "ClientOriginCreds_NoAuthOnOrigin_ProxyHasTargetCreds_ForwardClientCredsToTarget",
			originUsername:             "",
			originPassword:             "",
			targetUsername:             targetUsername,
			targetPassword:             targetPassword,
			proxyTargetUsername:        targetUsername,
			proxyTargetPassword:        targetPassword,
			proxyOriginUsername:        "",
			proxyOriginPassword:        "",
			clientUsername:             originUsername,
			clientPassword:             originPassword,
			success:                    false,
			initError:                  false,
			forwardClientCredsToOrigin: &false,
		},
		{
			name:                       "ClientTargetCreds_NoAuthOnOrigin_ProxyHasTargetCreds_ForwardClientCredsToOrigin",
			originUsername:             "",
			originPassword:             "",
			targetUsername:             targetUsername,
			targetPassword:             targetPassword,
			proxyTargetUsername:        targetUsername,
			proxyTargetPassword:        targetPassword,
			proxyOriginUsername:        "",
			proxyOriginPassword:        "",
			clientUsername:             targetUsername,
			clientPassword:             targetPassword,
			success:                    true,
			initError:                  false,
			forwardClientCredsToOrigin: &true, // proxy forwards to target anyway
		},
		{
			name:                       "ClientOriginCreds_NoAuthOnOrigin_ProxyHasTargetCreds_ForwardClientCredsToOrigin",
			originUsername:             "",
			originPassword:             "",
			targetUsername:             targetUsername,
			targetPassword:             targetPassword,
			proxyTargetUsername:        targetUsername,
			proxyTargetPassword:        targetPassword,
			proxyOriginUsername:        "",
			proxyOriginPassword:        "",
			clientUsername:             originUsername,
			clientPassword:             originPassword,
			success:                    false,
			initError:                  false,
			forwardClientCredsToOrigin: &true, // proxy forwards to target anyway
		},
		{
			name:                       "ClientNoCreds_NoAuthOnOrigin_ProxyHasTargetCreds_ForwardClientCredsToTarget",
			originUsername:             "",
			originPassword:             "",
			targetUsername:             targetUsername,
			targetPassword:             targetPassword,
			proxyTargetUsername:        targetUsername,
			proxyTargetPassword:        targetPassword,
			proxyOriginUsername:        "",
			proxyOriginPassword:        "",
			clientUsername:             "",
			clientPassword:             "",
			success:                    false,
			initError:                  false,
			forwardClientCredsToOrigin: &false,
		},
		{
			name:                       "ClientNoCreds_NoAuthOnOrigin_ProxyHasTargetCreds_ForwardClientCredsToOrigin",
			originUsername:             "",
			originPassword:             "",
			targetUsername:             targetUsername,
			targetPassword:             targetPassword,
			proxyTargetUsername:        targetUsername,
			proxyTargetPassword:        targetPassword,
			proxyOriginUsername:        "",
			proxyOriginPassword:        "",
			clientUsername:             "",
			clientPassword:             "",
			success:                    false,
			initError:                  false,
			forwardClientCredsToOrigin: &true,
		},



		{
			name:                       "ClientTargetCreds_NoAuthOnTarget_ProxyHasOriginCreds_ForwardClientCredsToTarget",
			originUsername:             originUsername,
			originPassword:             originPassword,
			targetUsername:             "",
			targetPassword:             "",
			proxyTargetUsername:        "",
			proxyTargetPassword:        "",
			proxyOriginUsername:        originUsername,
			proxyOriginPassword:        originPassword,
			clientUsername:             targetUsername,
			clientPassword:             targetPassword,
			success:                    false,
			initError:                  false,
			forwardClientCredsToOrigin: &false, // proxy forwards to origin anyway
		},
		{
			name:                       "ClientOriginCreds_NoAuthOnTarget_ProxyHasOriginCreds_ForwardClientCredsToTarget",
			originUsername:             originUsername,
			originPassword:             originPassword,
			targetUsername:             "",
			targetPassword:             "",
			proxyTargetUsername:        "",
			proxyTargetPassword:        "",
			proxyOriginUsername:        originUsername,
			proxyOriginPassword:        originPassword,
			clientUsername:             originUsername,
			clientPassword:             originPassword,
			success:                    true,
			initError:                  false,
			forwardClientCredsToOrigin: &false, // proxy forwards to origin anyway
		},
		{
			name:                       "ClientTargetCreds_NoAuthOnTarget_ProxyHasOriginCreds_ForwardClientCredsToOrigin",
			originUsername:             originUsername,
			originPassword:             originPassword,
			targetUsername:             "",
			targetPassword:             "",
			proxyTargetUsername:        "",
			proxyTargetPassword:        "",
			proxyOriginUsername:        originUsername,
			proxyOriginPassword:        originPassword,
			clientUsername:             targetUsername,
			clientPassword:             targetPassword,
			success:                    false,
			initError:                  false,
			forwardClientCredsToOrigin: &true,
		},
		{
			name:                       "ClientOriginCreds_NoAuthOnTarget_ProxyHasOriginCreds_ForwardClientCredsToOrigin",
			originUsername:             originUsername,
			originPassword:             originPassword,
			targetUsername:             "",
			targetPassword:             "",
			proxyTargetUsername:        "",
			proxyTargetPassword:        "",
			proxyOriginUsername:        originUsername,
			proxyOriginPassword:        originPassword,
			clientUsername:             originUsername,
			clientPassword:             originPassword,
			success:                    true,
			initError:                  false,
			forwardClientCredsToOrigin: &true,
		},
		{
			name:                       "ClientNoCreds_NoAuthOnTarget_ProxyHasOriginCreds_ForwardClientCredsToTarget",
			originUsername:             originUsername,
			originPassword:             originPassword,
			targetUsername:             "",
			targetPassword:             "",
			proxyTargetUsername:        "",
			proxyTargetPassword:        "",
			proxyOriginUsername:        originUsername,
			proxyOriginPassword:        originPassword,
			clientUsername:             "",
			clientPassword:             "",
			success:                    false,
			initError:                  false,
			forwardClientCredsToOrigin: &false,
		},
		{
			name:                       "ClientNoCreds_NoAuthOnTarget_ProxyHasOriginCreds_ForwardClientCredsToOrigin",
			originUsername:             originUsername,
			originPassword:             originPassword,
			targetUsername:             "",
			targetPassword:             "",
			proxyTargetUsername:        "",
			proxyTargetPassword:        "",
			proxyOriginUsername:        originUsername,
			proxyOriginPassword:        originPassword,
			clientUsername:             "",
			clientPassword:             "",
			success:                    false,
			initError:                  false,
			forwardClientCredsToOrigin: &true,
		},



		{
			name:                       "ClientTargetCreds_NoAuthOnEither_ProxyHasNoCreds_ForwardClientCredsToTarget",
			originUsername:             "",
			originPassword:             "",
			targetUsername:             "",
			targetPassword:             "",
			proxyTargetUsername:        "",
			proxyTargetPassword:        "",
			proxyOriginUsername:        "",
			proxyOriginPassword:        "",
			clientUsername:             targetUsername,
			clientPassword:             targetPassword,
			success:                    true,
			initError:                  false,
			forwardClientCredsToOrigin: &false, // proxy forwards to origin anyway
		},
		{
			name:                       "ClientOriginCreds_NoAuthOnEither_ProxyHasNoCreds_ForwardClientCredsToTarget",
			originUsername:             "",
			originPassword:             "",
			targetUsername:             "",
			targetPassword:             "",
			proxyTargetUsername:        "",
			proxyTargetPassword:        "",
			proxyOriginUsername:        "",
			proxyOriginPassword:        "",
			clientUsername:             originUsername,
			clientPassword:             originPassword,
			success:                    true,
			initError:                  false,
			forwardClientCredsToOrigin: &false, // proxy forwards to origin anyway
		},
		{
			name:                       "ClientTargetCreds_NoAuthOnEither_ProxyHasNoCreds_ForwardClientCredsToOrigin",
			originUsername:             "",
			originPassword:             "",
			targetUsername:             "",
			targetPassword:             "",
			proxyTargetUsername:        "",
			proxyTargetPassword:        "",
			proxyOriginUsername:        "",
			proxyOriginPassword:        "",
			clientUsername:             targetUsername,
			clientPassword:             targetPassword,
			success:                    true,
			initError:                  false,
			forwardClientCredsToOrigin: &true,
		},
		{
			name:                       "ClientOriginCreds_NoAuthOnEither_ProxyHasNoCreds_ForwardClientCredsToOrigin",
			originUsername:             "",
			originPassword:             "",
			targetUsername:             "",
			targetPassword:             "",
			proxyTargetUsername:        "",
			proxyTargetPassword:        "",
			proxyOriginUsername:        "",
			proxyOriginPassword:        "",
			clientUsername:             originUsername,
			clientPassword:             originPassword,
			success:                    true,
			initError:                  false,
			forwardClientCredsToOrigin: &true,
		},
		{
			name:                       "ClientNoCreds_NoAuthOnEither_ProxyHasNoCreds_ForwardClientCredsToTarget",
			originUsername:             "",
			originPassword:             "",
			targetUsername:             "",
			targetPassword:             "",
			proxyTargetUsername:        "",
			proxyTargetPassword:        "",
			proxyOriginUsername:        "",
			proxyOriginPassword:        "",
			clientUsername:             "",
			clientPassword:             "",
			success:                    true,
			initError:                  false,
			forwardClientCredsToOrigin: &false,
		},
		{
			name:                       "ClientNoCreds_NoAuthOnEither_ProxyHasNoCreds_ForwardClientCredsToOrigin",
			originUsername:             "",
			originPassword:             "",
			targetUsername:             "",
			targetPassword:             "",
			proxyTargetUsername:        "",
			proxyTargetPassword:        "",
			proxyOriginUsername:        "",
			proxyOriginPassword:        "",
			clientUsername:             "",
			clientPassword:             "",
			success:                    true,
			initError:                  false,
			forwardClientCredsToOrigin: &true,
		},


		{
			name:                       "InvalidClientCredentials_ForwardClientCredsToTarget",
			originUsername:             originUsername,
			originPassword:             originPassword,
			targetUsername:             targetUsername,
			targetPassword:             targetPassword,
			proxyTargetUsername:        targetUsername,
			proxyTargetPassword:        targetPassword,
			proxyOriginUsername:        originUsername,
			proxyOriginPassword:        originPassword,
			clientUsername:             "invalidTargetUsername",
			clientPassword:             "invalidTargetPassword",
			success:                    false,
			initError:                  false,
			forwardClientCredsToOrigin: &false,
		},
		{
			name:                       "InvalidClientCredentials_ForwardClientCredsToOrigin",
			originUsername:             originUsername,
			originPassword:             originPassword,
			targetUsername:             targetUsername,
			targetPassword:             targetPassword,
			proxyTargetUsername:        targetUsername,
			proxyTargetPassword:        targetPassword,
			proxyOriginUsername:        originUsername,
			proxyOriginPassword:        originPassword,
			clientUsername:             "invalidTargetUsername",
			clientPassword:             "invalidTargetPassword",
			success:                    false,
			initError:                  false,
			forwardClientCredsToOrigin: &true,
		},
	}

	originAddress := "127.0.1.1"
	targetAddress := "127.0.1.2"
	version := primitive.ProtocolVersion4

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			serverConf := setup.NewTestConfig(originAddress, targetAddress)
			serverConf.TargetCassandraUsername = tt.targetUsername
			serverConf.TargetCassandraPassword = tt.targetPassword
			serverConf.OriginCassandraUsername = tt.originUsername
			serverConf.OriginCassandraPassword = tt.originPassword

			testSetup, err := setup.NewCqlServerTestSetup(serverConf, true, false, false)
			require.Nil(t, err)
			defer testSetup.Cleanup()

			proxyConf := setup.NewTestConfig(originAddress, targetAddress)
			proxyConf.TargetCassandraUsername = tt.proxyTargetUsername
			proxyConf.TargetCassandraPassword = tt.proxyTargetPassword
			proxyConf.OriginCassandraUsername = tt.proxyOriginUsername
			proxyConf.OriginCassandraPassword = tt.proxyOriginPassword
			if tt.forwardClientCredsToOrigin != nil {
				proxyConf.ForwardClientCredentialsToOrigin = *tt.forwardClientCredsToOrigin
			}

			proxy, err := setup.NewProxyInstanceWithConfig(proxyConf)
			if proxy != nil {
				defer proxy.Shutdown()
			}

			if tt.initError {
				require.NotNil(t, err)
				require.Nil(t, proxy)
				return
			}

			require.Nil(t, err)

			testClient := client.NewCqlClient(
				fmt.Sprintf("%s:%d", proxyConf.ProxyQueryAddress, proxyConf.ProxyQueryPort),
				&client.AuthCredentials{
					Username: tt.clientUsername,
					Password: tt.clientPassword,
				})
			cqlConn, err := testClient.Connect(context.Background())
			require.Nil(t, err, "client connection failed: %v", err)
			defer cqlConn.Close()

			err = cqlConn.InitiateHandshake(primitive.ProtocolVersion4, 0)

			if !tt.success {
				require.NotNil(t, err, "expected failure in handshake")
				require.True(t, strings.Contains(err.Error(), (&message.AuthenticationError{ErrorMessage: "invalid credentials"}).String()), err.Error())
			} else {
				require.Nil(t, err, "handshake failed: %v", err)

				query := &message.Query{
					Query:   "SELECT * FROM system.peers",
					Options: &message.QueryOptions{Consistency: primitive.ConsistencyLevelOne},
				}

				response, err := cqlConn.SendAndReceive(frame.NewFrame(version, 0, query))
				require.Nil(t, err, "query request send failed: %s", err)

				require.Equal(t, primitive.OpCodeResult, response.Body.Message.GetOpCode(), response.Body.Message)
			}
		})
	}
}

func TestProxyStartupAndHealthCheckWithAuth(t *testing.T) {
	originUsername := "origin_username"
	originPassword := "originPassword"
	targetUsername := "target_username"
	targetPassword := "targetPassword"
	tests := []struct {
		name                string
		originUsername      string
		originPassword      string
		targetUsername      string
		targetPassword      string
		proxyTargetUsername string
		proxyTargetPassword string
		proxyOriginUsername string
		proxyOriginPassword string
		success             bool
	}{

		{
			name:                "NoAuthOnEither_ProxyHasBothCreds",
			originUsername:      "",
			originPassword:      "",
			targetUsername:      "",
			targetPassword:      "",
			proxyTargetUsername: targetUsername,
			proxyTargetPassword: targetPassword,
			proxyOriginUsername: originUsername,
			proxyOriginPassword: originPassword,
			success:             true,
		},
		{
			name:                "NoAuthOnTarget_ProxyHasBothCreds",
			originUsername:      originUsername,
			originPassword:      originPassword,
			targetUsername:      "",
			targetPassword:      "",
			proxyTargetUsername: targetUsername,
			proxyTargetPassword: targetPassword,
			proxyOriginUsername: originUsername,
			proxyOriginPassword: originPassword,
			success:             true,
		},
		{
			name:                "NoAuthOnOrigin_ProxyHasBothCreds",
			originUsername:      "",
			originPassword:      "",
			targetUsername:      targetUsername,
			targetPassword:      targetPassword,
			proxyTargetUsername: targetUsername,
			proxyTargetPassword: targetPassword,
			proxyOriginUsername: originUsername,
			proxyOriginPassword: originPassword,
			success:             true,
		},
		{
			name:                "AuthOnBoth_ProxyHasBothCreds",
			originUsername:      originUsername,
			originPassword:      originPassword,
			targetUsername:      targetUsername,
			targetPassword:      targetPassword,
			proxyTargetUsername: targetUsername,
			proxyTargetPassword: targetPassword,
			proxyOriginUsername: originUsername,
			proxyOriginPassword: originPassword,
			success:             true,
		},

		{
			name:                "NoAuthOnEither_ProxyHasTargetCreds",
			originUsername:      "",
			originPassword:      "",
			targetUsername:      "",
			targetPassword:      "",
			proxyTargetUsername: targetUsername,
			proxyTargetPassword: targetPassword,
			proxyOriginUsername: "",
			proxyOriginPassword: "",
			success:             true,
		},
		{
			name:                "NoAuthOnTarget_ProxyHasTargetCreds",
			originUsername:      originUsername,
			originPassword:      originPassword,
			targetUsername:      "",
			targetPassword:      "",
			proxyTargetUsername: targetUsername,
			proxyTargetPassword: targetPassword,
			proxyOriginUsername: "",
			proxyOriginPassword: "",
			success:             false,
		},
		{
			name:                "NoAuthOnOrigin_ProxyHasTargetCreds",
			originUsername:      "",
			originPassword:      "",
			targetUsername:      targetUsername,
			targetPassword:      targetPassword,
			proxyTargetUsername: targetUsername,
			proxyTargetPassword: targetPassword,
			proxyOriginUsername: "",
			proxyOriginPassword: "",
			success:             true,
		},
		{
			name:                "AuthOnBoth_ProxyHasTargetCreds",
			originUsername:      originUsername,
			originPassword:      originPassword,
			targetUsername:      targetUsername,
			targetPassword:      targetPassword,
			proxyTargetUsername: targetUsername,
			proxyTargetPassword: targetPassword,
			proxyOriginUsername: "",
			proxyOriginPassword: "",
			success:             false,
		},


		{
			name:                "NoAuthOnEither_ProxyHasOriginCreds",
			originUsername:      "",
			originPassword:      "",
			targetUsername:      "",
			targetPassword:      "",
			proxyTargetUsername: "",
			proxyTargetPassword: "",
			proxyOriginUsername: originUsername,
			proxyOriginPassword: originPassword,
			success:             true,
		},
		{
			name:                "NoAuthOnTarget_ProxyHasOriginCreds",
			originUsername:      originUsername,
			originPassword:      originPassword,
			targetUsername:      "",
			targetPassword:      "",
			proxyTargetUsername: "",
			proxyTargetPassword: "",
			proxyOriginUsername: originUsername,
			proxyOriginPassword: originPassword,
			success:             true,
		},
		{
			name:                "NoAuthOnOrigin_ProxyHasOriginCreds",
			originUsername:      "",
			originPassword:      "",
			targetUsername:      targetUsername,
			targetPassword:      targetPassword,
			proxyTargetUsername: "",
			proxyTargetPassword: "",
			proxyOriginUsername: originUsername,
			proxyOriginPassword: originPassword,
			success:             false,
		},
		{
			name:                "AuthOnBoth_ProxyHasOriginCreds",
			originUsername:      originUsername,
			originPassword:      originPassword,
			targetUsername:      targetUsername,
			targetPassword:      targetPassword,
			proxyTargetUsername: "",
			proxyTargetPassword: "",
			proxyOriginUsername: originUsername,
			proxyOriginPassword: originPassword,
			success:             false,
		},


		{
			name:                "NoAuthOnEither_ProxyHasNoCreds",
			originUsername:      "",
			originPassword:      "",
			targetUsername:      "",
			targetPassword:      "",
			proxyTargetUsername: "",
			proxyTargetPassword: "",
			proxyOriginUsername: "",
			proxyOriginPassword: "",
			success:             true,
		},
		{
			name:                "NoAuthOnTarget_ProxyHasNoCreds",
			originUsername:      originUsername,
			originPassword:      originPassword,
			targetUsername:      "",
			targetPassword:      "",
			proxyTargetUsername: "",
			proxyTargetPassword: "",
			proxyOriginUsername: "",
			proxyOriginPassword: "",
			success:             false,
		},
		{
			name:                "NoAuthOnOrigin_ProxyHasNoCreds",
			originUsername:      "",
			originPassword:      "",
			targetUsername:      targetUsername,
			targetPassword:      targetPassword,
			proxyTargetUsername: "",
			proxyTargetPassword: "",
			proxyOriginUsername: "",
			proxyOriginPassword: "",
			success:             false,
		},
		{
			name:                "AuthOnBoth_ProxyHasNoCreds",
			originUsername:      originUsername,
			originPassword:      originPassword,
			targetUsername:      targetUsername,
			targetPassword:      targetPassword,
			proxyTargetUsername: "",
			proxyTargetPassword: "",
			proxyOriginUsername: "",
			proxyOriginPassword: "",
			success:             false,
		},


		{
			name:                "AuthOnBoth_InvalidOriginCredentials",
			originUsername:      originUsername,
			originPassword:      originPassword,
			targetUsername:      targetUsername,
			targetPassword:      targetPassword,
			proxyTargetUsername: targetUsername,
			proxyTargetPassword: targetPassword,
			proxyOriginUsername: "invalidOriginUsername",
			proxyOriginPassword: "invalidOriginPassword",
			success:             false,
		},
		{
			name:                "AuthOnBoth_InvalidTargetCredentials",
			originUsername:      originUsername,
			originPassword:      originPassword,
			targetUsername:      targetUsername,
			targetPassword:      targetPassword,
			proxyTargetUsername: "invalidTargetUsername",
			proxyTargetPassword: "invalidTargetPassword",
			proxyOriginUsername: originUsername,
			proxyOriginPassword: originPassword,
			success:             false,
		},
		{
			name:                "AuthOnBoth_InvalidTargetAndOriginCredentials",
			originUsername:      originUsername,
			originPassword:      originPassword,
			targetUsername:      targetUsername,
			targetPassword:      targetPassword,
			proxyTargetUsername: "invalidTargetUsername",
			proxyTargetPassword: "invalidTargetPassword",
			proxyOriginUsername: "invalidOriginUsername",
			proxyOriginPassword: "invalidOriginPassword",
			success:             false,
		},
	}

	originAddress := "127.0.1.1"
	targetAddress := "127.0.1.2"

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			serverConf := setup.NewTestConfig(originAddress, targetAddress)
			serverConf.TargetCassandraUsername = tt.targetUsername
			serverConf.TargetCassandraPassword = tt.targetPassword
			serverConf.OriginCassandraUsername = tt.originUsername
			serverConf.OriginCassandraPassword = tt.originPassword

			testSetup, err := setup.NewCqlServerTestSetup(serverConf, true, false, false)
			require.Nil(t, err)
			defer testSetup.Cleanup()

			proxyConf := setup.NewTestConfig(originAddress, targetAddress)
			proxyConf.HeartbeatIntervalMs = 200
			proxyConf.HeartbeatRetryIntervalMaxMs = 500
			proxyConf.HeartbeatRetryIntervalMinMs = 200
			proxyConf.TargetCassandraUsername = tt.proxyTargetUsername
			proxyConf.TargetCassandraPassword = tt.proxyTargetPassword
			proxyConf.OriginCassandraUsername = tt.proxyOriginUsername
			proxyConf.OriginCassandraPassword = tt.proxyOriginPassword
			proxy, err := setup.NewProxyInstanceWithConfig(proxyConf)
			if proxy != nil {
				defer proxy.Shutdown()
			}
			if !tt.success {
				require.NotNil(t, err)
				require.Nil(t, proxy)
				return
			}

			require.Nil(t, err)

			if tt.success {
				time.Sleep(time.Duration(proxyConf.HeartbeatIntervalMs) * time.Millisecond * 5)
				r := health.PerformHealthCheck(proxy)
				require.Equal(t, health.UP, r.Status)
				require.Equal(t, health.UP, r.OriginStatus.Status)
				require.Equal(t, 0, r.OriginStatus.CurrentFailureCount)
				require.Equal(t, health.UP, r.TargetStatus.Status)
				require.Equal(t, 0, r.TargetStatus.CurrentFailureCount)
			}
		})
	}
}