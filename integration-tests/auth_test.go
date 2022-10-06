package integration_tests

import (
	"context"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/proxy/pkg/config"
	"github.com/datastax/zdm-proxy/proxy/pkg/health"
	"github.com/stretchr/testify/require"
	"strings"
	"sync"
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
			serverConf.TargetUsername = tt.targetUsername
			serverConf.TargetPassword = tt.targetPassword
			serverConf.OriginUsername = tt.originUsername
			serverConf.OriginPassword = tt.originPassword

			proxyConf := setup.NewTestConfig(originAddress, targetAddress)
			proxyConf.TargetUsername = tt.proxyTargetUsername
			proxyConf.TargetPassword = tt.proxyTargetPassword
			proxyConf.OriginUsername = tt.proxyOriginUsername
			proxyConf.OriginPassword = tt.proxyOriginPassword
			if tt.forwardClientCredsToOrigin != nil {
				proxyConf.ForwardClientCredentialsToOrigin = *tt.forwardClientCredsToOrigin
			}

			testFunc := func(t *testing.T, proxyConfig *config.Config) {
				testSetup, err := setup.NewCqlServerTestSetup(t, serverConf, false, false, false)
				require.Nil(t, err)
				defer testSetup.Cleanup()

				originRequestHandler := NewFakeRequestHandler()
				targetRequestHandler := NewFakeRequestHandler()

				testSetup.Origin.CqlServer.RequestHandlers = []client.RequestHandler{
					originRequestHandler.HandleRequest,
					client.NewDriverConnectionInitializationHandler("origin", "dc1", func(_ string) {}),
				}
				testSetup.Target.CqlServer.RequestHandlers = []client.RequestHandler{
					targetRequestHandler.HandleRequest,
					client.NewDriverConnectionInitializationHandler("target", "dc2", func(_ string) {}),
				}

				err = testSetup.Start(nil, false, primitive.ProtocolVersion4)
				require.Nil(t, err)

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

				var authCreds *client.AuthCredentials
				if tt.clientUsername != "" || tt.clientPassword != "" {
					authCreds = &client.AuthCredentials{
						Username: tt.clientUsername,
						Password: tt.clientPassword,
					}
				}
				testClient := client.NewCqlClient(
					fmt.Sprintf("%s:%d", proxyConf.ProxyListenAddress, proxyConf.ProxyListenPort),
					authCreds)
				cqlConn, err := testClient.Connect(context.Background())
				require.Nil(t, err, "client connection failed: %v", err)
				defer cqlConn.Close()

				err = cqlConn.InitiateHandshake(primitive.ProtocolVersion4, 0)

				originRequestsByConn := originRequestHandler.GetRequests()
				targetRequestsByConn := targetRequestHandler.GetRequests()

				if !tt.success {
					require.NotNil(t, err, "expected failure in handshake")
					if authCreds == nil {
						require.True(t, strings.Contains(err.Error(), "expected READY, got AUTHENTICATE"), err.Error())
					} else {
						require.True(t, strings.Contains(err.Error(), (&message.AuthenticationError{ErrorMessage: "invalid credentials"}).String()), err.Error())
					}
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

				checkedOrigin := false
				checkedTarget := false
				secondaryHandshakeIsTarget := true
				asyncIsTarget := true
				if proxyConf.PrimaryCluster == config.PrimaryClusterTarget {
					asyncIsTarget = false
				}

				var originRequests []*frame.Frame
				var targetRequests []*frame.Frame
				var asyncRequests []*frame.Frame

				controlConnection := 1
				if proxyConf.ReadMode == config.ReadModeDualAsyncOnSecondary {
					if asyncIsTarget {
						if len(targetRequestsByConn) == controlConnection+2 {
							asyncRequests = targetRequestsByConn[1]
						}
					} else {
						if len(originRequestsByConn) == controlConnection+2 {
							asyncRequests = originRequestsByConn[1]
						}
					}

					if asyncRequests == nil {
						require.Equal(t, controlConnection+1, len(targetRequestsByConn))
						require.Equal(t, controlConnection+1, len(originRequestsByConn))
					}
				} else {
					require.Equal(t, controlConnection+1, len(targetRequestsByConn))
					require.Equal(t, controlConnection+1, len(originRequestsByConn))
				}

				originRequests = originRequestsByConn[1]
				targetRequests = targetRequestsByConn[1]
				asyncHandshakeAttempted := true

				forwardClientCredsToOrigin := false
				if (tt.forwardClientCredsToOrigin != nil &&
					*tt.forwardClientCredsToOrigin &&
					tt.proxyOriginUsername != "" &&
					tt.proxyOriginPassword != "") ||
					(tt.proxyOriginUsername != "" &&
						tt.proxyOriginPassword != "" &&
						tt.proxyTargetUsername == "" &&
						tt.proxyTargetPassword == "") {
					forwardClientCredsToOrigin = true
				}

				if tt.clientPassword == "" && tt.clientUsername == "" {
					require.Equal(t, 1, len(originRequests))
					require.Equal(t, 1, len(targetRequests))
					checkedOrigin = true
					checkedTarget = true
					if tt.originUsername != "" || tt.originPassword != "" ||
						tt.targetUsername != "" || tt.targetPassword != "" {
						asyncHandshakeAttempted = false
					}
				}

				if tt.originUsername == "" && tt.originPassword == "" {
					require.Equal(t, 1, len(originRequests))
					checkedOrigin = true
					if tt.targetUsername != "" || tt.targetPassword != "" {
						secondaryHandshakeIsTarget = false
					}
				}

				if tt.targetUsername == "" && tt.targetPassword == "" {
					require.Equal(t, 1, len(targetRequests))
					checkedTarget = true
				}

				if secondaryHandshakeIsTarget {
					if forwardClientCredsToOrigin {
						if tt.clientUsername != tt.originUsername || tt.clientPassword != tt.originPassword {
							require.Equal(t, 1, len(targetRequests))
							checkedTarget = true
							asyncHandshakeAttempted = false
						}
					} else if tt.proxyOriginPassword != tt.originPassword || tt.proxyOriginUsername != tt.originUsername {
						require.Equal(t, 1, len(targetRequests))
						checkedTarget = true
						asyncHandshakeAttempted = false
					}
				} else {
					if !forwardClientCredsToOrigin {
						if tt.clientUsername != tt.targetUsername || tt.clientPassword != tt.targetPassword {
							require.Equal(t, 1, len(originRequests))
							checkedOrigin = true
							asyncHandshakeAttempted = false
						}
					} else if tt.proxyTargetPassword != tt.targetPassword || tt.proxyTargetUsername != tt.targetUsername {
						require.Equal(t, 1, len(originRequests))
						checkedOrigin = true
						asyncHandshakeAttempted = false
					}
				}

				if !checkedOrigin {
					require.Equal(t, 2, len(originRequests))
					require.Equal(t, primitive.OpCodeStartup, originRequests[0].Header.OpCode)
					require.Equal(t, primitive.OpCodeAuthResponse, originRequests[1].Header.OpCode)
				}

				if !checkedTarget {
					require.Equal(t, 2, len(targetRequests))
					require.Equal(t, primitive.OpCodeStartup, targetRequests[0].Header.OpCode)
					require.Equal(t, primitive.OpCodeAuthResponse, targetRequests[1].Header.OpCode)
				}

				if proxyConf.ReadMode == config.ReadModeDualAsyncOnSecondary {
					if asyncHandshakeAttempted {
						if (asyncIsTarget && len(targetRequests) == 2) || (!asyncIsTarget && len(originRequests) == 2) {
							require.Equal(t, 2, len(asyncRequests))
							require.Equal(t, primitive.OpCodeAuthResponse, asyncRequests[1].Header.OpCode)
						} else {
							require.Equal(t, 1, len(asyncRequests))
						}
						require.Equal(t, primitive.OpCodeStartup, asyncRequests[0].Header.OpCode)
					} else {
						require.Equal(t, 0, len(asyncRequests))
					}
				}
			}

			t.Run("NoAsyncReads", func(t *testing.T) {
				testFunc(t, proxyConf)
			})

			proxyConf.ReadMode = config.ReadModeDualAsyncOnSecondary
			proxyConf.PrimaryCluster = config.PrimaryClusterOrigin
			t.Run("AsyncReadsOnTarget", func(t *testing.T) {
				testFunc(t, proxyConf)
			})

			proxyConf.PrimaryCluster = config.PrimaryClusterTarget
			t.Run("AsyncReadsOnOrigin", func(t *testing.T) {
				testFunc(t, proxyConf)
			})
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
			serverConf.TargetUsername = tt.targetUsername
			serverConf.TargetPassword = tt.targetPassword
			serverConf.OriginUsername = tt.originUsername
			serverConf.OriginPassword = tt.originPassword

			testSetup, err := setup.NewCqlServerTestSetup(t, serverConf, true, false, false)
			require.Nil(t, err)
			defer testSetup.Cleanup()

			proxyConf := setup.NewTestConfig(originAddress, targetAddress)
			proxyConf.HeartbeatIntervalMs = 200
			proxyConf.HeartbeatRetryIntervalMaxMs = 500
			proxyConf.HeartbeatRetryIntervalMinMs = 200
			proxyConf.TargetUsername = tt.proxyTargetUsername
			proxyConf.TargetPassword = tt.proxyTargetPassword
			proxyConf.OriginUsername = tt.proxyOriginUsername
			proxyConf.OriginPassword = tt.proxyOriginPassword
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

type FakeRequestHandler struct {
	lock         *sync.Mutex
	contexts     map[*client.CqlServerConnection]client.RequestHandlerContext
	orderedConns []*client.CqlServerConnection
}

func NewFakeRequestHandler() *FakeRequestHandler {
	return &FakeRequestHandler{
		lock:     &sync.Mutex{},
		contexts: map[*client.CqlServerConnection]client.RequestHandlerContext{},
	}
}

func (recv *FakeRequestHandler) HandleRequest(
	request *frame.Frame,
	conn *client.CqlServerConnection,
	ctx client.RequestHandlerContext) (response *frame.Frame) {
	recv.lock.Lock()
	defer recv.lock.Unlock()
	reqs := ctx.GetAttribute("requests")
	if reqs == nil {
		reqs = make([]*frame.Frame, 0, 1)
		recv.orderedConns = append(recv.orderedConns, conn)
	}
	newReqs := append(reqs.([]*frame.Frame), request)
	ctx.PutAttribute("requests", newReqs)
	recv.contexts[conn] = ctx
	return nil
}

func (recv *FakeRequestHandler) GetRequests() [][]*frame.Frame {
	recv.lock.Lock()
	defer recv.lock.Unlock()
	output := make([][]*frame.Frame, 0, len(recv.contexts))
	for _, conn := range recv.orderedConns {
		reqs := recv.contexts[conn].GetAttribute("requests")
		if reqs == nil {
			reqs = make([]*frame.Frame, 0)
		}
		output = append(output, reqs.([]*frame.Frame))
	}
	return output
}

func (recv *FakeRequestHandler) Clear() {
	recv.lock.Lock()
	defer recv.lock.Unlock()
	for _, ctx := range recv.contexts {
		ctx.PutAttribute("requests", nil)
	}
	recv.orderedConns = make([]*client.CqlServerConnection, 0)
	recv.contexts = map[*client.CqlServerConnection]client.RequestHandlerContext{}
}
