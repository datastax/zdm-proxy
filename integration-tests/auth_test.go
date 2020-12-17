package integration_tests

import (
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/gocql/gocql"
	"github.com/riptano/cloud-gate/integration-tests/client"
	"github.com/riptano/cloud-gate/integration-tests/env"
	"github.com/riptano/cloud-gate/integration-tests/setup"
	"github.com/riptano/cloud-gate/integration-tests/utils"
	"github.com/riptano/cloud-gate/proxy/pkg/health"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

// BasicUpdate tests if update queries run correctly
// Unloads the originCluster database,
// performs an update where through the proxy
// then loads the unloaded data into the destination
func TestWithCcmAuth(t *testing.T) {
	if !env.UseCcm {
		t.Skip("Test requires CCM, set USE_CCM env variable to TRUE")
	}

	ccmSetup, err := setup.NewTemporaryCcmTestSetup(false)
	require.True(t, err == nil, "ccm setup failed: %s", err)

	defer ccmSetup.Cleanup()

	err = ccmSetup.Origin.UpdateConf("authenticator: PasswordAuthenticator")
	require.True(t, err == nil, "ccm origin updateconf failed: %s", err)

	err = ccmSetup.Target.UpdateConf("authenticator: PasswordAuthenticator")
	require.True(t, err == nil, "ccm target updateconf failed: %s", err)

	err = ccmSetup.Start(nil, "-Dcassandra.superuser_setup_delay_ms=0")
	require.True(t, err == nil, "start ccm setup failed: %s", err)

	originUsername := "origin_username"
	originPassword := "originPassword"
	targetUsername := "target_username"
	targetPassword := "targetPassword"

	PrepareAuthTest(t, ccmSetup, originUsername, originPassword, targetUsername, targetPassword)

	t.Run("TestAuth", func(t *testing.T) {
		testAuth(t, ccmSetup, originUsername, originPassword, targetUsername, targetPassword)
	})

	t.Run("TestHealthCheckWithAuth", func(t *testing.T) {
		testHealthCheckWithAuth(t, ccmSetup, originUsername, originPassword, targetUsername, targetPassword)
	})
}

func testAuth(
	t *testing.T, ccmSetup *setup.CcmTestSetup, originUsername string, originPassword string, targetUsername string, targetPassword string) {
	tests := []struct {
		name           string
		targetUsername string
		targetPassword string
		originUsername string
		originPassword string
		clientUsername string
		clientPassword string
		success        bool
		successOrigin  bool
		successTarget  bool
		authError      bool
	}{
		{
			name:           "CorrectCredentials",
			targetUsername: targetUsername,
			targetPassword: targetPassword,
			originUsername: originUsername,
			originPassword: originPassword,
			clientUsername: targetUsername,
			clientPassword: targetPassword,
			success:        true,
			successOrigin:  true,
			successTarget:  true,
			authError:      true,
		},
		{
			name:           "InvalidOriginCredentials",
			targetUsername: targetUsername,
			targetPassword: targetPassword,
			originUsername: "invalidOriginUsername",
			originPassword: "invalidOriginPassword",
			clientUsername: targetUsername,
			clientPassword: targetPassword,
			success:        false,
			successOrigin:  false,
			successTarget:  true,
			authError:      true,
		},
		{
			name:           "InvalidTargetCredentials",
			targetUsername: "invalidTargetUsername",
			targetPassword: "invalidTargetPassword",
			originUsername: originUsername,
			originPassword: originPassword,
			clientUsername: "invalidTargetUsername",
			clientPassword: "invalidTargetPassword",
			success:        false,
			successOrigin:  true,
			successTarget:  false,
			authError:      true,
		},
		{
			name:           "InvalidClientCredentials",
			targetUsername: targetUsername,
			targetPassword: targetPassword,
			originUsername: originUsername,
			originPassword: originPassword,
			clientUsername: "invalidTargetUsername",
			clientPassword: "invalidTargetPassword",
			success:        false,
			successOrigin:  true,
			successTarget:  false,
			authError:      true,
		},
		{
			name:           "InvalidTargetAndOriginCredentials",
			targetUsername: "invalidTargetUsername",
			targetPassword: "invalidTargetPassword",
			originUsername: "invalidOriginUsername",
			originPassword: "invalidOriginPassword",
			clientUsername: "invalidTargetUsername",
			clientPassword: "invalidTargetPassword",
			success:        false,
			successOrigin:  false,
			successTarget:  false,
			authError:      true,
		},
	}

	protocolVersions := []primitive.ProtocolVersion{primitive.ProtocolVersion3, primitive.ProtocolVersion4}

	for _, version := range protocolVersions {
		t.Run(fmt.Sprintf("for version %02x", version), func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {

					conf := setup.NewTestConfig(ccmSetup.Origin.GetInitialContactPoint(), ccmSetup.Target.GetInitialContactPoint())
					conf.TargetCassandraUsername = tt.targetUsername
					conf.TargetCassandraPassword = tt.targetPassword
					conf.OriginCassandraUsername = tt.originUsername
					conf.OriginCassandraPassword = tt.originPassword
					proxy := setup.NewProxyInstanceWithConfig(conf)
					defer proxy.Shutdown()

					testClient, err := client.NewTestClient("127.0.0.1:14002")
					require.True(t, err == nil, "testClient setup failed: %s", err)

					defer testClient.Shutdown()

					err = testClient.PerformHandshake(version, true, tt.clientUsername, tt.clientPassword)

					if !tt.success {
						if tt.authError {
							require.True(t, err != nil, "expected failure in handshake")
							require.True(t, strings.Contains(err.Error(), "expected auth success but received "), err.Error())
							require.True(t, strings.Contains(err.Error(), "ERROR AUTHENTICATION ERROR"), err.Error())
						} else {
							require.True(t, err != nil, "expected err")
							require.True(t, strings.Contains(err.Error(), "response channel closed"), "expected channel closed but got %v", err)
						}
					} else {
						require.True(t, err == nil, "handshake failed: %v", err)

						query := &message.Query{
							Query:   "SELECT * FROM system.peers",
							Options: &message.QueryOptions{Consistency: primitive.ConsistencyLevelOne},
						}

						response, _, err := testClient.SendMessage(version, query)
						require.True(t, err == nil, "query request send failed: %s", err)

						require.Equal(t, primitive.OpCodeResult, response.Body.Message.GetOpCode())
					}
				})
			}
		})
	}
}

func testHealthCheckWithAuth(
	t *testing.T, ccmSetup *setup.CcmTestSetup, originUsername string, originPassword string, targetUsername string, targetPassword string) {
	tests := []struct {
		name           string
		targetUsername string
		targetPassword string
		originUsername string
		originPassword string
		clientUsername string
		clientPassword string
		successOrigin  bool
		successTarget  bool
	}{
		{
			name:           "CorrectCredentials",
			targetUsername: targetUsername,
			targetPassword: targetPassword,
			originUsername: originUsername,
			originPassword: originPassword,
			clientUsername: originUsername,
			clientPassword: originPassword,
			successOrigin:  true,
			successTarget:  true,
		},
		{
			name:           "InvalidOriginCredentials",
			targetUsername: targetUsername,
			targetPassword: targetPassword,
			originUsername: "invalidOriginUsername",
			originPassword: "invalidOriginPassword",
			clientUsername: "invalidOriginUsername",
			clientPassword: "invalidOriginPassword",
			successOrigin:  false,
			successTarget:  true,
		},
		{
			name:           "InvalidTargetCredentials",
			targetUsername: "invalidTargetUsername",
			targetPassword: "invalidTargetPassword",
			originUsername: originUsername,
			originPassword: originPassword,
			clientUsername: originUsername,
			clientPassword: originPassword,
			successOrigin:  true,
			successTarget:  false,
		},
		{
			name:           "InvalidClientCredentials",
			targetUsername: targetUsername,
			targetPassword: targetPassword,
			originUsername: originUsername,
			originPassword: originPassword,
			clientUsername: "invalidTargetUsername",
			clientPassword: "invalidTargetPassword",
			successOrigin:  true,
			successTarget:  true,
		},
		{
			name:           "InvalidTargetAndOriginCredentials",
			targetUsername: "invalidTargetUsername",
			targetPassword: "invalidTargetPassword",
			originUsername: "invalidOriginUsername",
			originPassword: "invalidOriginPassword",
			clientUsername: "invalidOriginUsername",
			clientPassword: "invalidOriginPassword",
			successOrigin:  false,
			successTarget:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			conf := setup.NewTestConfig(ccmSetup.Origin.GetInitialContactPoint(), ccmSetup.Target.GetInitialContactPoint())
			conf.HeartbeatIntervalMs = 500
			conf.HeartbeatRetryIntervalMaxMs = 500
			conf.HeartbeatRetryIntervalMinMs = 100
			conf.TargetCassandraUsername = tt.targetUsername
			conf.TargetCassandraPassword = tt.targetPassword
			conf.OriginCassandraUsername = tt.originUsername
			conf.OriginCassandraPassword = tt.originPassword
			proxy := setup.NewProxyInstanceWithConfig(conf)
			defer proxy.Shutdown()

			if tt.successOrigin || tt.successTarget {
				time.Sleep(time.Duration(conf.HeartbeatIntervalMs) * time.Millisecond * 5)
				r := health.PerformHealthCheck(proxy)
				if tt.successOrigin && tt.successTarget {
					require.Equal(t, health.UP, r.Status)
				}
				if tt.successOrigin {
					require.Equal(t, health.UP, r.OriginStatus.Status)
					require.Equal(t, 0, r.OriginStatus.CurrentFailureCount)
				}
				if tt.successTarget {
					require.Equal(t, health.UP, r.TargetStatus.Status)
					require.Equal(t, 0, r.TargetStatus.CurrentFailureCount)
				}
			}

			if !tt.successOrigin || !tt.successTarget {
				utils.RequireWithRetries(t, func() (error, bool) {
					r := health.PerformHealthCheck(proxy)

					if !tt.successOrigin {
						if r.OriginStatus.CurrentFailureCount < r.OriginStatus.FailureCountThreshold {
							return fmt.Errorf(
								"expected current failure count on origin greater or equal than threshold but got %v",
								r.OriginStatus.CurrentFailureCount), false
						}
						require.Equal(t, health.DOWN, r.Status)
						require.Equal(t, health.DOWN, r.OriginStatus.Status)
					}
					if !tt.successTarget {
						if r.TargetStatus.CurrentFailureCount < r.TargetStatus.FailureCountThreshold {
							return fmt.Errorf(
								"expected current failure count on target greater or equal than threshold but got %v",
								r.TargetStatus.CurrentFailureCount), false
						}
						require.Equal(t, health.DOWN, r.Status)
						require.Equal(t, health.DOWN, r.TargetStatus.Status)
					}
					return nil, false
				}, 30, 100*time.Millisecond)
			}
		})
	}
}

func PrepareAuthTest(
	t *testing.T,
	ccmSetup *setup.CcmTestSetup,
	originUsername string,
	originPassword string,
	targetUsername string,
	targetPassword string) {
	CreateNewUsers(t, ccmSetup, originUsername, originPassword, targetUsername, targetPassword)
	ChangeDefaultUsers(t, ccmSetup, originUsername, originPassword, targetUsername, targetPassword)
}

func CreateNewUsers(
	t *testing.T,
	ccmSetup *setup.CcmTestSetup,
	originUsername string,
	originPassword string,
	targetUsername string,
	targetPassword string) {
	cluster := gocql.NewCluster(ccmSetup.Origin.GetInitialContactPoint())
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: "cassandra",
		Password: "cassandra",
	}
	originSession, err := cluster.CreateSession()
	require.True(t, err == nil, "origin session creation failed: %v", err)

	defer originSession.Close()

	cluster = gocql.NewCluster(ccmSetup.Target.GetInitialContactPoint())
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: "cassandra",
		Password: "cassandra",
	}
	targetSession, err := cluster.CreateSession()
	require.True(t, err == nil, "target session creation failed: %v", err)

	defer targetSession.Close()

	err = originSession.Query(
		fmt.Sprintf("CREATE ROLE %s WITH PASSWORD = '%s' "+
			"AND SUPERUSER = true "+
			"AND LOGIN = true", originUsername, originPassword)).Exec()
	require.True(t, err == nil, "origin user creation failed: %v", err)

	err = targetSession.Query(
		fmt.Sprintf("CREATE ROLE %s WITH PASSWORD = '%s' "+
			"AND SUPERUSER = true "+
			"AND LOGIN = true", targetUsername, targetPassword)).Exec()
	require.True(t, err == nil, "target user creation failed: %v", err)
}

func ChangeDefaultUsers(
	t *testing.T,
	ccmSetup *setup.CcmTestSetup,
	originUsername string,
	originPassword string,
	targetUsername string,
	targetPassword string) {
	cluster := gocql.NewCluster(ccmSetup.Origin.GetInitialContactPoint())
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: originUsername,
		Password: originPassword,
	}
	originSession, err := cluster.CreateSession()
	require.True(t, err == nil, "origin session creation failed: %v", err)

	defer originSession.Close()

	cluster = gocql.NewCluster(ccmSetup.Target.GetInitialContactPoint())
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: targetUsername,
		Password: targetPassword,
	}
	targetSession, err := cluster.CreateSession()
	require.True(t, err == nil, "target session creation failed: %v", err)

	defer targetSession.Close()

	err = originSession.Query("ALTER ROLE cassandra WITH PASSWORD='INVALIDPASSWORD' AND SUPERUSER=false").Exec()
	require.True(t, err == nil, "origin change default user password failed: %v", err)

	err = targetSession.Query("ALTER ROLE cassandra WITH PASSWORD='INVALIDPASSWORD' AND SUPERUSER=false").Exec()
	require.True(t, err == nil, "target change default user password failed: %v", err)
}
