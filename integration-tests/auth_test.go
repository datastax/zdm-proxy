package integration_tests

import (
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/gocql/gocql"
	"github.com/riptano/cloud-gate/integration-tests/client"
	"github.com/riptano/cloud-gate/integration-tests/env"
	"github.com/riptano/cloud-gate/integration-tests/setup"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

// BasicUpdate tests if update queries run correctly
// Unloads the originCluster database,
// performs an update where through the proxy
// then loads the unloaded data into the destination
func TestAuth(t *testing.T) {
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

	tests := []struct {
		name           string
		targetUsername string
		targetPassword string
		clientUsername string
		clientPassword string
		success        bool
		authError      bool
	}{
		{
			name:           "CorrectCredentials",
			targetUsername: targetUsername,
			targetPassword: targetPassword,
			clientUsername: originUsername,
			clientPassword: originPassword,
			success:        true,
			authError:      true,
		},
		{
			name:           "InvalidOriginCredentials",
			targetUsername: targetUsername,
			targetPassword: targetPassword,
			clientUsername: "invalidOriginUsername",
			clientPassword: "invalidOriginPassword",
			success:        false,
			authError:      true,
		},
		{
			name:           "InvalidTargetCredentials",
			targetUsername: "invalidTargetUsername",
			targetPassword: "invalidTargetPassword",
			clientUsername: originUsername,
			clientPassword: originPassword,
			success:        false,
			authError:      false,
		},
		{
			name:           "InvalidTargetAndOriginCredentials",
			targetUsername: "invalidTargetUsername",
			targetPassword: "invalidTargetPassword",
			clientUsername: "invalidOriginUsername",
			clientPassword: "invalidOriginPassword",
			success:        false,
			authError:      true,
		},
	}

	protocolVersions := []primitive.ProtocolVersion{primitive.ProtocolVersion3, primitive.ProtocolVersion4}

	for _, version := range protocolVersions {
		t.Run(fmt.Sprintf("for version %02x", version), func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {

					config := setup.NewTestConfig(ccmSetup.Origin, ccmSetup.Target)
					config.TargetCassandraUsername = tt.targetUsername
					config.TargetCassandraPassword = tt.targetPassword
					proxy := setup.NewProxyInstanceWithConfig(config)
					defer proxy.Shutdown()

					testClient, err := client.NewTestClient("127.0.0.1:14002")
					require.True(t, err == nil, "testClient setup failed: %s", err)

					defer testClient.Shutdown()

					err = testClient.PerformHandshake(version, true, tt.clientUsername, tt.clientPassword)

					if !tt.success {
						if tt.authError {
							require.True(t, err != nil, "expected failure in handshake")
							require.True(t, strings.Contains(err.Error(), "expected auth success but received "))
							require.True(t, strings.Contains(err.Error(), "ERROR AUTHENTICATION ERROR"))
							return
						} else {
							require.True(t, err != nil, "expected err")
							require.True(t, strings.Contains(err.Error(), "response channel closed"), "expected channel closed but got %v", err)
							return
						}
					}

					require.True(t, err == nil, "handshake failed: %v", err)

					query := &message.Query{
						Query:   "SELECT * FROM system.peers",
						Options: &message.QueryOptions{Consistency: primitive.ConsistencyLevelOne},
					}

					response, _, err := testClient.SendMessage(version, query)
					require.True(t, err == nil, "query request send failed: %s", err)

					require.Equal(t, primitive.OpCodeResult, response.Body.Message.GetOpCode())
				})
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
