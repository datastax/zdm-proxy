package integration_tests

import (
	"fmt"
	"github.com/bmizerany/assert"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/message"
	"github.com/gocql/gocql"
	"github.com/riptano/cloud-gate/integration-tests/client"
	"github.com/riptano/cloud-gate/integration-tests/env"
	"github.com/riptano/cloud-gate/integration-tests/setup"
	"testing"
)

// BasicUpdate tests if update queries run correctly
// Unloads the source database,
// performs an update where through the proxy
// then loads the unloaded data into the destination
func TestAuth(t *testing.T) {
	if !env.UseCcm {
		t.Skip("Test requires CCM, set USE_CCM env variable to TRUE")
	}

	ccmSetup, err := setup.NewTemporaryCcmTestSetup(false)
	assert.Tf(t, err == nil, "ccm setup failed: %s", err)

	defer ccmSetup.Cleanup()

	err = ccmSetup.Origin.UpdateConf("authenticator: PasswordAuthenticator")
	assert.Tf(t, err == nil, "ccm origin updateconf failed: %s", err)

	err = ccmSetup.Target.UpdateConf("authenticator: PasswordAuthenticator")
	assert.Tf(t, err == nil, "ccm target updateconf failed: %s", err)

	err = ccmSetup.Start(nil, "-Dcassandra.superuser_setup_delay_ms=0")
	assert.Tf(t, err == nil, "start ccm setup failed: %s", err)

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

	protocolVersions := []cassandraprotocol.ProtocolVersion{cassandraprotocol.ProtocolVersion3, cassandraprotocol.ProtocolVersion4}

	for _, version := range protocolVersions {
		t.Run(fmt.Sprintf("for version %02x", version), func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {

					config := setup.NewTestConfig(ccmSetup.Origin, ccmSetup.Target)
					config.TargetCassandraUsername = tt.targetUsername
					config.TargetCassandraPassword = tt.targetPassword
					proxy := setup.NewProxyInstanceWithConfig(config)
					defer proxy.Shutdown()

					testClient, err := client.NewTestClient("127.0.0.1:14002", 2048)
					assert.Tf(t, err == nil, "testClient setup failed: %s", err)

					defer testClient.Shutdown()

					startup := message.NewStartup()
					startupFrame, err := frame.NewRequestFrame(version, -1, false, nil, startup)
					assert.Tf(t, err == nil, "startup request creation failed: %s", err)

					response, _, err := testClient.SendRequest(startupFrame)
					assert.Tf(t, err == nil, "startup request send failed: %s", err)

					parsedAuthenticateResponse, ok := response.Body.Message.(*message.Authenticate)
					assert.Tf(t, ok, "authenticate response parse failed, got %02x instead", response.Body.Message.GetOpCode())

					authenticator := client.NewDsePlainTextAuthenticator(tt.clientUsername, tt.clientPassword)
					initialResponse, err := authenticator.InitialResponse(parsedAuthenticateResponse.Authenticator)
					assert.Tf(t, err == nil, "authenticator initial response creation failed: %s", err)

					authResponseRequest := &message.AuthResponse{
						Token: initialResponse,
					}
					authResponseRequestFrame, err := frame.NewRequestFrame(
						version, -1, false, nil, authResponseRequest)
					assert.Tf(t, err == nil, "auth response request creation failed: %s", err)

					response, _, err = testClient.SendRequest(authResponseRequestFrame)

					if !tt.success {
						if tt.authError {
							assert.Tf(t, err == nil, "auth response request send failed: %s", err)
							_, ok := response.Body.Message.(*message.AuthenticationError)
							assert.Tf(t, ok, "error was not auth error: %v", response.Body.Message)
							return
						} else {
							assert.Tf(t, err != nil, "expected err")
							assert.Tf(t, err.Error() == "response channel closed", "expected channel closed but got %v", err)
							return
						}
					}

					assert.Tf(t, err == nil, "auth response request send failed: %s", err)
					assert.Equal(t, cassandraprotocol.OpCodeAuthSuccess, response.Body.Message.GetOpCode(),
						fmt.Sprintf("got %v", response.Body.Message))

					query := &message.Query{
						Query:   "SELECT * FROM system.peers",
						Options: message.NewQueryOptions(),
					}
					queryFrame, err := frame.NewRequestFrame(
						version, -1, false, nil, query)
					assert.Tf(t, err == nil, "query request creation failed: %s", err)

					response, _, err = testClient.SendRequest(queryFrame)
					assert.Tf(t, err == nil, "query request send failed: %s", err)

					assert.Equal(t, cassandraprotocol.OpCodeResult, response.Body.Message.GetOpCode())
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
	assert.Tf(t, err == nil, "origin session creation failed: %v", err)

	defer originSession.Close()

	cluster = gocql.NewCluster(ccmSetup.Target.GetInitialContactPoint())
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: "cassandra",
		Password: "cassandra",
	}
	targetSession, err := cluster.CreateSession()
	assert.Tf(t, err == nil, "target session creation failed: %v", err)

	defer targetSession.Close()

	err = originSession.Query(
		fmt.Sprintf("CREATE ROLE %s WITH PASSWORD = '%s' "+
			"AND SUPERUSER = true "+
			"AND LOGIN = true", originUsername, originPassword)).Exec()
	assert.Tf(t, err == nil, "origin user creation failed: %v", err)

	err = targetSession.Query(
		fmt.Sprintf("CREATE ROLE %s WITH PASSWORD = '%s' "+
			"AND SUPERUSER = true "+
			"AND LOGIN = true", targetUsername, targetPassword)).Exec()
	assert.Tf(t, err == nil, "target user creation failed: %v", err)
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
	assert.Tf(t, err == nil, "origin session creation failed: %v", err)

	defer originSession.Close()

	cluster = gocql.NewCluster(ccmSetup.Target.GetInitialContactPoint())
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: targetUsername,
		Password: targetPassword,
	}
	targetSession, err := cluster.CreateSession()
	assert.Tf(t, err == nil, "target session creation failed: %v", err)

	defer targetSession.Close()

	err = originSession.Query("ALTER ROLE cassandra WITH PASSWORD='INVALIDPASSWORD' AND SUPERUSER=false").Exec()
	assert.Tf(t, err == nil, "origin change default user password failed: %v", err)

	err = targetSession.Query("ALTER ROLE cassandra WITH PASSWORD='INVALIDPASSWORD' AND SUPERUSER=false").Exec()
	assert.Tf(t, err == nil, "target change default user password failed: %v", err)
}
