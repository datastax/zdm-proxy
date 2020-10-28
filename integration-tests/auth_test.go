package integration_tests

import (
	"github.com/bmizerany/assert"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/message"
	"github.com/riptano/cloud-gate/integration-tests/client"
	"github.com/riptano/cloud-gate/integration-tests/env"
	"github.com/riptano/cloud-gate/integration-tests/setup"
	"testing"
)

// BasicUpdate tests if update queries run correctly
// Unloads the source database,
// performs an update where through the proxy
// then loads the unloaded data into the destination
func TestPlainTextAuth(t *testing.T) {
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

	config := setup.NewTestConfig(ccmSetup.Origin, ccmSetup.Target)
	err = ccmSetup.Start(config, "-Dcassandra.superuser_setup_delay_ms=0")
	assert.Tf(t, err == nil, "start ccm setup failed: %s", err)

	testClient, err := client.NewTestClient("127.0.0.1:14002", 2048)
	assert.Tf(t, err == nil, "testClient setup failed: %s", err)

	defer testClient.Shutdown()

	startup := message.NewStartup()
	startupFrame, err := frame.NewRequestFrame(
		cassandraprotocol.ProtocolVersion4, -1, false, nil, startup)
	assert.Tf(t, err == nil, "startup request creation failed: %s", err)

	response, _, err := testClient.SendRequest(startupFrame)
	assert.Tf(t, err == nil, "startup request send failed: %s", err)

	parsedAuthenticateResponse, ok := response.Body.Message.(*message.Authenticate)
	assert.Tf(t, ok, "authenticate response parse failed, got %02x instead", response.Body.Message.GetOpCode())

	authenticator := client.NewDsePlainTextAuthenticator("cassandra", "cassandra")
	initialResponse, err := authenticator.InitialResponse(parsedAuthenticateResponse.Authenticator)
	assert.Tf(t, err == nil, "authenticator initial response creation failed: %s", err)

	authResponseRequest := &message.AuthResponse{
		Token: initialResponse,
	}
	authResponseRequestFrame, err := frame.NewRequestFrame(
		cassandraprotocol.ProtocolVersion4, -1, false, nil, authResponseRequest)
	assert.Tf(t, err == nil, "auth response request creation failed: %s", err)

	response, _, err = testClient.SendRequest(authResponseRequestFrame)
	assert.Tf(t, err == nil, "auth response request send failed: %s", err)

	assert.Equal(t, cassandraprotocol.OpCodeAuthSuccess, response.Body.Message.GetOpCode())

	query := &message.Query{
		Query:   "SELECT * FROM system.peers",
		Options: message.NewQueryOptions(),
	}
	queryFrame, err := frame.NewRequestFrame(
		cassandraprotocol.ProtocolVersion4, -1, false, nil, query)
	assert.Tf(t, err == nil, "query request creation failed: %s", err)

	response, _, err = testClient.SendRequest(queryFrame)
	assert.Tf(t, err == nil, "query request send failed: %s", err)

	assert.Equal(t, cassandraprotocol.OpCodeResult, response.Body.Message.GetOpCode())
}
