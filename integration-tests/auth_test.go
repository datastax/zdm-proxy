package integration_tests

import (
	"github.com/bmizerany/assert"
	"github.com/riptano/cloud-gate/integration-tests/client"
	"github.com/riptano/cloud-gate/integration-tests/cql"
	"github.com/riptano/cloud-gate/integration-tests/env"
	"github.com/riptano/cloud-gate/integration-tests/setup"
	"github.com/riptano/cloud-gate/proxy/pkg/cloudgateproxy"
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

	startup, err := cql.NewStartupRequest(0x04)
	assert.Tf(t, err == nil, "startup request creation failed: %s", err)

	response, err := testClient.SendRequest(startup)
	assert.Tf(t, err == nil, "startup request send failed: %s", err)

	parsedAuthenticateResponse, err := response.ParseAuthenticateResponse()
	assert.Tf(t, err == nil, "authenticate response parse failed, got %02x instead: %s", response.Opcode, err)

	authenticator := client.NewDsePlainTextAuthenticator("cassandra", "cassandra")
	initialResponse, err := authenticator.InitialResponse(parsedAuthenticateResponse.AuthenticatorName)
	assert.Tf(t, err == nil, "authenticator initial response creation failed: %s", err)

	authResponseRequest, err := cql.NewAuthResponseRequest(0x04, initialResponse)
	assert.Tf(t, err == nil, "auth response request creation failed: %s", err)

	response, err = testClient.SendRequest(authResponseRequest)
	assert.Tf(t, err == nil, "auth response request send failed: %s", err)

	assert.Equal(t, cloudgateproxy.OpCodeAuthSuccess, response.Opcode)

	// TODO: send query to validate that handshake was successful
}
