package integration_tests

import (
	"github.com/bmizerany/assert"
	"github.com/riptano/cloud-gate/integration-tests/client"
	"github.com/riptano/cloud-gate/integration-tests/cql"
	"github.com/riptano/cloud-gate/integration-tests/setup"
	"github.com/riptano/cloud-gate/proxy/pkg/cloudgateproxy"
	"testing"
)

func TestPreparedIdProxyCacheMiss(t *testing.T) {

	simulacronSetup := setup.NewSimulacronTestSetup()
	defer simulacronSetup.Cleanup()

	testClient, err := client.NewTestClient("127.0.0.1:14002", 2048)
	assert.Tf(t, err == nil, "testClient setup failed: %s", err)

	testClient.PerformHandshake()		//TODO [Alice] - will fix this as per comment on PR 33
	assert.Tf(t, err == nil, "handshake failed: %s", err)

	defer testClient.Shutdown()

	preparedId := []byte{143, 7, 36, 50, 225, 104, 157, 89, 199, 177, 239, 231, 82, 201, 142, 253}

	execute, err := cql.NewExecuteRequest(0x04, preparedId)
	assert.Tf(t, err == nil, "execute request creation failed: %s", err)

	response, requestStreamId, err := testClient.SendRequest(execute)
	assert.Tf(t, err == nil, "execute request send failed: %s", err)
	assert.Tf(t, response != nil, "response received was null")

	errorResponse, err := response.ParseErrorResponse()
	assert.Tf(t, response.StreamId == requestStreamId, "streamId does not match expected value. Expected %d, received %d", requestStreamId, response.StreamId)
	assert.Tf(t, err == nil, "Error response could not be parsed: %s", err)
	assert.Tf(t, errorResponse.ErrorCode == int(cloudgateproxy.ErrorCodeUnprepared), "Error code received was not Unprepared. Received: %d", errorResponse.ErrorCode)
	assert.Tf(t, errorResponse.Message == string(preparedId), "Error body did not contain the expected preparedId. Received: %s", errorResponse.Message)

}

//TODO temporarily disabled to investigate intermittent failures later
//func TestPreparedIdPreparationMismatch(t *testing.T) {
//
//	simulacronSetup := setup.NewSimulacronTestSetup()
//	defer simulacronSetup.Cleanup()
//
//	testClient, err := client.NewTestClient("127.0.0.1:14002", 2048)
//	assert.Tf(t, err == nil, "testClient setup failed: %s", err)
//	testClient.PerformHandshake()
//	defer testClient.Shutdown()
//
//	tests := map[string]*simulacron.Cluster{
//		"origin": simulacronSetup.Origin,
//		"target": simulacronSetup.Target,
//	}
//
//	for name, cluster := range tests {
//		t.Run(name, func(t *testing.T) {
//
//			err := simulacronSetup.Origin.ClearPrimes()
//			assert.Tf(t, err == nil, "clear primes failed on origin: %s", err)
//
//			err = simulacronSetup.Target.ClearPrimes()
//			assert.Tf(t, err == nil, "clear primes failed on target: %s", err)
//
//			prepare, err := cql.NewPrepareRequest(0x04, "INSERT INTO ks1.table1 (c1, c2) VALUES (1, 2)")
//			assert.Tf(t, err == nil, "prepare request creation failed: %s", err)
//
//			response, requestStreamId, err := testClient.SendRequest(prepare)
//			assert.Tf(t, err == nil, "prepare request send failed: %s", err)
//
//			preparedResponse, err := response.ParsePreparedResponse()
//			assert.Tf(t, err == nil, "cannot parse prepared response: %s", err)
//
//			// clear primes only on selected cluster
//			err = cluster.ClearPrimes()
//			assert.Tf(t, err == nil, "clear primes failed: %s", err)
//
//			execute, err := cql.NewExecuteRequest(0x04, preparedResponse.PreparedId)
//			assert.Tf(t, err == nil, "execute request creation failed: %s", err)
//
//			response, requestStreamId, err = testClient.SendRequest(execute)
//			assert.Tf(t, err == nil, "execute request send failed: %s", err)
//
//			errorResponse, err := response.ParseErrorResponse()
//			assert.Tf(t, response.StreamId == requestStreamId, "streamId does not match expected value. Expected %d, received %d", requestStreamId, response.StreamId)
//			assert.Tf(t, err == nil, "Error response could not be parsed: %s", err)
//			assert.Tf(t, errorResponse.ErrorCode == int(cloudgateproxy.ErrorCodeUnprepared), "Error code received was not Unprepared. Received: %d", errorResponse.ErrorCode)
//			assert.Tf(t, errorResponse.Message == string(preparedResponse.PreparedId), "Error body did not contain the expected preparedId. Received: %s", errorResponse.Message)
//
//		})
//	}
//}
