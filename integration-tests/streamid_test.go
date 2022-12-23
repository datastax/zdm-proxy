package integration_tests

import (
	"context"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/zdm-proxy/integration-tests/client"
	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/integration-tests/simulacron"
	"github.com/datastax/zdm-proxy/proxy/pkg/metrics"
	"github.com/datastax/zdm-proxy/proxy/pkg/runner"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestStreamId(t *testing.T) {
	metricsHandler, _ := runner.SetupHandlers()
	config := setup.NewTestConfig("", "")
	config.ProxyRequestTimeoutMs = int(20 * time.Second)
	simulacronSetup, err := setup.NewSimulacronTestSetupWithConfig(t, config)
	require.Nil(t, err)
	defer simulacronSetup.Cleanup()

	testClient, err := client.NewTestClientWithRequestTimeout(context.Background(), "127.0.0.1:14002", 20 * time.Second)
	defer testClient.Shutdown()
	if err != nil {
		t.Log("Unable to connect to proxy session.")
		t.Fatal(err)
	}

	wg := &sync.WaitGroup{}
	defer wg.Wait()
	srv := startMetricsHandler(t, simulacronSetup.Proxy.Conf, wg, metricsHandler)
	defer func() {
		err := srv.Close()
		if err != nil {
			log.Warnf("error cleaning http server: %v", err)
		}
	}()

	EnsureMetricsServerListening(t, simulacronSetup.Proxy.Conf)

	queryString := "SELECT * FROM fakeks.faketb"
	primeClustersWithDelay(simulacronSetup, queryString)

	log.Info("Before")
	queryWg := &sync.WaitGroup{}
	queryWg.Add(1)
	go func() {
		defer queryWg.Done()
		executeQuery(t, testClient, queryString)
	}()
	time.Sleep(2*time.Second)
	lines := GatherMetrics(t, simulacronSetup.Proxy.Conf, true)
	prefix := "zdm"
	originEndpoint := fmt.Sprintf("%v:9042", simulacronSetup.Origin.InitialContactPoint)
	//targetEndpoint := fmt.Sprintf("%v:9042", simulacronSetup.Target.InitialContactPoint)
	require.Contains(t, lines, fmt.Sprintf("%v{node=\"%v\"} %v", getPrometheusName(prefix, metrics.OriginUsedStreamIds), originEndpoint, 1))
	//require.Contains(t, lines, fmt.Sprintf("%v{node=\"%v\"} %v", getPrometheusName(prefix, metrics.TargetUsedStreamIds), targetEndpoint, 1))
	queryWg.Wait()
	lines = GatherMetrics(t, simulacronSetup.Proxy.Conf, true)
	require.Contains(t, lines, fmt.Sprintf("%v{node=\"%v\"} %v", getPrometheusName(prefix, metrics.OriginUsedStreamIds), originEndpoint, 0))
	//require.Contains(t, lines, fmt.Sprintf("%v{node=\"%v\"} %v", getPrometheusName(prefix, metrics.TargetUsedStreamIds), targetEndpoint, 0))
}

// primeClustersWithDelay will prime Origin and Target clusters to artificially cause a delay in the response
// for every query dispatched to the clusters
func primeClustersWithDelay(setup *setup.SimulacronTestSetup, query string) {
	queryPrimeDelayedResponse :=
		simulacron.WhenQuery(
			query,
			simulacron.NewWhenQueryOptions()).
			ThenSuccess().WithDelay(10 * time.Second)


	setup.Origin.Prime(queryPrimeDelayedResponse)
	setup.Target.Prime(queryPrimeDelayedResponse)
}

// executeQuery sends the query string in a Frame message through the test client and handles any failures internally
// by failing the tests, otherwise, returns the response to the caller
func executeQuery(t *testing.T, client *client.TestClient, query string) *frame.Frame {
	q := &message.Query{
		Query: query,
	}
	response, _, err := client.SendMessage(context.Background(), primitive.ProtocolVersion4, q)
	if err != nil {
		t.Fatal("query failed:", err)
	}
	return response
}