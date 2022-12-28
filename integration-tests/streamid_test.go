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
	"net/http"
	"sync"
	"testing"
	"time"
)

type resources struct {
	setup  *setup.SimulacronTestSetup
	client *client.TestClient
	wg      *sync.WaitGroup
	metrics *http.Server
	close   func()
}

func TestStreamIdReads(t *testing.T) {
	resources := setupResources(t)
	defer resources.close()

	assertUsedStreamIds := initAsserts(t, resources.setup)
	asyncQuery := asyncRunner(t, resources.client)

	t.Run("Test SELECT statements", func(t *testing.T) {
		query := "SELECT * FROM fake.tbl"
		primeClustersWithDelay(resources.setup, query)

		wg := asyncQuery(query)
		time.Sleep(2*time.Second)

		assertUsedStreamIds(resources.setup.Origin, 1)
		assertUsedStreamIds(resources.setup.Target, 0)
		wg.Wait()
		assertUsedStreamIds(resources.setup.Origin, 0)
		assertUsedStreamIds(resources.setup.Target, 0)
	})

	t.Run("Test INSERT statements", func(t *testing.T) {
		query := "INSERT INTO test (a, b) VALUES (1, 2)"
		primeClustersWithDelay(resources.setup, query)

		wg := asyncQuery(query)
		time.Sleep(2*time.Second)

		assertUsedStreamIds(resources.setup.Origin, 1)
		assertUsedStreamIds(resources.setup.Target, 1)
		wg.Wait()
		assertUsedStreamIds(resources.setup.Origin, 0)
		assertUsedStreamIds(resources.setup.Target, 0)
	})

}

// asyncRunner is a higher-order function that holds a reference to the test client and returns a function that
// actually executes the query in an asynchronous fashion and returns an WaitGroup for synchronization
func asyncRunner(t *testing.T, client *client.TestClient) func(query string) *sync.WaitGroup {
	run := func(query string) *sync.WaitGroup {
		queryWg := &sync.WaitGroup{}
		queryWg.Add(1)
		go func() {
			defer queryWg.Done()
			executeQuery(t, client, query)
		}()
		return queryWg
	}
	return run
}

// setupResources initializes all the required resources, including the metrics server, that's shared across the
// tests
func setupResources(t *testing.T) *resources {
	metricsHandler, _ := runner.SetupHandlers()
	config := setup.NewTestConfig("", "")
	config.ProxyRequestTimeoutMs = int(20 * time.Second)
	simulacronSetup, err := setup.NewSimulacronTestSetupWithConfig(t, config)
	require.Nil(t, err)

	testClient, err := client.NewTestClientWithRequestTimeout(context.Background(), "127.0.0.1:14002", 20 * time.Second)
	if err != nil {
		t.Log("Unable to connect to proxy session.")
		t.Fatal(err)
	}

	wg := &sync.WaitGroup{}
	srv := startMetricsHandler(t, simulacronSetup.Proxy.Conf, wg, metricsHandler)

	EnsureMetricsServerListening(t, simulacronSetup.Proxy.Conf)

	return &resources{
		setup:   simulacronSetup,
		client:  testClient,
		wg:      wg,
		metrics: srv,
		close: func(){
			testClient.Shutdown()
			simulacronSetup.Cleanup()
			testClient.Shutdown()
			err := srv.Close()
			if err != nil {
				log.Warnf("error cleaning metrics server: %v", err)
			}
			wg.Wait()
		},
	}
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

// initAsserts is a higher-order function that holds a reference to the setup structs which
// allows the inner function to take only the necessary assertion params
func initAsserts(t *testing.T, setup *setup.SimulacronTestSetup) func (cluster *simulacron.Cluster, expectedValue int) {
	asserts := func (cluster *simulacron.Cluster, expectedValue int) {
		lines := GatherMetrics(t, setup.Proxy.Conf, true)
		prefix := "zdm"
		endpoint := fmt.Sprintf("%v:9042", cluster.InitialContactPoint)
		require.Contains(t, lines, fmt.Sprintf("%v{node=\"%v\"} %v", getPrometheusName(prefix, metricsFor(cluster, setup)), endpoint, expectedValue))
	}
	return asserts
}

// metricsFor returns the proper stream id metric for the given cluster
func metricsFor(cluster *simulacron.Cluster, setup *setup.SimulacronTestSetup) metrics.Metric {
	if cluster == setup.Origin {
		return metrics.OriginUsedStreamIds
	}
	return metrics.TargetUsedStreamIds
}