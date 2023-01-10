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
	"strings"
	"sync"
	"testing"
	"time"
)

type resources struct {
	setup  *setup.SimulacronTestSetup
	testClient *client.TestClient
	wg      *sync.WaitGroup
	metrics *http.Server
	close   func()
}

func TestStreamIdsMetrics(t *testing.T) {
	resources := setupResources(t)
	defer resources.close()

	assertUsedStreamIds := initAsserts(resources.setup)
	asyncQuery := asyncContextWrap(resources.testClient)

	t.Run("Test single SELECT statement", func(t *testing.T) {
		query := fmt.Sprintf("SELECT * FROM fake.%v", formatName(t))
		primeClustersWithDelay(resources.setup, query)

		wg := asyncQuery(t, query, 1)

		assertUsedStreamIds(t, resources.setup.Origin, 1)
		assertUsedStreamIds(t, resources.setup.Target, 0)
		wg.Wait()
		assertUsedStreamIds(t, resources.setup.Origin, 0)
		assertUsedStreamIds(t, resources.setup.Target, 0)
	})

	t.Run("Test single INSERT statement", func(t *testing.T) {
		query := fmt.Sprintf("INSERT INTO fake.%v (a, b) VALUES (1, 2)",  formatName(t))
		primeClustersWithDelay(resources.setup, query)

		wg := asyncQuery(t, query, 1)

		assertUsedStreamIds(t, resources.setup.Origin, 1)
		assertUsedStreamIds(t, resources.setup.Target, 1)
		wg.Wait()
		assertUsedStreamIds(t, resources.setup.Origin, 0)
		assertUsedStreamIds(t, resources.setup.Target, 0)
	})

	t.Run("Test multiple INSERT statements", func(t *testing.T) {
		query := fmt.Sprintf("INSERT INTO fake.%v (a, b) VALUES (1, 2)", formatName(t))
		primeClustersWithDelay(resources.setup, query)

		repeat := 50
		wg := asyncQuery(t, query, repeat)

		assertUsedStreamIds(t, resources.setup.Origin, repeat)
		assertUsedStreamIds(t, resources.setup.Target, repeat)
		wg.Wait()
		assertUsedStreamIds(t, resources.setup.Origin, 0)
		assertUsedStreamIds(t, resources.setup.Target, 0)
	})

	t.Run("Test multiple SELECT statements", func(t *testing.T) {
		query := fmt.Sprintf("SELECT * FROM fake.%v", formatName(t))
		primeClustersWithDelay(resources.setup, query)

		repeat := 50
		wg := asyncQuery(t, query, repeat)

		assertUsedStreamIds(t, resources.setup.Origin, repeat)
		assertUsedStreamIds(t, resources.setup.Target, 0)
		wg.Wait()
		assertUsedStreamIds(t, resources.setup.Origin, 0)
		assertUsedStreamIds(t, resources.setup.Target, 0)
	})

	t.Run("Test mixed SELECT/INSERT statements", func(t *testing.T) {
		querySelect := fmt.Sprintf("SELECT * FROM fake.%v", formatName(t))
		queryInsert := fmt.Sprintf("INSERT INTO fake.%v (a, b) VALUES (1, 2)", formatName(t))
		primeClustersWithDelay(resources.setup, querySelect)
		primeClustersWithDelay(resources.setup, queryInsert)

		repeat := 50
		wgSelect := asyncQuery(t, querySelect, repeat)
		wgInsert := asyncQuery(t, queryInsert, repeat)

		assertUsedStreamIds(t, resources.setup.Origin, repeat*2)
		assertUsedStreamIds(t, resources.setup.Target, repeat)
		wgSelect.Wait()
		wgInsert.Wait()
		assertUsedStreamIds(t, resources.setup.Origin, 0)
		assertUsedStreamIds(t, resources.setup.Target, 0)
	})

}

// asyncContextWrap is a higher-order function that holds a reference to the test client and returns a function that
// actually executes the query in an asynchronous fashion and returns an WaitGroup for synchronization
func asyncContextWrap(testClient *client.TestClient) func(t *testing.T, query string, repeat int) *sync.WaitGroup {
	run := func(t *testing.T, query string, repeat int) *sync.WaitGroup {
		// WaitGroup for controlling the dispatched/sent queries
		dispatchedWg := &sync.WaitGroup{}
		// WaitGroup for controlling the return of the response
		returnedWg := &sync.WaitGroup{}
		dispatchedWg.Add(repeat)
		returnedWg.Add(repeat)
		for i := 0; i < repeat; i++ {
			go func(testClient *client.TestClient, dispatched *sync.WaitGroup, returned *sync.WaitGroup) {
				defer returnedWg.Done()
				dispatchedWg.Done()
				executeQuery(t, testClient, query)
			}(testClient, dispatchedWg, returnedWg)
		}
		dispatchedWg.Wait()
		time.Sleep(1*time.Second) // give it some time to update the internal structs in the proxy for all the requests
		return returnedWg
	}
	return run
}

// setupResources initializes all the required resources, including the metrics server, that's shared across the
// tests
func setupResources(t *testing.T) *resources {
	metricsHandler, _ := runner.SetupHandlers()
	simulacronSetup, err := setup.NewSimulacronTestSetup(t)
	require.Nil(t, err)


	testClient, err := client.NewTestClientWithRequestTimeout(context.Background(), "127.0.0.1:14002", 10 * time.Second)
	require.Nil(t, err)
	testClient.PerformDefaultHandshake(context.Background(), primitive.ProtocolVersion3, false)

	wg := &sync.WaitGroup{}
	srv := startMetricsHandler(t, simulacronSetup.Proxy.Conf, wg, metricsHandler)

	EnsureMetricsServerListening(t, simulacronSetup.Proxy.Conf)

	return &resources{
		setup:   simulacronSetup,
		testClient:  testClient,
		wg:      wg,
		metrics: srv,
		close: func(){
			simulacronSetup.Cleanup()
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
			ThenSuccess().WithDelay(3 * time.Second)

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
func initAsserts(setup *setup.SimulacronTestSetup) func (t *testing.T, cluster *simulacron.Cluster, expectedValue int) {
	asserts := func (t *testing.T, cluster *simulacron.Cluster, expectedValue int) {
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

// formatName returns a string suitable for usage in a CQL statement based on the test name
func formatName(t *testing.T) string {
	return strings.ReplaceAll(t.Name(), "/", "_")
}