package integration_tests

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/require"

	"github.com/datastax/zdm-proxy/integration-tests/client"
	"github.com/datastax/zdm-proxy/integration-tests/env"
	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/integration-tests/simulacron"
	"github.com/datastax/zdm-proxy/proxy/pkg/metrics"
	"github.com/datastax/zdm-proxy/proxy/pkg/runner"
)

type resources struct {
	setup      *setup.SimulacronTestSetup
	testClient *client.TestClient
	close      func()
}

func TestStreamIdsMetrics(t *testing.T) {
	testCases := []struct {
		name                  string
		queries               []string
		repeat                int
		expectedUsedOriginIds int
		expectedUsedTargetIds int
	}{
		{
			name:                  "Test single SELECT statement",
			queries:               []string{"SELECT * FROM fake.%v"},
			repeat:                1,
			expectedUsedOriginIds: 1,
			expectedUsedTargetIds: 0,
		},
		{
			name:                  "Test single INSERT statement",
			queries:               []string{"INSERT INTO fake.%v (a, b) VALUES (1, 2)"},
			repeat:                1,
			expectedUsedOriginIds: 1,
			expectedUsedTargetIds: 1,
		},
		{
			name:                  "Test multiple INSERT statements",
			queries:               []string{"INSERT INTO fake.%v (a, b) VALUES (1, 2)"},
			repeat:                50,
			expectedUsedOriginIds: 50,
			expectedUsedTargetIds: 50,
		},
		{
			name:                  "Test multiple SELECT statements",
			queries:               []string{"SELECT * FROM fake.%v"},
			repeat:                50,
			expectedUsedOriginIds: 50,
			expectedUsedTargetIds: 0,
		},
		{
			name: "Test mixed SELECT/INSERT statements",
			queries: []string{
				"SELECT * FROM fake.%v",
				"INSERT INTO fake.%v (a, b) VALUES (1, 2)"},
			repeat:                50,
			expectedUsedOriginIds: 100,
			expectedUsedTargetIds: 50,
		},
	}

	testSetup, err := setup.NewSimulacronTestSetupWithSession(t, false, false)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	metricsHandler, _ := runner.SetupHandlers()
	wg := &sync.WaitGroup{}
	defaultConf := setup.NewTestConfig("", "")
	srv := startMetricsHandler(t, defaultConf, wg, metricsHandler)

	EnsureMetricsServerListening(t, defaultConf)

	defer func() {
		err := srv.Close()
		if err != nil {
			t.Logf("error cleaning metrics server: %v", err)
		}
		wg.Wait()
	}()

	t.Run("ParallelGroup", func(t *testing.T) {
		for i, tc := range testCases {
			testCase := tc
			index := i
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				metricsPrefix := fmt.Sprintf("zdm_%v", index)
				resources := setupResources(t, testSetup, metricsPrefix, 9042+index)
				defer resources.close()

				assertUsedStreamIds := initAsserts(resources.setup, metricsPrefix)
				asyncQuery := asyncContextWrap(env.DefaultProtocolVersionSimulacron, resources.testClient)
				for idx, query := range testCase.queries {
					replacedQuery := fmt.Sprintf(query, formatName(t))
					testCase.queries[idx] = replacedQuery
					primeClustersWithDelay(resources.setup, replacedQuery)
				}
				waitGroups := make([]*sync.WaitGroup, 0, len(testCase.queries))
				for _, query := range testCase.queries {
					waitGroups = append(waitGroups, asyncQuery(t, query, testCase.repeat))
				}
				assertUsedStreamIds(t, resources.setup.Origin, testCase.expectedUsedOriginIds)
				assertUsedStreamIds(t, resources.setup.Target, testCase.expectedUsedTargetIds)
				for _, wg := range waitGroups {
					wg.Wait()
				}
				assertUsedStreamIds(t, resources.setup.Origin, 0)
				assertUsedStreamIds(t, resources.setup.Target, 0)
			})
		}
	})
}

// asyncContextWrap is a higher-order function that holds a reference to the test client and returns a function that
// actually executes the query in an asynchronous fashion and returns an WaitGroup for synchronization
func asyncContextWrap(version primitive.ProtocolVersion, testClient *client.TestClient) func(t *testing.T, query string, repeat int) *sync.WaitGroup {
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
				executeQuery(t, version, testClient, query)
			}(testClient, dispatchedWg, returnedWg)
		}
		dispatchedWg.Wait()
		time.Sleep(1 * time.Second) // give it some time to update the internal structs in the proxy for all the requests
		return returnedWg
	}
	return run
}

// setupResources initializes all the required resources, including the metrics server, that's shared across the
// tests
func setupResources(t *testing.T, testSetup *setup.SimulacronTestSetup, metricsPrefix string, proxyPort int) *resources {
	cfg := setup.NewTestConfig("", "")
	cfg.MetricsPrefix = metricsPrefix
	cfg.OriginContactPoints = testSetup.Origin.GetInitialContactPoint()
	cfg.TargetContactPoints = testSetup.Target.GetInitialContactPoint()
	cfg.ProxyListenPort = proxyPort
	proxyInstance, err := setup.NewProxyInstanceWithConfig(cfg)
	require.Nil(t, err)

	testClient, err := client.NewTestClientWithRequestTimeout(context.Background(), fmt.Sprintf("127.0.0.1:%v", proxyPort), 10*time.Second)
	require.Nil(t, err)
	testClient.PerformDefaultHandshake(context.Background(), env.DefaultProtocolVersionSimulacron, false)

	return &resources{
		setup: &setup.SimulacronTestSetup{
			Origin: testSetup.Origin,
			Target: testSetup.Target,
			Proxy:  proxyInstance,
		},
		testClient: testClient,
		close: func() {
			proxyInstance.Shutdown()
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
func executeQuery(t *testing.T, version primitive.ProtocolVersion, client *client.TestClient, query string) *frame.Frame {
	q := &message.Query{
		Query: query,
	}
	response, _, err := client.SendMessage(context.Background(), version, q)
	if err != nil {
		t.Fatal("query failed:", err)
	}
	return response
}

// initAsserts is a higher-order function that holds a reference to the setup structs which
// allows the inner function to take only the necessary assertion params
func initAsserts(setup *setup.SimulacronTestSetup, prefix string) func(t *testing.T, cluster *simulacron.Cluster, expectedValue int) {
	asserts := func(t *testing.T, cluster *simulacron.Cluster, expectedValue int) {
		lines := GatherMetrics(t, setup.Proxy.Conf, true)
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
