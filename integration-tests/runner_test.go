package integration_tests

import (
	"context"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/zdm-proxy/integration-tests/client"
	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/integration-tests/utils"
	"github.com/datastax/zdm-proxy/proxy/pkg/config"
	"github.com/datastax/zdm-proxy/proxy/pkg/health"
	"github.com/datastax/zdm-proxy/proxy/pkg/httpzdmproxy"
	"github.com/datastax/zdm-proxy/proxy/pkg/metrics"
	"github.com/datastax/zdm-proxy/proxy/pkg/runner"
	"github.com/datastax/zdm-proxy/proxy/pkg/zdmproxy"
	"github.com/jpillora/backoff"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

/*
This file contains tests that require the http endpoints. Because the http handlers are global,
they are registered separately on the parent test.
*/

func TestWithHttpHandlers(t *testing.T) {
	metricsHandler, readinessHandler := runner.SetupHandlers()

	t.Run("testMetrics", func(t *testing.T) {
		testMetrics(t, metricsHandler)
	})

	metricsHandler.SetHandler(metrics.DefaultHttpHandler())

	t.Run("testHttpEndpointsWithProxyNotInitialized", func(t *testing.T) {
		testHttpEndpointsWithProxyNotInitialized(t, metricsHandler, readinessHandler)
	})

	t.Run("testHttpEndpointsWithProxyInitialized", func(t *testing.T) {
		testHttpEndpointsWithProxyInitialized(t, metricsHandler, readinessHandler)
	})

	t.Run("testHttpEndpointsWithUnavailableNode", func(t *testing.T) {
		testHttpEndpointsWithUnavailableNode(t, metricsHandler, readinessHandler)
	})

	t.Run("testMetricsWithUnavailableNode", func(t *testing.T) {
		testMetricsWithUnavailableNode(t, metricsHandler)
	})
}

func testHttpEndpointsWithProxyNotInitialized(
	t *testing.T, metricsHandler *httpzdmproxy.HandlerWithFallback, healthHandler *httpzdmproxy.HandlerWithFallback) {

	simulacronSetup, err := setup.NewSimulacronTestSetupWithSession(t, false, false)
	require.Nil(t, err)
	defer simulacronSetup.Cleanup()

	err = simulacronSetup.Origin.DisableConnectionListener()
	require.Nil(t, err, "origin disable listener failed: %v", err)
	err = simulacronSetup.Target.DisableConnectionListener()
	require.Nil(t, err, "target disable listener failed: %v", err)

	conf := setup.NewTestConfig(simulacronSetup.Origin.GetInitialContactPoint(), simulacronSetup.Target.GetInitialContactPoint())
	modifyConfForHealthTests(conf, 2)

	ctx, cancelFunc := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	defer func() {
		cancelFunc()
		wg.Wait()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		runner.RunMain(conf, ctx, metricsHandler, healthHandler)
	}()

	time.Sleep(500 * time.Millisecond)

	httpAddr := fmt.Sprintf("%s:%d", conf.MetricsAddress, conf.MetricsPort)

	require.Nil(t, utils.CheckMetricsEndpointResult(httpAddr, false))

	statusCode, report, err := utils.GetReadinessStatusReport(httpAddr)
	require.Equal(t, http.StatusServiceUnavailable, statusCode)
	require.Nil(t, err, "failed to get readiness response: %v", err)
	require.Nil(t, report.OriginStatus)
	require.Nil(t, report.TargetStatus)
	require.Equal(t, health.STARTUP, report.Status)
}

func testHttpEndpointsWithProxyInitialized(
	t *testing.T, metricsHandler *httpzdmproxy.HandlerWithFallback, healthHandler *httpzdmproxy.HandlerWithFallback) {

	simulacronSetup, err := setup.NewSimulacronTestSetupWithSession(t, false, false)
	require.Nil(t, err)
	defer simulacronSetup.Cleanup()

	conf := setup.NewTestConfig(simulacronSetup.Origin.GetInitialContactPoint(), simulacronSetup.Target.GetInitialContactPoint())
	modifyConfForHealthTests(conf, 2)

	ctx, cancelFunc := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	defer func() {
		cancelFunc()
		wg.Wait()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		runner.RunMain(conf, ctx, metricsHandler, healthHandler)
	}()

	httpAddr := fmt.Sprintf("%s:%d", conf.MetricsAddress, conf.MetricsPort)

	utils.RequireWithRetries(t, func() (err error, fatal bool) {
		fatal = false
		err = utils.CheckMetricsEndpointResult(httpAddr, true)
		return
	}, 10, 100*time.Millisecond)

	statusCode, report, err := utils.GetReadinessStatusReport(httpAddr)
	require.Nil(t, err, "failed to get readiness response: %v", err)
	require.Equal(t, http.StatusOK, statusCode)
	require.NotNil(t, report.OriginStatus)
	require.NotNil(t, report.TargetStatus)
	require.Equal(t, &health.ControlConnStatus{
		Addr:                  fmt.Sprintf("%s:%d", simulacronSetup.Origin.GetInitialContactPoint(), 9042),
		CurrentFailureCount:   0,
		FailureCountThreshold: conf.HeartbeatFailureThreshold,
		Status:                health.UP,
	}, report.OriginStatus)
	require.Equal(t, &health.ControlConnStatus{
		Addr:                  fmt.Sprintf("%s:%d", simulacronSetup.Target.GetInitialContactPoint(), 9042),
		CurrentFailureCount:   0,
		FailureCountThreshold: conf.HeartbeatFailureThreshold,
		Status:                health.UP,
	}, report.TargetStatus)
	require.Equal(t, health.UP, report.Status)
}

func testMetricsWithUnavailableNode(
	t *testing.T, metricsHandler *httpzdmproxy.HandlerWithFallback) {

	simulacronSetup, err := setup.NewSimulacronTestSetupWithSession(t, false, false)
	require.Nil(t, err)
	defer simulacronSetup.Cleanup()

	conf := setup.NewTestConfig(simulacronSetup.Origin.GetInitialContactPoint(), simulacronSetup.Target.GetInitialContactPoint())
	modifyConfForHealthTests(conf, 2)

	waitGroup := &sync.WaitGroup{}
	ctx, cancelFunc := context.WithCancel(context.Background())

	defer waitGroup.Wait()
	defer cancelFunc()

	srv := httpzdmproxy.StartHttpServer(fmt.Sprintf("%s:%d", conf.MetricsAddress, conf.MetricsPort), waitGroup)
	defer func(srv *http.Server, ctx context.Context) {
		err := srv.Shutdown(ctx)
		if err != nil {
			log.Error("Failed to shutdown metrics server:", err.Error())
		}
	}(srv, ctx)

	b := &backoff.Backoff{
		Factor: 2,
		Jitter: false,
		Min:    100 * time.Millisecond,
		Max:    500 * time.Millisecond,
	}
	proxy := atomic.Value{}
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		p, err := zdmproxy.RunWithRetries(conf, ctx, b)
		if err == nil {
			metricsHandler.SetHandler(p.GetMetricHandler().GetHttpHandler())
			proxy.Store(&p)
			<-ctx.Done()
			p.Shutdown()
		}
	}()

	httpAddr := fmt.Sprintf("%s:%d", conf.MetricsAddress, conf.MetricsPort)

	// check that metrics endpoint has been initialized
	utils.RequireWithRetries(t, func() (err error, fatal bool) {
		fatal = false
		err = utils.CheckMetricsEndpointResult(httpAddr, true)
		return
	}, 10, 100*time.Millisecond)

	// stop origin cluster
	err = simulacronSetup.Origin.DisableConnectionListener()
	require.Nil(t, err, "failed to disable origin connection listener: %v", err)
	err = simulacronSetup.Origin.DropAllConnections()
	require.Nil(t, err, "failed to drop origin connections: %v", err)

	// send a request
	testClient, err := client.NewTestClient(context.Background(), "127.0.0.1:14002")
	require.Nil(t, err)
	queryMsg := &message.Query{
		Query: "SELECT * FROM table1",
	}
	_, _, _ = testClient.SendMessage(context.Background(), primitive.ProtocolVersion4, queryMsg)

	utils.RequireWithRetries(t, func() (err error, fatal bool) {
		// expect connection failure to origin cluster
		statusCode, rspStr, err := utils.GetMetrics(httpAddr)
		require.Nil(t, err)
		require.Equal(t, http.StatusOK, statusCode)
		originEndpoint := fmt.Sprintf("%v:9042", simulacronSetup.Origin.GetInitialContactPoint())
		// search for:
		// zdm_proxy_failed_connections_total{cluster="origin"} 1
		// zdm_origin_failed_connections_total{node="127.0.0.40:9042"} 1
		if !strings.Contains(rspStr, fmt.Sprintf("%v 1", getPrometheusName("zdm", metrics.FailedConnectionsOrigin))) {
			err = fmt.Errorf("did not observe failed connection attempts at proxy metric")
		} else if !strings.Contains(rspStr, fmt.Sprintf("%v 1", getPrometheusNameWithNodeLabel("zdm", metrics.FailedOriginConnections, originEndpoint))) {
			err = fmt.Errorf("did not observe failed connection attempts at node metric")
		} else {
			err = nil
		}
		return
	}, 10, 500*time.Millisecond)
}

func testHttpEndpointsWithUnavailableNode(
	t *testing.T, metricsHandler *httpzdmproxy.HandlerWithFallback, healthHandler *httpzdmproxy.HandlerWithFallback) {

	simulacronSetup, err := setup.NewSimulacronTestSetupWithSession(t, false, false)
	require.Nil(t, err)
	defer simulacronSetup.Cleanup()

	conf := setup.NewTestConfig(simulacronSetup.Origin.GetInitialContactPoint(), simulacronSetup.Target.GetInitialContactPoint())
	modifyConfForHealthTests(conf, 5)

	ctx, cancelFunc := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	defer func() {
		cancelFunc()
		wg.Wait()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		runner.RunMain(conf, ctx, metricsHandler, healthHandler)
	}()

	httpAddr := fmt.Sprintf("%s:%d", conf.MetricsAddress, conf.MetricsPort)

	utils.RequireWithRetries(t, func() (err error, fatal bool) {
		fatal = false
		err = utils.CheckMetricsEndpointResult(httpAddr, true)
		return
	}, 10, 100*time.Millisecond)

	statusCode, msg, err := utils.GetLivenessResponse(httpAddr)
	require.Nil(t, err)
	require.Equal(t, http.StatusOK, statusCode)
	require.Equal(t, "OK", msg)

	statusCode, report, err := utils.GetReadinessStatusReport(httpAddr)
	require.Nil(t, err, "failed to get health report: %v", err)
	require.Equal(t, http.StatusOK, statusCode)
	require.Equal(t, health.UP, report.Status)

	// stop origin node
	err = simulacronSetup.Origin.DisableConnectionListener()
	require.Nil(t, err, "failed to disable origin connection listener: %v", err)
	err = simulacronSetup.Origin.DropAllConnections()
	require.Nil(t, err, "failed to drop origin connections: %v", err)

	healthReportPtr := new(*health.StatusReport)
	statusCodePtr := new(int)

	// check health report is OK when failure count is less than threshold
	utils.RequireWithRetries(t, func() (error, bool) {
		statusCode, r, err := utils.GetReadinessStatusReport(httpAddr)
		if err != nil {
			return fmt.Errorf("failed to get health report: %w", err), true
		}

		if r.OriginStatus.CurrentFailureCount == 0 {
			return fmt.Errorf("expected current failure count on origin greater than 0 but got %v",
				r.OriginStatus.CurrentFailureCount), false
		}

		if r.OriginStatus.CurrentFailureCount >= r.OriginStatus.FailureCountThreshold {
			return fmt.Errorf("expected current failure count on origin less than threshold (%v) but got %v",
				r.OriginStatus.FailureCountThreshold, r.OriginStatus.CurrentFailureCount), true
		}

		*healthReportPtr = r
		*statusCodePtr = statusCode
		return nil, false
	}, 20, 100*time.Millisecond)

	healthReport := *healthReportPtr
	statusCode = *statusCodePtr
	require.Equal(t, http.StatusOK, statusCode)
	require.Equal(t, health.UP, healthReport.Status)

	require.Equal(t, health.UP, healthReport.OriginStatus.Status)
	require.Equal(t, conf.HeartbeatFailureThreshold, healthReport.OriginStatus.FailureCountThreshold)
	require.Greater(t, healthReport.OriginStatus.CurrentFailureCount, 0)
	require.Less(t, healthReport.OriginStatus.CurrentFailureCount, healthReport.OriginStatus.FailureCountThreshold)

	require.Equal(t, health.UP, healthReport.TargetStatus.Status)
	require.Equal(t, conf.HeartbeatFailureThreshold, healthReport.TargetStatus.FailureCountThreshold)
	require.Equal(t, 0, healthReport.TargetStatus.CurrentFailureCount)

	require.Nil(t, utils.CheckMetricsEndpointResult(httpAddr, true))

	healthReportPtr = new(*health.StatusReport)
	statusCodePtr = new(int)

	// check health report fails when failure count is greater or equal than threshold
	utils.RequireWithRetries(t, func() (error, bool) {
		statusCode, r, err := utils.GetReadinessStatusReport(httpAddr)
		if err != nil {
			return fmt.Errorf("failed to get readiness response: %w", err), true
		}

		if r.OriginStatus.CurrentFailureCount < r.OriginStatus.FailureCountThreshold {
			return fmt.Errorf("expected current failure count on origin greater than threshold but got %v", r.OriginStatus.CurrentFailureCount), false
		}

		*healthReportPtr = r
		*statusCodePtr = statusCode
		return nil, false
	}, 200, 100*time.Millisecond)

	healthReport = *healthReportPtr
	statusCode = *statusCodePtr
	require.Equal(t, http.StatusServiceUnavailable, statusCode)
	require.Equal(t, health.DOWN, healthReport.Status)

	require.Equal(t, health.DOWN, healthReport.OriginStatus.Status)
	require.Equal(t, conf.HeartbeatFailureThreshold, healthReport.OriginStatus.FailureCountThreshold)
	require.GreaterOrEqual(t, healthReport.OriginStatus.CurrentFailureCount, healthReport.OriginStatus.FailureCountThreshold)

	require.Equal(t, health.UP, healthReport.TargetStatus.Status)
	require.Equal(t, conf.HeartbeatFailureThreshold, healthReport.TargetStatus.FailureCountThreshold)
	require.Equal(t, 0, healthReport.TargetStatus.CurrentFailureCount)

	require.Nil(t, utils.CheckMetricsEndpointResult(httpAddr, true))

	// stop target node
	err = simulacronSetup.Target.DisableConnectionListener()
	require.Nil(t, err, "failed to disable target connection listener: %v", err)
	err = simulacronSetup.Target.DropAllConnections()
	require.Nil(t, err, "failed to drop target connections: %v", err)

	healthReportPtr = new(*health.StatusReport)
	statusCodePtr = new(int)

	// check health report of target is OK when failure count is less than threshold
	utils.RequireWithRetries(t, func() (error, bool) {
		statusCode, r, err := utils.GetReadinessStatusReport(httpAddr)
		if err != nil {
			return fmt.Errorf("failed to get readiness response: %w", err), true
		}

		if r.TargetStatus.CurrentFailureCount == 0 {
			return fmt.Errorf("expected current failure count on target greater than 0 but got %v", r.TargetStatus.CurrentFailureCount), false
		}

		if r.TargetStatus.CurrentFailureCount >= r.TargetStatus.FailureCountThreshold {
			return fmt.Errorf("expected current failure count on target less than threshold (%v) but got %v",
				r.TargetStatus.FailureCountThreshold, r.TargetStatus.CurrentFailureCount), true
		}

		*healthReportPtr = r
		*statusCodePtr = statusCode
		return nil, false
	}, 20, 100*time.Millisecond)

	healthReport = *healthReportPtr
	statusCode = *statusCodePtr
	require.Equal(t, http.StatusServiceUnavailable, statusCode)
	require.Equal(t, health.DOWN, healthReport.Status)

	require.Equal(t, health.DOWN, healthReport.OriginStatus.Status)
	require.Equal(t, conf.HeartbeatFailureThreshold, healthReport.OriginStatus.FailureCountThreshold)
	require.GreaterOrEqual(t, healthReport.OriginStatus.CurrentFailureCount, healthReport.OriginStatus.FailureCountThreshold)

	require.Equal(t, health.UP, healthReport.TargetStatus.Status)
	require.Equal(t, conf.HeartbeatFailureThreshold, healthReport.TargetStatus.FailureCountThreshold)
	require.Greater(t, healthReport.TargetStatus.CurrentFailureCount, 0)
	require.Less(t, healthReport.TargetStatus.CurrentFailureCount, healthReport.TargetStatus.FailureCountThreshold)

	require.Nil(t, utils.CheckMetricsEndpointResult(httpAddr, true))

	healthReportPtr = new(*health.StatusReport)
	statusCodePtr = new(int)

	// check health report fails when failure count is greater or equal than threshold
	utils.RequireWithRetries(t, func() (error, bool) {
		statusCode, r, err := utils.GetReadinessStatusReport(httpAddr)
		if err != nil {
			return fmt.Errorf("failed to get health report: %w", err), true
		}

		if r.TargetStatus.CurrentFailureCount < r.TargetStatus.FailureCountThreshold {
			return fmt.Errorf("expected current failure count on target greater than threshold but got %v", r.TargetStatus.CurrentFailureCount), false
		}

		*healthReportPtr = r
		*statusCodePtr = statusCode
		return nil, false
	}, 200, 100*time.Millisecond)

	healthReport = *healthReportPtr
	statusCode = *statusCodePtr
	require.Equal(t, http.StatusServiceUnavailable, statusCode)
	require.Equal(t, health.DOWN, healthReport.Status)

	require.Equal(t, health.DOWN, healthReport.OriginStatus.Status)
	require.Equal(t, conf.HeartbeatFailureThreshold, healthReport.OriginStatus.FailureCountThreshold)
	require.GreaterOrEqual(t, healthReport.OriginStatus.CurrentFailureCount, healthReport.OriginStatus.FailureCountThreshold)

	require.Equal(t, health.DOWN, healthReport.TargetStatus.Status)
	require.Equal(t, conf.HeartbeatFailureThreshold, healthReport.TargetStatus.FailureCountThreshold)
	require.GreaterOrEqual(t, healthReport.TargetStatus.CurrentFailureCount, healthReport.TargetStatus.FailureCountThreshold)

	require.Nil(t, utils.CheckMetricsEndpointResult(httpAddr, true))
}

func modifyConfForHealthTests(config *config.Config, failureThreshold int) {
	config.HeartbeatRetryIntervalMinMs = 250
	config.HeartbeatRetryIntervalMaxMs = 500
	config.HeartbeatIntervalMs = 500
	config.OriginConnectionTimeoutMs = 2000
	config.TargetConnectionTimeoutMs = 2000
	config.HeartbeatFailureThreshold = failureThreshold
}
