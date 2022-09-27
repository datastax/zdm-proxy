package integration_tests

import (
	"context"
	"fmt"
	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/integration-tests/utils"
	"github.com/datastax/zdm-proxy/proxy/pkg/config"
	"github.com/datastax/zdm-proxy/proxy/pkg/health"
	"github.com/datastax/zdm-proxy/proxy/pkg/httpzdmproxy"
	"github.com/datastax/zdm-proxy/proxy/pkg/metrics"
	"github.com/datastax/zdm-proxy/proxy/pkg/runner"
	"github.com/stretchr/testify/require"
	"net/http"
	"sync"
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
}

func testHttpEndpointsWithProxyNotInitialized(
	t *testing.T, metricsHandler *httpzdmproxy.HandlerWithFallback, healthHandler *httpzdmproxy.HandlerWithFallback) {

	simulacronSetup, err := setup.NewSimulacronTestSetupWithSession(false, false)
	require.Nil(t, err)
	defer simulacronSetup.Cleanup()

	err = simulacronSetup.Origin.DisableConnectionListener()
	require.Nil(t, err, "origin disable listener failed: %v", err)
	err = simulacronSetup.Target.DisableConnectionListener()
	require.Nil(t, err, "target disable listener failed: %v", err)

	conf := setup.NewTestConfig(simulacronSetup.Origin.GetInitialContactPoint(), simulacronSetup.Target.GetInitialContactPoint())
	modifyConfForHealthTests(conf)

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

	simulacronSetup, err := setup.NewSimulacronTestSetupWithSession(false, false)
	require.Nil(t, err)
	defer simulacronSetup.Cleanup()

	conf := setup.NewTestConfig(simulacronSetup.Origin.GetInitialContactPoint(), simulacronSetup.Target.GetInitialContactPoint())
	modifyConfForHealthTests(conf)

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
	}, 10, 100 * time.Millisecond)

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

func testHttpEndpointsWithUnavailableNode(
	t *testing.T, metricsHandler *httpzdmproxy.HandlerWithFallback, healthHandler *httpzdmproxy.HandlerWithFallback) {

	simulacronSetup, err := setup.NewSimulacronTestSetupWithSession(false, false)
	require.Nil(t, err)
	defer simulacronSetup.Cleanup()

	conf := setup.NewTestConfig(simulacronSetup.Origin.GetInitialContactPoint(), simulacronSetup.Target.GetInitialContactPoint())
	modifyConfForHealthTests(conf)

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
	}, 10, 100 * time.Millisecond)

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
			return fmt.Errorf("expected current failure count on origin greater than 0 but got %v", r.OriginStatus.CurrentFailureCount), false
		}

		*healthReportPtr = r
		*statusCodePtr = statusCode
		return nil, false
	}, 100, 50*time.Millisecond)

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

		*healthReportPtr = r
		*statusCodePtr = statusCode
		return nil, false
	}, 100, 50*time.Millisecond)

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

func modifyConfForHealthTests(config *config.Config) {
	config.HeartbeatRetryIntervalMinMs = 250
	config.HeartbeatRetryIntervalMaxMs = 500
	config.HeartbeatIntervalMs = 500
	config.OriginConnectionTimeoutMs = 2000
	config.TargetConnectionTimeoutMs = 2000
	config.HeartbeatFailureThreshold = 2
}
