package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/riptano/cloud-gate/proxy/pkg/health"
	"github.com/stretchr/testify/require"
	"net/http"
	"strings"
	"testing"
	"time"
)

func RequireWithRetries(t *testing.T, f func() (err error, fatal bool), maxAttempts int, delayPerAttempt time.Duration) {
	var lastErr error
	for i := 0; i < maxAttempts; i++ {
		err, fatal := f()
		if err == nil {
			return
		}
		if fatal {
			require.FailNow(t, err.Error())
		} else {
			lastErr = err
			time.Sleep(delayPerAttempt)
		}
	}

	require.FailNowf(t, "requireWithRetries failed", "Failed retry assert after max attempts: %v", lastErr)
}

func GetMetrics(ipEndPoint string) (int, string, error) {
	rsp, err := http.Get(fmt.Sprintf("http://%s/metrics", ipEndPoint))
	if err != nil {
		return 0, "", fmt.Errorf("failed to get metrics: %w", err)
	}

	if rsp.StatusCode != http.StatusOK && rsp.StatusCode != http.StatusServiceUnavailable {
		return 0, "", fmt.Errorf("unexpected status code: %d", rsp.StatusCode)
	}

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(rsp.Body)
	if err != nil {
		return 0, "", fmt.Errorf("failed to read metrics response: %w", err)
	}

	return rsp.StatusCode, buf.String(), nil
}

func GetReadinessStatusReport(ipEndPoint string) (int, *health.StatusReport, error) {
	rsp, err := http.Get(fmt.Sprintf("http://%s/health/readiness", ipEndPoint))

	if err != nil {
		return 0, nil, fmt.Errorf("failed to get readiness response: %w", err)
	}

	if rsp.StatusCode != http.StatusOK && rsp.StatusCode != http.StatusServiceUnavailable {
		return 0, nil, fmt.Errorf("unexpected status code: %d", rsp.StatusCode)
	}

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(rsp.Body)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read readiness response: %w", err)
	}

	var report health.StatusReport
	err = json.Unmarshal(buf.Bytes(), &report)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to unmarshal readiness endpoint response: %w", err)
	}

	return rsp.StatusCode, &report, nil
}

func GetLivenessResponse(ipEndPoint string) (int, string, error) {
	rsp, err := http.Get(fmt.Sprintf("http://%s/health/liveness", ipEndPoint))

	if err != nil {
		return 0, "", fmt.Errorf("failed to get liveness response: %w", err)
	}

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(rsp.Body)
	if err != nil {
		return 0, "", fmt.Errorf("failed to read liveness response: %w", err)
	}

	return rsp.StatusCode, buf.String(), nil
}

func RequireMetricsEndpointResult(t *testing.T, httpAddr string, success bool) {
	statusCode, rspStr, err := GetMetrics(httpAddr)
	require.Nil(t, err, "failed to get metrics: %v", err)
	if success {
		require.Equal(t, http.StatusOK, statusCode)
		require.False(
			t,
			strings.Contains(rspStr,"Proxy metrics haven't been initialized yet."),
			"unexpected metrics msg: %v", rspStr)
	} else {
		require.Equal(t, http.StatusServiceUnavailable, statusCode)
		require.True(
			t,
			strings.Contains(rspStr,"Proxy metrics haven't been initialized yet."),
			"unexpected metrics msg: %v", rspStr)
	}
}