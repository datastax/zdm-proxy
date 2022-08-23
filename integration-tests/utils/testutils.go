package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/datastax/zdm-proxy/proxy/pkg/health"
	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"net/http"
	"strings"
	"sync"
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

func CheckMetricsEndpointResult(httpAddr string, success bool) error {
	statusCode, rspStr, err := GetMetrics(httpAddr)
	if err != nil {
		return err
	}
	expectedCode := http.StatusOK
	expectedUninitializedMetricsMsg := false
	if !success {
		expectedCode = http.StatusServiceUnavailable
		expectedUninitializedMetricsMsg = true
	}
	if expectedCode != statusCode {
		return fmt.Errorf("expected %v but got %v", expectedCode, statusCode)
	}
	if expectedUninitializedMetricsMsg != strings.Contains(rspStr, "Proxy metrics haven't been initialized yet.") {
		return fmt.Errorf("expected \"contains error message=\"%v but unexpected metrics msg: %v", expectedUninitializedMetricsMsg, rspStr)
	}
	return nil
}

// ConnectToCluster is used to connect to source and destination clusters
func ConnectToCluster(hostname string, username string, password string, port int) (*gocql.Session, error) {
	cluster := NewCluster(hostname, username, password, port)
	session, err := cluster.CreateSession()
	log.Debugf("Connection established with Cluster: %s:%d", cluster.Hosts[0], cluster.Port)
	if err != nil {
		return nil, err
	}
	return session, nil
}

// NewCluster initializes a ClusterConfig object with common settings
func NewCluster(hostname string, username string, password string, port int) *gocql.ClusterConfig {
	cluster := gocql.NewCluster(hostname)
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: username,
		Password: password,
	}
	cluster.Port = port
	return cluster
}

type ThreadsafeBuffer struct {
	b bytes.Buffer
	m sync.Mutex
}

func NewThreadsafeBuffer() *ThreadsafeBuffer {
	return &ThreadsafeBuffer{
		b: bytes.Buffer{},
		m: sync.Mutex{},
	}
}

func (b *ThreadsafeBuffer) Read(p []byte) (n int, err error) {
	b.m.Lock()
	defer b.m.Unlock()
	return b.b.Read(p)
}

func (b *ThreadsafeBuffer) Write(p []byte) (n int, err error) {
	b.m.Lock()
	defer b.m.Unlock()
	return b.b.Write(p)
}

func (b *ThreadsafeBuffer) String() string {
	b.m.Lock()
	defer b.m.Unlock()
	return b.b.String()
}
