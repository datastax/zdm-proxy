package integration_tests

import (
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/integration-tests/utils"
	"github.com/datastax/zdm-proxy/proxy/pkg/config"
	"github.com/datastax/zdm-proxy/proxy/pkg/httpzdmproxy"
	"github.com/datastax/zdm-proxy/proxy/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var nodeMetrics = []metrics.Metric{
	metrics.OriginRequestDuration,
	metrics.TargetRequestDuration,

	metrics.OriginClientTimeouts,
	metrics.OriginWriteTimeouts,
	metrics.OriginReadTimeouts,
	metrics.OriginUnpreparedErrors,
	metrics.OriginOtherErrors,

	metrics.TargetClientTimeouts,
	metrics.TargetWriteTimeouts,
	metrics.TargetReadTimeouts,
	metrics.TargetUnpreparedErrors,
	metrics.TargetOtherErrors,

	metrics.OpenOriginConnections,
	metrics.OpenTargetConnections,
}

var proxyMetrics = []metrics.Metric{
	metrics.FailedRequestsBothFailedOnTargetOnly,
	metrics.FailedRequestsBothFailedOnOriginOnly,
	metrics.FailedRequestsBoth,
	metrics.FailedRequestsOrigin,
	metrics.FailedRequestsTarget,

	metrics.PSCacheSize,
	metrics.PSCacheMissCount,

	metrics.ProxyRequestDurationTarget,
	metrics.ProxyRequestDurationOrigin,
	metrics.ProxyRequestDurationBoth,

	metrics.InFlightRequestsTarget,
	metrics.InFlightRequestsOrigin,
	metrics.InFlightRequestsBoth,

	metrics.OpenClientConnections,
}

var allMetrics = append(proxyMetrics, nodeMetrics...)

var insertQuery = frame.NewFrame(
	primitive.ProtocolVersion4,
	client.ManagedStreamId,
	&message.Query{Query: "INSERT INTO ks1.t1"},
)

var selectQuery = frame.NewFrame(
	primitive.ProtocolVersion4,
	client.ManagedStreamId,
	&message.Query{Query: "SELECT * FROM ks1.t1"},
)

func testMetrics(t *testing.T, metricsHandler *httpzdmproxy.HandlerWithFallback) {

	tests := []struct{
		name                  string
		dualReadsEnabled      bool
		asyncReadsOnSecondary bool
	}{
		{
			name:                  "default",
			dualReadsEnabled:      false,
			asyncReadsOnSecondary: false,
		},
		{
			name:                  "dual reads, async on secondary",
			dualReadsEnabled:      true,
			asyncReadsOnSecondary: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			originHost := "127.0.1.1"
			targetHost := "127.0.1.2"
			originEndpoint := fmt.Sprintf("%v:9042", originHost)
			targetEndpoint := fmt.Sprintf("%v:9042", targetHost)
			conf := setup.NewTestConfig(originHost, targetHost)
			conf.DualReadsEnabled = test.dualReadsEnabled
			conf.AsyncReadsOnSecondary = test.asyncReadsOnSecondary

			expectedAsyncConnections := 0
			if conf.AsyncReadsOnSecondary {
				expectedAsyncConnections = 1
			}

			asyncEndpoint := targetEndpoint
			if conf.ForwardReadsToTarget {
				asyncEndpoint = originEndpoint
			}

			testSetup, err := setup.NewCqlServerTestSetup(conf, false, false, false)
			require.Nil(t, err)
			defer testSetup.Cleanup()
			testSetup.Origin.CqlServer.RequestHandlers = []client.RequestHandler{client.RegisterHandler, client.HeartbeatHandler, client.HandshakeHandler, client.NewSystemTablesHandler("cluster1", "dc1"), handleReads, handleWrites}
			testSetup.Target.CqlServer.RequestHandlers = []client.RequestHandler{client.RegisterHandler, client.HeartbeatHandler, client.HandshakeHandler, client.NewSystemTablesHandler("cluster2", "dc2"), handleReads, handleWrites}

			err = testSetup.Start(conf, false, primitive.ProtocolVersion4)
			require.Nil(t, err)

			wg := &sync.WaitGroup{}
			defer wg.Wait()
			srv := startMetricsHandler(t, conf, wg, metricsHandler)
			defer func() {
				err := srv.Close()
				if err != nil {
					log.Warnf("error cleaning http server: %v", err)
				}
			}()

			lines := gatherMetrics(t, conf, false)
			checkMetrics(t, false, lines, conf.AsyncReadsOnSecondary, 0, 0, 0, 0, 0, 0, 0, 0, true, true, originEndpoint, targetEndpoint, asyncEndpoint)

			err = testSetup.Client.Connect(primitive.ProtocolVersion4)
			require.Nil(t, err)
			clientConn := testSetup.Client.CqlConnection

			lines = gatherMetrics(t, conf, true)
			// 1 on origin: AUTH_RESPONSE
			// 1 on target: AUTH_RESPONSE
			// 1 on both: STARTUP
			// 2 on async: AUTH_RESPONSE and STARTUP
			checkMetrics(t, true, lines, conf.AsyncReadsOnSecondary, 1, 1, 1, expectedAsyncConnections, 1, 1, 1, 2, true, true, originEndpoint, targetEndpoint, asyncEndpoint)

			_, err = clientConn.SendAndReceive(insertQuery)
			require.Nil(t, err)

			lines = gatherMetrics(t, conf, true)
			// 1 on origin: AUTH_RESPONSE
			// 1 on target: AUTH_RESPONSE
			// 2 on both: STARTUP and QUERY INSERT INTO
			// 2 on async: AUTH_RESPONSE and STARTUP
			checkMetrics(t, true, lines, conf.AsyncReadsOnSecondary, 1, 1, 1, expectedAsyncConnections, 2, 1, 1, 2, true, true, originEndpoint, targetEndpoint, asyncEndpoint)

			_, err = clientConn.SendAndReceive(selectQuery)
			require.Nil(t, err)

			lines = gatherMetrics(t, conf, true)
			// 2 on origin: AUTH_RESPONSE and QUERY SELECT
			// 1 on target: AUTH_RESPONSE
			// 2 on both: STARTUP and QUERY INSERT INTO
			// 3 on async: AUTH_RESPONSE, STARTUP AND QUERY SELECT
			if conf.AsyncReadsOnSecondary {
				utils.RequireWithRetries(t, func() (err error, fatal bool) {
					valueToCheck := fmt.Sprintf("%v %v", getPrometheusNameWithNodeLabel("cloudgate", metrics.InFlightRequestsAsync, asyncEndpoint), 0)
					if !containsLine(lines, valueToCheck) {
						return fmt.Errorf("%v does not contain %v", lines, valueToCheck), false
					}
					valueToCheck = fmt.Sprintf("%v{node=\"%v\"} %v", getPrometheusNameWithSuffix("cloudgate", metrics.AsyncRequestDuration, "count"), asyncEndpoint, 3)
					if !containsLine(lines, valueToCheck) {
						return fmt.Errorf("%v does not contain %v", lines, valueToCheck), false
					}
					return nil, false
				}, 10, 200 * time.Millisecond)
			}

			checkMetrics(t, true, lines, conf.AsyncReadsOnSecondary, 1, 1, 1, expectedAsyncConnections, 2, 2, 1, 3, false, true, originEndpoint, targetEndpoint, asyncEndpoint)
		})
	}
}

func containsLine(lines []string, line string) bool {
	for i := 0; i < len(lines); i++ {
		if lines[i] == line {
			return true
		}
	}
	return false
}

func startMetricsHandler(
	t *testing.T, conf *config.Config, wg *sync.WaitGroup, metricsHandler *httpzdmproxy.HandlerWithFallback) *http.Server {
	httpAddr := fmt.Sprintf("%s:%d", conf.ProxyMetricsAddress, conf.ProxyMetricsPort)
	srv := httpzdmproxy.StartHttpServer(httpAddr, wg)
	require.NotNil(t, srv)
	metricsHandler.SetHandler(promhttp.Handler())
	return srv
}

func gatherMetrics(t *testing.T, conf *config.Config, checkNodeMetrics bool) []string {
	httpAddr := fmt.Sprintf("%s:%d", conf.ProxyMetricsAddress, conf.ProxyMetricsPort)
	statusCode, rspStr, err := utils.GetMetrics(httpAddr)
	require.Nil(t, err)
	require.Equal(t, http.StatusOK, statusCode)
	require.NotEmpty(t, rspStr)
	for _, metric := range proxyMetrics {
		require.Contains(t, rspStr, metric.GetName())
		require.Contains(t, rspStr, metric.GetDescription())
	}
	if checkNodeMetrics {
		for _, metric := range nodeMetrics {
			require.Contains(t, rspStr, metric.GetName())
			require.Contains(t, rspStr, metric.GetDescription())
		}
	}
	var result []string
	lines := strings.Split(rspStr, "\n")
	for _, line := range lines {
		if !strings.HasPrefix(line, "#") {
			result = append(result, line)
		}
	}
	return result
}

func checkMetrics(
	t *testing.T,
	checkNodeMetrics bool,
	lines []string,
	asyncEnabled bool,
	openClientConns int,
	openOriginConns int,
	openTargetConns int,
	openAsyncConns int,
	successBoth int,
	successOrigin int,
	successTarget int,
	successAsync int,
	handshakeOnlyOrigin bool,
	handshakeOnlyTarget bool,
	originHost string,
	targetHost string,
	asyncHost string,
) {
	prefix := "cloudgate"
	require.Contains(t, lines, fmt.Sprintf("%v %v", getPrometheusName(prefix, metrics.OpenClientConnections), openClientConns))

	require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusName(prefix, metrics.FailedRequestsBoth)))
	require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusName(prefix, metrics.FailedRequestsBothFailedOnOriginOnly)))
	require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusName(prefix, metrics.FailedRequestsBothFailedOnTargetOnly)))
	require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusName(prefix, metrics.FailedRequestsTarget)))
	require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusName(prefix, metrics.FailedRequestsOrigin)))


	require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusName(prefix, metrics.InFlightRequestsBoth)))
	require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusName(prefix, metrics.InFlightRequestsOrigin)))
	require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusName(prefix, metrics.InFlightRequestsTarget)))

	if successOrigin == 0 {
		require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusNameWithSuffix(prefix, metrics.ProxyRequestDurationOrigin, "sum")))
	} else {
		if !handshakeOnlyOrigin {
			value, err := findMetricValue(lines, fmt.Sprintf("%v ", getPrometheusNameWithSuffix(prefix, metrics.ProxyRequestDurationOrigin, "sum")))
			require.Nil(t, err)
			require.Greater(t, value, 0.0)
		}
	}

	if successTarget == 0 {
		require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusNameWithSuffix(prefix, metrics.ProxyRequestDurationTarget, "sum")))
	} else {
		if !handshakeOnlyTarget {
			value, err := findMetricValue(lines, fmt.Sprintf("%v ", getPrometheusNameWithSuffix(prefix, metrics.ProxyRequestDurationTarget, "sum")))
			require.Nil(t, err)
			require.Greater(t, value, 0.0)
		}
	}

	if successBoth == 0 {
		require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusNameWithSuffix(prefix, metrics.ProxyRequestDurationBoth, "sum")))
	} else {
		value, err := findMetricValue(lines, fmt.Sprintf("%v ", getPrometheusNameWithSuffix(prefix, metrics.ProxyRequestDurationBoth, "sum")))
		require.Nil(t, err)
		require.Greater(t, value, 0.0)
	}

	require.Contains(t, lines, fmt.Sprintf("%v %v", getPrometheusNameWithSuffix(prefix, metrics.ProxyRequestDurationTarget, "count"), successTarget))
	require.Contains(t, lines, fmt.Sprintf("%v %v", getPrometheusNameWithSuffix(prefix, metrics.ProxyRequestDurationOrigin, "count"), successOrigin))
	require.Contains(t, lines, fmt.Sprintf("%v %v", getPrometheusNameWithSuffix(prefix, metrics.ProxyRequestDurationBoth, "count"), successBoth))

	if checkNodeMetrics {
		if asyncEnabled {
			require.Contains(t, lines, fmt.Sprintf("%v %v", getPrometheusNameWithNodeLabel(prefix, metrics.InFlightRequestsAsync, asyncHost), 0))
		} else {
			require.NotContains(t, lines, fmt.Sprintf("%v", getPrometheusName(prefix, metrics.InFlightRequestsAsync)))
		}

		require.Contains(t, lines, fmt.Sprintf("%v{node=\"%v\"} %v", getPrometheusName(prefix, metrics.OpenOriginConnections), originHost, openOriginConns))
		require.Contains(t, lines, fmt.Sprintf("%v{node=\"%v\"} %v", getPrometheusName(prefix, metrics.OpenTargetConnections), targetHost, openTargetConns))

		if asyncEnabled {
			require.Contains(t, lines, fmt.Sprintf("%v{node=\"%v\"} %v", getPrometheusName(prefix, metrics.OpenAsyncConnections), asyncHost, openAsyncConns))
			require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusNameWithNodeLabel(prefix, metrics.AsyncReadTimeouts, asyncHost)))
			require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusNameWithNodeLabel(prefix, metrics.AsyncWriteTimeouts, asyncHost)))
			require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusNameWithNodeLabel(prefix, metrics.AsyncOtherErrors, asyncHost)))
			require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusNameWithNodeLabel(prefix, metrics.AsyncClientTimeouts, asyncHost)))
			require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusNameWithNodeLabel(prefix, metrics.AsyncUnavailableErrors, asyncHost)))
			require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusNameWithNodeLabel(prefix, metrics.AsyncReadFailures, asyncHost)))
			require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusNameWithNodeLabel(prefix, metrics.AsyncWriteFailures, asyncHost)))
			require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusNameWithNodeLabel(prefix, metrics.AsyncOverloadedErrors, asyncHost)))
			require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusNameWithNodeLabel(prefix, metrics.AsyncUnpreparedErrors, asyncHost)))
		} else {
			require.NotContains(t, lines, fmt.Sprintf("%v", getPrometheusName(prefix, metrics.OpenAsyncConnections)))
			require.NotContains(t, lines, fmt.Sprintf("%v", getPrometheusName(prefix, metrics.AsyncReadTimeouts)))
			require.NotContains(t, lines, fmt.Sprintf("%v", getPrometheusName(prefix, metrics.AsyncWriteTimeouts)))
			require.NotContains(t, lines, fmt.Sprintf("%v", getPrometheusName(prefix, metrics.AsyncOtherErrors)))
			require.NotContains(t, lines, fmt.Sprintf("%v", getPrometheusName(prefix, metrics.AsyncClientTimeouts)))
			require.NotContains(t, lines, fmt.Sprintf("%v", getPrometheusName(prefix, metrics.AsyncUnavailableErrors)))
			require.NotContains(t, lines, fmt.Sprintf("%v", getPrometheusName(prefix, metrics.AsyncReadFailures)))
			require.NotContains(t, lines, fmt.Sprintf("%v", getPrometheusName(prefix, metrics.AsyncWriteFailures)))
			require.NotContains(t, lines, fmt.Sprintf("%v", getPrometheusName(prefix, metrics.AsyncOverloadedErrors)))
			require.NotContains(t, lines, fmt.Sprintf("%v", getPrometheusName(prefix, metrics.AsyncUnpreparedErrors)))
		}

		require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusNameWithNodeLabel(prefix, metrics.OriginReadTimeouts, originHost)))
		require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusNameWithNodeLabel(prefix, metrics.OriginWriteTimeouts, originHost)))
		require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusNameWithNodeLabel(prefix, metrics.OriginOtherErrors, originHost)))
		require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusNameWithNodeLabel(prefix, metrics.OriginClientTimeouts, originHost)))
		require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusNameWithNodeLabel(prefix, metrics.OriginUnavailableErrors, originHost)))
		require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusNameWithNodeLabel(prefix, metrics.OriginReadFailures, originHost)))
		require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusNameWithNodeLabel(prefix, metrics.OriginWriteFailures, originHost)))
		require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusNameWithNodeLabel(prefix, metrics.OriginOverloadedErrors, originHost)))

		require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusNameWithNodeLabel(prefix, metrics.TargetReadTimeouts, targetHost)))
		require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusNameWithNodeLabel(prefix, metrics.TargetWriteTimeouts, targetHost)))
		require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusNameWithNodeLabel(prefix, metrics.TargetOtherErrors, targetHost)))
		require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusNameWithNodeLabel(prefix, metrics.TargetClientTimeouts, targetHost)))
		require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusNameWithNodeLabel(prefix, metrics.TargetUnavailableErrors, targetHost)))
		require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusNameWithNodeLabel(prefix, metrics.TargetReadFailures, targetHost)))
		require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusNameWithNodeLabel(prefix, metrics.TargetWriteFailures, targetHost)))
		require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusNameWithNodeLabel(prefix, metrics.TargetOverloadedErrors, targetHost)))

		require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusNameWithNodeLabel(prefix, metrics.OriginUnpreparedErrors, originHost)))
		require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusNameWithNodeLabel(prefix, metrics.TargetUnpreparedErrors, targetHost)))
		if (successTarget + successBoth) == 0 {
			require.Contains(t, lines, fmt.Sprintf("%v{node=\"%v\"} 0", getPrometheusNameWithSuffix(prefix, metrics.TargetRequestDuration, "sum"), targetHost))
		} else {
			if successBoth != 0 || !handshakeOnlyTarget {
				value, err := findMetricValue(lines, fmt.Sprintf("%v{node=\"%v\"} ", getPrometheusNameWithSuffix(prefix, metrics.TargetRequestDuration, "sum"), targetHost))
				require.Nil(t, err)
				require.Greater(t, value, 0.0)
			}
		}

		if (successOrigin + successBoth) == 0 {
			require.Contains(t, lines, fmt.Sprintf("%v{node=\"%v\"} 0", getPrometheusNameWithSuffix(prefix, metrics.OriginRequestDuration, "sum"), originHost))
		} else {
			if successBoth != 0 || !handshakeOnlyOrigin {
				value, err := findMetricValue(lines, fmt.Sprintf("%v{node=\"%v\"} ", getPrometheusNameWithSuffix(prefix, metrics.OriginRequestDuration, "sum"), originHost))
				require.Nil(t, err)
				require.Greater(t, value, 0.0)
			}
		}

		if successAsync == 0 || !asyncEnabled {
			if asyncEnabled {
				require.Contains(t, lines, fmt.Sprintf("%v{node=\"%v\"} 0", getPrometheusNameWithSuffix(prefix, metrics.AsyncRequestDuration, "sum"), asyncHost))
			} else {
				require.NotContains(t, lines, fmt.Sprintf("%v", getPrometheusNameWithSuffix(prefix, metrics.AsyncRequestDuration, "sum")))
			}
		} else {
			value, err := findMetricValue(lines, fmt.Sprintf("%v{node=\"%v\"} ", getPrometheusNameWithSuffix(prefix, metrics.AsyncRequestDuration, "sum"), asyncHost))
			require.Nil(t, err)
			require.Greater(t, value, 0.0)
		}

		require.Contains(t, lines, fmt.Sprintf("%v{node=\"%v\"} %v", getPrometheusNameWithSuffix(prefix, metrics.TargetRequestDuration, "count"), targetHost, successTarget+successBoth))
		require.Contains(t, lines, fmt.Sprintf("%v{node=\"%v\"} %v", getPrometheusNameWithSuffix(prefix, metrics.OriginRequestDuration, "count"), originHost, successOrigin+successBoth))
		if asyncEnabled {
			require.Contains(t, lines, fmt.Sprintf("%v{node=\"%v\"} %v", getPrometheusNameWithSuffix(prefix, metrics.AsyncRequestDuration, "count"), asyncHost, successAsync))
		} else {
			require.NotContains(t, lines, fmt.Sprintf("%v", getPrometheusNameWithSuffix(prefix, metrics.OriginRequestDuration, "count")))
		}
	}
}

func findMetricValue(lines []string, prefix string) (float64, error) {
	for _, line := range lines {
		if strings.HasPrefix(line, prefix) {
			return strconv.ParseFloat(strings.TrimPrefix(line, prefix), 64)
		}
	}
	return -1, fmt.Errorf("no line with prefix: %v", prefix)
}

func handleReads(request *frame.Frame, _ *client.CqlServerConnection, _ client.RequestHandlerContext) (response *frame.Frame) {
	time.Sleep(50 * time.Millisecond)
	switch request.Header.OpCode {
	case primitive.OpCodeQuery:
		query := request.Body.Message.(*message.Query)
		if strings.HasPrefix(query.Query, "SELECT") {
			rows := &message.RowsResult{
				Metadata: &message.RowsMetadata{ColumnCount: 1},
				Data:     message.RowSet{message.Row{message.Column{0, 0, 0, 4, 1, 2, 3, 4}}},
			}
			response = frame.NewFrame(request.Header.Version, request.Header.StreamId, rows)
		}
	}
	return
}

func handleWrites(request *frame.Frame, _ *client.CqlServerConnection, _ client.RequestHandlerContext) (response *frame.Frame) {
	time.Sleep(50 * time.Millisecond)
	switch request.Header.OpCode {
	case primitive.OpCodeQuery:
		query := request.Body.Message.(*message.Query)
		if strings.HasPrefix(query.Query, "INSERT") {
			void := &message.VoidResult{}
			response = frame.NewFrame(request.Header.Version, request.Header.StreamId, void)
		}
	}
	return
}

func getPrometheusName(prefix string, mn metrics.Metric) string {
	return getPrometheusNameWithSuffix(prefix, mn, "")
}

func getPrometheusNameWithNodeLabel(prefix string, mn metrics.Metric, node string) string {
	return getPrometheusNameWithSuffixAndNodeLabel(prefix, mn, "", node)
}

func getPrometheusNameWithSuffixAndNodeLabel(prefix string, mn metrics.Metric, suffix string, node string) string {
	if suffix != "" {
		suffix = "_" + suffix
	}

	labels := mn.GetLabels()
	newLabels := make(map[string]string)
	for key, value := range labels {
		newLabels[key] = value
	}
	if node != "" {
		newLabels["node"] = node		
	}
	labels = newLabels
	if labels != nil && len(labels) > 0 {
		keys := make([]string, 0, len(labels))
		for k := range labels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		sb := strings.Builder{}
		first := true
		for _, k := range keys {
			if !first {
				sb.WriteString(",")
			} else {
				first = false
			}
			sb.WriteString(k)
			sb.WriteString("=\"")
			sb.WriteString(labels[k])
			sb.WriteString("\"")
		}
		return fmt.Sprintf("%v_%v%v{%v}", prefix, mn.GetName(), suffix, sb.String())
	}

	return fmt.Sprintf("%v_%v%v", prefix, mn.GetName(), suffix)
}

func getPrometheusNameWithSuffix(prefix string, mn metrics.Metric, suffix string) string {
	return getPrometheusNameWithSuffixAndNodeLabel(prefix, mn, suffix, "")
}
