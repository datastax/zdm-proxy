package integration_tests

import (
	"context"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/riptano/cloud-gate/integration-tests/setup"
	"github.com/riptano/cloud-gate/integration-tests/utils"
	"github.com/riptano/cloud-gate/proxy/pkg/cloudgateproxy"
	"github.com/riptano/cloud-gate/proxy/pkg/config"
	"github.com/riptano/cloud-gate/proxy/pkg/httpcloudgate"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics"
	"github.com/stretchr/testify/require"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var allMetrics = []metrics.Metric{
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

func testMetrics(t *testing.T, metricsHandler *httpcloudgate.HandlerWithFallback) {

	conf := setup.NewTestConfig("127.0.1.1", "127.0.1.2")
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	origin, target := createOriginAndTarget(conf)
	origin.RequestHandlers = []client.RequestHandler{client.HeartbeatHandler, client.HandshakeHandler, handleReads, handleWrites}
	target.RequestHandlers = []client.RequestHandler{client.HeartbeatHandler, client.HandshakeHandler, handleWrites}
	startOriginAndTarget(t, origin, target, ctx)

	startProxy(t, origin, target, conf, ctx)

	wg := &sync.WaitGroup{}
	defer wg.Wait()
	srv := startMetricsHandler(t, conf, wg, metricsHandler)
	defer func() { _ = srv.Close() }()

	lines := gatherMetrics(t, conf)
	checkMetrics(t, lines, 0, 0, 0, 0, 0, 0, true, true)

	clientConn := startClient(t, origin, target, conf, ctx)

	err := clientConn.InitiateHandshake(primitive.ProtocolVersion4, client.ManagedStreamId)
	require.Nil(t, err)

	lines = gatherMetrics(t, conf)
	// 2 on origin: STARTUP and AUTH_RESPONSE
	// 2 on target: STARTUP and AUTH_RESPONSE
	checkMetrics(t, lines, 1, 1, 1, 0, 2, 2, true, true)

	_, err = clientConn.SendAndReceive(insertQuery)
	require.Nil(t, err)

	lines = gatherMetrics(t, conf)
	// 2 on origin: STARTUP and AUTH_RESPONSE
	// 2 on target: STARTUP and AUTH_RESPONSE
	// 1 on both: QUERY INSERT INTO
	checkMetrics(t, lines, 1, 1, 1, 1, 2, 2, true, true)

	_, err = clientConn.SendAndReceive(selectQuery)
	require.Nil(t, err)

	lines = gatherMetrics(t, conf)
	// 3 on origin: STARTUP, AUTH_RESPONSE, QUERY SELECT
	// 2 on target: STARTUP and AUTH_RESPONSE
	// 1 on both: QUERY INSERT INTO
	checkMetrics(t, lines, 1, 1, 1, 1, 3, 2, false, true)

}

func createOriginAndTarget(conf *config.Config) (*client.CqlServer, *client.CqlServer) {
	originAddr := fmt.Sprintf("%s:%d", conf.OriginCassandraHostname, conf.OriginCassandraPort)
	origin := client.NewCqlServer(originAddr, &client.AuthCredentials{
		Username: conf.OriginCassandraUsername,
		Password: conf.OriginCassandraPassword,
	})
	targetAddr := fmt.Sprintf("%s:%d", conf.TargetCassandraHostname, conf.TargetCassandraPort)
	target := client.NewCqlServer(targetAddr, &client.AuthCredentials{
		Username: conf.TargetCassandraUsername,
		Password: conf.TargetCassandraPassword,
	})
	return origin, target
}

func startOriginAndTarget(
	t *testing.T,
	origin *client.CqlServer,
	target *client.CqlServer,
	ctx context.Context,
) {
	err := origin.Start(ctx)
	require.Nil(t, err)
	err = target.Start(ctx)
	require.Nil(t, err)
}

func startProxy(
	t *testing.T,
	origin *client.CqlServer,
	target *client.CqlServer,
	conf *config.Config,
	ctx context.Context,
) *cloudgateproxy.CloudgateProxy {
	proxy, err := cloudgateproxy.Run(conf, ctx)
	require.Nil(t, err)
	require.NotNil(t, proxy)
	go func() {
		<-ctx.Done()
		proxy.Shutdown()
	}()
	originControlConn, _ := origin.AcceptAny()
	targetControlConn, _ := target.AcceptAny()
	require.NotNil(t, originControlConn)
	require.NotNil(t, targetControlConn)
	return proxy
}

func startMetricsHandler(
	t *testing.T, conf *config.Config, wg *sync.WaitGroup, metricsHandler *httpcloudgate.HandlerWithFallback) *http.Server {
	httpAddr := fmt.Sprintf("%s:%d", conf.ProxyMetricsAddress, conf.ProxyMetricsPort)
	srv := httpcloudgate.StartHttpServer(httpAddr, wg)
	require.NotNil(t, srv)
	metricsHandler.SetHandler(promhttp.Handler())
	return srv
}

func startClient(
	t *testing.T,
	origin *client.CqlServer,
	target *client.CqlServer,
	conf *config.Config,
	ctx context.Context,
) *client.CqlClientConnection {
	proxyAddr := fmt.Sprintf("%s:%d", conf.ProxyQueryAddress, conf.ProxyQueryPort)
	clt := client.NewCqlClient(proxyAddr, &client.AuthCredentials{
		Username: conf.OriginCassandraUsername,
		Password: conf.OriginCassandraPassword,
	})
	clientConn, err := clt.Connect(ctx)
	require.Nil(t, err)
	require.NotNil(t, clientConn)
	go func() {
		<-ctx.Done()
		clientConn.Close()
	}()
	originClientConn, _ := origin.AcceptAny()
	targetClientConn, _ := target.AcceptAny()
	require.NotNil(t, originClientConn)
	require.NotNil(t, targetClientConn)
	return clientConn
}

func gatherMetrics(t *testing.T, conf *config.Config) []string {
	httpAddr := fmt.Sprintf("%s:%d", conf.ProxyMetricsAddress, conf.ProxyMetricsPort)
	statusCode, rspStr, err := utils.GetMetrics(httpAddr)
	require.Nil(t, err)
	require.Equal(t, http.StatusOK, statusCode)
	require.NotEmpty(t, rspStr)
	for _, metric := range allMetrics {
		require.Contains(t, rspStr, metric.GetName())
		require.Contains(t, rspStr, metric.GetDescription())
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
	lines []string,
	openClientConns int,
	openOriginConns int,
	openTargetConns int,
	successBoth int,
	successOrigin int,
	successTarget int,
	handshakeOnlyOrigin bool,
	handshakeOnlyTarget bool,
) {
	prefix := "cloudgate"
	require.Contains(t, lines, fmt.Sprintf("%v %v", getPrometheusName(prefix, metrics.OpenClientConnections), openClientConns))
	require.Contains(t, lines, fmt.Sprintf("%v %v", getPrometheusName(prefix, metrics.OpenOriginConnections), openOriginConns))
	require.Contains(t, lines, fmt.Sprintf("%v %v", getPrometheusName(prefix, metrics.OpenTargetConnections), openTargetConns))

	require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusName(prefix, metrics.FailedRequestsBoth)))
	require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusName(prefix, metrics.FailedRequestsBothFailedOnOriginOnly)))
	require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusName(prefix, metrics.FailedRequestsBothFailedOnTargetOnly)))
	require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusName(prefix, metrics.FailedRequestsTarget)))
	require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusName(prefix, metrics.FailedRequestsOrigin)))

	require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusName(prefix, metrics.InFlightRequestsBoth)))
	require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusName(prefix, metrics.InFlightRequestsOrigin)))
	require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusName(prefix, metrics.InFlightRequestsTarget)))

	require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusName(prefix, metrics.OriginReadTimeouts)))
	require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusName(prefix, metrics.OriginWriteTimeouts)))
	require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusName(prefix, metrics.OriginOtherErrors)))
	require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusName(prefix, metrics.OriginClientTimeouts)))

	require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusName(prefix, metrics.TargetReadTimeouts)))
	require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusName(prefix, metrics.TargetWriteTimeouts)))
	require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusName(prefix, metrics.TargetOtherErrors)))
	require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusName(prefix, metrics.TargetClientTimeouts)))

	require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusName(prefix, metrics.OriginUnpreparedErrors)))
	require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusName(prefix, metrics.TargetUnpreparedErrors)))

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

	if (successTarget + successBoth) == 0 {
		require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusNameWithSuffix(prefix, metrics.TargetRequestDuration, "sum")))
	} else {
		if successBoth != 0 || !handshakeOnlyTarget {
			value, err := findMetricValue(lines, fmt.Sprintf("%v ", getPrometheusNameWithSuffix(prefix, metrics.TargetRequestDuration, "sum")))
			require.Nil(t, err)
			require.Greater(t, value, 0.0)
		}
	}

	if (successOrigin + successBoth) == 0 {
		require.Contains(t, lines, fmt.Sprintf("%v 0", getPrometheusNameWithSuffix(prefix, metrics.OriginRequestDuration, "sum")))
	} else {
		if successBoth != 0 || !handshakeOnlyOrigin {
			value, err := findMetricValue(lines, fmt.Sprintf("%v ", getPrometheusNameWithSuffix(prefix, metrics.OriginRequestDuration, "sum")))
			require.Nil(t, err)
			require.Greater(t, value, 0.0)
		}
	}

	require.Contains(t, lines, fmt.Sprintf("%v %v", getPrometheusNameWithSuffix(prefix, metrics.TargetRequestDuration, "count"), successTarget+successBoth))
	require.Contains(t, lines, fmt.Sprintf("%v %v", getPrometheusNameWithSuffix(prefix, metrics.ProxyRequestDurationTarget, "count"), successTarget))
	require.Contains(t, lines, fmt.Sprintf("%v %v", getPrometheusNameWithSuffix(prefix, metrics.OriginRequestDuration, "count"), successOrigin+successBoth))
	require.Contains(t, lines, fmt.Sprintf("%v %v", getPrometheusNameWithSuffix(prefix, metrics.ProxyRequestDurationOrigin, "count"), successOrigin))
	require.Contains(t, lines, fmt.Sprintf("%v %v", getPrometheusNameWithSuffix(prefix, metrics.ProxyRequestDurationBoth, "count"), successBoth))
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

func getPrometheusNameWithSuffix(prefix string, mn metrics.Metric, suffix string) string {
	if suffix != "" {
		suffix = "_" + suffix
	}

	labels := mn.GetLabels()
	if labels != nil {
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
