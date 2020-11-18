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
	"strconv"
	"strings"
	"sync"
	"testing"
)

var allMetrics = []metrics.MetricName{
	metrics.SuccessReads,
	metrics.FailedReads,
	metrics.SuccessBothWrites,
	metrics.FailedOriginOnlyWrites,
	metrics.FailedTargetOnlyWrites,
	metrics.FailedBothWrites,
	metrics.TimeOutsProxyOrigin,
	metrics.TimeOutsProxyTarget,
	metrics.ReadTimeOutsOriginCluster,
	metrics.WriteTimeOutsOriginCluster,
	metrics.WriteTimeOutsTargetCluster,
	metrics.UnpreparedReads,
	metrics.UnpreparedOriginWrites,
	metrics.UnpreparedTargetWrites,
	metrics.PSCacheSize,
	metrics.PSCacheMissCount,
	metrics.ProxyReadLatencyHist,
	metrics.OriginReadLatencyHist,
	metrics.ProxyWriteLatencyHist,
	metrics.OriginWriteLatencyHist,
	metrics.TargetWriteLatencyHist,
	metrics.InFlightReadRequests,
	metrics.InFlightWriteRequests,
	metrics.OpenClientConnections,
	metrics.OpenOriginConnections,
	metrics.OpenTargetConnections,
}

var insertQuery, _ = frame.NewRequestFrame(
	primitive.ProtocolVersion4,
	client.ManagedStreamId,
	false,
	nil,
	&message.Query{Query: "INSERT INTO ks1.t1..."},
	false,
)

var selectQuery, _ = frame.NewRequestFrame(
	primitive.ProtocolVersion4,
	client.ManagedStreamId,
	false,
	nil,
	&message.Query{Query: "SELECT * FROM ks1.t1..."},
	false,
)

func TestMetrics(t *testing.T) {

	conf := setup.NewTestConfig("127.0.0.1", "127.0.1.1")
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	origin, target := startOriginAndTarget(t, conf, ctx)

	startProxy(t, origin, target, conf, ctx)

	wg := &sync.WaitGroup{}
	defer wg.Wait()
	srv := startMetricsHandler(t, conf, wg)
	defer func() { _ = srv.Close() }()

	lines := gatherMetrics(t, conf)
	checkMetrics(t, lines, 0, 0, 0, 0, 0)

	clientConn := startClient(t, origin, target, conf, ctx)

	lines = gatherMetrics(t, conf)
	// 2 "reads" on origin: STARTUP and AUTH_RESPONSE
	checkMetrics(t, lines, 1, 1, 1, 0, 2)

	_, err := clientConn.SendAndReceive(insertQuery)
	require.Nil(t, err)

	lines = gatherMetrics(t, conf)
	// 2 "reads" on origin: STARTUP and AUTH_RESPONSE
	// 1 write on both: INSERT INTO
	checkMetrics(t, lines, 1, 1, 1, 1, 2)

	_, err = clientConn.SendAndReceive(selectQuery)
	require.Nil(t, err)

	lines = gatherMetrics(t, conf)
	// 3 "reads" on origin: STARTUP, AUTH_RESPONSE, QUERY SELECT
	// 1 write on both: QUERY INSERT INTO
	checkMetrics(t, lines, 1, 1, 1, 1, 3)

}

func startOriginAndTarget(t *testing.T, conf *config.Config, ctx context.Context) (*client.CqlServer, *client.CqlServer) {
	originAddr := fmt.Sprintf("%s:%d", conf.OriginCassandraHostname, conf.OriginCassandraPort)
	targetAddr := fmt.Sprintf("%s:%d", conf.TargetCassandraHostname, conf.TargetCassandraPort)
	origin := client.NewCqlServer(originAddr, &client.AuthCredentials{
		Username: conf.OriginCassandraUsername,
		Password: conf.OriginCassandraPassword,
	})
	target := client.NewCqlServer(targetAddr, &client.AuthCredentials{
		Username: conf.TargetCassandraUsername,
		Password: conf.TargetCassandraPassword,
	})
	origin.RequestHandlers = []client.RequestHandler{client.HeartbeatHandler, client.HandshakeHandler, handleReads, handleWrites}
	target.RequestHandlers = []client.RequestHandler{client.HeartbeatHandler, client.HandshakeHandler, handleWrites}
	err := origin.Start(ctx)
	require.Nil(t, err)
	err = target.Start(ctx)
	require.Nil(t, err)
	return origin, target
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
	originControlConn, _ := origin.AcceptAny()
	targetControlConn, _ := target.AcceptAny()
	require.NotNil(t, originControlConn)
	require.NotNil(t, targetControlConn)
	return proxy
}

func startMetricsHandler(t *testing.T, conf *config.Config, wg *sync.WaitGroup) *http.Server {
	httpAddr := fmt.Sprintf("%s:%d", conf.ProxyMetricsAddress, conf.ProxyMetricsPort)
	srv := httpcloudgate.StartHttpServer(httpAddr, wg)
	require.NotNil(t, srv)
	http.Handle("/metrics", promhttp.Handler())
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
	originClientConn, _ := origin.AcceptAny()
	targetClientConn, _ := target.AcceptAny()
	require.NotNil(t, originClientConn)
	require.NotNil(t, targetClientConn)
	err = clientConn.InitiateHandshake(primitive.ProtocolVersion4, client.ManagedStreamId)
	require.Nil(t, err)
	return clientConn
}

func gatherMetrics(t *testing.T, conf *config.Config) []string {
	httpAddr := fmt.Sprintf("%s:%d", conf.ProxyMetricsAddress, conf.ProxyMetricsPort)
	statusCode, rspStr, err := utils.GetMetrics(httpAddr)
	require.Nil(t, err)
	require.Equal(t, http.StatusOK, statusCode)
	require.NotEmpty(t, rspStr)
	for _, metric := range allMetrics {
		require.Contains(t, rspStr, metric)
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
) {
	require.Contains(t, lines, fmt.Sprintf("%v %v", metrics.OpenClientConnections, openClientConns))
	require.Contains(t, lines, fmt.Sprintf("%v %v", metrics.OpenOriginConnections, openOriginConns))
	require.Contains(t, lines, fmt.Sprintf("%v %v", metrics.OpenTargetConnections, openTargetConns))

	require.Contains(t, lines, fmt.Sprintf("%v %v", metrics.SuccessBothWrites, successBoth))
	require.Contains(t, lines, fmt.Sprintf("%v %v", metrics.SuccessReads, successOrigin))

	require.Contains(t, lines, fmt.Sprintf("%v 0", metrics.FailedBothWrites))
	require.Contains(t, lines, fmt.Sprintf("%v 0", metrics.FailedOriginOnlyWrites))
	require.Contains(t, lines, fmt.Sprintf("%v 0", metrics.FailedTargetOnlyWrites))
	require.Contains(t, lines, fmt.Sprintf("%v 0", metrics.FailedReads))

	require.Contains(t, lines, fmt.Sprintf("%v 0", metrics.InFlightReadRequests))
	require.Contains(t, lines, fmt.Sprintf("%v 0", metrics.InFlightWriteRequests))

	require.Contains(t, lines, fmt.Sprintf("%v 0", metrics.ReadTimeOutsOriginCluster))
	require.Contains(t, lines, fmt.Sprintf("%v 0", metrics.WriteTimeOutsOriginCluster))
	require.Contains(t, lines, fmt.Sprintf("%v 0", metrics.WriteTimeOutsTargetCluster))
	require.Contains(t, lines, fmt.Sprintf("%v 0", metrics.TimeOutsProxyOrigin))
	require.Contains(t, lines, fmt.Sprintf("%v 0", metrics.TimeOutsProxyTarget))

	require.Contains(t, lines, fmt.Sprintf("%v 0", metrics.UnpreparedReads))
	require.Contains(t, lines, fmt.Sprintf("%v 0", metrics.UnpreparedOriginWrites))
	require.Contains(t, lines, fmt.Sprintf("%v 0", metrics.UnpreparedTargetWrites))

	if successOrigin == 0 {
		require.Contains(t, lines, fmt.Sprintf("%v_count 0", metrics.OriginReadLatencyHist))
		require.Contains(t, lines, fmt.Sprintf("%v_count 0", metrics.ProxyReadLatencyHist))
		require.Contains(t, lines, fmt.Sprintf("%v_sum 0", metrics.OriginReadLatencyHist))
		require.Contains(t, lines, fmt.Sprintf("%v_sum 0", metrics.ProxyReadLatencyHist))
	} else {
		require.Contains(t, lines, fmt.Sprintf("%v_count %v", metrics.OriginReadLatencyHist, successOrigin))
		require.Contains(t, lines, fmt.Sprintf("%v_count %v", metrics.ProxyReadLatencyHist, successOrigin+successBoth))
		value, err := findMetricValue(lines, fmt.Sprintf("%v_sum ", metrics.OriginReadLatencyHist))
		require.Nil(t, err)
		require.Greater(t, value, 0.0)
		value, err = findMetricValue(lines, fmt.Sprintf("%v_sum ", metrics.ProxyReadLatencyHist))
		require.Nil(t, err)
		require.Greater(t, value, 0.0)
	}
	if successBoth == 0 {
		require.Contains(t, lines, fmt.Sprintf("%v_count 0", metrics.OriginWriteLatencyHist))
		require.Contains(t, lines, fmt.Sprintf("%v_count 0", metrics.TargetWriteLatencyHist))
		require.Contains(t, lines, fmt.Sprintf("%v_count 0", metrics.ProxyWriteLatencyHist))
		require.Contains(t, lines, fmt.Sprintf("%v_sum 0", metrics.OriginWriteLatencyHist))
		require.Contains(t, lines, fmt.Sprintf("%v_sum 0", metrics.TargetWriteLatencyHist))
		require.Contains(t, lines, fmt.Sprintf("%v_sum 0", metrics.ProxyWriteLatencyHist))
	} else {
		require.Contains(t, lines, fmt.Sprintf("%v_count %v", metrics.OriginWriteLatencyHist, successBoth))
		require.Contains(t, lines, fmt.Sprintf("%v_count %v", metrics.TargetWriteLatencyHist, successBoth))
		require.Contains(t, lines, fmt.Sprintf("%v_count %v", metrics.ProxyWriteLatencyHist, successBoth))
		value, err := findMetricValue(lines, fmt.Sprintf("%v_sum ", metrics.OriginWriteLatencyHist))
		require.Nil(t, err)
		require.Greater(t, value, 0.0)
		value, err = findMetricValue(lines, fmt.Sprintf("%v_sum ", metrics.TargetWriteLatencyHist))
		require.Nil(t, err)
		require.Greater(t, value, 0.0)
		value, err = findMetricValue(lines, fmt.Sprintf("%v_sum ", metrics.ProxyWriteLatencyHist))
		require.Nil(t, err)
		require.Greater(t, value, 0.0)
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
	switch request.Header.OpCode {
	case primitive.OpCodeQuery:
		query := request.Body.Message.(*message.Query)
		if strings.HasPrefix(query.Query, "SELECT") {
			rows := &message.RowsResult{
				Metadata: &message.RowsMetadata{ColumnCount: 1},
				Data:     message.RowSet{message.Row{message.Column{0, 0, 0, 4, 1, 2, 3, 4}}},
			}
			response, _ = frame.NewResponseFrame(
				request.Header.Version, request.Header.StreamId, nil, nil, nil, rows, false)
		}
	}
	return
}

func handleWrites(request *frame.Frame, _ *client.CqlServerConnection, _ client.RequestHandlerContext) (response *frame.Frame) {
	switch request.Header.OpCode {
	case primitive.OpCodeQuery:
		query := request.Body.Message.(*message.Query)
		if strings.HasPrefix(query.Query, "INSERT") {
			void := &message.VoidResult{}
			response, _ = frame.NewResponseFrame(
				request.Header.Version, request.Header.StreamId, nil, nil, nil, void, false)
		}
	}
	return
}
