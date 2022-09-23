package zdmproxy

import (
	"encoding/hex"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/zdm-proxy/proxy/pkg/metrics"
	"github.com/datastax/zdm-proxy/proxy/pkg/metrics/noopmetrics"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func TestInspectFrame(t *testing.T) {
	type args struct {
		f                            *frame.RawFrame
		replacedTerms                []*term
		forwardReadsToTarget         bool
		forwardSystemQueriesToTarget bool
		forwardAuthToTarget          bool
	}
	originCacheEntry := &preparedDataImpl{
		originPreparedId:   []byte("ORIGIN"),
		targetPreparedId:   []byte("ORIGIN_TARGET"),
		prepareRequestInfo: NewPrepareRequestInfo(NewGenericRequestInfo(forwardToOrigin, false), nil, false, "", ""),
	}
	targetCacheEntry := &preparedDataImpl{
		originPreparedId:   []byte("TARGET"),
		targetPreparedId:   []byte("TARGET_TARGET"),
		prepareRequestInfo: NewPrepareRequestInfo(NewGenericRequestInfo(forwardToTarget, false), nil, false, "", ""),
	}
	bothCacheEntry := &preparedDataImpl{
		originPreparedId:   []byte("BOTH"),
		targetPreparedId:   []byte("BOTH_TARGET"),
		prepareRequestInfo: NewPrepareRequestInfo(NewGenericRequestInfo(forwardToBoth, false), nil, false, "", ""),
	}
	peersKsCacheEntry := &preparedDataImpl{
		originPreparedId:   []byte("PEERS_KS"),
		targetPreparedId:   []byte("PEERS_KS"),
		prepareRequestInfo: NewPrepareRequestInfo(NewInterceptedRequestInfo(peersV1, newStarSelectClause()), nil, false, "SELECT * FROM peers", "system"),
	}
	peersCacheEntry := &preparedDataImpl{
		originPreparedId:   []byte("PEERS"),
		targetPreparedId:   []byte("PEERS"),
		prepareRequestInfo: NewPrepareRequestInfo(NewInterceptedRequestInfo(peersV1, newStarSelectClause()), nil, false, "SELECT * FROM system.peers", ""),
	}
	localKsCacheEntry := &preparedDataImpl{
		originPreparedId:   []byte("LOCAL_KS"),
		targetPreparedId:   []byte("LOCAL_KS"),
		prepareRequestInfo: NewPrepareRequestInfo(NewInterceptedRequestInfo(local, newStarSelectClause()), nil, false, "SELECT * FROM local", "system"),
	}
	localCacheEntry := &preparedDataImpl{
		originPreparedId:   []byte("LOCAL"),
		targetPreparedId:   []byte("LOCAL"),
		prepareRequestInfo: NewPrepareRequestInfo(NewInterceptedRequestInfo(local, newStarSelectClause()), nil, false, "SELECT * FROM system.local", ""),
	}
	psCache := NewPreparedStatementCache()
	psCache.cache["BOTH"] = bothCacheEntry
	psCache.cache["ORIGIN"] = originCacheEntry
	psCache.cache["TARGET"] = targetCacheEntry
	psCache.interceptedCache["PEERS"] = peersCacheEntry
	psCache.interceptedCache["PEERS_KS"] = peersKsCacheEntry
	psCache.interceptedCache["LOCAL"] = localCacheEntry
	psCache.interceptedCache["LOCAL_KS"] = localKsCacheEntry
	mh := newFakeMetricHandler()
	km := ""
	forwardReadsToTarget := true
	forwardReadsToOrigin := false
	forwardAuthToTarget := true
	forwardAuthToOrigin := false
	tests := []struct {
		name     string
		args     args
		expected interface{}
	}{
		// QUERY
		{"OpCodeQuery SELECT", args{mockQueryFrame(t, "SELECT blah FROM ks1.t2"), []*term{}, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewGenericRequestInfo(forwardToOrigin, true)},
		{"OpCodeQuery SELECT forwardReadsToTarget", args{mockQueryFrame(t, "SELECT blah FROM ks1.t1"), []*term{}, forwardReadsToTarget, forwardReadsToTarget, forwardAuthToOrigin}, NewGenericRequestInfo(forwardToTarget, true)},
		{"OpCodeQuery SELECT system.local", args{mockQueryFrame(t, "SELECT * FROM system.local"), []*term{}, forwardReadsToTarget, forwardReadsToTarget, forwardAuthToOrigin}, NewInterceptedRequestInfo(local, newStarSelectClause())},
		{"OpCodeQuery SELECT system.local", args{mockQueryFrame(t, "SELECT * FROM system.local"), []*term{}, forwardReadsToOrigin, forwardReadsToTarget, forwardAuthToOrigin}, NewInterceptedRequestInfo(local, newStarSelectClause())},
		{"OpCodeQuery SELECT system.local forwardSystemQueriesToOrigin", args{mockQueryFrame(t, "SELECT * FROM system.local"), []*term{}, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewInterceptedRequestInfo(local, newStarSelectClause())},
		{"OpCodeQuery SELECT system.peers forwardSystemQueriesToOrigin", args{mockQueryFrame(t, "SELECT * FROM system.peers"), []*term{}, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewInterceptedRequestInfo(peersV1, newStarSelectClause())},
		{"OpCodeQuery SELECT system.peers", args{mockQueryFrame(t, "SELECT * FROM system.peers"), []*term{}, forwardReadsToOrigin, forwardReadsToTarget, forwardAuthToOrigin}, NewInterceptedRequestInfo(peersV1, newStarSelectClause())},
		{"OpCodeQuery SELECT system.peers_v2 forwardSystemQueriesToOrigin", args{mockQueryFrame(t, "SELECT * FROM system.peers_v2"), []*term{}, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewInterceptedRequestInfo(peersV2, newStarSelectClause())},
		{"OpCodeQuery SELECT system.peers_v2", args{mockQueryFrame(t, "SELECT * FROM system.peers_v2"), []*term{}, forwardReadsToOrigin, forwardReadsToTarget, forwardAuthToOrigin}, NewInterceptedRequestInfo(peersV2, newStarSelectClause())},
		{"OpCodeQuery SELECT system_auth.roles", args{mockQueryFrame(t, "SELECT * FROM system_auth.roles"), []*term{}, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewGenericRequestInfo(forwardToOrigin, true)},
		{"OpCodeQuery SELECT dse_insights.tokens", args{mockQueryFrame(t, "SELECT * FROM dse_insights.tokens"), []*term{}, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewGenericRequestInfo(forwardToOrigin, true)},
		{"OpCodeQuery INSERT INTO asd (a, b) VALUES (1, 2)", args{mockQueryFrame(t, "INSERT INTO asd (a, b) VALUES (1, 2)"), []*term{}, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewGenericRequestInfo(forwardToBoth, false)},
		{"OpCodeQuery UPDATE asd SET b = 2 WHERE a = 1", args{mockQueryFrame(t, "UPDATE asd SET b = 2 WHERE a = 1"), []*term{}, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewGenericRequestInfo(forwardToBoth, false)},
		{"OpCodeQuery UNKNOWN", args{mockQueryFrame(t, "UNKNOWN"), []*term{}, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewGenericRequestInfo(forwardToBoth, false)},

		// PREPARE
		{"OpCodePrepare SELECT", args{mockPrepareFrame(t, "SELECT blah FROM ks1.t1"), []*term{}, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewPrepareRequestInfo(NewGenericRequestInfo(forwardToOrigin, true), []*term{}, false, "SELECT blah FROM ks1.t1", "")},
		{"OpCodePrepare SELECT system.local forwardSystemQueriesToOrigin", args{mockPrepareFrame(t, "SELECT * FROM system.local"), []*term{}, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewPrepareRequestInfo(NewInterceptedRequestInfo(local, newStarSelectClause()), []*term{}, false, "SELECT * FROM system.local", "")},
		{"OpCodePrepare SELECT system.peers forwardSystemQueriesToOrigin", args{mockPrepareFrame(t, "SELECT * FROM system.peers"), []*term{}, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewPrepareRequestInfo(NewInterceptedRequestInfo(peersV1, newStarSelectClause()), []*term{}, false, "SELECT * FROM system.peers", "")},
		{"OpCodePrepare SELECT system.local", args{mockPrepareFrame(t, "SELECT * FROM system.local"), []*term{}, forwardReadsToOrigin, forwardReadsToTarget, forwardAuthToOrigin}, NewPrepareRequestInfo(NewInterceptedRequestInfo(local, newStarSelectClause()), []*term{}, false, "SELECT * FROM system.local", "")},
		{"OpCodePrepare SELECT local", args{mockPrepareFrameWithKeyspace(t, "SELECT * FROM local", "system"), []*term{}, forwardReadsToOrigin, forwardReadsToTarget, forwardAuthToOrigin}, NewPrepareRequestInfo(NewInterceptedRequestInfo(local, newStarSelectClause()), []*term{}, false, "SELECT * FROM local", "system")},
		{"OpCodePrepare SELECT system.peers", args{mockPrepareFrame(t, "SELECT * FROM system.peers"), []*term{}, forwardReadsToOrigin, forwardReadsToTarget, forwardAuthToOrigin}, NewPrepareRequestInfo(NewInterceptedRequestInfo(peersV1, newStarSelectClause()), []*term{}, false, "SELECT * FROM system.peers", "")},
		{"OpCodePrepare SELECT peers", args{mockPrepareFrameWithKeyspace(t, "SELECT * FROM peers", "system"), []*term{}, forwardReadsToOrigin, forwardReadsToTarget, forwardAuthToOrigin}, NewPrepareRequestInfo(NewInterceptedRequestInfo(peersV1, newStarSelectClause()), []*term{}, false, "SELECT * FROM peers", "system")},
		{"OpCodePrepare SELECT system.peers_v2", args{mockPrepareFrame(t, "SELECT * FROM system.peers_v2"), []*term{}, forwardReadsToOrigin, forwardReadsToTarget, forwardAuthToOrigin}, NewPrepareRequestInfo(NewInterceptedRequestInfo(peersV2, newStarSelectClause()), []*term{}, false, "SELECT * FROM system.peers_v2", "")},
		{"OpCodePrepare SELECT system.peers_v2 forwardSystemQueriesToOrigin", args{mockPrepareFrame(t, "SELECT * FROM system.peers_v2"), []*term{}, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewPrepareRequestInfo(NewInterceptedRequestInfo(peersV2, newStarSelectClause()), []*term{}, false, "SELECT * FROM system.peers_v2", "")},
		{"OpCodePrepare SELECT system_auth.roles", args{mockPrepareFrame(t, "SELECT * FROM system_auth.roles"), []*term{}, forwardReadsToOrigin, forwardReadsToTarget, forwardAuthToOrigin}, NewPrepareRequestInfo(NewGenericRequestInfo(forwardToTarget, true), []*term{}, false, "SELECT * FROM system_auth.roles", "")},
		{"OpCodePrepare SELECT dse_insights.tokens", args{mockPrepareFrame(t, "SELECT * FROM dse_insights.tokens"), []*term{}, forwardReadsToOrigin, forwardReadsToTarget, forwardAuthToOrigin}, NewPrepareRequestInfo(NewGenericRequestInfo(forwardToTarget, true), []*term{}, false, "SELECT * FROM dse_insights.tokens", "")},
		{"OpCodePrepare INSERT INTO asd (a, b) VALUES (1, 2)", args{mockPrepareFrame(t, "INSERT INTO asd (a, b) VALUES (1, 2)"), []*term{}, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewPrepareRequestInfo(NewGenericRequestInfo(forwardToBoth, false), []*term{}, false, "INSERT INTO asd (a, b) VALUES (1, 2)", "")},
		{"OpCodePrepare UPDATE asd SET b = 2 WHERE a = 1", args{mockPrepareFrame(t, "UPDATE asd SET b = 2 WHERE a = 1"), []*term{}, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewPrepareRequestInfo(NewGenericRequestInfo(forwardToBoth, false), []*term{}, false, "UPDATE asd SET b = 2 WHERE a = 1", "")},
		{"OpCodePrepare UNKNOWN", args{mockPrepareFrame(t, "UNKNOWN"), []*term{}, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewPrepareRequestInfo(NewGenericRequestInfo(forwardToBoth, false), []*term{}, false, "UNKNOWN", "")},

		// EXECUTE
		{"OpCodeExecute origin", args{mockExecuteFrame(t, "ORIGIN"), []*term{}, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewExecuteRequestInfo(originCacheEntry)},
		{"OpCodeExecute target", args{mockExecuteFrame(t, "TARGET"), []*term{}, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewExecuteRequestInfo(targetCacheEntry)},
		{"OpCodeExecute both", args{mockExecuteFrame(t, "BOTH"), []*term{}, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewExecuteRequestInfo(bothCacheEntry)},
		{"OpCodeExecute local ks", args{mockExecuteFrame(t, "LOCAL_KS"), []*term{}, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewExecuteRequestInfo(localKsCacheEntry)},
		{"OpCodeExecute local", args{mockExecuteFrame(t, "LOCAL"), []*term{}, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewExecuteRequestInfo(localCacheEntry)},
		{"OpCodeExecute peers ks", args{mockExecuteFrame(t, "PEERS_KS"), []*term{}, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewExecuteRequestInfo(peersKsCacheEntry)},
		{"OpCodeExecute peers", args{mockExecuteFrame(t, "PEERS"), []*term{}, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewExecuteRequestInfo(peersCacheEntry)},
		{"OpCodeExecute unknown", args{mockExecuteFrame(t, "UNKNOWN"), []*term{}, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, fmt.Sprintf("The preparedID of the statement to be executed (%v) does not exist in the proxy cache", hex.EncodeToString([]byte("UNKNOWN")))},
		// REGISTER
		{"OpCodeRegister", args{mockFrame(t, &message.Register{EventTypes: []primitive.EventType{primitive.EventTypeSchemaChange}}, primitive.ProtocolVersion4), []*term{}, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewGenericRequestInfo(forwardToBoth, false)},
		// BATCH
		{"OpCodeBatch simple", args{mockBatch(t, "simple query"), []*term{}, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewBatchRequestInfo(map[int]PreparedData{})},
		{"OpCodeBatch prepared", args{mockBatch(t, []byte("BOTH")), []*term{}, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewBatchRequestInfo(map[int]PreparedData{0: bothCacheEntry})},
		// AUTH_RESPONSE
		{"OpCodeAuthResponse ForwardAuthToTarget", args{mockAuthResponse(t), []*term{}, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToTarget}, NewGenericRequestInfo(forwardToTarget, false)},
		{"OpCodeAuthResponse ForwardAuthToOrigin", args{mockAuthResponse(t), []*term{}, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewGenericRequestInfo(forwardToOrigin, false)},
		// others
		{"OpCodeStartup", args{mockFrame(t, message.NewStartup(), primitive.ProtocolVersion4), []*term{}, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewGenericRequestInfo(forwardToBoth, false)},
		{"OpCodeOptions", args{mockFrame(t, &message.Options{}, primitive.ProtocolVersion4), []*term{}, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewGenericRequestInfo(forwardToBoth, true)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			timeUuidGenerator, err := GetDefaultTimeUuidGenerator()
			require.Nil(t, err)
			actual, err := buildRequestInfo(&frameDecodeContext{frame: tt.args.f}, []*statementReplacedTerms{{
				statementIndex: 0,
				replacedTerms:  tt.args.replacedTerms,
			}}, psCache, mh, km, tt.args.forwardReadsToTarget, tt.args.forwardSystemQueriesToTarget, true, tt.args.forwardAuthToTarget, timeUuidGenerator)
			if err != nil {
				if !reflect.DeepEqual(err.Error(), tt.expected) {
					t.Errorf("buildRequestInfo() actual = %v, expected %v", err, tt.expected)
				}
			} else if !reflect.DeepEqual(actual, tt.expected) {
				t.Errorf("buildRequestInfo() actual = %v, want %v", actual, tt.expected)
			}
		})
	}
}

func mockPrepareFrame(t *testing.T, query string) *frame.RawFrame {
	prepareMsg := &message.Prepare{
		Query:    query,
		Keyspace: "",
	}
	return mockFrame(t, prepareMsg, primitive.ProtocolVersion4)
}

func mockPrepareFrameWithKeyspace(t *testing.T, query string, ks string) *frame.RawFrame {
	prepareMsg := &message.Prepare{
		Query:    query,
		Keyspace: ks,
	}
	return mockFrame(t, prepareMsg, primitive.ProtocolVersionDse2)
}

func mockQueryFrame(t *testing.T, query string) *frame.RawFrame {
	queryMsg := &message.Query{
		Query: query,
	}
	return mockFrame(t, queryMsg, primitive.ProtocolVersion4)
}

func mockExecuteFrame(t *testing.T, preparedId string) *frame.RawFrame {
	executeMsg := &message.Execute{
		QueryId:          []byte(preparedId),
		ResultMetadataId: nil,
	}
	return mockFrame(t, executeMsg, primitive.ProtocolVersion4)
}

func mockBatch(t *testing.T, query interface{}) *frame.RawFrame {
	batchMsg := &message.Batch{Children: []*message.BatchChild{{QueryOrId: query}}}
	return mockFrame(t, batchMsg, primitive.ProtocolVersion4)
}

func mockBatchWithChildren(t *testing.T, children []*message.BatchChild) *frame.RawFrame {
	batchMsg := &message.Batch{Children: children}
	return mockFrame(t, batchMsg, primitive.ProtocolVersion4)
}

func mockAuthResponse(t *testing.T) *frame.RawFrame {
	authCreds := &AuthCredentials{
		AuthId:   "",
		Username: "user",
		Password: "password",
	}
	token := authCreds.Marshal()
	return mockFrame(t, &message.AuthResponse{Token: token}, primitive.ProtocolVersion4)
}

func mockFrame(t *testing.T, message message.Message, version primitive.ProtocolVersion) *frame.RawFrame {
	f := frame.NewFrame(version, 1, message)
	rawFrame, err := defaultCodec.ConvertToRawFrame(f)
	require.Nil(t, err)
	return rawFrame
}

func newFakeMetricHandler() *metrics.MetricHandler {
	return metrics.NewMetricHandler(noopmetrics.NewNoopMetricFactory(), []float64{}, []float64{}, []float64{}, newFakeProxyMetrics(), nil, nil, nil)
}

func newFakeProxyMetrics() *metrics.ProxyMetrics {
	return &metrics.ProxyMetrics{
		FailedRequestsOrigin:                 newFakeCounter(),
		FailedRequestsTarget:                 newFakeCounter(),
		FailedRequestsBothFailedOnOriginOnly: newFakeCounter(),
		FailedRequestsBothFailedOnTargetOnly: newFakeCounter(),
		FailedRequestsBoth:                   newFakeCounter(),
		PSCacheSize:                          newFakeGaugeFunc(),
		PSCacheMissCount:                     newFakeCounter(),
		ProxyRequestDurationOrigin:           newFakeHistogram(),
		ProxyRequestDurationTarget:           newFakeHistogram(),
		ProxyRequestDurationBoth:             newFakeHistogram(),
		InFlightRequestsOrigin:               newFakeGauge(),
		InFlightRequestsTarget:               newFakeGauge(),
		InFlightRequestsBoth:                 newFakeGauge(),
		OpenClientConnections:                newFakeGaugeFunc(),
	}
}

func newFakeCounter() metrics.Counter {
	c, _ := noopmetrics.NewNoopMetricFactory().GetOrCreateCounter(newFakeMetric())
	return c
}

func newFakeGauge() metrics.Gauge {
	g, _ := noopmetrics.NewNoopMetricFactory().GetOrCreateGauge(newFakeMetric())
	return g
}

func newFakeGaugeFunc() metrics.GaugeFunc {
	gf, _ := noopmetrics.NewNoopMetricFactory().GetOrCreateGaugeFunc(newFakeMetric(), func() float64 {
		return 0.0
	})
	return gf
}

func newFakeHistogram() metrics.Histogram {
	h, _ := noopmetrics.NewNoopMetricFactory().GetOrCreateHistogram(newFakeMetric(), []float64{})
	return h
}

type fakeMetric struct{}

func (recv *fakeMetric) GetName() string {
	return ""
}

func (recv *fakeMetric) GetLabels() map[string]string {
	return make(map[string]string)
}

func (recv *fakeMetric) GetDescription() string {
	return ""
}

func (recv *fakeMetric) String() string {
	return ""
}

func (recv *fakeMetric) WithLabels(map[string]string) metrics.Metric {
	return newFakeMetric()
}

func newFakeMetric() metrics.Metric {
	return &fakeMetric{}
}
