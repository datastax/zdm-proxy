package cloudgateproxy

import (
	"encoding/hex"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics/noopmetrics"
	"github.com/stretchr/testify/require"
	"reflect"
	"sync/atomic"
	"testing"
)

func TestInspectFrame(t *testing.T) {
	type args struct {
		f                            *frame.RawFrame
		psCache                      *PreparedStatementCache
		mh                           *metrics.MetricHandler
		km                           *atomic.Value
		forwardReadsToTarget         bool
		forwardSystemQueriesToTarget bool
		forwardAuthToTarget          bool
	}
	originCacheEntry := &preparedDataImpl{
		originPreparedId: []byte("ORIGIN"),
		targetPreparedId: []byte("ORIGIN_TARGET"),
		stmtInfo:         NewPreparedStatementInfo(NewGenericStatementInfo(forwardToOrigin)),
	}
	targetCacheEntry := &preparedDataImpl{
		originPreparedId: []byte("TARGET"),
		targetPreparedId: []byte("TARGET_TARGET"),
		stmtInfo:         NewPreparedStatementInfo(NewGenericStatementInfo(forwardToTarget)),
	}
	bothCacheEntry := &preparedDataImpl{
		originPreparedId: []byte("BOTH"),
		targetPreparedId: []byte("BOTH_TARGET"),
		stmtInfo:         NewPreparedStatementInfo(NewGenericStatementInfo(forwardToBoth)),
	}
	psCache := NewPreparedStatementCache()
	psCache.cache["BOTH"] = bothCacheEntry
	psCache.cache["ORIGIN"] = originCacheEntry
	psCache.cache["TARGET"] = targetCacheEntry
	mh := newFakeMetricHandler()
	km := new(atomic.Value)
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
		{"OpCodeQuery SELECT", args{mockQueryFrame("SELECT blah FROM ks1.t2"), psCache, mh, km, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewGenericStatementInfo(forwardToOrigin)},
		{"OpCodeQuery SELECT forwardReadsToTarget", args{mockQueryFrame("SELECT blah FROM ks1.t1"), psCache, mh, km, forwardReadsToTarget, forwardReadsToTarget, forwardAuthToOrigin}, NewGenericStatementInfo(forwardToTarget)},
		{"OpCodeQuery SELECT system.local", args{mockQueryFrame("SELECT * FROM system.local"), psCache, mh, km, forwardReadsToTarget, forwardReadsToTarget, forwardAuthToOrigin}, NewGenericStatementInfo(forwardToTarget)},
		{"OpCodeQuery SELECT system.local", args{mockQueryFrame("SELECT * FROM system.local"), psCache, mh, km, forwardReadsToOrigin, forwardReadsToTarget, forwardAuthToOrigin}, NewGenericStatementInfo(forwardToTarget)},
		{"OpCodeQuery SELECT system.local forwardSystemQueriesToOrigin", args{mockQueryFrame("SELECT * FROM system.local"), psCache, mh, km, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewGenericStatementInfo(forwardToOrigin)},
		{"OpCodeQuery SELECT system.peers forwardSystemQueriesToOrigin", args{mockQueryFrame("SELECT * FROM system.peers"), psCache, mh, km, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewGenericStatementInfo(forwardToOrigin)},
		{"OpCodeQuery SELECT system.peers", args{mockQueryFrame("SELECT * FROM system.peers"), psCache, mh, km, forwardReadsToOrigin, forwardReadsToTarget, forwardAuthToOrigin}, NewGenericStatementInfo(forwardToTarget)},
		{"OpCodeQuery SELECT system.peers_v2 forwardSystemQueriesToOrigin", args{mockQueryFrame("SELECT * FROM system.peers_v2"), psCache, mh, km, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewGenericStatementInfo(forwardToOrigin)},
		{"OpCodeQuery SELECT system.peers_v2", args{mockQueryFrame("SELECT * FROM system.peers_v2"), psCache, mh, km, forwardReadsToOrigin, forwardReadsToTarget, forwardAuthToOrigin}, NewGenericStatementInfo(forwardToTarget)},
		{"OpCodeQuery SELECT system_auth.roles", args{mockQueryFrame("SELECT * FROM system_auth.roles"), psCache, mh, km, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewGenericStatementInfo(forwardToOrigin)},
		{"OpCodeQuery SELECT dse_insights.tokens", args{mockQueryFrame("SELECT * FROM dse_insights.tokens"), psCache, mh, km, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewGenericStatementInfo(forwardToOrigin)},
		{"OpCodeQuery non SELECT", args{mockQueryFrame("INSERT blah"), psCache, mh, km, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewGenericStatementInfo(forwardToBoth)},
		// PREPARE
		{"OpCodePrepare SELECT", args{mockPrepareFrame("SELECT blah FROM ks1.t1"), psCache, mh, km, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewPreparedStatementInfo(NewGenericStatementInfo(forwardToOrigin))},
		{"OpCodePrepare SELECT system.local forwardSystemQueriesToOrigin", args{mockPrepareFrame("SELECT * FROM system.local"), psCache, mh, km, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewPreparedStatementInfo(NewGenericStatementInfo(forwardToOrigin))},
		{"OpCodePrepare SELECT system.peers forwardSystemQueriesToOrigin", args{mockPrepareFrame("SELECT * FROM system.peers"), psCache, mh, km, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewPreparedStatementInfo(NewGenericStatementInfo(forwardToOrigin))},
		{"OpCodePrepare SELECT system.local", args{mockPrepareFrame("SELECT * FROM system.local"), psCache, mh, km, forwardReadsToOrigin, forwardReadsToTarget, forwardAuthToOrigin}, NewPreparedStatementInfo(NewGenericStatementInfo(forwardToTarget))},
		{"OpCodePrepare SELECT system.peers", args{mockPrepareFrame("SELECT * FROM system.peers"), psCache, mh, km, forwardReadsToOrigin, forwardReadsToTarget, forwardAuthToOrigin}, NewPreparedStatementInfo(NewGenericStatementInfo(forwardToTarget))},
		{"OpCodePrepare SELECT system.peers_v2", args{mockPrepareFrame("SELECT * FROM system.peers_v2"), psCache, mh, km, forwardReadsToOrigin, forwardReadsToTarget, forwardAuthToOrigin}, NewPreparedStatementInfo(NewGenericStatementInfo(forwardToTarget))},
		{"OpCodePrepare SELECT system.peers_v2 forwardSystemQueriesToOrigin", args{mockPrepareFrame("SELECT * FROM system.peers_v2"), psCache, mh, km, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewPreparedStatementInfo(NewGenericStatementInfo(forwardToOrigin))},
		{"OpCodePrepare SELECT system_auth.roles", args{mockPrepareFrame("SELECT * FROM system_auth.roles"), psCache, mh, km, forwardReadsToOrigin, forwardReadsToTarget, forwardAuthToOrigin}, NewPreparedStatementInfo(NewGenericStatementInfo(forwardToTarget))},
		{"OpCodePrepare SELECT dse_insights.tokens", args{mockPrepareFrame("SELECT * FROM dse_insights.tokens"), psCache, mh, km, forwardReadsToOrigin, forwardReadsToTarget, forwardAuthToOrigin}, NewPreparedStatementInfo(NewGenericStatementInfo(forwardToTarget))},
		{"OpCodePrepare non SELECT", args{mockPrepareFrame("INSERT blah"), psCache, mh, km, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewPreparedStatementInfo(NewGenericStatementInfo(forwardToBoth))},
		// EXECUTE
		{"OpCodeExecute origin", args{mockExecuteFrame("ORIGIN"), psCache, mh, km, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewBoundStatementInfo(originCacheEntry)},
		{"OpCodeExecute target", args{mockExecuteFrame("TARGET"), psCache, mh, km, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewBoundStatementInfo(targetCacheEntry)},
		{"OpCodeExecute both", args{mockExecuteFrame("BOTH"), psCache, mh, km, forwardReadsToOrigin,forwardReadsToOrigin, forwardAuthToOrigin}, NewBoundStatementInfo(bothCacheEntry)},
		{"OpCodeExecute unknown", args{mockExecuteFrame("UNKNOWN"), psCache, mh, km, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, fmt.Sprintf("The preparedID of the statement to be executed (%v) does not exist in the proxy cache", hex.EncodeToString([]byte("UNKNOWN")))},
		// REGISTER
		{"OpCodeRegister", args{mockFrame(&message.Register{EventTypes: []primitive.EventType{primitive.EventTypeSchemaChange}}), psCache, mh, km, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewGenericStatementInfo(forwardToBoth)},
		// BATCH
		{"OpCodeBatch simple", args{mockBatch("simple query"), psCache, mh, km, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewBatchStatementInfo(map[int]PreparedData{})},
		{"OpCodeBatch prepared", args{mockBatch([]byte("BOTH")), psCache, mh, km, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewBatchStatementInfo(map[int]PreparedData{0: bothCacheEntry})},
		// AUTH_RESPONSE
		{"OpCodeAuthResponse ForwardAuthToTarget", args{mockAuthResponse(), psCache, mh, km, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToTarget}, NewGenericStatementInfo(forwardToTarget)},
		{"OpCodeAuthResponse ForwardAuthToOrigin", args{mockAuthResponse(), psCache, mh, km, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewGenericStatementInfo(forwardToOrigin)},
		// others
		{"OpCodeStartup", args{mockFrame(message.NewStartup()), psCache, mh, km, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewGenericStatementInfo(forwardToBoth)},
		{"OpCodeOptions", args{mockFrame(&message.Options{}), psCache, mh, km, forwardReadsToOrigin, forwardReadsToOrigin, forwardAuthToOrigin}, NewGenericStatementInfo(forwardToBoth)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := parseStatement(&frameDecodeContext{frame: tt.args.f}, tt.args.psCache, tt.args.mh, tt.args.km, tt.args.forwardReadsToTarget, tt.args.forwardSystemQueriesToTarget, false, tt.args.forwardAuthToTarget)
			if err != nil {
				if !reflect.DeepEqual(err.Error(), tt.expected) {
					t.Errorf("parseStatement() actual = %v, expected %v", err, tt.expected)
				}
			} else if !reflect.DeepEqual(actual, tt.expected) {
				t.Errorf("parseStatement() actual = %v, want %v", actual, tt.expected)
			}
		})
	}
}

func TestModifyFrame(t *testing.T) {
	type args struct {
		f                    *frame.RawFrame
	}
	tests := []struct {
		name              string
		args              args
		positionsReplaced []int
		statementType     statementType
	}{
		// QUERY
		{"OpCodeQuery SELECT", args{mockQueryFrame("SELECT blah FROM ks1.t2")}, []int{}, statementTypeSelect},

		{"OpCodeQuery INSERT", args{mockQueryFrame("INSERT INTO blah (a, b) VALUES (now(), 1)")}, []int{0}, statementTypeInsert},

		{"OpCodeQuery UPDATE", args{mockQueryFrame("UPDATE blah SET a = ?, b = now() WHERE a = now()")}, []int{1,2}, statementTypeUpdate},
		{"OpCodeQuery UPDATE Conditional", args{mockQueryFrame("UPDATE blah SET a = ?, b = 123 WHERE a = now() IF b = now()")}, []int{2,3}, statementTypeUpdate},
		{"OpCodeQuery UPDATE Complex", args{mockQueryFrame("UPDATE blah SET a[?] = ?, b[now()] = 123, c[1] = now() WHERE a = 123")}, []int{2,5}, statementTypeUpdate},

		{"OpCodeQuery DELETE No Operation", args{mockQueryFrame("DELETE FROM blah WHERE b = 123 AND a = now()")}, []int{1}, statementTypeDelete},
		{"OpCodeQuery DELETE", args{mockQueryFrame("DELETE a FROM blah WHERE b = 123 AND a = now()")}, []int{1}, statementTypeDelete},
		{"OpCodeQuery DELETE Conditional", args{mockQueryFrame("DELETE a FROM blah WHERE a = now() IF b = now()")}, []int{0, 1}, statementTypeDelete},
		{"OpCodeQuery DELETE Complex", args{mockQueryFrame("DELETE c[1], a[?], b[now()] FROM blah WHERE b = 123 AND a = now()")}, []int{2,4}, statementTypeDelete},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			context := &frameDecodeContext{frame: test.args.f}
			queryInfo, err := context.GetOrInspectQuery()
			require.Nil(t, err)
			newContext, err := modifyFrame(context)
			require.Nil(t, err)

			require.Equal(t, test.statementType, queryInfo.getStatementType())
			require.Equal(t, test.statementType, newContext.queryInfo.getStatementType())

			if len(test.positionsReplaced) != 0 {
				require.NotEqual(t, context.frame, newContext.frame)
				require.Equal(t, context.frame.Header.OpCode, newContext.frame.Header.OpCode)
				require.Equal(t, context.frame.Header.StreamId, newContext.frame.Header.StreamId)
				require.Equal(t, context.frame.Header.Flags, newContext.frame.Header.Flags)
				require.Equal(t, context.frame.Header.Version, newContext.frame.Header.Version)
				require.NotEqual(t, context.frame.Body, newContext.frame.Body)
				require.NotEqual(t, queryInfo, newContext.queryInfo)
			} else {
				require.Equal(t, context.frame, newContext.frame)
				require.Equal(t, context.frame.Body, newContext.frame.Body)
				require.Equal(t, context.frame.Header, newContext.frame.Header)
				require.Equal(t, queryInfo, newContext.queryInfo)
			}

			if len(queryInfo.getParsedStatements()) == 0 {
				require.Equal(t, 0, len(test.positionsReplaced))
				return
			}

			parsedStatement := queryInfo.getParsedStatements()[0]
			oldTerms := parsedStatement.terms
			newParsedStatement := newContext.queryInfo.getParsedStatements()[0]
			newTerms := newParsedStatement.terms

			require.Equal(t, len(oldTerms), len(newTerms))
			for idx, oldTerm := range oldTerms {
				newTerm := newTerms[idx]
				if contains(test.positionsReplaced, idx) {
					require.NotEqual(t, oldTerm, newTerm)

					require.True(t, oldTerm.isFunctionCall())
					require.False(t, newTerm.isFunctionCall())

					require.False(t, oldTerm.isLiteral())
					require.True(t, newTerm.isLiteral())
				} else {
					require.Equal(t, oldTerm, newTerm)
				}
			}
		})
	}

}

func contains(s []int, e int) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func mockPrepareFrame(query string) *frame.RawFrame {
	prepareMsg := &message.Prepare{
		Query:    query,
		Keyspace: "",
	}
	return mockFrame(prepareMsg)
}

func mockQueryFrame(query string) *frame.RawFrame {
	queryMsg := &message.Query{
		Query: query,
	}
	return mockFrame(queryMsg)
}

func mockExecuteFrame(preparedId string) *frame.RawFrame {
	executeMsg := &message.Execute{
		QueryId:          []byte(preparedId),
		ResultMetadataId: nil,
	}
	return mockFrame(executeMsg)
}

func mockBatch(query interface{}) *frame.RawFrame {
	batchMsg := &message.Batch{Children: []*message.BatchChild{{QueryOrId: query}}}
	return mockFrame(batchMsg)
}

func mockAuthResponse() *frame.RawFrame {
	authCreds := &AuthCredentials{
		AuthId:   "",
		Username: "user",
		Password: "password",
	}
	token := authCreds.Marshal()
	return mockFrame(&message.AuthResponse{Token: token})
}

func mockFrame(message message.Message) *frame.RawFrame {
	f := frame.NewFrame(primitive.ProtocolVersion4, 1, message)
	rawFrame, _ := defaultCodec.ConvertToRawFrame(f)
	return rawFrame
}

func newFakeMetricHandler() *metrics.MetricHandler {
	return metrics.NewMetricHandler(noopmetrics.NewNoopMetricFactory(), []float64{}, []float64{}, newFakeProxyMetrics(), nil, nil)
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

type fakeMetric struct { }

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

func (recv *fakeMetric) WithLabels(map[string] string) metrics.Metric {
	return newFakeMetric()
}

func newFakeMetric() metrics.Metric {
	return &fakeMetric{}
}
