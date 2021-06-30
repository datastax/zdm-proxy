package cloudgateproxy

import (
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics"
	"github.com/stretchr/testify/require"
	"net/http"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func TestInspectFrame(t *testing.T) {
	type args struct {
		f                    *frame.RawFrame
		psCache              *PreparedStatementCache
		mh                   metrics.IMetricsHandler
		km                   *atomic.Value
		forwardReadsToTarget bool
	}
	psCache := NewPreparedStatementCache()
	psCache.cache["BOTH"] = NewPreparedStatementInfo(NewGenericStatementInfo(forwardToBoth))
	psCache.cache["ORIGIN"] = NewPreparedStatementInfo(NewGenericStatementInfo(forwardToOrigin))
	psCache.cache["TARGET"] = NewPreparedStatementInfo(NewGenericStatementInfo(forwardToTarget))
	mh := mockMetricsHandler{}
	km := new(atomic.Value)
	forwardReadsToTarget := true
	forwardReadsToOrigin := false
	tests := []struct {
		name     string
		args     args
		expected interface{}
	}{
		// QUERY
		{"OpCodeQuery SELECT", args{mockQueryFrame("SELECT blah FROM ks1.t2"), psCache, mh, km, forwardReadsToOrigin}, NewGenericStatementInfo(forwardToOrigin)},
		{"OpCodeQuery SELECT forwardReadsToTarget", args{mockQueryFrame("SELECT blah FROM ks1.t1"), psCache, mh, km, forwardReadsToTarget}, NewGenericStatementInfo(forwardToTarget)},
		{"OpCodeQuery SELECT system.local", args{mockQueryFrame("SELECT * FROM system.local"), psCache, mh, km, forwardReadsToTarget}, NewGenericStatementInfo(forwardToTarget)},
		{"OpCodeQuery SELECT system.local", args{mockQueryFrame("SELECT * FROM system.local"), psCache, mh, km, forwardReadsToOrigin}, NewGenericStatementInfo(forwardToTarget)},
		{"OpCodeQuery SELECT system.peers", args{mockQueryFrame("SELECT * FROM system.peers"), psCache, mh, km, forwardReadsToOrigin}, NewGenericStatementInfo(forwardToTarget)},
		{"OpCodeQuery SELECT system.peers_v2", args{mockQueryFrame("SELECT * FROM system.peers_v2"), psCache, mh, km, forwardReadsToOrigin}, NewGenericStatementInfo(forwardToTarget)},
		{"OpCodeQuery SELECT system_auth.roles", args{mockQueryFrame("SELECT * FROM system_auth.roles"), psCache, mh, km, forwardReadsToOrigin}, NewGenericStatementInfo(forwardToTarget)},
		{"OpCodeQuery SELECT dse_insights.tokens", args{mockQueryFrame("SELECT * FROM dse_insights.tokens"), psCache, mh, km, forwardReadsToOrigin}, NewGenericStatementInfo(forwardToTarget)},
		{"OpCodeQuery non SELECT", args{mockQueryFrame("INSERT blah"), psCache, mh, km, forwardReadsToOrigin}, NewGenericStatementInfo(forwardToBoth)},
		// PREPARE
		{"OpCodePrepare SELECT", args{mockPrepareFrame("SELECT blah FROM ks1.t1"), psCache, mh, km, forwardReadsToOrigin}, NewPreparedStatementInfo(NewGenericStatementInfo(forwardToOrigin))},
		{"OpCodePrepare SELECT system.local", args{mockPrepareFrame("SELECT * FROM system.local"), psCache, mh, km, forwardReadsToOrigin}, NewPreparedStatementInfo(NewGenericStatementInfo(forwardToTarget))},
		{"OpCodePrepare SELECT system.peers", args{mockPrepareFrame("SELECT * FROM system.peers"), psCache, mh, km, forwardReadsToOrigin}, NewPreparedStatementInfo(NewGenericStatementInfo(forwardToTarget))},
		{"OpCodePrepare SELECT system.peers_v2", args{mockPrepareFrame("SELECT * FROM system.peers_v2"), psCache, mh, km, forwardReadsToOrigin}, NewPreparedStatementInfo(NewGenericStatementInfo(forwardToTarget))},
		{"OpCodePrepare SELECT system_auth.roles", args{mockPrepareFrame("SELECT * FROM system_auth.roles"), psCache, mh, km, forwardReadsToOrigin}, NewPreparedStatementInfo(NewGenericStatementInfo(forwardToTarget))},
		{"OpCodePrepare SELECT dse_insights.tokens", args{mockPrepareFrame("SELECT * FROM dse_insights.tokens"), psCache, mh, km, forwardReadsToOrigin}, NewPreparedStatementInfo(NewGenericStatementInfo(forwardToTarget))},
		{"OpCodePrepare non SELECT", args{mockPrepareFrame("INSERT blah"), psCache, mh, km, forwardReadsToOrigin}, NewPreparedStatementInfo(NewGenericStatementInfo(forwardToBoth))},
		// EXECUTE
		{"OpCodeExecute origin", args{mockExecuteFrame("ORIGIN"), psCache, mh, km, forwardReadsToOrigin}, NewGenericStatementInfo(forwardToOrigin)},
		{"OpCodeExecute target", args{mockExecuteFrame("TARGET"), psCache, mh, km, forwardReadsToOrigin}, NewGenericStatementInfo(forwardToTarget)},
		{"OpCodeExecute both", args{mockExecuteFrame("BOTH"), psCache, mh, km, forwardReadsToOrigin}, NewGenericStatementInfo(forwardToBoth)},
		{"OpCodeExecute unknown", args{mockExecuteFrame("UNKNOWN"), psCache, mh, km, forwardReadsToOrigin}, "The preparedID of the statement to be executed (UNKNOWN) does not exist in the proxy cache"},
		// REGISTER
		{"OpCodeRegister", args{mockFrame(&message.Register{EventTypes: []primitive.EventType{primitive.EventTypeSchemaChange}}), psCache, mh, km, forwardReadsToOrigin}, NewGenericStatementInfo(forwardToBoth)},
		// others
		{"OpCodeBatch", args{mockBatch("irrelevant"), psCache, mh, km, forwardReadsToOrigin}, NewGenericStatementInfo(forwardToBoth)},
		{"OpCodeStartup", args{mockFrame(message.NewStartup()), psCache, mh, km, forwardReadsToOrigin}, NewGenericStatementInfo(forwardToOrigin)},
		{"OpCodeOptions", args{mockFrame(&message.Options{}), psCache, mh, km, forwardReadsToOrigin}, NewGenericStatementInfo(forwardToBoth)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := inspectFrame(&frameDecodeContext{frame: tt.args.f}, tt.args.psCache, tt.args.mh, tt.args.km, tt.args.forwardReadsToTarget, false)
			if err != nil {
				if !reflect.DeepEqual(err.Error(), tt.expected) {
					t.Errorf("inspectFrame() actual = %v, expected %v", err, tt.expected)
				}
			} else if !reflect.DeepEqual(actual, tt.expected) {
				t.Errorf("inspectFrame() actual = %v, want %v", actual, tt.expected)
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

type mockMetricsHandler struct{}

//goland:noinspection GoUnusedParameter
func (h mockMetricsHandler) AddCounter(mn metrics.Metric) error { return nil }

//goland:noinspection GoUnusedParameter
func (h mockMetricsHandler) AddGauge(mn metrics.Metric) error { return nil }

//goland:noinspection GoUnusedParameter
func (h mockMetricsHandler) AddGaugeFunction(mn metrics.Metric, mf func() float64) error {
	return nil
}

//goland:noinspection GoUnusedParameter
func (h mockMetricsHandler) AddHistogram(mn metrics.Metric, buckets []float64) error { return nil }

//goland:noinspection GoUnusedParameter
func (h mockMetricsHandler) IncrementCountByOne(mn metrics.Metric) error { return nil }

//goland:noinspection GoUnusedParameter
func (h mockMetricsHandler) DecrementCountByOne(mn metrics.Metric) error { return nil }

//goland:noinspection GoUnusedParameter
func (h mockMetricsHandler) AddToCount(mn metrics.Metric, valueToAdd int) error { return nil }

//goland:noinspection GoUnusedParameter
func (h mockMetricsHandler) SubtractFromCount(mn metrics.Metric, valueToSubtract int) error {
	return nil
}

//goland:noinspection GoUnusedParameter
func (h mockMetricsHandler) TrackInHistogram(mn metrics.Metric, timeToTrack time.Time) error {
	return nil
}
func (h mockMetricsHandler) UnregisterAllMetrics() error { return nil }

func (h mockMetricsHandler) Handler() http.Handler { return nil }

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

func mockBatch(query string) *frame.RawFrame {
	batchMsg := &message.Batch{Children: []*message.BatchChild{{QueryOrId: query}}}
	return mockFrame(batchMsg)
}

func mockFrame(message message.Message) *frame.RawFrame {
	f := frame.NewFrame(primitive.ProtocolVersion4, 1, message)
	rawFrame, _ := defaultCodec.ConvertToRawFrame(f)
	return rawFrame
}
