package cloudgateproxy

import (
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics"
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
	psCache.cache["BOTH"] = NewPreparedStatementInfo(forwardToBoth)
	psCache.cache["ORIGIN"] = NewPreparedStatementInfo(forwardToOrigin)
	psCache.cache["TARGET"] = NewPreparedStatementInfo(forwardToTarget)
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
		{"OpCodePrepare SELECT", args{mockPrepareFrame("SELECT blah FROM ks1.t1"), psCache, mh, km, forwardReadsToOrigin}, NewPreparedStatementInfo(forwardToOrigin)},
		{"OpCodePrepare SELECT system.local", args{mockPrepareFrame("SELECT * FROM system.local"), psCache, mh, km, forwardReadsToOrigin}, NewPreparedStatementInfo(forwardToTarget)},
		{"OpCodePrepare SELECT system.peers", args{mockPrepareFrame("SELECT * FROM system.peers"), psCache, mh, km, forwardReadsToOrigin}, NewPreparedStatementInfo(forwardToTarget)},
		{"OpCodePrepare SELECT system.peers_v2", args{mockPrepareFrame("SELECT * FROM system.peers_v2"), psCache, mh, km, forwardReadsToOrigin}, NewPreparedStatementInfo(forwardToTarget)},
		{"OpCodePrepare SELECT system_auth.roles", args{mockPrepareFrame("SELECT * FROM system_auth.roles"), psCache, mh, km, forwardReadsToOrigin}, NewPreparedStatementInfo(forwardToTarget)},
		{"OpCodePrepare SELECT dse_insights.tokens", args{mockPrepareFrame("SELECT * FROM dse_insights.tokens"), psCache, mh, km, forwardReadsToOrigin}, NewPreparedStatementInfo(forwardToTarget)},
		{"OpCodePrepare non SELECT", args{mockPrepareFrame("INSERT blah"), psCache, mh, km, forwardReadsToOrigin}, NewPreparedStatementInfo(forwardToBoth)},
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
			actual, err := inspectFrame(tt.args.f, tt.args.psCache, tt.args.mh, tt.args.km, tt.args.forwardReadsToTarget)
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
