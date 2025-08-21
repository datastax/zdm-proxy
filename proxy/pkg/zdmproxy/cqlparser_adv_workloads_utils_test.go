package zdmproxy

import (
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/zdm-proxy/proxy/pkg/common"
	"github.com/datastax/zdm-proxy/proxy/pkg/metrics"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

type params struct {
	psCache                      *PreparedStatementCache
	mh                           *metrics.MetricHandler
	kn                           string
	primaryCluster               common.ClusterType
	forwardSystemQueriesToTarget bool
	forwardAuthToTarget          bool
	virtualizationEnabled        bool
	timeUuidGenerator            TimeUuidGenerator
}

func getGeneralParamsForTests(t *testing.T) params {
	timeUuidGen, err := GetDefaultTimeUuidGenerator()
	require.Nil(t, err)

	return params{
		psCache:                      NewPreparedStatementCache(),
		mh:                           newFakeMetricHandler(),
		kn:                           "",
		primaryCluster:               common.ClusterTypeOrigin,
		forwardSystemQueriesToTarget: false,
		forwardAuthToTarget:          false,
		virtualizationEnabled:        false,
		timeUuidGenerator:            timeUuidGen,
	}
}

func buildQueryMessageForTests(queryString string) *message.Query {
	var defaultTimestamp int64 = 1647023221311969
	var serialConsistency = primitive.ConsistencyLevelLocalSerial
	return &message.Query{
		Query: queryString,
		Options: &message.QueryOptions{
			Consistency:       primitive.ConsistencyLevelOne,
			PositionalValues:  nil,
			NamedValues:       nil,
			SkipMetadata:      true,
			PageSize:          5000,
			PageSizeInBytes:   false,
			PagingState:       nil,
			SerialConsistency: &serialConsistency,
			DefaultTimestamp:  &defaultTimestamp,
			Keyspace:          "",
			NowInSeconds:      nil,
			ContinuousPagingOptions: &message.ContinuousPagingOptions{
				MaxPages:       0,
				PagesPerSecond: 0,
				NextPages:      4,
			},
		},
	}
}

func convertEncodedRequestToRawFrameForTests(queryFrame *frame.Frame, t *testing.T) *frame.RawFrame {
	codec := frame.NewRawCodec()
	queryRawFrame, err := codec.ConvertToRawFrame(queryFrame)
	require.Nil(t, err)
	return queryRawFrame
}

func parseEncodedRequestForTests(queryRawFrame *frame.RawFrame, t *testing.T) (RequestInfo, error) {
	generalParams := getGeneralParamsForTests(t)

	return buildRequestInfo(&frameDecodeContext{frame: queryRawFrame, compression: primitive.CompressionNone},
		[]*statementReplacedTerms{},
		generalParams.psCache,
		generalParams.mh,
		generalParams.kn,
		generalParams.primaryCluster,
		generalParams.forwardSystemQueriesToTarget,
		generalParams.virtualizationEnabled,
		generalParams.forwardAuthToTarget,
		generalParams.timeUuidGenerator)
}

func checkExpectedForwardDecisionOrErrorForTests(actualRequestInfo RequestInfo, actualError error, expected interface{}, t *testing.T) {
	if actualError != nil {
		require.True(t, reflect.DeepEqual(actualError.Error(), expected), "buildRequestInfo() actual error = %v, expected error %v", actualError, expected)
	} else {
		require.True(t, reflect.DeepEqual(actualRequestInfo, expected), "buildRequestInfo() actual statement = %v, expected statement %v", actualRequestInfo, expected)
	}
}
