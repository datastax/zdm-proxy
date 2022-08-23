package cloudgateproxy

import (
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/zdm-proxy/proxy/pkg/metrics"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

type params struct {
	psCache                      *PreparedStatementCache
	mh                           *metrics.MetricHandler
	kn                           string
	forwardReadsToTarget         bool
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
		forwardReadsToTarget:         false,
		forwardSystemQueriesToTarget: false,
		forwardAuthToTarget:          false,
		virtualizationEnabled:        false,
		timeUuidGenerator:            timeUuidGen,
	}
}

func buildQueryMessageForTests(queryString string) *message.Query {
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
			SerialConsistency: &primitive.NillableConsistencyLevel{Value: primitive.ConsistencyLevelLocalSerial},
			DefaultTimestamp:  &primitive.NillableInt64{Value: 1647023221311969},
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

	return buildRequestInfo(&frameDecodeContext{frame: queryRawFrame},
		[]*statementReplacedTerms{},
		generalParams.psCache,
		generalParams.mh,
		generalParams.kn,
		generalParams.forwardReadsToTarget,
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
