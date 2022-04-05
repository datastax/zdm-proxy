package cloudgateproxy

import (
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"testing"
)

func TestParseAndInspect_SearchRequests(t *testing.T) {

	type testParams struct {
		name        string
		queryString string
		expected    interface{}
	}

	tests := []testParams{
		{"Query using CONTAINS",
			"select * from person where hometown contains 'Bangkok';",
			NewGenericStatementInfo(forwardToOrigin),
		},
		{"Query using greater and less than",
			"select * from person where age > 35 and age < 80;",
			NewGenericStatementInfo(forwardToOrigin),
		},
		{"Query using basic solr_query clause",
			"select * from person where solr_query='firstname: Olga firstname: Raymond -hometown: Bangkok';",
			NewGenericStatementInfo(forwardToOrigin),
		},
		{"Query using JSON solr_query clause",
			"select * from person where solr_query='{\"q\":\"hometown:Bangkok\"}';",
			NewGenericStatementInfo(forwardToOrigin),
		},
		{"Query using JSON solr_query clause with faceting",
			"select * from person where solr_query='{\"q\":\"id:*\",\"facet\":{\"field\":\"hometown\"}}';",
			NewGenericStatementInfo(forwardToOrigin),
		},
		{"Query using JSON solr_query clause for generic single pass search",
			"select * from person where solr_query='{\"q\" : \"*:*\", \"distrib.singlePass\" : true}';",
			NewGenericStatementInfo(forwardToOrigin),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			queryFrame := &frame.Frame{
				Header: getSearchQueryHeaderForTests(),
				Body: &frame.Body{
					Message: buildQueryMessageForTests(tt.queryString),
				},
			}
			queryRawFrame := convertEncodedRequestToRawFrameForTests(queryFrame, t)
			stmt, err := parseEncodedRequestForTests(queryRawFrame, t)
			checkExpectedForwardDecisionOrErrorForTests(stmt, err, tt.expected, t)
		})
	}

}

func getSearchQueryHeaderForTests() *frame.Header {
	return &frame.Header{
		IsResponse: false,
		Version:    primitive.ProtocolVersionDse2,
		Flags:      primitive.HeaderFlag(0x00),
		StreamId:   0,
		OpCode:     primitive.OpCodeQuery,
		BodyLength: 0,
	}
}
