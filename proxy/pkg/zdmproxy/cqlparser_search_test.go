package zdmproxy

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
			NewGenericRequestInfo(forwardToOrigin, true, true),
		},
		{"Query using greater and less than",
			"select * from person where age > 35 and age < 80;",
			NewGenericRequestInfo(forwardToOrigin, true, true),
		},
		{"Query using basic solr_query clause",
			"select * from person where solr_query='firstname: Olga firstname: Raymond -hometown: Bangkok';",
			NewGenericRequestInfo(forwardToOrigin, true, true),
		},
		{"Query using JSON solr_query clause",
			"select * from person where solr_query='{\"q\":\"hometown:Bangkok\"}';",
			NewGenericRequestInfo(forwardToOrigin, true, true),
		},
		{"Query using JSON solr_query clause with faceting",
			"select * from person where solr_query='{\"q\":\"id:*\",\"facet\":{\"field\":\"hometown\"}}';",
			NewGenericRequestInfo(forwardToOrigin, true, true),
		},
		{"Query using JSON solr_query clause for generic single pass search",
			"select * from person where solr_query='{\"q\" : \"*:*\", \"distrib.singlePass\" : true}';",
			NewGenericRequestInfo(forwardToOrigin, true, true),
		},
		{"Query using SAI fuzzy search operator",
			"select * from person where nick_name:'fuzzy';",
			NewGenericRequestInfo(forwardToOrigin, true, true),
		},
		{"Query using OR operator in SAI index",
			"select * from person where nick_name = 'Foo' or nick_name = 'Bar' and age > 10;",
			NewGenericRequestInfo(forwardToOrigin, true, true),
		},
		{"Query using CONTAINS KEY operator in SAI index",
			"SELECT * FROM cyclist_teams WHERE teams CONTAINS KEY 2014;",
			NewGenericRequestInfo(forwardToOrigin, true, true),
		},
		{"Query using CONTAINS operator and UDT",
			"SELECT * FROM cycling.cyclist_races WHERE races CONTAINS { race_title:'Rabobank 7-Dorpenomloop Aalburg', race_date:'2015-05-09', race_time:'02:58:33' };",
			NewGenericRequestInfo(forwardToOrigin, true, true),
		},
		{"Query using CONTAINS KEY operator in SAI index",
			"SELECT * FROM cyclist_teams WHERE teams CONTAINS 'Team Garmin - Cervelo';",
			NewGenericRequestInfo(forwardToOrigin, true, true),
		},
		{"Query IN operator",
			"SELECT * FROM cycling.comments_vs WHERE created_at IN ('2017-03-21 21:11:09.999000+0000', '2017-03-22 01:16:59.001000+0000');",
			NewGenericRequestInfo(forwardToOrigin, true, true),
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
