package zdmproxy

import (
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"testing"
)

func TestParseAndInspect_GraphRequests_Core_StringAPI(t *testing.T) {

	type testParams struct {
		name                            string
		includeGraphNameInCustomPayload bool
		queryString                     string
		expected                        interface{}
	}

	tests := []testParams{
		{"Create graph",
			false,
			"system.graph('friendship').ifNotExists().create()",
			NewGenericRequestInfo(forwardToBoth, false, true, primitive.OpCodeExecute),
		},
		{"Create graph schema",
			true,
			"schema.vertexLabel('person').ifNotExists().partitionBy('id', Text)" +
				".property('firstname', Text).property('surname', Text)" +
				".property('hometown', Text).property('age', Int).create();" +
				"schema.edgeLabel('is_friend_of').ifNotExists().from('person').to('person')" +
				".property('friendshipStartDate', Date).create();" +
				"schema.vertexLabel('person').searchIndex().ifNotExists().by('firstname').asText().by('surname').asText()" +
				".by('hometown').asText().by('age').create();" +
				"schema.edgeLabel('is_friend_of').from('person').to('person')" +
				".materializedView('person__is_parent_of__person_by_in_id').ifNotExists().inverse().create()}} ",
			NewGenericRequestInfo(forwardToBoth, false, true, primitive.OpCodeExecute),
		},
		{"Write data to graph",
			true,
			"g.V().has('person','id', p1_id).as('p1')" +
				".addV('person').property('id', p2_id)" +
				".property('firstname', p2_firstname).property('surname', p2_surname)" +
				".property('hometown', p2_hometown).property('age', p2_age).as('p2')" +
				".addE('is_friend_of').from('p1').to('p2').property('friendshipStartDate', fsd);",
			NewGenericRequestInfo(forwardToBoth, false, true, primitive.OpCodeExecute),
		},
		{"Select person vertex by id (Brenda_Peterson)",
			true,
			"g.V().has('person','id', p1_id).elementMap()",
			NewGenericRequestInfo(forwardToBoth, false, true, primitive.OpCodeQuery),
		},
		{"Select person vertices by age range (70-90)",
			true,
			"g.V().has('person','age', gt(lower_end)).has('person','age', lt(upper_end)).elementMap()",
			NewGenericRequestInfo(forwardToBoth, false, true, primitive.OpCodeQuery),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			queryFrame := &frame.Frame{
				Header: getGraphQueryHeaderForTests(),
				Body: &frame.Body{
					CustomPayload: getCustomPayloadForStringAPIForTests(tt.includeGraphNameInCustomPayload),
					Message:       buildQueryMessageForTests(tt.queryString),
				},
			}
			queryRawFrame := convertEncodedRequestToRawFrameForTests(queryFrame, t)
			stmt, err := parseEncodedRequestForTests(queryRawFrame, t)
			checkExpectedForwardDecisionOrErrorForTests(stmt, err, tt.expected, t)
		})
	}

}

func TestParseAndInspect_GraphRequests_Core_FluentAPI(t *testing.T) {
	type testParams struct {
		name       string
		queryBytes []byte
		expected   interface{}
	}

	// Graph and schema creation operations are only available through the String API
	tests := []testParams{
		{"Write data to graph",
			[]byte{21, 0, 0, 0, 0, 14, 0, 0, 0, 1, 86, 0, 0, 0, 0, 0, 0, 0, 3, 104, 97, 115, 0, 0, 0, 3, 3, 0, 0, 0, 0, 6, 112, 101, 114, 115, 111,
				110, 3, 0, 0, 0, 0, 2, 105, 100, 3, 0, 0, 0, 0, 12, 69, 114, 105, 99, 97, 95, 72, 117, 110, 116, 101, 114, 0, 0, 0, 2, 97, 115, 0, 0, 0, 1, 3,
				0, 0, 0, 0, 2, 112, 49, 0, 0, 0, 4, 97, 100, 100, 86, 0, 0, 0, 1, 3, 0, 0, 0, 0, 6, 112, 101, 114, 115, 111, 110, 0, 0, 0, 8, 112, 114, 111, 112,
				101, 114, 116, 121, 0, 0, 0, 2, 3, 0, 0, 0, 0, 2, 105, 100, 3, 0, 0, 0, 0, 16, 66, 101, 115, 115, 105, 101, 95, 70, 101, 114, 110, 97, 110,
				100, 101, 122, 0, 0, 0, 8, 112, 114, 111, 112, 101, 114, 116, 121, 0, 0, 0, 2, 3, 0, 0, 0, 0, 9, 102, 105, 114, 115, 116, 110, 97, 109, 101,
				3, 0, 0, 0, 0, 6, 66, 101, 115, 115, 105, 101, 0, 0, 0, 8, 112, 114, 111, 112, 101, 114, 116, 121, 0, 0, 0, 2, 3, 0, 0, 0, 0, 7, 115, 117, 114,
				110, 97, 109, 101, 3, 0, 0, 0, 0, 9, 70, 101, 114, 110, 97, 110, 100, 101, 122, 0, 0, 0, 8, 112, 114, 111, 112, 101, 114, 116, 121, 0, 0,
				0, 2, 3, 0, 0, 0, 0, 8, 104, 111, 109, 101, 116, 111, 119, 110, 3, 0, 0, 0, 0, 5, 83, 101, 111, 117, 108, 0, 0, 0, 8, 112, 114, 111, 112, 101,
				114, 116, 121, 0, 0, 0, 2, 3, 0, 0, 0, 0, 3, 97, 103, 101, 1, 0, 0, 0, 0, 50, 0, 0, 0, 2, 97, 115, 0, 0, 0, 1, 3, 0, 0, 0, 0, 2, 112, 50, 0, 0, 0, 4,
				97, 100, 100, 69, 0, 0, 0, 1, 3, 0, 0, 0, 0, 12, 105, 115, 95, 102, 114, 105, 101, 110, 100, 95, 111, 102, 0, 0, 0, 4, 102, 114, 111, 109,
				0, 0, 0, 1, 3, 0, 0, 0, 0, 2, 112, 49, 0, 0, 0, 2, 116, 111, 0, 0, 0, 1, 3, 0, 0, 0, 0, 2, 112, 50, 0, 0, 0, 8, 112, 114, 111, 112, 101, 114, 116,
				121, 0, 0, 0, 2, 3, 0, 0, 0, 0, 19, 102, 114, 105, 101, 110, 100, 115, 104, 105, 112, 83, 116, 97, 114, 116, 68, 97, 116, 101, 132, 0, 0,
				0, 7, 169, 6, 16, 0, 0, 0, 0},
			NewGenericRequestInfo(forwardToBoth, false, true, primitive.OpCodeExecute),
		},
		{"Select person vertex by id (Brenda_Peterson)",
			[]byte{21, 0, 0, 0, 0, 3, 0, 0, 0, 1, 86, 0, 0, 0, 0, 0, 0, 0, 3, 104, 97, 115, 0, 0, 0, 3, 3, 0, 0, 0, 0, 6, 112, 101, 114, 115, 111,
				110, 3, 0, 0, 0, 0, 2, 105, 100, 3, 0, 0, 0, 0, 15, 66, 114, 101, 110, 100, 97, 95, 80, 101, 116, 101, 114, 115, 111, 110, 0, 0, 0, 10, 101,
				108, 101, 109, 101, 110, 116, 77, 97, 112, 0, 0, 0, 0, 0, 0, 0, 0},
			NewGenericRequestInfo(forwardToBoth, false, true, primitive.OpCodeQuery),
		},
		{"Select person vertices by age range (70-90)",
			[]byte{21, 0, 0, 0, 0, 4, 0, 0, 0, 1, 86, 0, 0, 0, 0, 0, 0, 0, 3, 104, 97, 115, 0, 0, 0, 3, 3, 0, 0, 0, 0, 6, 112, 101, 114, 115, 111,
				110, 3, 0, 0, 0, 0, 3, 97, 103, 101, 30, 0, 0, 0, 0, 2, 103, 116, 0, 0, 0, 1, 1, 0, 0, 0, 0, 70, 0, 0, 0, 3, 104, 97, 115, 0, 0, 0, 3, 3, 0, 0, 0,
				0, 6, 112, 101, 114, 115, 111, 110, 3, 0, 0, 0, 0, 3, 97, 103, 101, 30, 0, 0, 0, 0, 2, 108, 116, 0, 0, 0, 1, 1, 0, 0, 0, 0, 90, 0, 0, 0, 10,
				101, 108, 101, 109, 101, 110, 116, 77, 97, 112, 0, 0, 0, 0, 0, 0, 0, 0},
			NewGenericRequestInfo(forwardToBoth, false, true, primitive.OpCodeQuery),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			queryFrame := &frame.Frame{
				Header: getGraphQueryHeaderForTests(),
				Body: &frame.Body{
					CustomPayload: getCustomPayloadForFluentAPIForTests(tt.queryBytes),
					Message:       buildQueryMessageForTests(""),
				},
			}
			queryRawFrame := convertEncodedRequestToRawFrameForTests(queryFrame, t)
			stmt, err := parseEncodedRequestForTests(queryRawFrame, t)
			checkExpectedForwardDecisionOrErrorForTests(stmt, err, tt.expected, t)
		})
	}

}

func getGraphQueryHeaderForTests() *frame.Header {
	return &frame.Header{
		IsResponse: false,
		Version:    primitive.ProtocolVersionDse2,
		Flags:      primitive.HeaderFlagCustomPayload,
		StreamId:   0,
		OpCode:     primitive.OpCodeQuery,
		BodyLength: 0,
	}
}

func getCustomPayloadForStringAPIForTests(includeGraphName bool) map[string][]byte {
	customPayload := createCommonGraphCustomPayloadForTests(includeGraphName)
	customPayload["graph-language"] = []byte{103, 114, 101, 109, 108, 105, 110, 45, 103, 114, 111, 111, 118, 121}
	return customPayload
}

func getCustomPayloadForFluentAPIForTests(queryBytes []byte) map[string][]byte {
	customPayload := createCommonGraphCustomPayloadForTests(true)
	customPayload["graph-language"] = []byte{98, 121, 116, 101, 99, 111, 100, 101, 45, 106, 115, 111, 110}
	customPayload["graph-binary-query"] = queryBytes
	return customPayload
}

func createCommonGraphCustomPayloadForTests(includeGraphName bool) map[string][]byte {
	customPayload := make(map[string][]byte, 0)
	customPayload["graph-source"] = []byte{103}
	customPayload["graph-results"] = []byte{103, 114, 97, 112, 104, 45, 98, 105, 110, 97, 114, 121, 45, 49, 46, 48}
	if includeGraphName {
		customPayload["graph-name"] = []byte{102, 114, 105, 101, 110, 100, 115, 104, 105, 112}
	}

	return customPayload
}
