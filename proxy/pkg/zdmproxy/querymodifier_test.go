package zdmproxy

import (
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/zdm-proxy/proxy/pkg/config"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReplaceQueryString(t *testing.T) {
	tests := []struct {
		name                 string
		f                    *frame.RawFrame
		replacedTerms        []*statementReplacedTerms
		positionsReplaced    map[int][][]int
		namedGeneratedValues bool
		statementTypes       map[int]statementType
	}{
		// QUERY
		{"OpCodeQuery SELECT",
			mockQueryFrame(t, "SELECT blah FROM ks1.t2"),
			[]*statementReplacedTerms{}, map[int][][]int{}, false, map[int]statementType{0: statementTypeSelect}},
		{"OpCodeQuery INSERT",
			mockQueryFrame(t, "INSERT INTO blah (a, b) VALUES (now(), 1)"),
			[]*statementReplacedTerms{{statementIndex: 0, replacedTerms: []*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 32, 36), -1)}}},
			map[int][][]int{0: {{0}}}, false, map[int]statementType{0: statementTypeInsert}},
		{"OpCodeQuery INSERT NAMED",
			mockQueryFrame(t, "INSERT INTO blah (a, b) VALUES (now(), :bparam)"),
			[]*statementReplacedTerms{{statementIndex: 0, replacedTerms: []*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 32, 36), -1)}}},
			map[int][][]int{0: {{0}}}, false, map[int]statementType{0: statementTypeInsert}},
		{"OpCodePrepare INSERT",
			mockPrepareFrame(t, "INSERT INTO blah (a, b) VALUES (now(), 1)"),
			[]*statementReplacedTerms{{statementIndex: 0, replacedTerms: []*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 32, 36), -1)}}},
			map[int][][]int{0: {{0}}}, false, map[int]statementType{0: statementTypeInsert}},
		{"OpCodePrepare INSERT NAMED",
			mockPrepareFrame(t, "INSERT INTO blah (a, b) VALUES (now(), :bparam)"),
			[]*statementReplacedTerms{{statementIndex: 0, replacedTerms: []*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 32, 36), -1)}}},
			map[int][][]int{0: {{0}}}, true, map[int]statementType{0: statementTypeInsert}},
		{"OpCodeQuery UPDATE",
			mockQueryFrame(t, "UPDATE blah SET a = ?, b = now() WHERE a = now()"),
			[]*statementReplacedTerms{{statementIndex: 0, replacedTerms: []*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 27, 31), 0),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 43, 47), 0)}}},
			map[int][][]int{0: {{1, 2}}}, false, map[int]statementType{0: statementTypeUpdate}},
		{"OpCodeQuery UPDATE NAMED",
			mockQueryFrame(t, "UPDATE blah SET a = :aparam, b = now() WHERE a = now()"),
			[]*statementReplacedTerms{{statementIndex: 0, replacedTerms: []*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 33, 37), -1),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 49, 53), -1)}}},
			map[int][][]int{0: {{1, 2}}}, false, map[int]statementType{0: statementTypeUpdate}},
		{"OpCodeQuery UPDATE Conditional",
			mockQueryFrame(t, "UPDATE blah SET a = ?, b = 123 WHERE a = now() IF b = now()"),
			[]*statementReplacedTerms{{statementIndex: 0, replacedTerms: []*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 41, 45), 0),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 54, 58), 0),
			}}}, map[int][][]int{0: {{2, 3}}}, false, map[int]statementType{0: statementTypeUpdate}},
		{"OpCodeQuery UPDATE Complex",
			mockQueryFrame(t, "UPDATE blah SET a[?] = ?, b[now()] = 123, c[1] = now() WHERE a = 123"),
			[]*statementReplacedTerms{{statementIndex: 0, replacedTerms: []*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 28, 32), 1),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 49, 53), 1),
			}}},
			map[int][][]int{0: {{2, 5}}}, false, map[int]statementType{0: statementTypeUpdate}},
		{"OpCodeQuery UPDATE Complex 2",
			mockQueryFrame(t,
				"UPDATE blah SET a = ?, b = 123 "+
					"WHERE f[now()] = ? IF "+
					"g[123] IN (2, 3, ?, now(), ?, now()) AND "+
					"d IN ? AND "+
					"c IN (?, now(), 2) AND "+
					"a = now()"),
			[]*statementReplacedTerms{{statementIndex: 0, replacedTerms: []*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 39, 43), 0),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 73, 77), 2),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 83, 87), 3),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 114, 118), 5),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 132, 136), 5),
			}}}, map[int][][]int{0: {{2, 8, 10, 13, 15}}}, false, map[int]statementType{0: statementTypeUpdate}},
		{"OpCodeQuery UPDATE Complex 3",
			mockQueryFrame(t, ""+
				"UPDATE blah USING TIMESTAMP ? AND TTL ? "+
				"SET a = ?, b = now() "+
				"WHERE "+
				"(a IN ?) AND "+
				"(b IN (now(), ?)) AND "+
				"(a, b, c) IN ? AND "+
				"(a, b, c) IN ((1, 2, ?), (now(), 5, 6)) AND "+
				"(a, b, c) IN (?, ?, ?) AND "+
				"(a, b, c) > (1, now(), ?)"),
			[]*statementReplacedTerms{{statementIndex: 0, replacedTerms: []*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 55, 59), 2),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 87, 91), 3),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 147, 151), 6),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 208, 212), 9),
			}}}, map[int][][]int{0: {{3, 5, 11, 18}}}, false, map[int]statementType{0: statementTypeUpdate}},
		{"OpCodeQuery DELETE No Operation",
			mockQueryFrame(t, "DELETE FROM blah WHERE b = 123 AND a = now()"),
			[]*statementReplacedTerms{{statementIndex: 0, replacedTerms: []*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 39, 43), -1),
			}}}, map[int][][]int{0: {{1}}}, false, map[int]statementType{0: statementTypeDelete}},
		{"OpCodeQuery DELETE",
			mockQueryFrame(t, "DELETE a FROM blah WHERE b = 123 AND a = now()"),
			[]*statementReplacedTerms{{statementIndex: 0, replacedTerms: []*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 41, 45), -1),
			}}}, map[int][][]int{0: {{1}}}, false, map[int]statementType{0: statementTypeDelete}},
		{"OpCodeQuery DELETE Conditional",
			mockQueryFrame(t, "DELETE a FROM blah WHERE a = now() IF b = now()"),
			[]*statementReplacedTerms{{statementIndex: 0, replacedTerms: []*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 29, 33), -1),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 42, 46), -1),
			}}}, map[int][][]int{0: {{0, 1}}}, false, map[int]statementType{0: statementTypeDelete}},
		{"OpCodeQuery DELETE Complex",
			mockQueryFrame(t, "DELETE c[1], a[?], b[now()] FROM blah WHERE b = 123 AND a = now()"),
			[]*statementReplacedTerms{{statementIndex: 0, replacedTerms: []*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 21, 25), 0),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 60, 64), 0),
			}}}, map[int][][]int{0: {{2, 4}}}, false, map[int]statementType{0: statementTypeDelete}},
		{"OpCodeBatch Mixed Prepared and Simple",
			mockBatchWithChildren(t, []*message.BatchChild{
				{
					Query: "UPDATE blah SET a = ?, b = 123 " +
						"WHERE f[now()] = ? IF " +
						"g[123] IN (2, 3, ?, now(), ?, now()) AND " +
						"d IN ? AND " +
						"c IN (?, now(), 2) AND " +
						"a = now()",
					Values: []*primitive.Value{}, // not used by the SUT (system under test)
				},
				{
					Id:     []byte{0},
					Values: []*primitive.Value{}, // not used by the SUT
				},
				{
					Query:  "DELETE FROM blah WHERE b = 123 AND a = now()",
					Values: []*primitive.Value{}, // not used by the SUT
				}}),
			[]*statementReplacedTerms{
				{statementIndex: 0, replacedTerms: []*term{
					NewFunctionCallTerm(NewFunctionCall("", "now", 0, 39, 43), 0),
					NewFunctionCallTerm(NewFunctionCall("", "now", 0, 73, 77), 2),
					NewFunctionCallTerm(NewFunctionCall("", "now", 0, 83, 87), 3),
					NewFunctionCallTerm(NewFunctionCall("", "now", 0, 114, 118), 5),
					NewFunctionCallTerm(NewFunctionCall("", "now", 0, 132, 136), 5),
				}},
				{statementIndex: 2, replacedTerms: []*term{
					NewFunctionCallTerm(NewFunctionCall("", "now", 0, 39, 43), -1),
				}}}, map[int][][]int{0: {{2, 8, 10, 13, 15}}, 2: {{1}}}, false, map[int]statementType{0: statementTypeUpdate, 2: statementTypeDelete}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			context := &frameDecodeContext{frame: test.f, compression: primitive.CompressionNone}
			timeUuidGenerator, err := GetDefaultTimeUuidGenerator()
			require.Nil(t, err)
			statementsQueryData, err := context.GetOrInspectAllStatements("", timeUuidGenerator)
			require.Nil(t, err)
			conf := config.New()
			conf.ReplaceCqlFunctions = true
			queryModifier := NewQueryModifier(timeUuidGenerator, nil, conf)
			decodedFrame, statementQuery, err := context.GetOrDecodeAndInspect("", timeUuidGenerator)
			require.Nil(t, err)
			_, decodedFrame, statementQuery, statementsReplacedTerms, err := queryModifier.replaceQueryString(decodedFrame, statementQuery)
			newRawFrame, err := defaultCodec.ConvertToRawFrame(decodedFrame)
			newContext := NewInitializedFrameDecodeContext(newRawFrame, primitive.CompressionNone, decodedFrame, statementQuery)
			require.Nil(t, err)
			require.Equal(t, len(test.positionsReplaced), len(statementsReplacedTerms))
			require.Equal(t, len(test.replacedTerms), len(statementsReplacedTerms))
			if len(test.positionsReplaced) != 0 {
				require.NotEqual(t, context.frame, newContext.frame)
				require.Equal(t, context.frame.Header.OpCode, newContext.frame.Header.OpCode)
				require.Equal(t, context.frame.Header.StreamId, newContext.frame.Header.StreamId)
				require.Equal(t, context.frame.Header.Flags, newContext.frame.Header.Flags)
				require.Equal(t, context.frame.Header.Version, newContext.frame.Header.Version)
				require.NotEqual(t, context.frame.Body, newContext.frame.Body)
			} else {
				require.Equal(t, context.frame, newContext.frame)
				require.Equal(t, context.frame.Body, newContext.frame.Body)
				require.Equal(t, context.frame.Header, newContext.frame.Header)
			}

			require.Equal(t, test.replacedTerms, statementsReplacedTerms)
			require.Equal(t, len(test.statementTypes), len(statementsQueryData))
			for idx, stmtQueryData := range statementsQueryData {
				newStmtQueryData := newContext.statementsQueryData[idx]

				require.Equal(t, test.statementTypes[stmtQueryData.statementIndex], stmtQueryData.queryData.getStatementType())
				require.Equal(t, test.statementTypes[stmtQueryData.statementIndex], newStmtQueryData.queryData.getStatementType())

				positionsReplaced, ok := test.positionsReplaced[stmtQueryData.statementIndex]
				if ok {
					require.NotEqual(t, stmtQueryData, newStmtQueryData)
				} else {
					require.Equal(t, stmtQueryData, newStmtQueryData)
				}

				require.Equal(t, len(stmtQueryData.queryData.getParsedStatements()), len(newStmtQueryData.queryData.getParsedStatements()))
				if len(stmtQueryData.queryData.getParsedStatements()) == 0 {
					require.Equal(t, 0, len(positionsReplaced))
					continue
				}

				for parsedStmtIdx, parsedStmt := range stmtQueryData.queryData.getParsedStatements() {
					oldTerms := parsedStmt.terms
					newParsedStatement := newStmtQueryData.queryData.getParsedStatements()[parsedStmtIdx]
					newTerms := newParsedStatement.terms
					positionsReplaced := positionsReplaced[parsedStmtIdx]
					require.Equal(t, len(oldTerms), len(newTerms))
					for termIdx, oldTerm := range oldTerms {
						newTerm := newTerms[termIdx]
						if contains(positionsReplaced, termIdx) {
							require.NotEqual(t, oldTerm, newTerm)
							require.True(t, oldTerm.isFunctionCall())
							require.False(t, newTerm.isFunctionCall())

							require.False(t, oldTerm.isLiteral())
							if test.f.Header.OpCode == primitive.OpCodePrepare {
								if test.namedGeneratedValues {
									require.True(t, newTerm.isNamedBindMarker())
									require.Equal(t, "zdm__now", newTerm.bindMarkerName)
								} else {
									require.True(t, newTerm.isPositionalBindMarker())
								}
							} else {
								require.True(t, newTerm.isLiteral())
							}
						} else {
							if len(positionsReplaced) != 0 && test.f.Header.OpCode == primitive.OpCodePrepare {
								// positional index might be different in this case so check other fields only
								require.Equal(t, oldTerm.bindMarkerName, newTerm.bindMarkerName)
								require.Equal(t, oldTerm.functionCall, newTerm.functionCall)
								require.Equal(t, oldTerm.literal, newTerm.literal)
							} else {
								require.Equal(t, oldTerm, newTerm)
							}
						}
					}
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
