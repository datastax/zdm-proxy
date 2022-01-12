package cloudgateproxy

import (
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReplaceQueryString(t *testing.T) {
	type args struct {
		f                    *frame.RawFrame
		replacedTerms        []*term
	}
	tests := []struct {
		name                 string
		args                 args
		positionsReplaced    []int
		namedGeneratedValues bool
		statementType        statementType
	}{
		// QUERY
		{"OpCodeQuery SELECT", args{
			mockQueryFrame("SELECT blah FROM ks1.t2"),
			[]*term{}}, []int{}, false, statementTypeSelect},
		{"OpCodeQuery INSERT", args{
			mockQueryFrame("INSERT INTO blah (a, b) VALUES (now(), 1)"),
			[]*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 32, 36), -1)}},
			[]int{0}, false, statementTypeInsert},
		{"OpCodeQuery INSERT NAMED", args{
			mockQueryFrame("INSERT INTO blah (a, b) VALUES (now(), :bparam)"),
			[]*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 32, 36), -1)}},
			[]int{0}, false, statementTypeInsert},
		{"OpCodePrepare INSERT", args{
			mockPrepareFrame("INSERT INTO blah (a, b) VALUES (now(), 1)"),
			[]*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 32, 36), -1)}},
			[]int{0}, false, statementTypeInsert},
		{"OpCodePrepare INSERT NAMED", args{
			mockPrepareFrame("INSERT INTO blah (a, b) VALUES (now(), :bparam)"),
			[]*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 32, 36), -1)}},
			[]int{0}, true, statementTypeInsert},
		{"OpCodeQuery UPDATE", args{
			mockQueryFrame("UPDATE blah SET a = ?, b = now() WHERE a = now()"),
			[]*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 27, 31), 0),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 43, 47), 0)}},
			[]int{1, 2}, false, statementTypeUpdate},
		{"OpCodeQuery UPDATE NAMED", args{
			mockQueryFrame("UPDATE blah SET a = :aparam, b = now() WHERE a = now()"),
			[]*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 33, 37), -1),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 49, 53), -1)}},
			[]int{1, 2}, false, statementTypeUpdate},
		{"OpCodeQuery UPDATE Conditional", args{
			mockQueryFrame("UPDATE blah SET a = ?, b = 123 WHERE a = now() IF b = now()"),
			[]*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 41, 45), 0),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 54, 58), 0),
			}}, []int{2, 3}, false, statementTypeUpdate},
		{"OpCodeQuery UPDATE Complex", args{
			mockQueryFrame("UPDATE blah SET a[?] = ?, b[now()] = 123, c[1] = now() WHERE a = 123"),
			[]*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 28, 32), 1),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 49, 53), 1),
			}},
			[]int{2, 5}, false, statementTypeUpdate},
		{"OpCodeQuery UPDATE Complex 2", args{
			mockQueryFrame(
				"UPDATE blah SET a = ?, b = 123 " +
					"WHERE f[now()] = ? IF " +
					"g[123] IN (2, 3, ?, now(), ?, now()) AND " +
					"d IN ? AND " +
					"c IN (?, now(), 2) AND " +
					"a = now()"),
			[]*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 39, 43), 0),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 73, 77), 2),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 83, 87), 3),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 114, 118), 5),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 132, 136), 5),
			}}, []int{2, 8, 10, 13, 15}, false, statementTypeUpdate},
		{"OpCodeQuery UPDATE Complex 3", args{
			mockQueryFrame("" +
				"UPDATE blah USING TIMESTAMP ? AND TTL ? " +
				"SET a = ?, b = now() " +
				"WHERE " +
				"(a IN ?) AND " +
				"(b IN (now(), ?)) AND " +
				"(a, b, c) IN ? AND " +
				"(a, b, c) IN ((1, 2, ?), (now(), 5, 6)) AND " +
				"(a, b, c) IN (?, ?, ?) AND " +
				"(a, b, c) > (1, now(), ?)"),
			[]*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 55, 59), 2),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 87, 91), 3),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 147, 151), 6),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 208, 212), 9),
			}}, []int{3, 5, 11, 18}, false, statementTypeUpdate},
		{"OpCodeQuery DELETE No Operation", args{
			mockQueryFrame("DELETE FROM blah WHERE b = 123 AND a = now()"),
			[]*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 39, 43), -1),
			}}, []int{1}, false, statementTypeDelete},
		{"OpCodeQuery DELETE", args{
			mockQueryFrame("DELETE a FROM blah WHERE b = 123 AND a = now()"),
			[]*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 41, 45), -1),
			}}, []int{1}, false, statementTypeDelete},
		{"OpCodeQuery DELETE Conditional", args{
			mockQueryFrame("DELETE a FROM blah WHERE a = now() IF b = now()"),
			[]*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 29, 33), -1),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 42, 46), -1),
			}}, []int{0, 1}, false, statementTypeDelete},
		{"OpCodeQuery DELETE Complex", args{
			mockQueryFrame("DELETE c[1], a[?], b[now()] FROM blah WHERE b = 123 AND a = now()"),
			[]*term{
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 21, 25), 0),
				NewFunctionCallTerm(NewFunctionCall("", "now", 0, 60, 64), 0),
			}}, []int{2,4}, false, statementTypeDelete},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			context := &frameDecodeContext{frame: test.args.f}
			queryInfo, err := context.GetOrInspectQuery()
			require.Nil(t, err)
			timeUuidGenerator, err := GetDefaultTimeUuidGenerator()
			require.Nil(t, err)
			queryModifier := NewQueryModifier(timeUuidGenerator)
			newContext, replacedTerms, err := queryModifier.replaceQueryString(context)
			require.Nil(t, err)

			require.Equal(t, test.statementType, queryInfo.getStatementType())
			require.Equal(t, test.statementType, newContext.queryInfo.getStatementType())
			require.Equal(t, test.args.replacedTerms, replacedTerms)

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
					if test.args.f.Header.OpCode == primitive.OpCodePrepare {
						if test.namedGeneratedValues {
							require.True(t, newTerm.isNamedBindMarker())
							require.Equal(t, "cloudgate__now", newTerm.bindMarkerName)
						} else {
							require.True(t, newTerm.isPositionalBindMarker())
						}
					} else {
						require.True(t, newTerm.isLiteral())
					}
				} else {
					if len(test.positionsReplaced) != 0 && test.args.f.Header.OpCode == primitive.OpCodePrepare {
						// positional index might be different in this case so check other fields only
						require.Equal(t, oldTerm.bindMarkerName, newTerm.bindMarkerName)
						require.Equal(t, oldTerm.functionCall, newTerm.functionCall)
						require.Equal(t, oldTerm.literal, newTerm.literal)
					} else {
						require.Equal(t, oldTerm, newTerm)
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