package integration_tests

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/datacodec"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/google/uuid"
	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/integration-tests/simulacron"
	"github.com/stretchr/testify/require"
	"regexp"
	"testing"
)

type param struct {
	name            string
	isReplacedNow   bool
	value           interface{}
	valueSimulacron interface{}
	dataType        datatype.DataType
	simulacronType  simulacron.DataType
}

type simpleStatementTestArgs struct {
	name      string
	query     string
	regex     string
	matches   int
	queryOpts *message.QueryOptions
}

type preparedStatementTestArgs struct {
	name          string
	originalQuery string
	modifiedQuery string
	params        []*param
}

type batchChildStmt struct {
	prepared      bool
	originalQuery string
	modifiedQuery string
	params        []*param
	matches       int
}

type batchTestArgs struct {
	name       string
	statements []*batchChildStmt
}

func TestNowFunctionReplacementSimpleStatement(t *testing.T) {

	tests := []simpleStatementTestArgs{
		{
			name:    "Insert",
			query:   "INSERT INTO ks.table (name, id) VALUES (?, now())",
			regex:   `^INSERT INTO ks\.table \(name, id\) VALUES \(\?, (.*)\)$`,
			matches: 1,
			queryOpts: &message.QueryOptions{
				PositionalValues: []*primitive.Value{
					{
						Type: primitive.ValueTypeNull,
					},
				},
			},
		},
		{
			name:    "InsertConditional",
			query:   "INSERT INTO ks.table1 (name, id) VALUES (?, now()) IF NOT EXISTS",
			regex:   `^INSERT INTO ks\.table1 \(name, id\) VALUES \(\?, (.*)\) IF NOT EXISTS$`,
			matches: 1,
			queryOpts: &message.QueryOptions{
				PositionalValues: []*primitive.Value{
					{
						Type: primitive.ValueTypeNull,
					},
				},
			},
		},
		{
			name:    "Update",
			query:   "UPDATE ks.table SET name = ? WHERE id = now()",
			regex:   `^UPDATE ks\.table SET name = \? WHERE id = (.*)$`,
			matches: 1,
			queryOpts: &message.QueryOptions{
				PositionalValues: []*primitive.Value{
					{
						Type: primitive.ValueTypeNull,
					},
				},
			},
		},
		{
			name:    "UpdateConditional",
			query:   "UPDATE ks.table SET name = ?, id = now() WHERE id = now() IF id = now()",
			regex:   `^UPDATE ks\.table SET name = \?, id = (.*) WHERE id = (.*) IF id = (.*)$`,
			matches: 3,
			queryOpts: &message.QueryOptions{
				PositionalValues: []*primitive.Value{
					{
						Type: primitive.ValueTypeNull,
					},
				},
			},
		},
		{
			name:    "UpdateConditionalExists",
			query:   "UPDATE ks.table SET name = ?, id = now() WHERE id = now() IF EXISTS",
			regex:   `^UPDATE ks\.table SET name = \?, id = (.*) WHERE id = (.*) IF EXISTS$`,
			matches: 2,
			queryOpts: &message.QueryOptions{
				PositionalValues: []*primitive.Value{
					{
						Type: primitive.ValueTypeNull,
					},
				},
			},
		},
		{
			name:      "Delete",
			query:     "DELETE FROM ks.table WHERE id = now()",
			regex:     `^DELETE FROM ks\.table WHERE id = (.*)$`,
			matches:   1,
			queryOpts: nil,
		},
		{
			name:      "DeleteComplex",
			query:     "DELETE a[now()] FROM ks.table WHERE id = now()",
			regex:     `^DELETE a\[(.*)\] FROM ks\.table WHERE id = (.*)$`,
			matches:   2,
			queryOpts: nil,
		},
	}

	runTests := func(tests []simpleStatementTestArgs, enableNowReplacement bool, t *testing.T) {
		var simulacronSetup *setup.SimulacronTestSetup
		var err error

		if enableNowReplacement {
			simulacronSetup, err = createSimulacronTestSetupWithServerSideFunctionReplacement()
		} else {
			simulacronSetup, err = createSimulacronTestSetupWithoutServerSideFunctionReplacement()
		}

		require.Nil(t, err)
		defer simulacronSetup.Cleanup()

		testClient := client.NewCqlClient("127.0.0.1:14002", nil)
		cqlConn, err := testClient.ConnectAndInit(context.Background(), primitive.ProtocolVersion4, 1)
		require.Nil(t, err, "testClient setup failed: %v", err)

		defer cqlConn.Close()

		for _, test := range tests {
			t.Run(test.name, func(tt *testing.T) {
				queryMsg := &message.Query{
					Query:   test.query,
					Options: test.queryOpts,
				}

				f := frame.NewFrame(primitive.ProtocolVersion4, 2, queryMsg)
				_, err := cqlConn.SendAndReceive(f)
				require.Nil(tt, err)

				var re = regexp.MustCompile(test.regex)

				getQueryLogEntriesMatchingRegex := func(cluster *simulacron.Cluster) []*simulacron.RequestLogEntry {
					logs, err := cluster.GetLogsByType(simulacron.QueryTypeQuery)
					require.Nil(tt, err)

					var matching []*simulacron.RequestLogEntry
					for _, logEntry := range logs.Datacenters[0].Nodes[0].Queries {
						if re.MatchString(logEntry.Query) {
							matching = append(matching, logEntry)
						}
					}
					return matching
				}

				validateForwardedQuery := func(cluster *simulacron.Cluster) {

					matching := getQueryLogEntriesMatchingRegex(cluster)
					require.Equal(tt, 1, len(matching))
					if enableNowReplacement {
						require.NotEqual(tt, test.query, matching[0].Query)

						matches := re.FindStringSubmatch(matching[0].Query)
						require.Equal(tt, test.matches+1, len(matches))
						if test.matches > 0 {
							for _, m := range matches[1:] {
								uid, err := uuid.Parse(m)
								require.Nil(tt, err)
								require.Equal(tt, uuid.Version(1), uid.Version())
							}
						}

					} else {
						require.Equal(tt, test.query, matching[0].Query)
					}

				}

				validateForwardedQuery(simulacronSetup.Origin)
				validateForwardedQuery(simulacronSetup.Target)
			})
		}

	}

	t.Run("now replacement enabled", func (t *testing.T) {
		runTests(tests, true, t)
	})

	t.Run("now replacement disabled", func (t *testing.T) {
		runTests(tests, false, t)
	})
}

func TestNowFunctionReplacementPreparedStatement(t *testing.T) {

	timeUuidStart, err := uuid.NewUUID()
	require.Nil(t, err)

	tests := []preparedStatementTestArgs{
		{
			name:          "Insert",
			originalQuery: "INSERT INTO ks.table (name, id) VALUES (?, now())",
			modifiedQuery: "INSERT INTO ks.table (name, id) VALUES (?, ?)",
			params: []*param{
				{
					name:            "",
					isReplacedNow:   false,
					value:           "testval",
					valueSimulacron: "testval",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
			},
		},
		{
			name:          "Insert_Named",
			originalQuery: "INSERT INTO ks.table (name, id) VALUES (:nameparam, now())",
			modifiedQuery: "INSERT INTO ks.table (name, id) VALUES (:nameparam, :cloudgate__now)",
			params: []*param{
				{
					name:            "nameparam",
					isReplacedNow:   false,
					value:           "testval",
					valueSimulacron: "testval",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "cloudgate__now",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
			},
		},
		{
			name:          "Update",
			originalQuery: "UPDATE blah SET a = ?, b = now() WHERE a = now()",
			modifiedQuery: "UPDATE blah SET a = ?, b = ? WHERE a = ?",
			params: []*param{
				{
					name:            "",
					isReplacedNow:   false,
					value:           "testval",
					valueSimulacron: "testval",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
				{
					name:            "",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
			},
		},
		{
			name:          "Update_Named",
			originalQuery: "UPDATE blah SET a = :aparam, b = now() WHERE a = now()",
			modifiedQuery: "UPDATE blah SET a = :aparam, b = :cloudgate__now WHERE a = :cloudgate__now",
			params: []*param{
				{
					name:            "aparam",
					isReplacedNow:   false,
					value:           "testval",
					valueSimulacron: "testval",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "cloudgate__now",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
			},
		},
		{
			name:          "UPDATE_Conditional",
			originalQuery: "UPDATE blah SET a = ?, b = 123 WHERE a = now() IF b = now()",
			modifiedQuery: "UPDATE blah SET a = ?, b = 123 WHERE a = ? IF b = ?",
			params: []*param{
				{
					name:            "",
					isReplacedNow:   false,
					value:           "testval",
					valueSimulacron: "testval",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
				{
					name:            "",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
			},
		},
		{
			name:          "Update_Conditional_Named",
			originalQuery: "UPDATE blah SET a = :aparam, b = 123 WHERE a = now() IF b = now()",
			modifiedQuery: "UPDATE blah SET a = :aparam, b = 123 WHERE a = :cloudgate__now IF b = :cloudgate__now",
			params: []*param{
				{
					name:            "aparam",
					isReplacedNow:   false,
					value:           "testval",
					valueSimulacron: "testval",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "cloudgate__now",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
			},
		},
		{
			name:          "UPDATE_Complex",
			originalQuery: "UPDATE blah SET a[?] = ?, b[now()] = 123, c[1] = now() WHERE a = 123",
			modifiedQuery: "UPDATE blah SET a[?] = ?, b[?] = 123, c[1] = ? WHERE a = 123",
			params: []*param{
				{
					name:            "",
					isReplacedNow:   false,
					value:           "testval1",
					valueSimulacron: "testval1",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "",
					isReplacedNow:   false,
					value:           "testval2",
					valueSimulacron: "testval2",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
				{
					name:            "",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
			},
		},
		{
			name:          "Update_Complex_Named",
			originalQuery: "UPDATE blah SET a[:aparam] = :avalueparam, b[now()] = 123, c[1] = now() WHERE a = 123",
			modifiedQuery: "UPDATE blah SET a[:aparam] = :avalueparam, b[:cloudgate__now] = 123, c[1] = :cloudgate__now WHERE a = 123",
			params: []*param{
				{
					name:            "aparam",
					isReplacedNow:   false,
					value:           "testval100",
					valueSimulacron: "testval100",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "avalueparam",
					isReplacedNow:   false,
					value:           "testval200",
					valueSimulacron: "testval200",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "cloudgate__now",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
			},
		},
		{
			name: "UPDATE_Complex_2",
			originalQuery: "UPDATE blah SET a = ?, b = 123 " +
				"WHERE f[now()] = ? " +
				"IF " +
				"g[123] IN (2, 3, ?, now(), ?, now()) AND " +
				"j = ? AND " +
				"d IN ? AND " +
				"c IN (?, now(), 2) AND " +
				"a = now()",
			modifiedQuery: "UPDATE blah SET a = ?, b = 123 " +
				"WHERE f[?] = ? " +
				"IF " +
				"g[123] IN (2, 3, ?, ?, ?, ?) AND " +
				"j = ? AND " +
				"d IN ? AND " +
				"c IN (?, ?, 2) AND " +
				"a = ?",
			params: []*param{

				{
					name:            "",
					isReplacedNow:   false,
					value:           "testval11",
					valueSimulacron: "testval11",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
				{
					name:            "",
					isReplacedNow:   false,
					value:           "testval22",
					valueSimulacron: "testval22",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "",
					isReplacedNow:   false,
					value:           "testval33",
					valueSimulacron: "testval33",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
				{
					name:            "",
					isReplacedNow:   false,
					value:           "testval44",
					valueSimulacron: "testval44",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
				{
					name:            "",
					isReplacedNow:   false,
					value:           "testval55",
					valueSimulacron: "testval55",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "",
					isReplacedNow:   false,
					value:           "testval66",
					valueSimulacron: "testval66",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "",
					isReplacedNow:   false,
					value:           "testval77",
					valueSimulacron: "testval77",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
				{
					name:            "",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
			},
		},
		{
			name: "Update_Complex_2_Named",
			originalQuery: "UPDATE blah SET a = :aparam, b = 123 " +
				"WHERE f[now()] = :fparam " +
				"IF " +
				"g[123] IN (2, 3, :gparam, now(), :gtwoparam, now()) AND " +
				"j = :aparam AND " +
				"d IN :dparam AND " +
				"c IN (:cparam, now(), 2) AND " +
				"a = now()",
			modifiedQuery: "UPDATE blah SET a = :aparam, b = 123 " +
				"WHERE f[:cloudgate__now] = :fparam " +
				"IF " +
				"g[123] IN (2, 3, :gparam, :cloudgate__now, :gtwoparam, :cloudgate__now) AND " +
				"j = :aparam AND " +
				"d IN :dparam AND " +
				"c IN (:cparam, :cloudgate__now, 2) AND " +
				"a = :cloudgate__now",
			params: []*param{

				{
					name:            "aparam",
					isReplacedNow:   false,
					value:           "testval1100",
					valueSimulacron: "testval1100",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "cloudgate__now",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
				{
					name:            "fparam",
					isReplacedNow:   false,
					value:           "testval2200",
					valueSimulacron: "testval2200",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "gparam",
					isReplacedNow:   false,
					value:           "testval3300",
					valueSimulacron: "testval3300",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "gtwoparam",
					isReplacedNow:   false,
					value:           "testval4400",
					valueSimulacron: "testval4400",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "dparam",
					isReplacedNow:   false,
					value:           "testval5500",
					valueSimulacron: "testval5500",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "cparam",
					isReplacedNow:   false,
					value:           "testval6600",
					valueSimulacron: "testval6600",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
			},
		},
		{
			name: "UPDATE_Complex_3",
			originalQuery: "UPDATE blah " +
				"USING TIMESTAMP ? AND TTL ? " +
				"SET a = ?, b = now() " +
				"WHERE " +
				"(a IN ?) AND " +
				"(b IN (now(), ?)) AND " +
				"(a, b, c) IN ? AND " +
				"(a, b, c) IN ((1, 2, ?), (now(), 5, 6)) AND " +
				"(a, b, c) IN (?, ?, ?) AND " +
				"(a, b, c) > (1, now(), ?)",
			modifiedQuery: "UPDATE blah " +
				"USING TIMESTAMP ? AND TTL ? " +
				"SET a = ?, b = ? " +
				"WHERE " +
				"(a IN ?) AND " +
				"(b IN (?, ?)) AND " +
				"(a, b, c) IN ? AND " +
				"(a, b, c) IN ((1, 2, ?), (?, 5, 6)) AND " +
				"(a, b, c) IN (?, ?, ?) AND " +
				"(a, b, c) > (1, ?, ?)",
			params: []*param{
				{
					name:            "",
					isReplacedNow:   false,
					value:           "testval11",
					valueSimulacron: "testval11",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "",
					isReplacedNow:   false,
					value:           "testval22",
					valueSimulacron: "testval22",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "",
					isReplacedNow:   false,
					value:           "testval33",
					valueSimulacron: "testval33",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
				{
					name:            "",
					isReplacedNow:   false,
					value:           "testval44",
					valueSimulacron: "testval44",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
				{
					name:            "",
					isReplacedNow:   false,
					value:           "testval55",
					valueSimulacron: "testval55",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "",
					isReplacedNow:   false,
					value:           "testval66",
					valueSimulacron: "testval66",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "",
					isReplacedNow:   false,
					value:           "testval77",
					valueSimulacron: "testval77",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
				{
					name:            "",
					isReplacedNow:   false,
					value:           "testval88",
					valueSimulacron: "testval88",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "",
					isReplacedNow:   false,
					value:           "testval99",
					valueSimulacron: "testval99",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "",
					isReplacedNow:   false,
					value:           "testval1010",
					valueSimulacron: "testval1010",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
				{
					name:            "",
					isReplacedNow:   false,
					value:           "testval1111",
					valueSimulacron: "testval1111",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
			},
		},
		{
			name: "Update_Complex_3_Named",
			originalQuery: "UPDATE blah " +
				"USING TIMESTAMP :ts AND TTL :ttl " +
				"SET a = :aparam, b = now() " +
				"WHERE " +
				"(a IN :a_col_param) AND " +
				"(b IN (now(), :bparam)) AND " +
				"(a, b, c) IN :abc_col_param AND " +
				"(a, b, c) IN ((1, 2, :cparam), (now(), 5, 6)) AND " +
				"(a, b, c) IN (:a_param, :b_param, :c_param) AND " +
				"(a, b, c) > (1, now(), :c_last_param)",
			modifiedQuery: "UPDATE blah " +
				"USING TIMESTAMP :ts AND TTL :ttl " +
				"SET a = :aparam, b = :cloudgate__now " +
				"WHERE " +
				"(a IN :a_col_param) AND " +
				"(b IN (:cloudgate__now, :bparam)) AND " +
				"(a, b, c) IN :abc_col_param AND " +
				"(a, b, c) IN ((1, 2, :cparam), (:cloudgate__now, 5, 6)) AND " +
				"(a, b, c) IN (:a_param, :b_param, :c_param) AND " +
				"(a, b, c) > (1, :cloudgate__now, :c_last_param)",
			params: []*param{
				{
					name:            "ts",
					isReplacedNow:   false,
					value:           11,
					valueSimulacron: "11",
					dataType:        datatype.Int,
					simulacronType:  simulacron.DataTypeInt,
				},
				{
					name:            "ttl",
					isReplacedNow:   false,
					value:           22,
					valueSimulacron: "22",
					dataType:        datatype.Int,
					simulacronType:  simulacron.DataTypeInt,
				},
				{
					name:            "aparam",
					isReplacedNow:   false,
					value:           "testval33",
					valueSimulacron: "testval33",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "cloudgate__now",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
				{
					name:            "a_col_param",
					isReplacedNow:   false,
					value:           []int{11, 22, 33},
					valueSimulacron: []int{11, 22, 33},
					dataType:        datatype.NewListType(datatype.Int),
					simulacronType:  "list<int>",
				},
				{
					name:            "bparam",
					isReplacedNow:   false,
					value:           "testval55",
					valueSimulacron: "testval55",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:          "abc_col_param",
					isReplacedNow: false,
					value: [][]int{
						{1, 2, 3},
						{2, 3, 4},
					},
					valueSimulacron: []struct {
						a int
						b int
						c int
					}{
						{1, 2, 3},
						{2, 3, 4},
					},
					dataType:       datatype.NewListType(datatype.NewTupleType(datatype.Int, datatype.Int, datatype.Int)),
					simulacronType: "list<tuple<int, int, int>>",
				},
				{
					name:            "cparam",
					isReplacedNow:   false,
					value:           "testval77",
					valueSimulacron: "testval77",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "c_last_param",
					isReplacedNow:   false,
					value:           "testval88",
					valueSimulacron: "testval88",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
			},
		},

		{
			name:          "Delete",
			originalQuery: "DELETE FROM blah WHERE b = 123 AND a = now()",
			modifiedQuery: "DELETE FROM blah WHERE b = 123 AND a = ?",
			params: []*param{
				{
					name:            "",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
			},
		},
		{
			name:          "Delete_Using",
			originalQuery: "DELETE FROM blah USING TIMESTAMP ? WHERE b = 123 AND c = ? AND a = now()",
			modifiedQuery: "DELETE FROM blah USING TIMESTAMP ? WHERE b = 123 AND c = ? AND a = ?",
			params: []*param{
				{
					name:            "",
					isReplacedNow:   false,
					value:           123,
					valueSimulacron: 123,
					dataType:        datatype.Int,
					simulacronType:  simulacron.DataTypeInt,
				},
				{
					name:            "",
					isReplacedNow:   false,
					value:           123,
					valueSimulacron: 123,
					dataType:        datatype.Int,
					simulacronType:  simulacron.DataTypeInt,
				},
				{
					name:            "",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
			},
		},
		{
			name:          "Delete_Using_Named",
			originalQuery: "DELETE FROM blah USING TIMESTAMP :ts WHERE b = 123 AND c = :cparam AND a = now()",
			modifiedQuery: "DELETE FROM blah USING TIMESTAMP :ts WHERE b = 123 AND c = :cparam AND a = :cloudgate__now",
			params: []*param{
				{
					name:            "ts",
					isReplacedNow:   false,
					value:           123,
					valueSimulacron: 123,
					dataType:        datatype.Int,
					simulacronType:  simulacron.DataTypeInt,
				},
				{
					name:            "cparam",
					isReplacedNow:   false,
					value:           123,
					valueSimulacron: 123,
					dataType:        datatype.Int,
					simulacronType:  simulacron.DataTypeInt,
				},
				{
					name:            "cloudgate__now",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
			},
		},
		{
			name:          "Delete_WithOperation",
			originalQuery: "DELETE a FROM blah WHERE b = 123 AND a = now()",
			modifiedQuery: "DELETE a FROM blah WHERE b = 123 AND a = ?",
			params: []*param{
				{
					name:            "",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
			},
		},
		{
			name:          "Delete_Conditional_WithOperation",
			originalQuery: "DELETE a FROM blah WHERE b = ? AND a = now() IF b = now()",
			modifiedQuery: "DELETE a FROM blah WHERE b = ? AND a = ? IF b = ?",
			params: []*param{
				{
					name:            "",
					isReplacedNow:   false,
					value:           "testval",
					valueSimulacron: "testval",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
				{
					name:            "",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
			},
		},
		{
			name:          "Delete_Conditional_Named",
			originalQuery: "DELETE a FROM blah WHERE b = :bparam AND a = now() IF b = now()",
			modifiedQuery: "DELETE a FROM blah WHERE b = :bparam AND a = :cloudgate__now IF b = :cloudgate__now",
			params: []*param{
				{
					name:            "bparam",
					isReplacedNow:   false,
					value:           "testval",
					valueSimulacron: "testval",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "cloudgate__now",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
			},
		},
		{
			name:          "Delete_Complex_WithOperation",
			originalQuery: "DELETE c[1], a[?], b[now()] FROM blah WHERE b = 123 AND a = now()",
			modifiedQuery: "DELETE c[1], a[?], b[?] FROM blah WHERE b = 123 AND a = ?",
			params: []*param{
				{
					name:            "",
					isReplacedNow:   false,
					value:           "testval",
					valueSimulacron: "testval",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
				{
					name:            "",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
			},
		},
		{
			name:          "Delete_Complex_Named",
			originalQuery: "DELETE c[1], a[:aparam], b[now()] FROM blah WHERE b = 123 AND a = now()",
			modifiedQuery: "DELETE c[1], a[:aparam], b[:cloudgate__now] FROM blah WHERE b = 123 AND a = :cloudgate__now",
			params: []*param{
				{
					name:            "aparam",
					isReplacedNow:   false,
					value:           "testval",
					valueSimulacron: "testval",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "cloudgate__now",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
			},
		},
		{
			name: "Batch",
			originalQuery: "BEGIN UNLOGGED BATCH " +
				"UPDATE blah USING TTL ? AND TIMESTAMP ? SET a = ?, b = now() WHERE a = now(); " +
				"UPDATE blahh SET a = ?, b = 123 WHERE a = now() IF b = now() " +
				"APPLY BATCH;",
			modifiedQuery: "BEGIN UNLOGGED BATCH " +
				"UPDATE blah USING TTL ? AND TIMESTAMP ? SET a = ?, b = ? WHERE a = ?; " +
				"UPDATE blahh SET a = ?, b = 123 WHERE a = ? IF b = ? " +
				"APPLY BATCH;",
			params: []*param{
				{
					name:            "",
					isReplacedNow:   false,
					value:           7,
					valueSimulacron: 7,
					dataType:        datatype.Int,
					simulacronType:  simulacron.DataTypeInt,
				},
				{
					name:            "",
					isReplacedNow:   false,
					value:           8,
					valueSimulacron: 8,
					dataType:        datatype.Int,
					simulacronType:  simulacron.DataTypeInt,
				},
				{
					name:            "",
					isReplacedNow:   false,
					value:           "testval",
					valueSimulacron: "testval",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
				{
					name:            "",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
				{
					name:            "",
					isReplacedNow:   false,
					value:           "testval1",
					valueSimulacron: "testval1",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
				{
					name:            "",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
			},
		},
		{
			name: "Batch_Using",
			originalQuery: "BEGIN UNLOGGED BATCH USING TIMESTAMP ? AND TTL ? " +
				"UPDATE blah USING TTL ? AND TIMESTAMP ? SET a = ?, b = now() WHERE a = now(); " +
				"UPDATE blahh SET a = ?, b = 123 WHERE a = now() IF b = now() " +
				"APPLY BATCH;",
			modifiedQuery: "BEGIN UNLOGGED BATCH USING TIMESTAMP ? AND TTL ? " +
				"UPDATE blah USING TTL ? AND TIMESTAMP ? SET a = ?, b = ? WHERE a = ?; " +
				"UPDATE blahh SET a = ?, b = 123 WHERE a = ? IF b = ? " +
				"APPLY BATCH;",
			params: []*param{
				{
					name:            "",
					isReplacedNow:   false,
					value:           5,
					valueSimulacron: 5,
					dataType:        datatype.Int,
					simulacronType:  simulacron.DataTypeInt,
				},
				{
					name:            "",
					isReplacedNow:   false,
					value:           6,
					valueSimulacron: 6,
					dataType:        datatype.Int,
					simulacronType:  simulacron.DataTypeInt,
				},
				{
					name:            "",
					isReplacedNow:   false,
					value:           7,
					valueSimulacron: 7,
					dataType:        datatype.Int,
					simulacronType:  simulacron.DataTypeInt,
				},
				{
					name:            "",
					isReplacedNow:   false,
					value:           8,
					valueSimulacron: 8,
					dataType:        datatype.Int,
					simulacronType:  simulacron.DataTypeInt,
				},
				{
					name:            "",
					isReplacedNow:   false,
					value:           "testval",
					valueSimulacron: "testval",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
				{
					name:            "",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
				{
					name:            "",
					isReplacedNow:   false,
					value:           "testval1",
					valueSimulacron: "testval1",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
				{
					name:            "",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
			},
		},
		{
			name: "Batch_Using_Named",
			originalQuery: "BEGIN UNLOGGED BATCH USING TIMESTAMP :ts AND TTL :ttl " +
				"UPDATE blah USING TTL :ts1 AND TIMESTAMP :ttl1 SET a = :aparam, b = now() WHERE a = now(); " +
				"UPDATE blahh SET a = :a2param, b = 123 WHERE a = now() IF b = now() " +
				"APPLY BATCH;",
			modifiedQuery: "BEGIN UNLOGGED BATCH USING TIMESTAMP :ts AND TTL :ttl " +
				"UPDATE blah USING TTL :ts1 AND TIMESTAMP :ttl1 SET a = :aparam, b = :cloudgate__now WHERE a = :cloudgate__now; " +
				"UPDATE blahh SET a = :a2param, b = 123 WHERE a = :cloudgate__now IF b = :cloudgate__now " +
				"APPLY BATCH;",
			params: []*param{
				{
					name:            "ts",
					isReplacedNow:   false,
					value:           5,
					valueSimulacron: 5,
					dataType:        datatype.Int,
					simulacronType:  simulacron.DataTypeInt,
				},
				{
					name:            "ttl",
					isReplacedNow:   false,
					value:           6,
					valueSimulacron: 6,
					dataType:        datatype.Int,
					simulacronType:  simulacron.DataTypeInt,
				},
				{
					name:            "ts1",
					isReplacedNow:   false,
					value:           7,
					valueSimulacron: 7,
					dataType:        datatype.Int,
					simulacronType:  simulacron.DataTypeInt,
				},
				{
					name:            "ttl1",
					isReplacedNow:   false,
					value:           8,
					valueSimulacron: 8,
					dataType:        datatype.Int,
					simulacronType:  simulacron.DataTypeInt,
				},
				{
					name:            "aparam",
					isReplacedNow:   false,
					value:           "testval",
					valueSimulacron: "testval",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
				{
					name:            "cloudgate__now",
					isReplacedNow:   true,
					value:           primitive.UUID(timeUuidStart),
					valueSimulacron: timeUuidStart.String(),
					dataType:        datatype.Timeuuid,
					simulacronType:  simulacron.DataTypeTimeuuid,
				},
				{
					name:            "a2param",
					isReplacedNow:   false,
					value:           "testval1",
					valueSimulacron: "testval1",
					dataType:        datatype.Ascii,
					simulacronType:  simulacron.DataTypeText,
				},
			},
		},
	}

	runTests := func(tests []preparedStatementTestArgs, enableNowReplacement bool, timeUuidStart *uuid.UUID, t *testing.T) {
		var simulacronSetup *setup.SimulacronTestSetup
		var err error

		if enableNowReplacement {
			simulacronSetup, err = createSimulacronTestSetupWithServerSideFunctionReplacement()
		} else {
			simulacronSetup, err = createSimulacronTestSetupWithoutServerSideFunctionReplacement()
		}
		require.Nil(t, err)
		defer simulacronSetup.Cleanup()

		testClient := client.NewCqlClient("127.0.0.1:14002", nil)
		cqlConn, err := testClient.ConnectAndInit(context.Background(), primitive.ProtocolVersion4, 1)
		require.Nil(t, err, "testClient setup failed: %v", err)

		defer cqlConn.Close()

		getExecuteLogEntriesMatchingPrepareId := func(cluster *simulacron.Cluster, prepareId []byte) []*simulacron.ExecuteMessage {
			logs, err := cluster.GetLogsByType(simulacron.QueryTypeExecute)
			require.Nil(t, err)
			var matching []*simulacron.ExecuteMessage
			for _, logEntry := range logs.Datacenters[0].Nodes[0].Queries {
				var executeMsg simulacron.ExecuteMessage
				err = json.Unmarshal(logEntry.Frame.Message, &executeMsg)
				if err == nil && executeMsg.Id == base64.StdEncoding.EncodeToString(prepareId) {
					matching = append(matching, &executeMsg)
				}
			}
			return matching
		}

		getPrepareLogEntriesMatchingTestQuery := func(cluster *simulacron.Cluster, testQuery string) []*simulacron.RequestLogEntry {
			logs, err := cluster.GetLogsByType(simulacron.QueryTypePrepare)
			require.Nil(t, err)

			var matching []*simulacron.RequestLogEntry
			for _, logEntry := range logs.Datacenters[0].Nodes[0].Queries {
				if logEntry.Query == testQuery {
					matching = append(matching, logEntry)
				}
			}
			return matching
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {

				when := simulacron.NewWhenQueryOptions()
				for _, p := range test.params {
					if p.name == "" {
						when = when.WithPositionalParameter(p.simulacronType, p.valueSimulacron)
					} else {
						when = when.WithNamedParameter(p.name, p.simulacronType, p.valueSimulacron)
					}
				}

				var testQuery string
				if enableNowReplacement {
					testQuery = test.modifiedQuery
				} else {
					testQuery = test.originalQuery
				}

				queryPrime := simulacron.WhenQuery(testQuery, when).ThenSuccess()

				err = simulacronSetup.Origin.Prime(queryPrime)
				require.Nil(t, err)
				err = simulacronSetup.Target.Prime(queryPrime)
				require.Nil(t, err)

				queryMsg := &message.Prepare{
					Query: test.originalQuery,
				}

				f := frame.NewFrame(primitive.ProtocolVersion4, 0, queryMsg)
				resp, err := cqlConn.SendAndReceive(f)
				require.Nil(t, err)

				validateForwardedPrepare := func(cluster *simulacron.Cluster) {
					matching := getPrepareLogEntriesMatchingTestQuery(cluster, testQuery)
					require.Equal(t, 1, len(matching))
					if enableNowReplacement {
						require.NotEqual(t, test.originalQuery, matching[0].Query)
					} else {
						require.Equal(t, test.originalQuery, matching[0].Query)
					}
				}

				validateForwardedPrepare(simulacronSetup.Origin)
				validateForwardedPrepare(simulacronSetup.Target)

				prepared, ok := resp.Body.Message.(*message.PreparedResult)
				require.True(t, ok)

				queryOpts := &message.QueryOptions{}
				var queryOptsNamed *message.QueryOptions
				for _, p := range test.params {
					if p.isReplacedNow {
						continue
					}
					codec, err := datacodec.NewCodec(p.dataType)
					require.Nil(t, err)
					value, err := codec.Encode(p.value, resp.Header.Version)
					require.Nil(t, err)
					queryOpts.PositionalValues = append(queryOpts.PositionalValues, primitive.NewValue(value))
					if p.name != "" {
						if queryOptsNamed == nil {
							queryOptsNamed = &message.QueryOptions{}
						}
						if queryOptsNamed.NamedValues == nil {
							queryOptsNamed.NamedValues = map[string]*primitive.Value{}
						}
						queryOptsNamed.NamedValues[p.name] = primitive.NewValue(value)
					}
				}

				executeMsg := &message.Execute{
					QueryId:          prepared.PreparedQueryId,
					ResultMetadataId: prepared.ResultMetadataId,
					Options:          queryOpts,
				}
				f = frame.NewFrame(primitive.ProtocolVersion4, 0, executeMsg)
				resp, err = cqlConn.SendAndReceive(f)
				require.Nil(t, err)

				validateForwardedExecute := func(cluster *simulacron.Cluster, namedParameters bool, generatedValues []primitive.UUID) []primitive.UUID {
					matching := getExecuteLogEntriesMatchingPrepareId(cluster, prepared.PreparedQueryId)
					require.Equal(t, 1, len(matching))

					var expectedParams []*param
					if enableNowReplacement {
						expectedParams = test.params
					} else {
						for _, prm := range test.params {
							if !prm.isReplacedNow {
								expectedParams = append(expectedParams, prm)
							}
						}
					}

					if len(expectedParams) > 0 {
						require.NotNil(t, matching[0].Options)

						if len(matching[0].Options.PositionalValues) == 0 {
							require.Equal(t, len(expectedParams), len(matching[0].Options.NamedValues))
						} else {
							require.Equal(t, len(expectedParams), len(matching[0].Options.PositionalValues))
							require.Equal(t, 0, len(matching[0].Options.NamedValues))
						}
					} else {
						require.True(t,
							matching[0].Options == nil ||
								(len(matching[0].Options.PositionalValues) == 0 && len(matching[0].Options.PositionalValues) == 0))
					}

					assertGeneratedValues := true
					if generatedValues == nil {
						assertGeneratedValues = false
					}
					assertGeneratedValidIdx := 0
					var generatedNowValue *primitive.UUID

					namedParams := map[string]string{}
					for idx, p := range expectedParams {
						codec, err := datacodec.NewCodec(p.dataType)
						require.Nil(t, err)
						expectedValue, err := codec.Encode(p.value, resp.Header.Version)
						require.Nil(t, err)
						b64ExpectedValue := base64.StdEncoding.EncodeToString(expectedValue)
						var actualValue string
						if !namedParameters {
							require.Less(t, idx, len(matching[0].Options.PositionalValues))
							actualValue = matching[0].Options.PositionalValues[idx]
						} else {
							require.NotNil(t, matching[0].Options.NamedValues)
							actualValue, ok = matching[0].Options.NamedValues[p.name]
							require.True(t, ok)
							existingNamedParam, ok := namedParams[p.name]
							if !ok {
								namedParams[p.name] = actualValue
							} else {
								require.Equal(t, existingNamedParam, actualValue)
							}
						}

						if p.isReplacedNow {
							require.Equal(t, datatype.Timeuuid, p.dataType)
							timeUuidValue, err := base64.StdEncoding.DecodeString(actualValue)
							require.Nil(t, err)
							var decodedVal primitive.UUID
							wasNull, err := codec.Decode(timeUuidValue, &decodedVal, resp.Header.Version)
							require.False(t, wasNull)
							require.Nil(t, err)
							require.Greater(t, int64(uuid.UUID(decodedVal).Time()), int64(timeUuidStart.Time()))
							if generatedNowValue == nil {
								generatedNowValue = &decodedVal
							} else {
								require.NotEqual(t, *generatedNowValue, decodedVal)
								generatedNowValue = &decodedVal
							}
							if assertGeneratedValues {
								require.Greater(t, len(generatedValues), assertGeneratedValidIdx)
								require.Equal(t, generatedValues[assertGeneratedValidIdx], decodedVal)
								assertGeneratedValidIdx++
							} else {
								generatedValues = append(generatedValues, decodedVal)
							}
						} else {
							require.Equal(t, b64ExpectedValue, actualValue)
						}
					}
					return generatedValues
				}

				generatedVals := validateForwardedExecute(simulacronSetup.Origin, false, nil)
				_ = validateForwardedExecute(simulacronSetup.Target, false, generatedVals)

				if queryOptsNamed != nil {
					err = simulacronSetup.Origin.DeleteLogs()
					require.Nil(t, err)
					err = simulacronSetup.Target.DeleteLogs()
					require.Nil(t, err)

					matching := getExecuteLogEntriesMatchingPrepareId(simulacronSetup.Origin, prepared.PreparedQueryId)
					require.Equal(t, 0, len(matching))
					matching = getExecuteLogEntriesMatchingPrepareId(simulacronSetup.Target, prepared.PreparedQueryId)
					require.Equal(t, 0, len(matching))

					executeMsg = &message.Execute{
						QueryId:          prepared.PreparedQueryId,
						ResultMetadataId: prepared.ResultMetadataId,
						Options:          queryOptsNamed,
					}
					f = frame.NewFrame(primitive.ProtocolVersion4, 0, executeMsg)
					_, err = cqlConn.SendAndReceive(f)
					require.Nil(t, err)

					generatedVals = validateForwardedExecute(simulacronSetup.Origin, true, nil)
					_ = validateForwardedExecute(simulacronSetup.Target, true, generatedVals)
				}

			})
		}
	}

	t.Run("now replacement enabled", func (t *testing.T) {
		runTests(tests, true, &timeUuidStart, t)
	})

	t.Run("now replacement disabled", func (t *testing.T) {
		runTests(tests, false, nil, t)
	})

}

func TestNowFunctionReplacementBatchStatement(t *testing.T) {
	timeUuidStart, uuidErr := uuid.NewUUID()
	require.Nil(t, uuidErr)

	tests := []batchTestArgs{
		{
			name: "All_Prepared",
			statements: []*batchChildStmt{
				{
					prepared:      true,
					originalQuery: "UPDATE blah SET a = ?, b = now() WHERE a = now()",
					modifiedQuery: "UPDATE blah SET a = ?, b = ? WHERE a = ?",
					params: []*param{
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval",
							valueSimulacron: "testval",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
						{
							name:            "",
							isReplacedNow:   true,
							value:           primitive.UUID(timeUuidStart),
							valueSimulacron: timeUuidStart.String(),
							dataType:        datatype.Timeuuid,
							simulacronType:  simulacron.DataTypeTimeuuid,
						},
						{
							name:            "",
							isReplacedNow:   true,
							value:           primitive.UUID(timeUuidStart),
							valueSimulacron: timeUuidStart.String(),
							dataType:        datatype.Timeuuid,
							simulacronType:  simulacron.DataTypeTimeuuid,
						},
					},
				},
				{
					prepared:      true,
					originalQuery:         "DELETE c[1], a[?], b[now()] FROM blah WHERE b = 123 AND a = now()",
					modifiedQuery: "DELETE c[1], a[?], b[?] FROM blah WHERE b = 123 AND a = ?",
					params: []*param{
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval",
							valueSimulacron: "testval",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
						{
							name:            "",
							isReplacedNow:   true,
							value:           primitive.UUID(timeUuidStart),
							valueSimulacron: timeUuidStart.String(),
							dataType:        datatype.Timeuuid,
							simulacronType:  simulacron.DataTypeTimeuuid,
						},
						{
							name:            "",
							isReplacedNow:   true,
							value:           primitive.UUID(timeUuidStart),
							valueSimulacron: timeUuidStart.String(),
							dataType:        datatype.Timeuuid,
							simulacronType:  simulacron.DataTypeTimeuuid,
						},
					},
				},
				{
					prepared: true,
					originalQuery: "UPDATE blah " +
						"USING TIMESTAMP ? AND TTL ? " +
						"SET a = ?, b = now() " +
						"WHERE " +
						"(a IN ?) AND " +
						"(b IN (now(), ?)) AND " +
						"(a, b, c) IN ? AND " +
						"(a, b, c) IN ((1, 2, ?), (now(), 5, 6)) AND " +
						"(a, b, c) IN (?, ?, ?) AND " +
						"(a, b, c) > (1, now(), ?)",
					modifiedQuery: "UPDATE blah " +
						"USING TIMESTAMP ? AND TTL ? " +
						"SET a = ?, b = ? " +
						"WHERE " +
						"(a IN ?) AND " +
						"(b IN (?, ?)) AND " +
						"(a, b, c) IN ? AND " +
						"(a, b, c) IN ((1, 2, ?), (?, 5, 6)) AND " +
						"(a, b, c) IN (?, ?, ?) AND " +
						"(a, b, c) > (1, ?, ?)",
					params: []*param{
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval11",
							valueSimulacron: "testval11",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval22",
							valueSimulacron: "testval22",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval33",
							valueSimulacron: "testval33",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
						{
							name:            "",
							isReplacedNow:   true,
							value:           primitive.UUID(timeUuidStart),
							valueSimulacron: timeUuidStart.String(),
							dataType:        datatype.Timeuuid,
							simulacronType:  simulacron.DataTypeTimeuuid,
						},
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval44",
							valueSimulacron: "testval44",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
						{
							name:            "",
							isReplacedNow:   true,
							value:           primitive.UUID(timeUuidStart),
							valueSimulacron: timeUuidStart.String(),
							dataType:        datatype.Timeuuid,
							simulacronType:  simulacron.DataTypeTimeuuid,
						},
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval55",
							valueSimulacron: "testval55",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval66",
							valueSimulacron: "testval66",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval77",
							valueSimulacron: "testval77",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
						{
							name:            "",
							isReplacedNow:   true,
							value:           primitive.UUID(timeUuidStart),
							valueSimulacron: timeUuidStart.String(),
							dataType:        datatype.Timeuuid,
							simulacronType:  simulacron.DataTypeTimeuuid,
						},
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval88",
							valueSimulacron: "testval88",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval99",
							valueSimulacron: "testval99",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval1010",
							valueSimulacron: "testval1010",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
						{
							name:            "",
							isReplacedNow:   true,
							value:           primitive.UUID(timeUuidStart),
							valueSimulacron: timeUuidStart.String(),
							dataType:        datatype.Timeuuid,
							simulacronType:  simulacron.DataTypeTimeuuid,
						},
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval1111",
							valueSimulacron: "testval1111",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
					},
				},
			},
		},
		{
			name: "All_Simple",
			statements: []*batchChildStmt{
				{
					prepared:      false,
					originalQuery: "UPDATE blah SET a = ?, b = now() WHERE a = now()",
					modifiedQuery: `^UPDATE blah SET a = \?, b = (.*) WHERE a = (.*)$`,
					matches:       2,
					params: []*param{
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval",
							valueSimulacron: "testval",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
					},
				},
				{
					prepared:      false,
					originalQuery: "DELETE c[1], a[?], b[now()] FROM blah WHERE b = 123 AND a = now()",
					modifiedQuery: `^DELETE c\[1\], a\[\?\], b\[(.*)\] FROM blah WHERE b = 123 AND a = (.*)$`,
					matches:       2,
					params: []*param{
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval",
							valueSimulacron: "testval",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
					},
				},
				{
					prepared: false,
					originalQuery: "UPDATE blah " +
						"USING TIMESTAMP ? AND TTL ? " +
						"SET a = ?, b = now() " +
						"WHERE " +
						"(a IN ?) AND " +
						"(b IN (now(), ?)) AND " +
						"(a, b, c) IN ? AND " +
						"(a, b, c) IN ((1, 2, ?), (now(), 5, 6)) AND " +
						"(a, b, c) IN (?, ?, ?) AND " +
						"(a, b, c) > (1, now(), ?)",
					modifiedQuery: `^UPDATE blah ` +
						`USING TIMESTAMP \? AND TTL \? ` +
						`SET a = \?, b = (.*) ` +
						"WHERE " +
						"\\(a IN \\?\\) AND " +
						"\\(b IN \\((.*), \\?\\)\\) AND " +
						"\\(a, b, c\\) IN \\? AND " +
						"\\(a, b, c\\) IN \\(\\(1, 2, \\?\\), \\((.*), 5, 6\\)\\) AND " +
						"\\(a, b, c\\) IN \\(\\?, \\?, \\?\\) AND " +
						"\\(a, b, c\\) \\> \\(1, (.*), \\?\\)$",
					matches: 4,
					params: []*param{
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval11",
							valueSimulacron: "testval11",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval22",
							valueSimulacron: "testval22",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval33",
							valueSimulacron: "testval33",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval44",
							valueSimulacron: "testval44",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval55",
							valueSimulacron: "testval55",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval66",
							valueSimulacron: "testval66",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval77",
							valueSimulacron: "testval77",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval88",
							valueSimulacron: "testval88",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval99",
							valueSimulacron: "testval99",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval1010",
							valueSimulacron: "testval1010",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval1111",
							valueSimulacron: "testval1111",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
					},
				},
			},
		},
		{
			name: "Mixed",
			statements: []*batchChildStmt{
				{
					prepared:      true,
					originalQuery: "INSERT INTO ks.table (name, id) VALUES (?, now())",
					modifiedQuery: "INSERT INTO ks.table (name, id) VALUES (?, ?)",
					params: []*param{
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval",
							valueSimulacron: "testval",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
						{
							name:            "",
							isReplacedNow:   true,
							value:           primitive.UUID(timeUuidStart),
							valueSimulacron: timeUuidStart.String(),
							dataType:        datatype.Timeuuid,
							simulacronType:  simulacron.DataTypeTimeuuid,
						},
					},
				},
				{
					prepared:      false,
					originalQuery: "UPDATE blah SET a = ?, b = now() WHERE a = now()",
					modifiedQuery: `^UPDATE blah SET a = \?, b = (.*) WHERE a = (.*)$`,
					matches:       2,
					params: []*param{
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval",
							valueSimulacron: "testval",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
					},
				},
				{
					prepared:      true,
					originalQuery: "DELETE c[1], a[?], b[now()] FROM blah WHERE b = 123 AND a = now()",
					modifiedQuery: "DELETE c[1], a[?], b[?] FROM blah WHERE b = 123 AND a = ?",
					params: []*param{
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval",
							valueSimulacron: "testval",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
						{
							name:            "",
							isReplacedNow:   true,
							value:           primitive.UUID(timeUuidStart),
							valueSimulacron: timeUuidStart.String(),
							dataType:        datatype.Timeuuid,
							simulacronType:  simulacron.DataTypeTimeuuid,
						},
						{
							name:            "",
							isReplacedNow:   true,
							value:           primitive.UUID(timeUuidStart),
							valueSimulacron: timeUuidStart.String(),
							dataType:        datatype.Timeuuid,
							simulacronType:  simulacron.DataTypeTimeuuid,
						},
					},
				},
				{
					prepared: false,
					originalQuery: "UPDATE blah " +
						"USING TIMESTAMP ? AND TTL ? " +
						"SET a = ?, b = now() " +
						"WHERE " +
						"(a IN ?) AND " +
						"(b IN (now(), ?)) AND " +
						"(a, b, c) IN ? AND " +
						"(a, b, c) IN ((1, 2, ?), (now(), 5, 6)) AND " +
						"(a, b, c) IN (?, ?, ?) AND " +
						"(a, b, c) > (1, now(), ?)",
					modifiedQuery: `^UPDATE blah ` +
						`USING TIMESTAMP \? AND TTL \? ` +
						`SET a = \?, b = (.*) ` +
						"WHERE " +
						"\\(a IN \\?\\) AND " +
						"\\(b IN \\((.*), \\?\\)\\) AND " +
						"\\(a, b, c\\) IN \\? AND " +
						"\\(a, b, c\\) IN \\(\\(1, 2, \\?\\), \\((.*), 5, 6\\)\\) AND " +
						"\\(a, b, c\\) IN \\(\\?, \\?, \\?\\) AND " +
						"\\(a, b, c\\) \\> \\(1, (.*), \\?\\)$",
					matches: 4,
					params: []*param{
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval11",
							valueSimulacron: "testval11",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval22",
							valueSimulacron: "testval22",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval33",
							valueSimulacron: "testval33",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval44",
							valueSimulacron: "testval44",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval55",
							valueSimulacron: "testval55",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval66",
							valueSimulacron: "testval66",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval77",
							valueSimulacron: "testval77",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval88",
							valueSimulacron: "testval88",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval99",
							valueSimulacron: "testval99",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval1010",
							valueSimulacron: "testval1010",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
						{
							name:            "",
							isReplacedNow:   false,
							value:           "testval1111",
							valueSimulacron: "testval1111",
							dataType:        datatype.Ascii,
							simulacronType:  simulacron.DataTypeText,
						},
					},
				},
			},
		},
	}

	runTests := func(tests []batchTestArgs, enableNowReplacement bool, timeUuidStart *uuid.UUID, t *testing.T) {
		var simulacronSetup *setup.SimulacronTestSetup
		var err error

		if enableNowReplacement {
			simulacronSetup, err = createSimulacronTestSetupWithServerSideFunctionReplacement()
		} else {
			simulacronSetup, err = createSimulacronTestSetupWithoutServerSideFunctionReplacement()
		}

		require.Nil(t, err)
		defer simulacronSetup.Cleanup()

		testClient := client.NewCqlClient("127.0.0.1:14002", nil)
		cqlConn, err := testClient.ConnectAndInit(context.Background(), primitive.ProtocolVersion4, 1)
		require.Nil(t, err, "testClient setup failed: %v", err)

		defer cqlConn.Close()

		getBatchLogEntries := func(cluster *simulacron.Cluster) []*simulacron.BatchMessage {
			logs, err := cluster.GetLogsByType(simulacron.QueryTypeBatch)
			require.Nil(t, err)
			var matching []*simulacron.BatchMessage
			for _, logEntry := range logs.Datacenters[0].Nodes[0].Queries {
				var batchMsg simulacron.BatchMessage
				err = json.Unmarshal(logEntry.Frame.Message, &batchMsg)
				if err == nil {
					matching = append(matching, &batchMsg)
				}
			}
			return matching
		}

		getPrepareLogEntriesMatchingChildQuery := func(cluster *simulacron.Cluster, childStatement *batchChildStmt) []*simulacron.RequestLogEntry {
			logs, err := cluster.GetLogsByType(simulacron.QueryTypePrepare)
			require.Nil(t, err)

			var queryToMatch string
			if enableNowReplacement {
				queryToMatch = childStatement.modifiedQuery
			} else {
				queryToMatch = childStatement.originalQuery
			}

			var matching []*simulacron.RequestLogEntry
			for _, logEntry := range logs.Datacenters[0].Nodes[0].Queries {
				if logEntry.Query == queryToMatch {
					matching = append(matching, logEntry)
				}
			}
			return matching
		}

		validateForwardedPrepare := func(cluster *simulacron.Cluster, childStatement *batchChildStmt) {
			matching := getPrepareLogEntriesMatchingChildQuery(cluster, childStatement)
			require.Equal(t, 1, len(matching))
			if enableNowReplacement {
				require.NotEqual(t, childStatement.originalQuery, matching[0].Query)
			} else {
				require.Equal(t, childStatement.originalQuery, matching[0].Query)
			}

		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				var batchChildStatements []*message.BatchChild
				var expectedBatchChildQueries []*simulacron.BatchQuery
				for _, childStatement := range test.statements {

					var expectedChildQuery string
					var expectedChildQueryParams []*param
					if enableNowReplacement {
						expectedChildQuery = childStatement.modifiedQuery
						expectedChildQueryParams = childStatement.params
					} else {
						expectedChildQuery = childStatement.originalQuery
						for _, prm := range childStatement.params {
							if !prm.isReplacedNow {
								expectedChildQueryParams = append(expectedChildQueryParams, prm)
							}
						}
					}

					expectedBatchChildQuery := simulacron.NewBatchQuery(expectedChildQuery)
					var positionalValues []*primitive.Value
					for _, p := range expectedChildQueryParams {
						if p.name == "" {
							expectedBatchChildQuery = expectedBatchChildQuery.WithPositionalParameter(p.simulacronType, p.valueSimulacron)
						} else {
							expectedBatchChildQuery = expectedBatchChildQuery.WithNamedParameter(p.name, p.simulacronType, p.valueSimulacron)
						}

						if !p.isReplacedNow {
							codec, err := datacodec.NewCodec(p.dataType)
							require.Nil(t, err)
							value, err := codec.Encode(p.value, primitive.ProtocolVersion4)
							require.Nil(t, err)
							positionalValues = append(positionalValues, primitive.NewValue(value))
						}
					}
					expectedBatchChildQueries = append(expectedBatchChildQueries, expectedBatchChildQuery)

					var queryOrId interface{}
					if childStatement.prepared {
						when := simulacron.NewWhenQueryOptions()
						for _, p := range expectedChildQueryParams {
							if p.name == "" {
								when = when.WithPositionalParameter(p.simulacronType, p.valueSimulacron)
							} else {
								when = when.WithNamedParameter(p.name, p.simulacronType, p.valueSimulacron)
							}
						}
						queryPrime := simulacron.WhenQuery(expectedChildQuery, when).ThenSuccess()
						err := simulacronSetup.Origin.Prime(queryPrime)
						require.Nil(t, err)
						err = simulacronSetup.Target.Prime(queryPrime)
						require.Nil(t, err)

						prepareMsg := &message.Prepare{
							Query: childStatement.originalQuery,
						}
						f := frame.NewFrame(primitive.ProtocolVersion4, 0, prepareMsg)
						resp, err := cqlConn.SendAndReceive(f)
						require.Nil(t, err)
						prepared, ok := resp.Body.Message.(*message.PreparedResult)
						require.True(t, ok)
						queryOrId = prepared.PreparedQueryId

						validateForwardedPrepare(simulacronSetup.Origin, childStatement)
						validateForwardedPrepare(simulacronSetup.Target, childStatement)
					} else {
						queryOrId = childStatement.originalQuery
					}

					batchChildStatements = append(batchChildStatements, &message.BatchChild{
						QueryOrId: queryOrId,
						Values:    positionalValues,
					})
				}

				batchMsg := &message.Batch{
					Children: batchChildStatements,
				}

				f := frame.NewFrame(primitive.ProtocolVersion4, 0, batchMsg)
				resp, err := cqlConn.SendAndReceive(f)
				require.Nil(t, err)

				_, ok := resp.Body.Message.(*message.VoidResult)
				require.True(t, ok)
				validateForwardedBatch := func(cluster *simulacron.Cluster, generatedValues []primitive.UUID) []primitive.UUID {
					matching := getBatchLogEntries(cluster)
					require.Equal(t, 1, len(matching))
					require.NotNil(t, matching[0].QueriesOrIds)
					require.NotNil(t, matching[0].Values)
					require.Equal(t, len(matching[0].QueriesOrIds), len(matching[0].Values))
					require.Equal(t, len(test.statements), len(matching[0].QueriesOrIds))
					assertGeneratedValues := true
					if generatedValues == nil {
						assertGeneratedValues = false
					}
					assertGeneratedValidIdx := 0
					for idx, childStatement := range test.statements {
						actualStmt := matching[0].QueriesOrIds[idx]
						actualParams := matching[0].Values[idx]
						if childStatement.prepared {
							b64ExpectedValue := base64.StdEncoding.EncodeToString(batchChildStatements[idx].QueryOrId.([]byte))
							require.Equal(t, b64ExpectedValue, actualStmt, idx)
						} else {
							if enableNowReplacement {
								var re = regexp.MustCompile(childStatement.modifiedQuery)
								require.True(t, re.MatchString(actualStmt), idx)
								matches := re.FindStringSubmatch(actualStmt)
								require.Equal(t, childStatement.matches+1, len(matches))
								if childStatement.matches > 0 {
									for _, m := range matches[1:] {
										uid, err := uuid.Parse(m)
										require.Nil(t, err)
										require.Equal(t, uuid.Version(1), uid.Version())
										require.NotEqual(t, uuid.UUID{}, uid)
									}
								}
							} else {
								require.Equal(t, childStatement.originalQuery, actualStmt)
							}

						}

						var expectedChildQueryParams []*param
						if enableNowReplacement {
							expectedChildQueryParams = childStatement.params
						} else {
							for _, prm := range childStatement.params {
								if !prm.isReplacedNow {
									expectedChildQueryParams = append(expectedChildQueryParams, prm)
								}
							}
						}

						require.Equal(t, len(expectedChildQueryParams), len(actualParams))

						var generatedNowValue *primitive.UUID
						for idx, p := range expectedChildQueryParams {
							codec, err := datacodec.NewCodec(p.dataType)
							require.Nil(t, err)
							expectedValue, err := codec.Encode(p.value, resp.Header.Version)
							require.Nil(t, err)
							b64ExpectedValue := base64.StdEncoding.EncodeToString(expectedValue)
							actualValue := actualParams[idx]

							if p.isReplacedNow {
								require.Equal(t, datatype.Timeuuid, p.dataType)
								timeUuidValue, err := base64.StdEncoding.DecodeString(actualValue)
								require.Nil(t, err)
								var decodedVal primitive.UUID
								wasNull, err := codec.Decode(timeUuidValue, &decodedVal, resp.Header.Version)
								require.False(t, wasNull)
								require.Nil(t, err)
								require.Greater(t, int64(uuid.UUID(decodedVal).Time()), int64(timeUuidStart.Time()))
								if generatedNowValue == nil {
									generatedNowValue = &decodedVal
								} else {
									require.NotEqual(t, *generatedNowValue, decodedVal)
									generatedNowValue = &decodedVal
								}
								if assertGeneratedValues {
									require.Greater(t, len(generatedValues), assertGeneratedValidIdx)
									require.Equal(t, generatedValues[assertGeneratedValidIdx], decodedVal)
									assertGeneratedValidIdx++
								} else {
									generatedValues = append(generatedValues, decodedVal)
								}
							} else {
								require.Equal(t, b64ExpectedValue, actualValue)
							}
						}
					}
					return generatedValues
				}

				generatedVals := validateForwardedBatch(simulacronSetup.Origin, nil)
				_ = validateForwardedBatch(simulacronSetup.Target, generatedVals)

				err = simulacronSetup.Origin.DeleteLogs()
				require.Nil(t, err)
				err = simulacronSetup.Target.DeleteLogs()
				require.Nil(t, err)
			})
		}

	}

	t.Run("now replacement enabled", func (t *testing.T) {
		runTests(tests, true, &timeUuidStart, t)
	})

	t.Run("now replacement disabled", func (t *testing.T) {
		runTests(tests, false, nil, t)
	})
}

func createSimulacronTestSetupWithServerSideFunctionReplacement() (*setup.SimulacronTestSetup, error) {
	return createSimulacronTestSetupWithServerSideFunctionReplacementConfig(true)
}

func createSimulacronTestSetupWithoutServerSideFunctionReplacement() (*setup.SimulacronTestSetup, error) {
	return createSimulacronTestSetupWithServerSideFunctionReplacementConfig(false)
}

func createSimulacronTestSetupWithServerSideFunctionReplacementConfig(replaceServerSideFunctions bool) (*setup.SimulacronTestSetup, error) {
	originAddress := "127.0.1.1"
	targetAddress := "127.0.1.2"

	conf := setup.NewTestConfig(originAddress, targetAddress)
	conf.ReplaceCqlFunctions = replaceServerSideFunctions
	return setup.NewSimulacronTestSetupWithConfig(conf)
}
