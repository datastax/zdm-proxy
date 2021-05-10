package integration_tests

import (
	"context"
	"github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/google/uuid"
	"github.com/riptano/cloud-gate/integration-tests/setup"
	"github.com/riptano/cloud-gate/integration-tests/simulacron"
	"github.com/stretchr/testify/require"
	"regexp"
	"testing"
)

func TestNowFunctionReplacement(t *testing.T) {

	simulacronSetup, err := setup.NewSimulacronTestSetup()
	require.Nil(t, err)
	defer simulacronSetup.Cleanup()

	testClient := client.NewCqlClient("127.0.0.1:14002", nil)
	cqlConn, err := testClient.ConnectAndInit(context.Background(), primitive.ProtocolVersion4, 1)
	require.Nil(t, err, "testClient setup failed: %v", err)

	defer cqlConn.Close()

	type testArgs struct {
		name      string
		query     string
		regex     string
		matches   int
		queryOpts *message.QueryOptions
	}

	tests := []testArgs{
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
			name:    "Delete",
			query:   "DELETE FROM ks.table WHERE id = now()",
			regex:   `^DELETE FROM ks\.table WHERE id = (.*)$`,
			matches: 1,
			queryOpts: nil,
		},
		{
			name:    "DeleteComplex",
			query:   "DELETE a[now()] FROM ks.table WHERE id = now()",
			regex:   `^DELETE a\[(.*)\] FROM ks\.table WHERE id = (.*)$`,
			matches: 2,
			queryOpts: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			queryMsg := &message.Query{
				Query:   test.query,
				Options: test.queryOpts,
			}

			f := frame.NewFrame(primitive.ProtocolVersion4, 2, queryMsg)
			_, err = cqlConn.SendAndReceive(f)
			require.Nil(tt, err)

			var re = regexp.MustCompile(test.regex)
			assertQueryModified := func (cluster *simulacron.Cluster) {
				logs, err := cluster.GetLogsByType(simulacron.QueryTypeQuery)
				require.Nil(tt, err)

				var matching []*simulacron.RequestLogEntry
				for _, logEntry := range logs.Datacenters[0].Nodes[0].Queries {
					if re.MatchString(logEntry.Query) {
						matching = append(matching, logEntry)
					}
				}

				require.Equal(tt, 1, len(matching))
				require.NotEqual(tt, test.query, matching[0].Query)

				matches := re.FindStringSubmatch(matching[0].Query)
				require.Equal(tt, test.matches + 1, len(matches))
				if test.matches > 0 {
					for _, m := range matches[1:] {
						uid, err := uuid.Parse(m)
						require.Nil(tt, err)
						require.Equal(tt,  uuid.Version(1), uid.Version())
					}
				}
			}

			assertQueryModified(simulacronSetup.Origin)
			assertQueryModified(simulacronSetup.Target)
		})
	}
}
