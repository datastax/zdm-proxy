package integration_tests

import (
	"bytes"
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
	"strings"
	"testing"
)

func TestNowFunctionReplacementInsert(t *testing.T) {

	simulacronSetup := setup.NewSimulacronTestSetup()
	defer simulacronSetup.Cleanup()

	testClient := client.NewCqlClient("127.0.0.1:14002", nil)
	cqlConn, err := testClient.ConnectAndInit(context.Background(), primitive.ProtocolVersion4, 1)
	require.Nil(t, err, "testClient setup failed: %v", err)

	defer cqlConn.Close()

	nameBytes := &bytes.Buffer{}
	err = primitive.WriteString("john", nameBytes)
	require.Nil(t, err, "encode name failed: %v", err)
	queryMsg := &message.Query{
		Query:   "INSERT INTO ks.table (name, id) VALUES (?, now())",
		Options: &message.QueryOptions{
			PositionalValues:        []*primitive.Value{
				&primitive.Value{
					Type:     primitive.ValueTypeRegular,
					Contents: nameBytes.Bytes(),
				},
			},
		},
	}

	f := frame.NewFrame(primitive.ProtocolVersion4, 2, queryMsg)
	_, err = cqlConn.SendAndReceive(f)
	require.Nil(t, err)

	assertQueryModified := func (cluster *simulacron.Cluster) {
		logs, err := cluster.GetLogsByType(simulacron.QueryTypeQuery)
		require.Nil(t, err)

		var matching []*simulacron.RequestLogEntry
		for _, logEntry := range logs.Datacenters[0].Nodes[0].Queries {
			if strings.Contains(logEntry.Query, "INSERT INTO") {
				matching = append(matching, logEntry)
			}
		}

		require.Equal(t, 1, len(matching))
		require.NotEqual(t, "INSERT INTO ks.table (name, id) VALUES (?, now())", matching[0].Query)
		var re = regexp.MustCompile(`INSERT INTO ks\.table \(name, id\) VALUES \(\?, (.*)\)`)

		matches := re.FindStringSubmatch(matching[0].Query)
		require.Equal(t, 2, len(matches))
		uid, err := uuid.Parse(matches[1])
		require.Nil(t, err)
		require.Equal(t,  uuid.Version(1), uid.Version())
	}

	assertQueryModified(simulacronSetup.Origin)
	assertQueryModified(simulacronSetup.Target)
}
