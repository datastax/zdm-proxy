package integration_tests

import (
	"context"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/datacodec"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/zdm-proxy/integration-tests/env"
	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/integration-tests/utils"
	"github.com/datastax/zdm-proxy/proxy/pkg/zdmproxy"
	"github.com/gocql/gocql"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"math/big"
	"math/rand"
	"net"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

type connectObserver struct {
	channel  chan gocql.ObservedConnect
	closed   *bool
	chanLock *sync.Mutex
}

func (recv *connectObserver) ObserveConnect(a gocql.ObservedConnect) {
	recv.chanLock.Lock()
	defer recv.chanLock.Unlock()
	if *recv.closed {
		return
	}
	select {
	case recv.channel <- a:
	default:
		log.Errorf("OBSERVE CONNECT FAILED: %v", a)
	}
}

func TestVirtualizationNumberOfConnections(t *testing.T) {
	testSetup, err := setup.NewSimulacronTestSetupWithSessionAndNodes(false, false, 3)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	type test struct {
		name                        string
		proxyIndexes                []int
		proxyAddresses              [][]string
		proxyInstanceCount          int
		proxyInstanceCreationCount  int
		hostAssignmentEnabledOrigin bool
		hostAssignmentEnabledTarget bool
		expectedConns               []int
	}

	tests := []test{
		{
			name:         "3 nodes, 3 instances, 3 virtual hosts, assignment enabled",
			proxyIndexes: []int{0, 1, 2},
			proxyAddresses: [][]string{
				{"127.0.0.1", "127.0.0.2", "127.0.0.3"},
				{"127.0.0.1", "127.0.0.2", "127.0.0.3"},
				{"127.0.0.1", "127.0.0.2", "127.0.0.3"}},
			proxyInstanceCount:          -1,
			proxyInstanceCreationCount:  3,
			hostAssignmentEnabledOrigin: true,
			hostAssignmentEnabledTarget: true,
			expectedConns:               []int{3, 1, 1},
		},
		{
			name:         "3 nodes, 4 instances, 4 virtual hosts, assignment enabled",
			proxyIndexes: []int{0, 1, 2, 3},
			proxyAddresses: [][]string{
				{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4"},
				{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4"},
				{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4"},
				{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4"}},
			proxyInstanceCount:          -1,
			proxyInstanceCreationCount:  4,
			hostAssignmentEnabledOrigin: true,
			hostAssignmentEnabledTarget: true,
			expectedConns:               []int{3, 1, 1, 1},
		},
		{
			name:                        "3 nodes, 3 instances, 1 virtual host, assignment enabled",
			proxyIndexes:                []int{0, 0, 0},
			proxyAddresses:              [][]string{{"127.0.0.1"}, {"127.0.0.2"}, {"127.0.0.3"}},
			proxyInstanceCount:          3,
			proxyInstanceCreationCount:  3,
			hostAssignmentEnabledOrigin: true,
			hostAssignmentEnabledTarget: true,
			expectedConns:               []int{3, 0, 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proxies := make([]*zdmproxy.CloudgateProxy, tt.proxyInstanceCreationCount)
			for i := 0; i < tt.proxyInstanceCreationCount; i++ {
				proxies[i], err = LaunchProxyWithTopologyConfig(
					strings.Join(tt.proxyAddresses[i], ","), tt.proxyIndexes[i], tt.proxyInstanceCount,
					fmt.Sprintf("%s%d", "127.0.0.", i+1), 8, testSetup.Origin, testSetup.Target)
				j := i
				require.Nil(t, err)
				//goland:noinspection GoDeferInLoop
				defer proxies[j].Shutdown()
			}

			observeChannel := make(chan gocql.ObservedConnect, 100)
			hostsMap := make(map[string]int)
			hostsMapLock := &sync.Mutex{}
			errors := make([]error, 0)
			closedChannel := false
			channelLock := &sync.Mutex{}
			wg := &sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					observedConnect, ok := <-observeChannel
					if !ok {
						return
					}
					hostsMapLock.Lock()
					hostAddr := observedConnect.Host.ConnectAddress()
					counter, exists := hostsMap[hostAddr.String()]
					if !exists {
						counter = 0
					}
					if observedConnect.Err != nil {
						errors = append(errors, observedConnect.Err)
					} else {
						counter++
						hostsMap[hostAddr.String()] = counter
					}
					hostsMapLock.Unlock()
				}
			}()

			closeChanFunc := func(closed *bool) {
				channelLock.Lock()
				defer channelLock.Unlock()
				if !*closed {
					*closed = true
					close(observeChannel)
				}
			}

			defer closeChanFunc(&closedChannel)

			cluster := gocql.NewCluster("127.0.0.1")
			cluster.NumConns = 1
			cluster.Port = 14002
			cluster.ConnectObserver = &connectObserver{channel: observeChannel, chanLock: channelLock, closed: &closedChannel}
			session, err := gocql.NewSession(*cluster)
			require.Nil(t, err)

			session.Close()
			closeChanFunc(&closedChannel)
			wg.Wait()

			hostsMapLock.Lock()
			defer hostsMapLock.Unlock()

			// gocql always opens and closes first connection to discover protocol version
			// then opens control connection + request connection = 3 total connection attempts for control host
			require.Equal(t, tt.expectedConns[0], hostsMap["127.0.0.1"], "hostsMap = %v", hostsMap)

			for i := 1; i < tt.proxyInstanceCreationCount; i++ {
				// single request connection for other hosts
				require.Equal(t, tt.expectedConns[i], hostsMap[fmt.Sprintf("%s%d", "127.0.0.", i+1)], "hostsMap = %v", hostsMap)
			}
		})
	}
}

func TestVirtualizationTokenAwareness(t *testing.T) {
	if !env.UseCcm {
		t.Skip("Test requires CCM, set USE_CCM env variable to TRUE")
	}

	type test struct {
		name                        string
		proxyIndexes                []int
		proxyAddresses              [][]string
		proxyInstanceCount          int
		proxyInstanceCreationCount  int
		hostAssignmentEnabledOrigin bool
		hostAssignmentEnabledTarget bool
		expectedConns               []int
		nodes                       int
		expectedReplicas            []string
		numTokens                   int
	}

	tests := []test{
		{
			name:         "3 nodes, 3 instances, 3 virtual hosts, assignment enabled",
			proxyIndexes: []int{0, 1, 2},
			proxyAddresses: [][]string{
				{"127.0.0.1", "127.0.0.2", "127.0.0.3"},
				{"127.0.0.1", "127.0.0.2", "127.0.0.3"},
				{"127.0.0.1", "127.0.0.2", "127.0.0.3"}},
			proxyInstanceCount:          -1,
			proxyInstanceCreationCount:  3,
			hostAssignmentEnabledOrigin: true,
			hostAssignmentEnabledTarget: true,
			expectedConns:               []int{3, 1, 1},
			nodes:                       3,
			expectedReplicas:            []string{"127.0.0.1", "127.0.0.2", "127.0.0.3"},
			numTokens:                   8,
		},
		{
			name:         "3 nodes, 4 instances, 4 virtual hosts, assignment enabled",
			proxyIndexes: []int{0, 1, 2, 3},
			proxyAddresses: [][]string{
				{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4"},
				{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4"},
				{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4"},
				{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4"}},
			proxyInstanceCount:          -1,
			proxyInstanceCreationCount:  4,
			hostAssignmentEnabledOrigin: true,
			hostAssignmentEnabledTarget: true,
			expectedConns:               []int{3, 1, 1, 1},
			nodes:                       3,
			expectedReplicas:            []string{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4"},
			numTokens:                   8,
		},
		{
			name:                        "3 nodes, 3 instances, 1 virtual host, assignment enabled",
			proxyIndexes:                []int{0, 0, 0},
			proxyAddresses:              [][]string{{"127.0.0.1"}, {"127.0.0.2"}, {"127.0.0.3"}},
			proxyInstanceCount:          3,
			proxyInstanceCreationCount:  3,
			hostAssignmentEnabledOrigin: true,
			hostAssignmentEnabledTarget: true,
			expectedConns:               []int{3, 0, 0},
			nodes:                       3,
			expectedReplicas:            []string{"127.0.0.1"},
			numTokens:                   8,
		},
	}

	origin, err := setup.GetGlobalTestClusterOrigin()
	require.Nil(t, err)
	target, err := setup.GetGlobalTestClusterTarget()
	require.Nil(t, err)

	err = origin.GetSession().Query(
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.test_virtualization (key int PRIMARY KEY)", setup.TestKeyspace)).Exec()
	require.Nil(t, err)
	err = target.GetSession().Query(
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.test_virtualization (key int PRIMARY KEY)", setup.TestKeyspace)).Exec()
	require.Nil(t, err)

	ctx, cancelFn := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFn()
	err = origin.GetSession().AwaitSchemaAgreement(ctx)
	ctx2, cancelFn2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFn2()
	err = origin.GetSession().AwaitSchemaAgreement(ctx2)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proxies := make([]*zdmproxy.CloudgateProxy, tt.proxyInstanceCreationCount)
			for i := 0; i < tt.proxyInstanceCreationCount; i++ {
				proxies[i], err = LaunchProxyWithTopologyConfig(
					strings.Join(tt.proxyAddresses[i], ","), tt.proxyIndexes[i], tt.proxyInstanceCount,
					fmt.Sprintf("%s%d", "127.0.0.", i+1), tt.numTokens, origin, target)
				j := i
				require.Nil(t, err)
				//goland:noinspection GoDeferInLoop
				defer proxies[j].Shutdown()
			}

			cluster := gocql.NewCluster("127.0.0.1")
			cluster.NumConns = 1
			cluster.Port = 14002
			cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
			session, err := gocql.NewSession(*cluster)
			require.Nil(t, err)
			defer session.Close()

			type query struct {
				str          string
				arg          interface{}
				murmur3Token int64
			}

			queries := []query{
				{fmt.Sprintf("SELECT * FROM %s.test_virtualization WHERE key = ?", setup.TestKeyspace), 1, -4069959284402364209}, // token: -4069959284402364209
				{fmt.Sprintf("SELECT * FROM %s.test_virtualization WHERE key = ?", setup.TestKeyspace), 2, -3248873570005575792}, // -3248873570005575792
				{fmt.Sprintf("SELECT * FROM %s.test_virtualization WHERE key = ?", setup.TestKeyspace), 3, 9010454139840013625},  // 9010454139840013625
				{fmt.Sprintf("SELECT * FROM %s.test_virtualization WHERE key = ?", setup.TestKeyspace), 4, -2729420104000364805}, // -2729420104000364805
				{fmt.Sprintf("SELECT * FROM %s.test_virtualization WHERE key = ?", setup.TestKeyspace), 5, -7509452495886106294}, // -7509452495886106294
				{fmt.Sprintf("SELECT * FROM %s.test_virtualization WHERE key = ?", setup.TestKeyspace), 6, 2705480034054113608},  // 2705480034054113608
			}

			rnd := rand.New(rand.NewSource(time.Now().Unix()))
			queriesMap := make(map[int][]*gocql.HostInfo)
			sameQueryExecutions := 10
			for n := 0; n < sameQueryExecutions; n++ {
				start := rnd.Int() % len(queries)
				for i := 0; i < len(queries); i++ {
					idx := (i + start) % len(queries)
					hosts := queriesMap[idx]
					host := session.Query(queries[idx].str, queries[idx].arg).Iter().Host()
					queriesMap[idx] = append(hosts, host)
				}
			}

			replicas := computeReplicas(len(tt.expectedReplicas), tt.numTokens)

			for i := 0; i < len(queries); i++ {
				hosts := queriesMap[i]
				require.GreaterOrEqual(t, len(hosts), sameQueryExecutions)
				host := hosts[0]
				replica := getReplicaForToken(replicas, queries[i].murmur3Token)
				require.Equal(t, tt.expectedReplicas[replica.replicaIndex], host.ConnectAddress().String())
				for j := 1; j < len(hosts); j++ {
					require.Equal(t, host.HostID(), hosts[j].HostID())
					require.Equal(t, host.ConnectAddress(), hosts[j].ConnectAddress())
				}
			}
		})
	}
}

/*
cqlsh> describe system.local;
#3.11
CREATE TABLE system.local (
	key text PRIMARY KEY,
	bootstrapped text,
	broadcast_address inet,
	cluster_name text,
	cql_version text,
	data_center text,
	gossip_generation int,
	host_id uuid,
	listen_address inet,
	native_protocol_version text,
	partitioner text,
	rack text,
	release_version text,
	rpc_address inet,
	schema_version uuid,
	thrift_version text,
	tokens set<text>,
	truncated_at map<uuid, blob>
)
 cqlsh> describe system.peers;
 #3.11
 CREATE TABLE system.peers (
     peer inet PRIMARY KEY,
     data_center text,
     host_id uuid,
     preferred_ip inet,
     rack text,
     release_version text,
     rpc_address inet,
     schema_version uuid,
     tokens set<text>
 )

*/
func TestInterceptedQueries(t *testing.T) {
	testSetup, err := setup.NewSimulacronTestSetupWithSessionAndNodes(false, false, 3)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	expectedLocalCols := []string{
		"key", "bootstrapped", "broadcast_address", "cluster_name", "cql_version", "data_center", "dse_version", "graph",
		"host_id", "listen_address", "partitioner", "rack", "release_version", "rpc_address", "schema_version", "tokens",
		"truncated_at",
	}

	expectedPeersCols := []string{
		"peer", "data_center", "dse_version", "graph", "host_id", "preferred_ip", "rack", "release_version", "rpc_address",
		"schema_version", "tokens",
	}

	hostId1 := uuid.NewSHA1(uuid.Nil, net.ParseIP("127.0.0.1"))
	primitiveHostId1 := primitive.UUID(hostId1)
	hostId2 := uuid.NewSHA1(uuid.Nil, net.ParseIP("127.0.0.2"))
	primitiveHostId2 := primitive.UUID(hostId2)
	hostId3 := uuid.NewSHA1(uuid.Nil, net.ParseIP("127.0.0.3"))
	primitiveHostId3 := primitive.UUID(hostId3)

	numTokens := 8

	type testDefinition struct {
		query              string
		expectedCols       []string
		expectedValues     [][]interface{}
		errExpected        message.Message
		proxyInstanceCount int
		connectProxyIndex  int
	}

	tests := []testDefinition{
		{
			query:        "SELECT * FROM system.local",
			expectedCols: expectedLocalCols,
			expectedValues: [][]interface{}{
				{
					"local", "COMPLETED", net.ParseIP("127.0.0.1").To4(), testSetup.Origin.Name, "3.2.0", "dc1", env.DseVersion, false, primitiveHostId1,
					net.ParseIP("127.0.0.1").To4(), "org.apache.cassandra.dht.Murmur3Partitioner", "rack0", env.CassandraVersion, net.ParseIP("127.0.0.1").To4(), nil,
					[]string{"1241"}, nil,
				},
			},
			errExpected:        nil,
			proxyInstanceCount: 3,
			connectProxyIndex:  0,
		},
		{
			query:        "SELECT rack FROM system.local",
			expectedCols: []string{"rack"},
			expectedValues: [][]interface{}{
				{
					"rack0",
				},
			},
			errExpected:        nil,
			proxyInstanceCount: 3,
			connectProxyIndex:  0,
		},
		{
			query:        "SELECT rack as r FROM system.local",
			expectedCols: []string{"r"},
			expectedValues: [][]interface{}{
				{
					"rack0",
				},
			},
			errExpected:        nil,
			proxyInstanceCount: 3,
			connectProxyIndex:  0,
		},
		{
			query:        "SELECT count(*) FROM system.local",
			expectedCols: []string{"count"},
			expectedValues: [][]interface{}{
				{
					int32(1),
				},
			},
			errExpected:        nil,
			proxyInstanceCount: 3,
			connectProxyIndex:  0,
		},
		{
			query:              "SELECT dsa, key, asd FROM system.local",
			expectedCols:       nil,
			expectedValues:     nil,
			errExpected:        &message.Invalid{ErrorMessage: "Undefined column name dsa"},
			proxyInstanceCount: 3,
			connectProxyIndex:  0,
		},
		{
			query:              "SELECT dsa FROM system.local",
			expectedCols:       nil,
			expectedValues:     nil,
			errExpected:        &message.Invalid{ErrorMessage: "Undefined column name dsa"},
			proxyInstanceCount: 3,
			connectProxyIndex:  0,
		},
		{
			query:              "SELECT key, asd FROM system.local",
			expectedCols:       nil,
			expectedValues:     nil,
			errExpected:        &message.Invalid{ErrorMessage: "Undefined column name asd"},
			proxyInstanceCount: 3,
			connectProxyIndex:  0,
		},
		{
			query:        "SELECT rack as r, count(*) as c, rack FROM system.peers",
			expectedCols: []string{"r", "c", "rack"},
			expectedValues: [][]interface{}{
				{
					"rack0", int32(2), "rack0",
				},
			},
			errExpected:        nil,
			proxyInstanceCount: 3,
			connectProxyIndex:  0,
		},
		{
			query:        "SELECT * FROM system.peers",
			expectedCols: expectedPeersCols,
			expectedValues: [][]interface{}{
				{
					net.ParseIP("127.0.0.2").To4(), "dc1", env.DseVersion, false, primitiveHostId2, net.ParseIP("127.0.0.2").To4(), "rack0", env.CassandraVersion, net.ParseIP("127.0.0.2").To4(), nil, []string{"1234"},
				},
				{
					net.ParseIP("127.0.0.3").To4(), "dc1", env.DseVersion, false, primitiveHostId3, net.ParseIP("127.0.0.3").To4(), "rack0", env.CassandraVersion, net.ParseIP("127.0.0.3").To4(), nil, []string{"1234"},
				},
			},
			errExpected:        nil,
			proxyInstanceCount: 3,
			connectProxyIndex:  0,
		},
		{
			query:        "SELECT * FROM system.peers",
			expectedCols: expectedPeersCols,
			expectedValues: [][]interface{}{
				{
					net.ParseIP("127.0.0.1").To4(), "dc1", env.DseVersion, false, primitiveHostId1, net.ParseIP("127.0.0.1").To4(), "rack0", env.CassandraVersion, net.ParseIP("127.0.0.1").To4(), nil, []string{"1234"},
				},
				{
					net.ParseIP("127.0.0.3").To4(), "dc1", env.DseVersion, false, primitiveHostId3, net.ParseIP("127.0.0.3").To4(), "rack0", env.CassandraVersion, net.ParseIP("127.0.0.3").To4(), nil, []string{"1234"},
				},
			},
			errExpected:        nil,
			proxyInstanceCount: 3,
			connectProxyIndex:  1,
		},
		{
			query:              "SELECT * FROM system.peers",
			expectedCols:       expectedPeersCols,
			expectedValues:     [][]interface{}{},
			errExpected:        nil,
			proxyInstanceCount: 1,
			connectProxyIndex:  0,
		},
		{
			query:        "SELECT rack FROM system.peers",
			expectedCols: []string{"rack"},
			expectedValues: [][]interface{}{
				{
					"rack0",
				},
				{
					"rack0",
				},
			},
			errExpected:        nil,
			proxyInstanceCount: 3,
			connectProxyIndex:  0,
		},
		{
			query:        "SELECT rack as r FROM system.peers",
			expectedCols: []string{"r"},
			expectedValues: [][]interface{}{
				{
					"rack0",
				},
				{
					"rack0",
				},
			},
			errExpected:        nil,
			proxyInstanceCount: 3,
			connectProxyIndex:  0,
		},
		{
			query:        "SELECT peer, count(*) FROM system.peers",
			expectedCols: []string{"peer", "count"},
			expectedValues: [][]interface{}{
				{
					net.ParseIP("127.0.0.2").To4(), int32(2),
				},
			},
			errExpected:        nil,
			proxyInstanceCount: 3,
			connectProxyIndex:  0,
		},
		{
			query:        "SELECT peer, count(*), count(*) as c, peer as p FROM system.peers",
			expectedCols: []string{"peer", "count", "c", "p"},
			expectedValues: [][]interface{}{
				{
					nil, int32(0), int32(0), nil,
				},
			},
			errExpected:        nil,
			proxyInstanceCount: 1,
			connectProxyIndex:  0,
		},
		{
			query:        "SELECT count(*) FROM system.peers",
			expectedCols: []string{"count"},
			expectedValues: [][]interface{}{
				{
					int32(2),
				},
			},
			errExpected:        nil,
			proxyInstanceCount: 3,
			connectProxyIndex:  0,
		},
		{
			query:        "SELECT count(*) FROM system.peers",
			expectedCols: []string{"count"},
			expectedValues: [][]interface{}{
				{
					int32(0),
				},
			},
			errExpected:        nil,
			proxyInstanceCount: 1,
			connectProxyIndex:  0,
		},
		{
			query:              "SELECT asd, peer, dsa FROM system.peers",
			expectedCols:       nil,
			expectedValues:     nil,
			errExpected:        &message.Invalid{ErrorMessage: "Undefined column name asd"},
			proxyInstanceCount: 3,
			connectProxyIndex:  0,
		},
		{
			query:              "SELECT asd FROM system.peers",
			expectedCols:       nil,
			expectedValues:     nil,
			errExpected:        &message.Invalid{ErrorMessage: "Undefined column name asd"},
			proxyInstanceCount: 3,
			connectProxyIndex:  0,
		},
		{
			query:              "SELECT peer, dsa FROM system.peers",
			expectedCols:       nil,
			expectedValues:     nil,
			errExpected:        &message.Invalid{ErrorMessage: "Undefined column name dsa"},
			proxyInstanceCount: 3,
			connectProxyIndex:  0,
		},
		{
			query:        "SELECT peer as p, count(*) as c, peer FROM system.peers",
			expectedCols: []string{"p", "c", "peer"},
			expectedValues: [][]interface{}{
				{
					net.ParseIP("127.0.0.2").To4(), int32(2), net.ParseIP("127.0.0.2").To4(),
				},
			},
			errExpected:        nil,
			proxyInstanceCount: 3,
			connectProxyIndex:  0,
		},
	}

	checkRowsResultFunc := func(t *testing.T, testVars testDefinition, queryResponseFrame *frame.Frame) {
		queryRowsResult, ok := queryResponseFrame.Body.Message.(*message.RowsResult)
		require.True(t, ok, queryResponseFrame.Body.Message)
		require.Equal(t, len(testVars.expectedValues), len(queryRowsResult.Data))
		var resultCols []string
		for _, colMetadata := range queryRowsResult.Metadata.Columns {
			resultCols = append(resultCols, colMetadata.Name)
		}
		require.Equal(t, testVars.expectedCols, resultCols)
		for i, row := range queryRowsResult.Data {
			require.Equal(t, len(testVars.expectedValues[i]), len(row))
			for j, value := range row {
				dcodec, err := datacodec.NewCodec(queryRowsResult.Metadata.Columns[j].Type)
				require.Nil(t, err)
				var dest interface{}
				wasNull, err := dcodec.Decode(value, &dest, primitive.ProtocolVersion4)
				require.Nil(t, err)
				switch queryRowsResult.Metadata.Columns[j].Name {
				case "schema_version":
					require.IsType(t, primitive.UUID{}, dest)
					require.NotNil(t, dest)
					require.NotEqual(t, primitive.UUID{}, dest)
				case "tokens":
					tokens, ok := dest.([]*string)
					require.True(t, ok)
					require.Equal(t, numTokens, len(tokens))
					for _, token := range tokens {
						require.NotNil(t, token)
						require.NotEqual(t, "", *token)
					}
				default:
					if wasNull {
						require.Nil(t, testVars.expectedValues[i][j], queryRowsResult.Metadata.Columns[j].Name)
					} else {
						require.Equal(t, testVars.expectedValues[i][j], dest, queryRowsResult.Metadata.Columns[j].Name)
					}
				}
			}
		}
	}
	for _, testVars := range tests {
		t.Run(fmt.Sprintf("%s_proxy%d_%dtotalproxies", testVars.query, testVars.connectProxyIndex, testVars.proxyInstanceCount), func(t *testing.T) {
			proxyInstanceCount := 3
			proxyAddresses := []string{"127.0.0.1", "127.0.0.2", "127.0.0.3"}
			if testVars.proxyInstanceCount == 1 {
				proxyAddresses = []string{"127.0.0.1"}
				proxyInstanceCount = 1
			} else if testVars.proxyInstanceCount != 3 {
				require.Fail(t, "unsupported proxy instance count %v", testVars.proxyInstanceCount)
			}
			proxyAddressToConnect := fmt.Sprintf("127.0.0.%v", testVars.connectProxyIndex+1)
			proxy, err := LaunchProxyWithTopologyConfig(
				strings.Join(proxyAddresses, ","), testVars.connectProxyIndex, proxyInstanceCount,
				proxyAddressToConnect, numTokens, testSetup.Origin, testSetup.Target)
			require.Nil(t, err)
			defer proxy.Shutdown()

			testClient := client.NewCqlClient(fmt.Sprintf("%v:14002", proxyAddressToConnect), nil)
			cqlConnection, err := testClient.ConnectAndInit(context.Background(), primitive.ProtocolVersion4, 0)
			require.Nil(t, err)
			defer cqlConnection.Close()

			queryMsg := &message.Query{
				Query:   testVars.query,
				Options: nil,
			}
			queryFrame := frame.NewFrame(primitive.ProtocolVersion4, 0, queryMsg)
			queryResponseFrame, err := cqlConnection.SendAndReceive(queryFrame)
			require.Nil(t, err)
			if testVars.errExpected != nil {
				require.Equal(t, testVars.errExpected, queryResponseFrame.Body.Message)
			} else {
				checkRowsResultFunc(t, testVars, queryResponseFrame)
			}

			prepareMsg := &message.Prepare{
				Query:    testVars.query,
				Keyspace: "",
			}
			prepareFrame := frame.NewFrame(primitive.ProtocolVersion4, 0, prepareMsg)
			prepareResponseFrame, err := cqlConnection.SendAndReceive(prepareFrame)
			require.Nil(t, err)
			if testVars.errExpected != nil {
				require.Equal(t, testVars.errExpected, prepareResponseFrame.Body.Message)
			} else {
				preparedMsg, ok := prepareResponseFrame.Body.Message.(*message.PreparedResult)
				require.True(t, ok, prepareResponseFrame.Body.Message)
				executeMsg := &message.Execute{
					QueryId:          preparedMsg.PreparedQueryId,
					ResultMetadataId: preparedMsg.ResultMetadataId,
					Options:          nil,
				}
				executeFrame := frame.NewFrame(primitive.ProtocolVersion4, 0, executeMsg)
				executeResponseFrame, err := cqlConnection.SendAndReceive(executeFrame)
				require.Nil(t, err)
				checkRowsResultFunc(t, testVars, executeResponseFrame)
			}
		})
	}
}

func TestVirtualizationPartitioner(t *testing.T) {

	type test struct {
		name               string
		originPartitioner  string
		targetPartitioner  string
		proxyShouldStartUp bool
	}

	tests := []test{
		{
			name:               "Origin Random, Target Murmur3",
			originPartitioner:  randomPartitioner,
			targetPartitioner:  murmur3Partitioner,
			proxyShouldStartUp: true,
		},
		{
			name:               "Origin Murmur3, Target Murmur3",
			originPartitioner:  murmur3Partitioner,
			targetPartitioner:  murmur3Partitioner,
			proxyShouldStartUp: true,
		},
		{
			name:               "Origin ByteOrdered, Target Murmur3",
			originPartitioner:  byteOrderedPartitioner,
			targetPartitioner:  murmur3Partitioner,
			proxyShouldStartUp: false,
		},
		{
			name:               "Origin Murmur3, Target Random",
			originPartitioner:  murmur3Partitioner,
			targetPartitioner:  randomPartitioner,
			proxyShouldStartUp: true,
		},
		{
			name:               "Origin Murmur3, Target ByteOrdered",
			originPartitioner:  murmur3Partitioner,
			targetPartitioner:  byteOrderedPartitioner,
			proxyShouldStartUp: false,
		},
		{
			name:               "Origin Random, Target ByteOrdered",
			originPartitioner:  randomPartitioner,
			targetPartitioner:  byteOrderedPartitioner,
			proxyShouldStartUp: false,
		},
		{
			name:               "Origin ByteOrdered, Target Random",
			originPartitioner:  byteOrderedPartitioner,
			targetPartitioner:  randomPartitioner,
			proxyShouldStartUp: false,
		},
		{
			name:               "Origin Random, Target Random",
			originPartitioner:  randomPartitioner,
			targetPartitioner:  randomPartitioner,
			proxyShouldStartUp: true,
		},
		{
			name:               "Origin ByteOrdered, Target ByteOrdered",
			originPartitioner:  byteOrderedPartitioner,
			targetPartitioner:  byteOrderedPartitioner,
			proxyShouldStartUp: false,
		},
	}

	originAddress := "127.0.1.1"
	targetAddress := "127.0.1.2"
	proxyAddress := "127.0.0.1"

	credentials := &client.AuthCredentials{
		Username: "cassandra",
		Password: "cassandra",
	}

	runTestWithSystemQueryForwarding := func(originPartitioner string, targetPartitioner string, systemQueriesToTarget bool, proxyShouldStartUp bool) {

		serverConf := setup.NewTestConfig(originAddress, targetAddress)

		testSetup, err := setup.NewCqlServerTestSetup(serverConf, false, false, false)
		require.Nil(t, err)
		defer testSetup.Cleanup()

		originSystemLocalRequestHandler := NewCustomSystemTablesHandler("origin", "dc1", originAddress, map[string]int{"dc1": 0}, originPartitioner)
		targetSystemLocalRequestHandler := NewCustomSystemTablesHandler("target", "dc2", targetAddress, map[string]int{"dc2": 0}, targetPartitioner)

		testSetup.Origin.CqlServer.RequestHandlers = []client.RequestHandler{
			originSystemLocalRequestHandler,
			client.NewDriverConnectionInitializationHandler("origin", "dc1", func(_ string) {}),
		}

		testSetup.Target.CqlServer.RequestHandlers = []client.RequestHandler{
			targetSystemLocalRequestHandler,
			client.NewDriverConnectionInitializationHandler("target", "dc2", func(_ string) {}),
		}

		err = testSetup.Start(nil, false, primitive.ProtocolVersion4)
		require.Nil(t, err)

		validatePartitionerFromSystemLocal(t, originAddress+":9042", credentials, originPartitioner)
		validatePartitionerFromSystemLocal(t, targetAddress+":9042", credentials, targetPartitioner)

		var buffer *utils.ThreadsafeBuffer
		if proxyShouldStartUp {
			buffer = createLogHooks(log.InfoLevel)
		} else {
			buffer = createLogHooks(log.WarnLevel)
		}
		defer log.StandardLogger().ReplaceHooks(make(log.LevelHooks))

		require.NotNil(t, buffer)

		proxyConfig := setup.NewTestConfig(originAddress, targetAddress)
		proxyConfig.ProxyAddresses = "127.0.0.1" // needed to enable virtualization. TODO Remove once ZDM-321 is fixed
		proxyConfig.ForwardSystemQueriesToTarget = systemQueriesToTarget
		proxy, err := setup.NewProxyInstanceWithConfig(proxyConfig)
		defer func() {
			if proxy != nil {
				proxy.Shutdown()
			}
		}()

		logMessages := buffer.String()
		if proxyShouldStartUp {
			require.Nil(t, err, "proxy failed to start due to ", err)
			require.NotNil(t, proxy, "proxy failed to start and is nil")
		} else {
			require.NotNil(t, err, "proxy should have failed to start but did not")
			require.Nil(t, proxy, "proxy should have failed to start but did not")

			if originPartitioner == byteOrderedPartitioner {
				require.True(t, strings.Contains(logMessages, "Error while initializing a new cql connection for the control connection of ORIGIN: "+
					"virtualization is enabled and partitioner is not Murmur3 or Random but instead org.apache.cassandra.dht.ByteOrderedPartitioner"),
					"Unsupported partitioner warning message not found")
			} else if targetPartitioner == byteOrderedPartitioner {
				require.True(t, strings.Contains(logMessages, "Error while initializing a new cql connection for the control connection of TARGET: "+
					"virtualization is enabled and partitioner is not Murmur3 or Random but instead org.apache.cassandra.dht.ByteOrderedPartitioner"),
					"Unsupported partitioner warning message not found")
			} else {
				t.Fatal("The proxy failed to start with an unexpected type of partitioner for at least one cluster. " +
					"Please check the proxy logs and the test configuration")
			}
			return
		}

		validatePartitionerFromSystemLocal(t, proxyAddress+":14002", credentials, murmur3Partitioner)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runTestWithSystemQueryForwarding(tt.originPartitioner, tt.targetPartitioner, false, tt.proxyShouldStartUp)
			runTestWithSystemQueryForwarding(tt.originPartitioner, tt.targetPartitioner, true, tt.proxyShouldStartUp)
		})
	}

}

func LaunchProxyWithTopologyConfig(
	proxyAddresses string, proxyIndex int, instanceCount int, listenAddress string, numTokens int,
	origin setup.TestCluster, target setup.TestCluster) (*zdmproxy.CloudgateProxy, error) {
	conf := setup.NewTestConfig(origin.GetInitialContactPoint(), target.GetInitialContactPoint())
	conf.ProxyIndex = proxyIndex
	conf.ProxyInstanceCount = instanceCount
	conf.ProxyAddresses = proxyAddresses
	conf.ProxyQueryAddress = listenAddress
	conf.ProxyMetricsAddress = listenAddress
	conf.ProxyNumTokens = numTokens
	return setup.NewProxyInstanceWithConfig(conf)
}

type replica struct {
	replicaIndex int
	token        int64
}

func getReplicaForToken(tokens []*replica, t int64) *replica {
	p := sort.Search(len(tokens), func(i int) bool {
		return tokens[i].token >= t
	})

	if p >= len(tokens) {
		// rollover
		p = 0
	}

	return tokens[p]
}

func computeReplicas(n int, numTokens int) []*replica {
	twoPow64 := new(big.Int).Exp(big.NewInt(2), big.NewInt(64), nil)
	twoPow63 := new(big.Int).Exp(big.NewInt(2), big.NewInt(63), nil)
	numTokensBig := big.NewInt(int64(numTokens))
	proxyAddressesCountBig := big.NewInt(int64(n))
	replicas := make([]*replica, n*numTokens)
	for i := 0; i < n; i++ {
		for t := 0; t < numTokens; t++ {
			a := new(big.Int).Div(
				twoPow64,
				new(big.Int).Mul(
					numTokensBig, proxyAddressesCountBig))

			b := new(big.Int).Add(
				new(big.Int).Mul(
					big.NewInt(int64(t)), proxyAddressesCountBig),
				big.NewInt(int64(i)))

			tokenInt := new(big.Int).Sub(new(big.Int).Mul(a, b), twoPow63).Int64()
			replicas[i*numTokens+t] = &replica{i, tokenInt}
		}
	}
	sort.Slice(replicas[:], func(i, j int) bool {
		return replicas[i].token < replicas[j].token
	})
	return replicas
}

func validatePartitionerFromSystemLocal(t *testing.T, remoteEndpoint string, credentials *client.AuthCredentials, expectedPartitioner string) {

	testClient := client.NewCqlClient(remoteEndpoint, credentials)
	cqlConn, err := testClient.ConnectAndInit(context.Background(), primitive.ProtocolVersion4, 1)
	require.Nil(t, err, "testClient setup failed", err)
	require.NotNil(t, cqlConn, "cql connection could not be opened")
	defer func() {
		if cqlConn != nil {
			_ = cqlConn.Close()
		}
	}()

	requestMsg := &message.Query{
		Query: fmt.Sprintf("SELECT * FROM system.local"),
		Options: &message.QueryOptions{
			Consistency: primitive.ConsistencyLevelOne,
		},
	}

	queryFrame := frame.NewFrame(primitive.ProtocolVersion4, 0, requestMsg)
	response, err := cqlConn.SendAndReceive(queryFrame)
	require.Nil(t, err)

	systemLocalRowsResult, ok := response.Body.Message.(*message.RowsResult)
	require.True(t, ok, "The response is of an unexpected result type")
	require.NotNil(t, systemLocalRowsResult.Metadata.Columns[7])
	require.Equal(t, "partitioner", systemLocalRowsResult.Metadata.Columns[7].Name)

	var decodedPartitionerValue string
	datacodec.Varchar.Decode(systemLocalRowsResult.Data[0][7], &decodedPartitionerValue, response.Header.Version)
	require.Equal(t, expectedPartitioner, decodedPartitionerValue)

}
