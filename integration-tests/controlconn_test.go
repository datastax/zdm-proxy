package integration_tests

import (
	"bytes"
	"context"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/google/uuid"
	"github.com/riptano/cloud-gate/integration-tests/env"
	"github.com/riptano/cloud-gate/integration-tests/setup"
	"github.com/riptano/cloud-gate/integration-tests/simulacron"
	"github.com/riptano/cloud-gate/integration-tests/utils"
	"github.com/riptano/cloud-gate/proxy/pkg/cloudgateproxy"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGetHosts(t *testing.T) {
	testSetup, err := setup.NewSimulacronTestSetupWithSessionAndNodes(true, false, 3)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	checkHostsFunc := func(t *testing.T, cc *cloudgateproxy.ControlConn, cluster *simulacron.Cluster) {
		clusterName := cc.GetClusterName()
		require.Equal(t, cluster.Name, clusterName)

		hosts, err := cc.GetHosts()
		require.Nil(t, err)
		require.Equal(t, 3, len(hosts))
		nodesByAddress := make(map[string]*simulacron.Node, 3)
		for _, n := range cluster.Datacenters[0].Nodes {
			nodesByAddress[n.Address] = n
		}

		for _, h := range hosts {
			hostId, err := uuid.FromBytes(h.HostId[:])
			require.Nil(t, err)
			require.NotNil(t, hostId)
			require.NotEqual(t, uuid.Nil, hostId)
			endpt := fmt.Sprintf("%s:%d", h.Address, h.Port)
			_, addressInMap := nodesByAddress[endpt]
			require.True(t, addressInMap, fmt.Sprintf("%s does not match a node address in %v", endpt, nodesByAddress))
			require.Equal(t, 9042, h.Port)
			require.Equal(t, "rack1", h.Rack)
			require.Equal(t, env.DseVersion, *(h.DseVersion.AsNillableString()))
			require.Equal(t, env.CassandraVersion, *h.ReleaseVersion)
			require.Equal(t, "dc1", h.Datacenter)
			require.NotNil(t, h.Tokens)
			require.Equal(t, 1, len(h.Tokens))
		}
	}

	checkHostsFunc(t, testSetup.Proxy.GetOriginControlConn(), testSetup.Origin)
	checkHostsFunc(t, testSetup.Proxy.GetTargetControlConn(), testSetup.Target)
}

func TestGetAssignedHosts(t *testing.T) {
	testSetup, err := setup.NewSimulacronTestSetupWithSessionAndNodes(false, false, 3)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	type test struct {
		name          string
		instanceCount int
		index         int
		assigned      []int
	}

	tests := []test{
		{
			name:          "1 instances, 0 index, 3 hosts",
			instanceCount: 1,
			index:         0,
			assigned:      []int{0, 1, 2},
		},
		{
			name:          "2 instances, 0 index, 3 hosts",
			instanceCount: 2,
			index:         0,
			assigned:      []int{0, 2},
		},
		{
			name:          "2 instances, 1 index, 3 hosts",
			instanceCount: 2,
			index:         1,
			assigned:      []int{1},
		},
		{
			name:          "3 instances, 1 index, 3 hosts",
			instanceCount: 3,
			index:         1,
			assigned:      []int{1},
		},
		{
			name:          "4 instances, 1 index, 3 hosts",
			instanceCount: 4,
			index:         1,
			assigned:      []int{1},
		},
		{
			name:          "4 instances, 3 index, 3 hosts",
			instanceCount: 4,
			index:         3,
			assigned:      []int{0},
		},
		{
			name:          "4 instances, 0 index, 3 hosts",
			instanceCount: 4,
			index:         0,
			assigned:      []int{0},
		},
		{
			name:          "12 instances, 10 index, 3 hosts",
			instanceCount: 12,
			index:         10,
			assigned:      []int{1},
		},
	}

	checkAssignedHostsFunc := func(t *testing.T, cc *cloudgateproxy.ControlConn, cluster *simulacron.Cluster, tt test) {

		hosts, err := cc.GetHosts()
		require.Nil(t, err)
		require.Equal(t, 3, len(hosts))

		assignedHosts, err := cc.GetAssignedHosts()
		require.Nil(t, err)
		require.Equal(t, len(tt.assigned), len(assignedHosts))

		t.Logf("Expect assigned %v, Actual assigned %v, Hosts %v", tt.assigned, assignedHosts, hosts)

		for _, assignedHostIndex := range tt.assigned {
			require.Contains(t, assignedHosts, hosts[assignedHostIndex])
		}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			config := setup.NewTestConfig(testSetup.Origin.GetInitialContactPoint(), testSetup.Target.GetInitialContactPoint())
			config.ProxyInstanceCount = tt.instanceCount
			config.ProxyIndex = tt.index
			proxy, err := setup.NewProxyInstanceWithConfig(config)
			require.Nil(t, err)
			defer proxy.Shutdown()

			checkAssignedHostsFunc(t, proxy.GetOriginControlConn(), testSetup.Origin, tt)
			checkAssignedHostsFunc(t, proxy.GetTargetControlConn(), testSetup.Target, tt)
		})
	}
}

func TestNextAssignedHost(t *testing.T) {
	testSetup, err := setup.NewSimulacronTestSetupWithSessionAndNodes(false, false, 3)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	type test struct {
		name          string
		instanceCount int
		index         int
		assigned      []int
	}

	tests := []test{
		{
			name:          "1 instances, 0 index, 3 hosts",
			instanceCount: 1,
			index:         0,
			assigned:      []int{0, 1, 2},
		},
		{
			name:          "2 instances, 0 index, 3 hosts",
			instanceCount: 2,
			index:         0,
			assigned:      []int{0, 2},
		},
		{
			name:          "2 instances, 1 index, 3 hosts",
			instanceCount: 2,
			index:         1,
			assigned:      []int{1},
		},
		{
			name:          "3 instances, 1 index, 3 hosts",
			instanceCount: 3,
			index:         1,
			assigned:      []int{1},
		},
		{
			name:          "4 instances, 1 index, 3 hosts",
			instanceCount: 4,
			index:         1,
			assigned:      []int{1},
		},
		{
			name:          "4 instances, 3 index, 3 hosts",
			instanceCount: 4,
			index:         3,
			assigned:      []int{0},
		},
		{
			name:          "4 instances, 0 index, 3 hosts",
			instanceCount: 4,
			index:         0,
			assigned:      []int{0},
		},
		{
			name:          "12 instances, 10 index, 3 hosts",
			instanceCount: 12,
			index:         10,
			assigned:      []int{1},
		},
	}

	checkAssignedHostsCounterFunc := func(t *testing.T, cc *cloudgateproxy.ControlConn, cluster *simulacron.Cluster, tt test) {

		hosts, err := cc.GetHosts()
		require.Nil(t, err)
		require.Equal(t, 3, len(hosts))

		assignedHosts, err := cc.GetAssignedHosts()
		require.Nil(t, err)
		require.Equal(t, len(tt.assigned), len(assignedHosts))

		wg := &sync.WaitGroup{}
		taskChannels := make([]chan error, 10)
		assignedHostsAllTasks := make([][]*cloudgateproxy.Host, 10)
		for i := 0; i < 10; i++ {
			taskAssignedHosts := make([]*cloudgateproxy.Host, 100000)
			assignedHostsAllTasks[i] = taskAssignedHosts
			ch := make(chan error, 1)
			taskChannels[i] = ch
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 100000; j++ {
					taskAssignedHosts[j], err = cc.NextAssignedHost()
					if err != nil {
						ch <- err
						return
					}
				}
				ch <- nil
				return
			}()
		}

		wg.Wait()

		assignCountersPerHost := make(map[*cloudgateproxy.Host]int)
		for taskIndex, taskChannel := range taskChannels {
			err := <-taskChannel
			require.Nil(t, err)

			for _, h := range assignedHostsAllTasks[taskIndex] {
				counter, exists := assignCountersPerHost[h]
				if !exists {
					assignCountersPerHost[h] = 1
				} else {
					assignCountersPerHost[h] = counter + 1
				}
			}
		}

		expectedCounter := 100000 * 10 / len(tt.assigned)
		for h, counter := range assignCountersPerHost {
			if counter == 0 {
				// if counter is 0, make sure host is not in expected assigned hosts slice
				t.Logf("[%v] Counter 0 for host %v, Assigned: %v", cluster.Name, h, assignedHosts)
				for _, assignedHostIndex := range tt.assigned {
					require.NotEqual(t, hosts[assignedHostIndex], h)
				}
			} else {
				t.Logf("[%v] Counter %v for host %v, Assigned: %v", cluster.Name, counter, h, assignedHosts)
				require.Contains(t, []int{expectedCounter, expectedCounter - 1, expectedCounter + 1}, assignCountersPerHost[hosts[tt.assigned[0]]])
			}
		}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			config := setup.NewTestConfig(testSetup.Origin.GetInitialContactPoint(), testSetup.Target.GetInitialContactPoint())
			config.TargetEnableHostAssignment = true
			config.OriginEnableHostAssignment = true
			config.ProxyInstanceCount = tt.instanceCount
			config.ProxyIndex = tt.index
			proxy, err := setup.NewProxyInstanceWithConfig(config)
			require.Nil(t, err)
			defer proxy.Shutdown()

			checkAssignedHostsCounterFunc(t, proxy.GetOriginControlConn(), testSetup.Origin, tt)
			checkAssignedHostsCounterFunc(t, proxy.GetTargetControlConn(), testSetup.Target, tt)
		})
	}
}

func TestConnectionAssignment(t *testing.T) {
	const nodes = 3
	const nrRequests = 90

	testSetup, err := setup.NewSimulacronTestSetupWithSessionAndNodes(false, false, 3)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	type test struct {
		name                  string
		instanceCount         int
		index                 int
		assigned              []int
		hostAssignmentEnabled bool
	}

	tests := []test{
		{
			name:                  "1 instances, 0 index, 3 hosts, no assignment",
			instanceCount:         1,
			index:                 0,
			assigned:              []int{0, 1, 2},
			hostAssignmentEnabled: false,
		},
		{
			name:                  "1 instances, 0 index, 3 hosts",
			instanceCount:         1,
			index:                 0,
			assigned:              []int{0, 1, 2},
			hostAssignmentEnabled: true,
		},
		{
			name:                  "2 instances, 0 index, 3 hosts",
			instanceCount:         2,
			index:                 0,
			assigned:              []int{0, 2},
			hostAssignmentEnabled: true,
		},
		{
			name:                  "2 instances, 1 index, 3 hosts",
			instanceCount:         2,
			index:                 1,
			assigned:              []int{1},
			hostAssignmentEnabled: true,
		},
		{
			name:                  "3 instances, 1 index, 3 hosts",
			instanceCount:         3,
			index:                 1,
			assigned:              []int{1},
			hostAssignmentEnabled: true,
		},
		{
			name:                  "4 instances, 1 index, 3 hosts",
			instanceCount:         4,
			index:                 1,
			assigned:              []int{1},
			hostAssignmentEnabled: true,
		},
		{
			name:                  "4 instances, 3 index, 3 hosts",
			instanceCount:         4,
			index:                 3,
			assigned:              []int{0},
			hostAssignmentEnabled: true,
		},
		{
			name:                  "4 instances, 0 index, 3 hosts",
			instanceCount:         4,
			index:                 0,
			assigned:              []int{0},
			hostAssignmentEnabled: true,
		},
		{
			name:                  "12 instances, 10 index, 3 hosts",
			instanceCount:         12,
			index:                 10,
			assigned:              []int{1},
			hostAssignmentEnabled: true,
		},
	}

	checkRequestsPerNode := func(t *testing.T, cc *cloudgateproxy.ControlConn, cluster *simulacron.Cluster, tt test, queryString string) {

		hosts, err := cc.GetHosts()
		require.Nil(t, err)
		require.Equal(t, 3, len(hosts))

		assignedHosts, err := cc.GetAssignedHosts()
		require.Nil(t, err)
		require.Equal(t, len(tt.assigned), len(assignedHosts))

		var expectedRequestsPerNode int
		if tt.hostAssignmentEnabled {
			expectedRequestsPerNode = nrRequests / len(tt.assigned)
		} else {
			expectedRequestsPerNode = nrRequests
		}

		logsPerNode := make(map[string]*simulacron.ClusterLogs, 0)
		for _, node := range cluster.Datacenters[0].Nodes {
			logs, err := node.GetLogsWithFilter(func(entry *simulacron.RequestLogEntry) bool {
				return entry.Query == queryString
			})
			require.Nil(t, err)
			logsPerNode[node.Address] = logs
		}

		require.Equal(t, nodes, len(logsPerNode))
		if tt.hostAssignmentEnabled {
			for _, h := range assignedHosts {
				addr := fmt.Sprintf("%s:%d", h.Address.String(), h.Port)
				t.Logf("[%v] For host %v, expected %v, actual %v", cluster.Name, h.Address, expectedRequestsPerNode, len(logsPerNode[addr].Datacenters[0].Nodes[0].Queries))
				require.Equal(t, expectedRequestsPerNode, len(logsPerNode[addr].Datacenters[0].Nodes[0].Queries))
				delete(logsPerNode, addr)
			}
			require.Equal(t, nodes - len(tt.assigned), len(logsPerNode))
		} else {
			addr := fmt.Sprintf("%s:%d", cluster.InitialContactPoint, 9042)
			logs := logsPerNode[addr]
			t.Logf("[%v] For host %v, expected %v, actual %v", cluster.Name, addr, expectedRequestsPerNode, len(logs.Datacenters[0].Nodes[0].Queries))
			require.Equal(t, expectedRequestsPerNode, len(logs.Datacenters[0].Nodes[0].Queries))
			delete(logsPerNode, addr)
			require.Equal(t, nodes - 1, len(logsPerNode))
		}

		for addr, logs := range logsPerNode {
			t.Logf("[%v] For host %v, expected 0, actual %v", cluster.Name, addr, len(logs.Datacenters[0].Nodes[0].Queries))
			require.Equal(t, 0, len(logs.Datacenters[0].Nodes[0].Queries))
		}
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			config := setup.NewTestConfig(testSetup.Origin.GetInitialContactPoint(), testSetup.Target.GetInitialContactPoint())
			config.TargetEnableHostAssignment = tt.hostAssignmentEnabled
			config.OriginEnableHostAssignment = tt.hostAssignmentEnabled
			config.ProxyInstanceCount = tt.instanceCount
			config.ProxyIndex = tt.index
			proxy, err := setup.NewProxyInstanceWithConfig(config)
			require.Nil(t, err)
			defer proxy.Shutdown()

			testClient := client.NewCqlClient("127.0.0.1:14002", nil)

			queryString := fmt.Sprintf("INSERT INTO testconnections_%d (a) VALUES ('a')", i)

			openConnectionAndSendRequestFunc := func() {
				cqlConn, err := testClient.ConnectAndInit(context.Background(), primitive.ProtocolVersion4, 1)
				require.Nil(t, err, "testClient setup failed: %v", err)
				defer cqlConn.Close()

				queryMsg := &message.Query{
					Query:   queryString,
					Options: nil,
				}

				queryFrame := frame.NewFrame(primitive.ProtocolVersion4, 5, queryMsg)
				_, err = cqlConn.SendAndReceive(queryFrame)
				require.Nil(t, err)
			}

			for i := 0; i < nrRequests; i++ {
				openConnectionAndSendRequestFunc()
			}

			checkRequestsPerNode(t, proxy.GetOriginControlConn(), testSetup.Origin, tt, queryString)
			checkRequestsPerNode(t, proxy.GetTargetControlConn(), testSetup.Target, tt, queryString)
		})
	}
}

func TestRefreshTopologyEventHandler(t *testing.T) {
	checkHosts := func (t *testing.T, controlConn *cloudgateproxy.ControlConn, expectedDc string, peersIpPrefix string, peersCount int) (err error, fatal bool) {
		hosts, err := controlConn.GetHosts()
		if err != nil {
			return fmt.Errorf("err should be nil: %v", err), true
		}

		t.Logf("check hosts, expectedDc %v prefix %v count %v hosts count %v", expectedDc, peersIpPrefix, peersCount, len(hosts))

		if len(hosts) != peersCount + 1 {
			return fmt.Errorf("hosts len should be %d: %v", peersCount + 1, hosts), false
		}

		expectedAddresses := []string{peersIpPrefix} // local host

		for i := 0; i < peersCount; i++ {
			expectedAddresses = append(expectedAddresses, fmt.Sprintf("%s%d", peersIpPrefix, i + 1))
		}

		hostsByIp := make(map[string]*cloudgateproxy.Host)
		for _, h := range hosts {
			t.Logf("check host %v prefix %v count %v hosts count %v", h.Address.String(), peersIpPrefix, peersCount, len(hosts))
			hostsByIp[h.Address.String()] = h
			if h.Datacenter != expectedDc {
				return fmt.Errorf("expected dc %v in host %v but actual was %v", expectedDc, h.Address.String(), h.Datacenter), false
			}
			found := false
			for _, expectedAddr := range expectedAddresses {
				if expectedAddr == h.Address.String() {
					found = true
				}
			}

			if !found {
				return fmt.Errorf("unexpected host address found: %v", h.Address.String()), false
			}
		}

		if len(hostsByIp) != peersCount + 1 {
			return fmt.Errorf("hosts by address len should be %d: %v", peersCount + 1, hostsByIp), false
		}

		return nil, false
	}

	beforeSleepTestFunc := func (t *testing.T, controlConn *cloudgateproxy.ControlConn, handler *atomic.Value,
		cluster string, expectedDc string, peersIpPrefix string, peersCount int, newPeersCount int) {
		err, _ := checkHosts(t, controlConn, expectedDc, peersIpPrefix, peersCount)
		require.Nil(t, err)

		handler.Store(newRefreshTopologyTestHandler(cluster, expectedDc + "_1", peersIpPrefix, newPeersCount))
	}

	afterSleepTestFunc := func (
		t *testing.T, controlConn *cloudgateproxy.ControlConn, handler *atomic.Value, server *client.CqlServer,
		cluster string, expectedDc string, peersIpPrefix string, oldPeersCount int, newPeersCount int) {
		serverConns, err := server.AllAcceptedClients()
		require.Nil(t, err)
		require.Equal(t, 1, len(serverConns))
		serverConn := serverConns[0]

		err, _ = checkHosts(t, controlConn, expectedDc, peersIpPrefix, oldPeersCount)
		require.Nil(t, err)

		topologyEvent := &message.TopologyChangeEvent{
			ChangeType: primitive.TopologyChangeTypeNewNode,
			Address:    &primitive.Inet{
				Addr: net.ParseIP(fmt.Sprintf("%s%d", peersIpPrefix, 1)),
				Port: 9042,
			},
		}
		topologyEventFrame := frame.NewFrame(primitive.ProtocolVersion4, -1, topologyEvent)
		err = serverConn.Send(topologyEventFrame)
		require.Nil(t, err)

		utils.RequireWithRetries(t, func() (err error, fatal bool) {
			return checkHosts(t, controlConn, expectedDc + "_1", peersIpPrefix, newPeersCount)
		}, 50, 100 * time.Millisecond)
	}

	type test struct {
		oldOriginPeersCount int
		newOriginPeersCount int
		oldTargetPeersCount int
		newTargetPeersCount int
	}

	tests := []test{
		{
			oldOriginPeersCount: 10,
			newOriginPeersCount: 11,
			oldTargetPeersCount: 5,
			newTargetPeersCount: 6,
		},
		{
			oldOriginPeersCount: 6,
			newOriginPeersCount: 5,
			oldTargetPeersCount: 3,
			newTargetPeersCount: 2,
		},
		{
			oldOriginPeersCount: 1,
			newOriginPeersCount: 0,
			oldTargetPeersCount: 0,
			newTargetPeersCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("oldOrigin %v newOrigin %v oldTarget %v newTarget %v",
			tt.oldOriginPeersCount, tt.newOriginPeersCount, tt.oldTargetPeersCount, tt.newTargetPeersCount), func(t *testing.T) {
			conf := setup.NewTestConfig("127.0.1.1", "127.0.1.2")

			originHandler := &atomic.Value{}
			originHandler.Store(newRefreshTopologyTestHandler("cluster1", "dc1", "127.0.1.1", tt.oldOriginPeersCount))

			targetHandler := &atomic.Value{}
			targetHandler.Store(newRefreshTopologyTestHandler("cluster2", "dc2", "127.0.1.2", tt.oldTargetPeersCount))

			createMutableHandler := func(handler *atomic.Value) func(request *frame.Frame, conn *client.CqlServerConnection, ctx client.RequestHandlerContext) (response *frame.Frame) {
				return func(request *frame.Frame, conn *client.CqlServerConnection, ctx client.RequestHandlerContext) (response *frame.Frame) {
					return handler.Load().(client.RequestHandler)(request, conn, ctx)
				}
			}

			testSetup, err := setup.NewCqlServerTestSetup(conf, false, false, false)
			require.Nil(t, err)
			defer testSetup.Cleanup()
			testSetup.Origin.CqlServer.RequestHandlers = []client.RequestHandler{createMutableHandler(originHandler)}
			testSetup.Target.CqlServer.RequestHandlers = []client.RequestHandler{createMutableHandler(targetHandler)}
			err = testSetup.Start(conf, false, primitive.ProtocolVersion4)
			require.Nil(t, err)
			proxy := testSetup.Proxy
			beforeSleepTestFunc(t, proxy.GetOriginControlConn(), originHandler, "cluster1", "dc1", "127.0.1.1", tt.oldOriginPeersCount, tt.newOriginPeersCount)
			beforeSleepTestFunc(t, proxy.GetTargetControlConn(), targetHandler, "cluster2", "dc2", "127.0.1.2", tt.oldTargetPeersCount, tt.newTargetPeersCount)
			time.Sleep(5 * time.Second)
			afterSleepTestFunc(t, proxy.GetOriginControlConn(), originHandler, testSetup.Origin.CqlServer, "cluster1", "dc1", "127.0.1.1", tt.oldOriginPeersCount, tt.newOriginPeersCount)
			afterSleepTestFunc(t, proxy.GetTargetControlConn(), targetHandler, testSetup.Target.CqlServer, "cluster2", "dc2", "127.0.1.2", tt.oldTargetPeersCount, tt.newTargetPeersCount)
		})
	}
}

func newRefreshTopologyTestHandler(cluster string, datacenter string, peersIpPrefix string, peersCount int) client.RequestHandler {
	return client.NewCompositeRequestHandler(
		client.HeartbeatHandler,
		client.HandshakeHandler,
		client.NewSetKeyspaceHandler(func(_ string) { }),
		client.RegisterHandler,
		newSystemTablesHandler(cluster, datacenter, peersIpPrefix, peersCount),
	)
}

// Creates a new RequestHandler to handle queries to system tables (system.local and system.peers).
func newSystemTablesHandler(cluster string, datacenter string, peersIpPrefix string, peersCount int) client.RequestHandler {
	return func(request *frame.Frame, conn *client.CqlServerConnection, _ client.RequestHandlerContext) (response *frame.Frame) {
		if query, ok := request.Body.Message.(*message.Query); ok {
			q := strings.TrimSpace(strings.ToLower(query.Query))
			q = strings.Join(strings.Fields(q), " ") // remove extra whitespace
			if strings.HasPrefix(q, "select * from system.local") {
				log.Debugf("%v: [system tables handler]: returning full system.local", conn)
				response = fullSystemLocal(cluster, datacenter, request, conn)
			} else if strings.HasPrefix(q, "select schema_version from system.local") {
				log.Debugf("%v: [system tables handler]: returning schema_version", conn)
				response = schemaVersion(request)
			} else if strings.HasPrefix(q, "select cluster_name from system.local") {
				log.Debugf("%v: [system tables handler]: returning cluster_name", conn)
				response = clusterName(cluster, request)
			} else if strings.Contains(q, "from system.peers") {
				log.Debugf("%v: [system tables handler]: returning empty system.peers", conn)
				response = fullSystemPeers(datacenter, request, conn, peersIpPrefix, peersCount)
			}
		}
		return
	}
}

var (
	keyColumn              = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "key", Type: datatype.Varchar}
	broadcastAddressColumn = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "broadcast_address", Type: datatype.Inet}
	clusterNameColumn      = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "cluster_name", Type: datatype.Varchar}
	cqlVersionColumn       = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "cql_version", Type: datatype.Varchar}
	datacenterColumn       = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "data_center", Type: datatype.Varchar}
	hostIdColumn           = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "host_id", Type: datatype.Uuid}
	listenAddressColumn    = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "listen_address", Type: datatype.Inet}
	partitionerColumn      = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "partitioner", Type: datatype.Varchar}
	rackColumn             = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "rack", Type: datatype.Varchar}
	releaseVersionColumn   = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "release_version", Type: datatype.Varchar}
	rpcAddressColumn       = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "rpc_address", Type: datatype.Inet}
	schemaVersionColumn    = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "schema_version", Type: datatype.Uuid}
	tokensColumn           = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "tokens", Type: datatype.NewSetType(datatype.Varchar)}
)

// These columns are a subset of the total columns returned by OSS C* 3.11.2, and contain all the information that
// drivers need in order to establish the cluster topology and determine its characteristics.
var systemLocalColumns = []*message.ColumnMetadata{
	keyColumn,
	broadcastAddressColumn,
	clusterNameColumn,
	cqlVersionColumn,
	datacenterColumn,
	hostIdColumn,
	listenAddressColumn,
	partitionerColumn,
	rackColumn,
	releaseVersionColumn,
	rpcAddressColumn,
	schemaVersionColumn,
	tokensColumn,
}

var (
	peerColumn                = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "peer", Type: datatype.Inet}
	datacenterPeersColumn     = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "data_center", Type: datatype.Varchar}
	hostIdPeersColumn         = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "host_id", Type: datatype.Uuid}
	rackPeersColumn           = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "rack", Type: datatype.Varchar}
	releaseVersionPeersColumn = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "release_version", Type: datatype.Varchar}
	rpcAddressPeersColumn     = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "rpc_address", Type: datatype.Inet}
	schemaVersionPeersColumn  = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "schema_version", Type: datatype.Uuid}
	tokensPeersColumn         = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "tokens", Type: datatype.NewSetType(datatype.Varchar)}
)

// These columns are a subset of the total columns returned by OSS C* 3.11.2, and contain all the information that
// drivers need in order to establish the cluster topology and determine its characteristics.
var systemPeersColumns = []*message.ColumnMetadata{
	peerColumn,
	datacenterColumn,
	hostIdColumn,
	rackColumn,
	releaseVersionColumn,
	rpcAddressColumn,
	schemaVersionColumn,
	tokensColumn,
}

var (
	keyValue            = message.Column("local")
	cqlVersionValue     = message.Column("3.4.4")
	hostIdValue         = message.Column{0xC0, 0xD1, 0xD2, 0x1E, 0xBB, 0x01, 0x41, 0x96, 0x86, 0xDB, 0xBC, 0x31, 0x7B, 0xC1, 0x79, 0x6A}
	partitionerValue    = message.Column("org.apache.cassandra.dht.Murmur3Partitioner")
	rackValue           = message.Column("rack1")
	releaseVersionValue = message.Column("3.11.2")
	schemaVersionValue  = message.Column{0xC0, 0xD1, 0xD2, 0x1E, 0xBB, 0x01, 0x41, 0x96, 0x86, 0xDB, 0xBC, 0x31, 0x7B, 0xC1, 0x79, 0x6A}
)

func systemLocalRow(cluster string, datacenter string, addr net.Addr, version primitive.ProtocolVersion) message.Row {
	addrBuf := &bytes.Buffer{}
	inetAddr := addr.(*net.TCPAddr).IP
	if inetAddr.To4() != nil {
		addrBuf.Write(inetAddr.To4())
	} else {
		addrBuf.Write(inetAddr)
	}
	// emulate {'-9223372036854775808'} (entire ring)
	tokensBuf := &bytes.Buffer{}
	if version >= primitive.ProtocolVersion3 {
		_ = primitive.WriteInt(1, tokensBuf)
		_ = primitive.WriteInt(int32(len("-9223372036854775808")), tokensBuf)
	} else {
		_ = primitive.WriteShort(1, tokensBuf)
		_ = primitive.WriteShort(uint16(len("-9223372036854775808")), tokensBuf)
	}
	tokensBuf.WriteString("-9223372036854775808")
	return message.Row{
		keyValue,
		addrBuf.Bytes(),
		message.Column(cluster),
		cqlVersionValue,
		message.Column(datacenter),
		hostIdValue,
		addrBuf.Bytes(),
		partitionerValue,
		rackValue,
		releaseVersionValue,
		addrBuf.Bytes(),
		schemaVersionValue,
		tokensBuf.Bytes(),
	}
}

func fullSystemLocal(cluster string, datacenter string, request *frame.Frame, conn *client.CqlServerConnection) *frame.Frame {
	systemLocalRow := systemLocalRow(cluster, datacenter, conn.LocalAddr(), request.Header.Version)
	msg := &message.RowsResult{
		Metadata: &message.RowsMetadata{
			ColumnCount: int32(len(systemLocalColumns)),
			Columns:     systemLocalColumns,
		},
		Data: message.RowSet{systemLocalRow},
	}
	return frame.NewFrame(request.Header.Version, request.Header.StreamId, msg)
}

func systemPeersRow(datacenter string, addr net.Addr, version primitive.ProtocolVersion) message.Row {
	addrBuf := &bytes.Buffer{}
	inetAddr := addr.(*net.TCPAddr).IP
	if inetAddr.To4() != nil {
		addrBuf.Write(inetAddr.To4())
	} else {
		addrBuf.Write(inetAddr)
	}

	uuidBuf := &bytes.Buffer{}
	hostId, _ := primitive.ParseUuid(uuid.New().String())
	_ = primitive.WriteUuid(hostId, uuidBuf)

	// emulate {'-9223372036854775808'} (entire ring)
	tokensBuf := &bytes.Buffer{}
	if version >= primitive.ProtocolVersion3 {
		_ = primitive.WriteInt(1, tokensBuf)
		_ = primitive.WriteInt(int32(len("-9223372036854775808")), tokensBuf)
	} else {
		_ = primitive.WriteShort(1, tokensBuf)
		_ = primitive.WriteShort(uint16(len("-9223372036854775808")), tokensBuf)
	}
	tokensBuf.WriteString("-9223372036854775808")
	return message.Row{
		addrBuf.Bytes(),
		message.Column(datacenter),
		uuidBuf.Bytes(),
		rackValue,
		releaseVersionValue,
		addrBuf.Bytes(),
		schemaVersionValue,
		tokensBuf.Bytes(),
	}
}

func fullSystemPeers(
	datacenter string, request *frame.Frame, localConn *client.CqlServerConnection, ipPrefix string, peersCount int) *frame.Frame {
	systemLocalRows := make(message.RowSet, peersCount)
	localAddr := localConn.LocalAddr().(*net.TCPAddr)
	for i := 0; i < peersCount; i++ {
		systemLocalRows[i] = systemPeersRow(
			datacenter,
			&net.TCPAddr{
				IP:   net.ParseIP(fmt.Sprintf("%s%d", ipPrefix, i+1)),
				Port: localAddr.Port,
				Zone: localAddr.Zone,
			},
			request.Header.Version)
	}
	msg := &message.RowsResult{
		Metadata: &message.RowsMetadata{
			ColumnCount: int32(len(systemPeersColumns)),
			Columns:     systemPeersColumns,
		},
		Data: systemLocalRows,
	}
	return frame.NewFrame(request.Header.Version, request.Header.StreamId, msg)
}

func schemaVersion(request *frame.Frame) *frame.Frame {
	msg := &message.RowsResult{
		Metadata: &message.RowsMetadata{
			ColumnCount: 1,
			Columns:     []*message.ColumnMetadata{schemaVersionColumn},
		},
		Data: message.RowSet{message.Row{schemaVersionValue}},
	}
	return frame.NewFrame(request.Header.Version, request.Header.StreamId, msg)
}

func clusterName(cluster string, request *frame.Frame) *frame.Frame {
	msg := &message.RowsResult{
		Metadata: &message.RowsMetadata{
			ColumnCount: 1,
			Columns:     []*message.ColumnMetadata{clusterNameColumn},
		},
		Data: message.RowSet{message.Row{message.Column(cluster)}},
	}
	return frame.NewFrame(request.Header.Version, request.Header.StreamId, msg)
}