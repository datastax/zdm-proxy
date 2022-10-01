package integration_tests

import (
	"context"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/zdm-proxy/integration-tests/env"
	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/integration-tests/simulacron"
	"github.com/datastax/zdm-proxy/integration-tests/utils"
	"github.com/datastax/zdm-proxy/proxy/pkg/zdmproxy"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGetHosts(t *testing.T) {
	testSetup, err := setup.NewSimulacronTestSetupWithSessionAndNodes(t, true, false, 3)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	checkHostsFunc := func(t *testing.T, cc *zdmproxy.ControlConn, cluster *simulacron.Cluster) {
		clusterName := cc.GetClusterName()
		require.Equal(t, cluster.Name, clusterName)

		hosts, err := cc.GetOrderedHostsInLocalDatacenter()
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
			require.Equal(t, env.DseVersion, *(h.ColumnData["dse_version"].AsNillableString()))
			require.Equal(t, env.CassandraVersion, *(h.ColumnData["release_version"].AsNillableString()))
			require.Equal(t, "dc1", h.Datacenter)
			require.NotNil(t, h.Tokens)
			require.Equal(t, 1, len(h.Tokens))
		}
	}

	checkHostsFunc(t, testSetup.Proxy.GetOriginControlConn(), testSetup.Origin)
	checkHostsFunc(t, testSetup.Proxy.GetTargetControlConn(), testSetup.Target)
}

func ipAddresses(count int) string {
	ipAddresses := ""
	for i := 0; i < count; i++ {
		if i == count-1 {
			ipAddresses += fmt.Sprintf("127.0.0.%d", i+1)
		} else {
			ipAddresses += fmt.Sprintf("127.0.0.%d,", i+1)
		}
	}
	return ipAddresses
}

func TestGetAssignedHosts(t *testing.T) {
	testSetup, err := setup.NewSimulacronTestSetupWithSessionAndNodes(t, false, false, 3)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	type test struct {
		name              string
		topologyAddresses string
		index             int
		assigned          []int
	}

	tests := []test{
		{
			name:              "1 instances, 0 index, 3 hosts",
			topologyAddresses: ipAddresses(1),
			index:             0,
			assigned:          []int{0, 1, 2},
		},
		{
			name:              "2 instances, 0 index, 3 hosts",
			topologyAddresses: ipAddresses(2),
			index:             0,
			assigned:          []int{0, 2},
		},
		{
			name:              "2 instances, 1 index, 3 hosts",
			topologyAddresses: ipAddresses(2),
			index:             1,
			assigned:          []int{1},
		},
		{
			name:              "3 instances, 1 index, 3 hosts",
			topologyAddresses: ipAddresses(3),
			index:             1,
			assigned:          []int{1},
		},
		{
			name:              "4 instances, 1 index, 3 hosts",
			topologyAddresses: ipAddresses(4),
			index:             1,
			assigned:          []int{1},
		},
		{
			name:              "4 instances, 3 index, 3 hosts",
			topologyAddresses: ipAddresses(4),
			index:             3,
			assigned:          []int{0},
		},
		{
			name:              "4 instances, 0 index, 3 hosts",
			topologyAddresses: ipAddresses(4),
			index:             0,
			assigned:          []int{0},
		},
		{
			name:              "12 instances, 10 index, 3 hosts",
			topologyAddresses: ipAddresses(12),
			index:             10,
			assigned:          []int{1},
		},
	}

	checkAssignedHostsFunc := func(t *testing.T, cc *zdmproxy.ControlConn, cluster *simulacron.Cluster, tt test) {

		hosts, err := cc.GetOrderedHostsInLocalDatacenter()
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
			config.ProxyTopologyIndex = tt.index
			config.ProxyTopologyAddresses = tt.topologyAddresses
			proxy, err := setup.NewProxyInstanceWithConfig(config)
			require.Nil(t, err)
			defer proxy.Shutdown()

			checkAssignedHostsFunc(t, proxy.GetOriginControlConn(), testSetup.Origin, tt)
			checkAssignedHostsFunc(t, proxy.GetTargetControlConn(), testSetup.Target, tt)
		})
	}
}

func TestNextAssignedHost(t *testing.T) {
	testSetup, err := setup.NewSimulacronTestSetupWithSessionAndNodes(t, false, false, 3)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	type test struct {
		name              string
		topologyAddresses string
		index             int
		assigned          []int
	}

	tests := []test{
		{
			name:              "1 instances, 0 index, 3 hosts",
			topologyAddresses: ipAddresses(1),
			index:             0,
			assigned:          []int{0, 1, 2},
		},
		{
			name:              "2 instances, 0 index, 3 hosts",
			topologyAddresses: ipAddresses(2),
			index:             0,
			assigned:          []int{0, 2},
		},
		{
			name:              "2 instances, 1 index, 3 hosts",
			topologyAddresses: ipAddresses(2),
			index:             1,
			assigned:          []int{1},
		},
		{
			name:              "3 instances, 1 index, 3 hosts",
			topologyAddresses: ipAddresses(3),
			index:             1,
			assigned:          []int{1},
		},
		{
			name:              "4 instances, 1 index, 3 hosts",
			topologyAddresses: ipAddresses(4),
			index:             1,
			assigned:          []int{1},
		},
		{
			name:              "4 instances, 3 index, 3 hosts",
			topologyAddresses: ipAddresses(4),
			index:             3,
			assigned:          []int{0},
		},
		{
			name:              "4 instances, 0 index, 3 hosts",
			topologyAddresses: ipAddresses(4),
			index:             0,
			assigned:          []int{0},
		},
		{
			name:              "12 instances, 10 index, 3 hosts",
			topologyAddresses: ipAddresses(12),
			index:             10,
			assigned:          []int{1},
		},
	}

	checkAssignedHostsCounterFunc := func(t *testing.T, cc *zdmproxy.ControlConn, cluster *simulacron.Cluster, tt test) {

		hosts, err := cc.GetOrderedHostsInLocalDatacenter()
		require.Nil(t, err)
		require.Equal(t, 3, len(hosts))

		assignedHosts, err := cc.GetAssignedHosts()
		require.Nil(t, err)
		require.Equal(t, len(tt.assigned), len(assignedHosts))

		wg := &sync.WaitGroup{}
		taskChannels := make([]chan error, 10)
		assignedHostsAllTasks := make([][]*zdmproxy.Host, 10)
		for i := 0; i < 10; i++ {
			taskAssignedHosts := make([]*zdmproxy.Host, 100000)
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

		assignCountersPerHost := make(map[*zdmproxy.Host]int)
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
			config.ProxyTopologyIndex = tt.index
			config.ProxyTopologyAddresses = tt.topologyAddresses
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

	testSetup, err := setup.NewSimulacronTestSetupWithSessionAndNodes(t, false, false, 3)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	type test struct {
		name                  string
		topologyAddresses     string
		index                 int
		assigned              []int
		hostAssignmentEnabled bool
	}

	tests := []test{
		{
			name:                  "1 instances, 0 index, 3 hosts, no assignment",
			topologyAddresses:     ipAddresses(1),
			index:                 0,
			assigned:              []int{0, 1, 2},
			hostAssignmentEnabled: false,
		},
		{
			name:                  "1 instances, 0 index, 3 hosts",
			topologyAddresses:     ipAddresses(1),
			index:                 0,
			assigned:              []int{0, 1, 2},
			hostAssignmentEnabled: true,
		},
		{
			name:                  "2 instances, 0 index, 3 hosts",
			topologyAddresses:     ipAddresses(2),
			index:                 0,
			assigned:              []int{0, 2},
			hostAssignmentEnabled: true,
		},
		{
			name:                  "2 instances, 1 index, 3 hosts",
			topologyAddresses:     ipAddresses(2),
			index:                 1,
			assigned:              []int{1},
			hostAssignmentEnabled: true,
		},
		{
			name:                  "3 instances, 1 index, 3 hosts",
			topologyAddresses:     ipAddresses(3),
			index:                 1,
			assigned:              []int{1},
			hostAssignmentEnabled: true,
		},
		{
			name:                  "4 instances, 1 index, 3 hosts",
			topologyAddresses:     ipAddresses(4),
			index:                 1,
			assigned:              []int{1},
			hostAssignmentEnabled: true,
		},
		{
			name:                  "4 instances, 3 index, 3 hosts",
			topologyAddresses:     ipAddresses(4),
			index:                 3,
			assigned:              []int{0},
			hostAssignmentEnabled: true,
		},
		{
			name:                  "4 instances, 0 index, 3 hosts",
			topologyAddresses:     ipAddresses(4),
			index:                 0,
			assigned:              []int{0},
			hostAssignmentEnabled: true,
		},
		{
			name:                  "12 instances, 10 index, 3 hosts",
			topologyAddresses:     ipAddresses(12),
			index:                 10,
			assigned:              []int{1},
			hostAssignmentEnabled: true,
		},
	}

	checkRequestsPerNode := func(t *testing.T, cc *zdmproxy.ControlConn, cluster *simulacron.Cluster, tt test, queryString string) {

		hosts, err := cc.GetOrderedHostsInLocalDatacenter()
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
			require.Equal(t, nodes-len(tt.assigned), len(logsPerNode))
		} else {
			addr := fmt.Sprintf("%s:%d", cluster.InitialContactPoint, 9042)
			logs := logsPerNode[addr]
			t.Logf("[%v] For host %v, expected %v, actual %v", cluster.Name, addr, expectedRequestsPerNode, len(logs.Datacenters[0].Nodes[0].Queries))
			require.Equal(t, expectedRequestsPerNode, len(logs.Datacenters[0].Nodes[0].Queries))
			delete(logsPerNode, addr)
			require.Equal(t, nodes-1, len(logsPerNode))
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
			config.ProxyTopologyIndex = tt.index
			config.ProxyTopologyAddresses = tt.topologyAddresses
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
	checkHosts := func(t *testing.T, controlConn *zdmproxy.ControlConn, localHostDc string, peersIpPrefix string,
		peersCount map[string]int, expectedHostsCountPerDc map[string]int) (err error, fatal bool) {
		hosts, err := controlConn.GetOrderedHostsInLocalDatacenter()
		if err != nil {
			return fmt.Errorf("err should be nil: %v", err), true
		}

		hostsPerDc := groupHostsPerDc(hosts)

		t.Logf("check hosts, localHostDc %v prefix %v count %v hosts count %v", localHostDc, peersIpPrefix, expectedHostsCountPerDc, len(hosts))

		if len(expectedHostsCountPerDc) != len(hostsPerDc) {
			return fmt.Errorf("dc length mismatch %v vs %v", len(expectedHostsCountPerDc), len(hostsPerDc)), false
		}

		for dc, count := range expectedHostsCountPerDc {
			if len(hostsPerDc[dc]) != count {
				return fmt.Errorf("hosts len should be %d: %v", count, hostsPerDc[dc]), false
			}
		}

		expectedAddresses := map[string][]string{localHostDc: {peersIpPrefix}} // local host

		totalExpectedPeersCount := 0
		peersKeys := make([]string, 0)
		for key := range peersCount {
			peersKeys = append(peersKeys, key)
		}
		sort.Strings(peersKeys)
		for _, dc := range peersKeys {
			count := peersCount[dc]
			expectedAddressesForDc := expectedAddresses[dc]
			for i := 0; i < count; i++ {
				expectedAddressesForDc = append(expectedAddressesForDc, fmt.Sprintf("%s%d", peersIpPrefix, totalExpectedPeersCount+(i+1)))
			}
			expectedAddresses[dc] = expectedAddressesForDc
			totalExpectedPeersCount += count
		}

		hostsByIp := make(map[string]*zdmproxy.Host)
		for _, hostsForDc := range hostsPerDc {
			for _, h := range hostsForDc {
				t.Logf("check host %v prefix %v count %v hosts count %v", h.Address.String(), peersIpPrefix, expectedHostsCountPerDc, len(hosts))
				hostsByIp[h.Address.String()] = h
				found := 0
				for expectedDc, expectedAddressesForDc := range expectedAddresses {
					for _, expectedAddr := range expectedAddressesForDc {
						if expectedAddr == h.Address.String() {
							found++
							if h.Datacenter != expectedDc {
								return fmt.Errorf("expected expectedDc %v in host %v but actual was %v", expectedDc, h.Address.String(), h.Datacenter), false
							}
						}
					}
				}

				if found != 1 {
					return fmt.Errorf("unexpected host address found: %v, found counter %v", h.Address.String(), found), false
				}
			}
		}

		if len(hostsByIp) > 0 && len(expectedHostsCountPerDc) == 0 {
			return fmt.Errorf("expected hosts length is 0 but hostsByIp length is not 0 (%v)", len(hostsByIp)), false
		}

		for _, hostsDc := range expectedHostsCountPerDc {
			if len(hostsByIp) != hostsDc {
				return fmt.Errorf("hosts by address len should be %d: %v", hostsDc, hostsByIp), false
			}

			// only 1 dc
			break
		}

		return nil, false
	}

	beforeSleepTestFunc := func(t *testing.T, controlConn *zdmproxy.ControlConn, handler *atomic.Value,
		cluster string, oldLocalHostDc string, newLocalHostDc string, peersIpPrefix string, peersCount map[string]int,
		newPeersCount map[string]int, expectedOldHosts map[string]int) {
		err, _ := checkHosts(t, controlConn, oldLocalHostDc, peersIpPrefix, peersCount, expectedOldHosts)
		require.Nil(t, err)

		handler.Store(newRefreshTopologyTestHandler(cluster, newLocalHostDc, peersIpPrefix, newPeersCount))
	}

	afterSleepTestFunc := func(
		t *testing.T, controlConn *zdmproxy.ControlConn, handler *atomic.Value, server *client.CqlServer,
		cluster string, oldLocalHostDc string, newLocalHostDc string, peersIpPrefix string,
		oldPeersCount map[string]int, newPeersCount map[string]int,
		expectedOldHosts map[string]int, expectedNewHosts map[string]int) {
		serverConns, err := server.AllAcceptedClients()
		require.Nil(t, err)
		require.Equal(t, 1, len(serverConns))
		serverConn := serverConns[0]

		err, _ = checkHosts(t, controlConn, oldLocalHostDc, peersIpPrefix, oldPeersCount, expectedOldHosts)
		require.Nil(t, err)

		topologyEvent := &message.TopologyChangeEvent{
			ChangeType: primitive.TopologyChangeTypeNewNode,
			Address: &primitive.Inet{
				Addr: net.ParseIP(fmt.Sprintf("%s%d", peersIpPrefix, 1)),
				Port: 9042,
			},
		}
		topologyEventFrame := frame.NewFrame(primitive.ProtocolVersion4, -1, topologyEvent)
		err = serverConn.Send(topologyEventFrame)
		require.Nil(t, err)

		utils.RequireWithRetries(t, func() (err error, fatal bool) {
			return checkHosts(t, controlConn, newLocalHostDc, peersIpPrefix, newPeersCount, expectedNewHosts)
		}, 50, 100*time.Millisecond)
	}

	type test struct {
		name                        string
		originDcConf                string
		targetDcConf                string
		oldOriginLocalHostDc        string
		newOriginLocalHostDc        string
		oldTargetLocalHostDc        string
		newTargetLocalHostDc        string
		oldOriginPeersCount         map[string]int
		newOriginPeersCount         map[string]int
		oldTargetPeersCount         map[string]int
		newTargetPeersCount         map[string]int
		expectedOldOriginHostsCount map[string]int
		expectedNewOriginHostsCount map[string]int
		expectedOldTargetHostsCount map[string]int
		expectedNewTargetHostsCount map[string]int
	}

	tests := []test{
		{
			name:                        "single_dc_add_peer",
			originDcConf:                "",
			targetDcConf:                "",
			oldOriginLocalHostDc:        "dc1",
			newOriginLocalHostDc:        "dc1",
			oldTargetLocalHostDc:        "dc2",
			newTargetLocalHostDc:        "dc2",
			oldOriginPeersCount:         map[string]int{"dc1": 10},
			newOriginPeersCount:         map[string]int{"dc1": 11},
			oldTargetPeersCount:         map[string]int{"dc2": 5},
			newTargetPeersCount:         map[string]int{"dc2": 6},
			expectedOldOriginHostsCount: map[string]int{"dc1": 11},
			expectedNewOriginHostsCount: map[string]int{"dc1": 12},
			expectedOldTargetHostsCount: map[string]int{"dc2": 6},
			expectedNewTargetHostsCount: map[string]int{"dc2": 7},
		},
		{
			name:                        "single_dc_remove_peer",
			originDcConf:                "",
			targetDcConf:                "",
			oldOriginLocalHostDc:        "dc1",
			newOriginLocalHostDc:        "dc1",
			oldTargetLocalHostDc:        "dc2",
			newTargetLocalHostDc:        "dc2",
			oldOriginPeersCount:         map[string]int{"dc1": 6},
			newOriginPeersCount:         map[string]int{"dc1": 5},
			oldTargetPeersCount:         map[string]int{"dc2": 3},
			newTargetPeersCount:         map[string]int{"dc2": 2},
			expectedOldOriginHostsCount: map[string]int{"dc1": 7},
			expectedNewOriginHostsCount: map[string]int{"dc1": 6},
			expectedOldTargetHostsCount: map[string]int{"dc2": 4},
			expectedNewTargetHostsCount: map[string]int{"dc2": 3},
		},
		{
			name:                        "single_dc_remove_all_peers",
			originDcConf:                "",
			targetDcConf:                "",
			oldOriginLocalHostDc:        "dc1",
			newOriginLocalHostDc:        "dc1",
			oldTargetLocalHostDc:        "dc2",
			newTargetLocalHostDc:        "dc2",
			oldOriginPeersCount:         map[string]int{"dc1": 1},
			newOriginPeersCount:         map[string]int{"dc1": 0},
			oldTargetPeersCount:         map[string]int{"dc2": 0},
			newTargetPeersCount:         map[string]int{"dc2": 1},
			expectedOldOriginHostsCount: map[string]int{"dc1": 2},
			expectedNewOriginHostsCount: map[string]int{"dc1": 1},
			expectedOldTargetHostsCount: map[string]int{"dc2": 1},
			expectedNewTargetHostsCount: map[string]int{"dc2": 2},
		},
		{
			name:                        "two_dcs_add_remote_peer",
			originDcConf:                "",
			targetDcConf:                "",
			oldOriginLocalHostDc:        "dc1",
			newOriginLocalHostDc:        "dc1",
			oldTargetLocalHostDc:        "dc2",
			newTargetLocalHostDc:        "dc2",
			oldOriginPeersCount:         map[string]int{"dc1": 10, "dc11": 20},
			newOriginPeersCount:         map[string]int{"dc1": 10, "dc11": 21},
			oldTargetPeersCount:         map[string]int{"dc2": 5, "dc21": 15},
			newTargetPeersCount:         map[string]int{"dc2": 5, "dc21": 16},
			expectedOldOriginHostsCount: map[string]int{"dc1": 11},
			expectedNewOriginHostsCount: map[string]int{"dc1": 11},
			expectedOldTargetHostsCount: map[string]int{"dc2": 6},
			expectedNewTargetHostsCount: map[string]int{"dc2": 6},
		},
		{
			name:                        "two_dcs_add_local_peer",
			originDcConf:                "",
			targetDcConf:                "",
			oldOriginLocalHostDc:        "dc1",
			newOriginLocalHostDc:        "dc1",
			oldTargetLocalHostDc:        "dc2",
			newTargetLocalHostDc:        "dc2",
			oldOriginPeersCount:         map[string]int{"dc1": 12, "dc11": 20},
			newOriginPeersCount:         map[string]int{"dc1": 13, "dc11": 20},
			oldTargetPeersCount:         map[string]int{"dc2": 6, "dc21": 15},
			newTargetPeersCount:         map[string]int{"dc2": 7, "dc21": 15},
			expectedOldOriginHostsCount: map[string]int{"dc1": 13},
			expectedNewOriginHostsCount: map[string]int{"dc1": 14},
			expectedOldTargetHostsCount: map[string]int{"dc2": 7},
			expectedNewTargetHostsCount: map[string]int{"dc2": 8},
		},
		{
			name:                        "two_dcs_local_dc_different_than_conf_dc_add_peer",
			originDcConf:                "dc11",
			targetDcConf:                "dc21",
			oldOriginLocalHostDc:        "dc1",
			newOriginLocalHostDc:        "dc1",
			oldTargetLocalHostDc:        "dc2",
			newTargetLocalHostDc:        "dc2",
			oldOriginPeersCount:         map[string]int{"dc1": 12, "dc11": 20},
			newOriginPeersCount:         map[string]int{"dc1": 12, "dc11": 21},
			oldTargetPeersCount:         map[string]int{"dc2": 6, "dc21": 15},
			newTargetPeersCount:         map[string]int{"dc2": 6, "dc21": 16},
			expectedOldOriginHostsCount: map[string]int{"dc11": 20},
			expectedNewOriginHostsCount: map[string]int{"dc11": 21},
			expectedOldTargetHostsCount: map[string]int{"dc21": 15},
			expectedNewTargetHostsCount: map[string]int{"dc21": 16},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := setup.NewTestConfig("127.0.1.1", "127.0.1.2")
			conf.OriginLocalDatacenter = tt.originDcConf
			conf.TargetLocalDatacenter = tt.targetDcConf

			originRegisterMessages := make([]*message.Register, 0)
			originRegisterLock := &sync.Mutex{}
			targetRegisterMessages := make([]*message.Register, 0)
			targetRegisterLock := &sync.Mutex{}

			originHandler := &atomic.Value{}
			originHandler.Store(newRefreshTopologyTestHandler("cluster1", tt.oldOriginLocalHostDc, "127.0.1.1", tt.oldOriginPeersCount))

			targetHandler := &atomic.Value{}
			targetHandler.Store(newRefreshTopologyTestHandler("cluster2", tt.oldTargetLocalHostDc, "127.0.1.2", tt.oldTargetPeersCount))

			createMutableHandler := func(handler *atomic.Value) func(request *frame.Frame, conn *client.CqlServerConnection, ctx client.RequestHandlerContext) (response *frame.Frame) {
				return func(request *frame.Frame, conn *client.CqlServerConnection, ctx client.RequestHandlerContext) (response *frame.Frame) {
					return handler.Load().(client.RequestHandler)(request, conn, ctx)
				}
			}

			testSetup, err := setup.NewCqlServerTestSetup(t, conf, false, false, false)
			require.Nil(t, err)
			defer testSetup.Cleanup()
			testSetup.Origin.CqlServer.RequestHandlers = []client.RequestHandler{
				newRegisterHandler(&originRegisterMessages, originRegisterLock), createMutableHandler(originHandler)}
			testSetup.Target.CqlServer.RequestHandlers = []client.RequestHandler{
				newRegisterHandler(&targetRegisterMessages, targetRegisterLock), createMutableHandler(targetHandler)}
			err = testSetup.Start(conf, false, primitive.ProtocolVersion4)
			require.Nil(t, err)
			checkRegisterMessages(t, originRegisterMessages, originRegisterLock)
			checkRegisterMessages(t, targetRegisterMessages, targetRegisterLock)
			proxy := testSetup.Proxy
			beforeSleepTestFunc(t, proxy.GetOriginControlConn(), originHandler, "cluster1", tt.oldOriginLocalHostDc, tt.newOriginLocalHostDc, "127.0.1.1", tt.oldOriginPeersCount, tt.newOriginPeersCount, tt.expectedOldOriginHostsCount)
			beforeSleepTestFunc(t, proxy.GetTargetControlConn(), targetHandler, "cluster2", tt.oldTargetLocalHostDc, tt.newTargetLocalHostDc, "127.0.1.2", tt.oldTargetPeersCount, tt.newTargetPeersCount, tt.expectedOldTargetHostsCount)
			afterSleepTestFunc(t, proxy.GetOriginControlConn(), originHandler, testSetup.Origin.CqlServer, "cluster1", tt.oldOriginLocalHostDc, tt.newOriginLocalHostDc, "127.0.1.1", tt.oldOriginPeersCount, tt.newOriginPeersCount, tt.expectedOldOriginHostsCount, tt.expectedNewOriginHostsCount)
			afterSleepTestFunc(t, proxy.GetTargetControlConn(), targetHandler, testSetup.Target.CqlServer, "cluster2", tt.oldTargetLocalHostDc, tt.newTargetLocalHostDc, "127.0.1.2", tt.oldTargetPeersCount, tt.newTargetPeersCount, tt.expectedOldTargetHostsCount, tt.expectedNewTargetHostsCount)
			checkRegisterMessages(t, originRegisterMessages, originRegisterLock)
			checkRegisterMessages(t, targetRegisterMessages, targetRegisterLock)
		})
	}
}

func checkRegisterMessages(t *testing.T, registerMessages []*message.Register, lock *sync.Mutex) {
	lock.Lock()
	defer lock.Unlock()
	require.Equal(t, 1, len(registerMessages))
	registerMsg := registerMessages[0]
	require.Equal(t, []primitive.EventType{primitive.EventTypeTopologyChange}, registerMsg.EventTypes)
}

func groupHostsPerDc(hosts []*zdmproxy.Host) map[string][]*zdmproxy.Host {
	hostsPerDc := make(map[string][]*zdmproxy.Host)
	for _, h := range hosts {
		hostsPerDc[h.Datacenter] = append(hostsPerDc[h.Datacenter], h)
	}
	return hostsPerDc
}

func newRefreshTopologyTestHandler(cluster string, localHostDc string, peersIpPrefix string, peersCount map[string]int) client.RequestHandler {
	return client.NewCompositeRequestHandler(
		client.HeartbeatHandler,
		client.HandshakeHandler,
		client.NewSetKeyspaceHandler(func(_ string) {}),
		client.RegisterHandler,
		NewCustomSystemTablesHandler(cluster, localHostDc, peersIpPrefix, peersCount, ""),
	)
}

// Creates a new RequestHandler to handle queries to system tables (system.local and system.peers).
func newRegisterHandler(registerMessages *[]*message.Register, lock *sync.Mutex) client.RequestHandler {
	return func(request *frame.Frame, conn *client.CqlServerConnection, handlerCtx client.RequestHandlerContext) (response *frame.Frame) {
		if register, ok := request.Body.Message.(*message.Register); ok {
			lock.Lock()
			*registerMessages = append(*registerMessages, register)
			lock.Unlock()
			log.Debugf("%v: [custom register handler]: received REGISTER: %v", conn, register.EventTypes)
			return frame.NewFrame(request.Header.Version, request.Header.StreamId, &message.Ready{})
		}
		return nil
	}
}
