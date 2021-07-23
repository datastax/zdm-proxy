package integration_tests

import (
	"context"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/riptano/cloud-gate/integration-tests/env"
	"github.com/riptano/cloud-gate/integration-tests/setup"
	"github.com/riptano/cloud-gate/proxy/pkg/cloudgateproxy"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"math/big"
	"math/rand"
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
			name:                        "3 nodes, 3 instances, 3 virtual hosts, assignment enabled",
			proxyIndexes:                []int{0, 1, 2},
			proxyAddresses:              [][]string{
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
			name:                        "3 nodes, 4 instances, 4 virtual hosts, assignment enabled",
			proxyIndexes:                []int{0, 1, 2, 3},
			proxyAddresses:              [][]string{
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
			proxyAddresses:              [][]string{{"127.0.0.1"},{"127.0.0.2"},{"127.0.0.3"}},
			proxyInstanceCount:          3,
			proxyInstanceCreationCount:  3,
			hostAssignmentEnabledOrigin: true,
			hostAssignmentEnabledTarget: true,
			expectedConns:               []int{3, 0, 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proxies := make([]*cloudgateproxy.CloudgateProxy, tt.proxyInstanceCreationCount)
			for i := 0; i < tt.proxyInstanceCreationCount; i++ {
				proxies[i], err = LaunchProxyWithVirtualizationConfig(
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
					observedConnect, ok := <- observeChannel
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
				require.Equal(t, tt.expectedConns[i], hostsMap[fmt.Sprintf("%s%d", "127.0.0.", i + 1)], "hostsMap = %v", hostsMap)
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

	ctx, cancelFn := context.WithTimeout(context.Background(), 10 * time.Second)
	defer cancelFn()
	err = origin.GetSession().AwaitSchemaAgreement(ctx)
	ctx2, cancelFn2 := context.WithTimeout(context.Background(), 10 * time.Second)
	defer cancelFn2()
	err = origin.GetSession().AwaitSchemaAgreement(ctx2)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proxies := make([]*cloudgateproxy.CloudgateProxy, tt.proxyInstanceCreationCount)
			for i := 0; i < tt.proxyInstanceCreationCount; i++ {
				proxies[i], err = LaunchProxyWithVirtualizationConfig(
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

			queries := []query {
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

func LaunchProxyWithVirtualizationConfig(
	proxyAddresses string, proxyIndex int, instanceCount int, listenAddress string, numTokens int,
	origin setup.TestCluster, target setup.TestCluster) (*cloudgateproxy.CloudgateProxy, error) {
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