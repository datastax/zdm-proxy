package cloudgateproxy

import (
	"context"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/google/uuid"
	"github.com/jpillora/backoff"
	"github.com/riptano/cloud-gate/proxy/pkg/config"
	log "github.com/sirupsen/logrus"
	"math"
	"math/big"
	"math/rand"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ControlConn struct {
	conf                     *config.Config
	topologyConfig           *config.TopologyConfig
	cqlConn                  CqlConnection
	retryBackoffPolicy       *backoff.Backoff
	heartbeatPeriod          time.Duration
	context                  context.Context
	defaultPort              int
	connConfig               ConnectionConfig
	currentContactPoint      Endpoint
	username                 string
	password                 string
	counterLock              *sync.RWMutex
	consecutiveFailures      int
	OpenConnectionTimeout    time.Duration
	cqlConnLock              *sync.Mutex
	genericTypeCodec         *GenericTypeCodec
	topologyLock             *sync.RWMutex
	datacenter               string
	hostsInLocalDc           []*Host
	assignedHosts            []*Host
	currentAssignment        int64
	refreshHostsDebouncer    chan bool
	systemLocalInfo          *systemLocalInfo
	preferredIpColumnExists  bool
	virtualHosts             []*VirtualHost
	proxyRand                *rand.Rand
}

const ProxyVirtualRack = "rack0"
const ccProtocolVersion = primitive.ProtocolVersion3
const ccWriteTimeout = 5 * time.Second
const ccReadTimeout = 10 * time.Second

func NewControlConn(ctx context.Context, defaultPort int, connConfig ConnectionConfig,
	username string, password string, conf *config.Config, topologyConfig *config.TopologyConfig, proxyRand *rand.Rand) *ControlConn {
	return &ControlConn{
		conf:           conf,
		topologyConfig: topologyConfig,
		cqlConn:        nil,
		retryBackoffPolicy: &backoff.Backoff{
			Factor: conf.HeartbeatRetryBackoffFactor,
			Jitter: true,
			Min:    time.Duration(conf.HeartbeatRetryIntervalMinMs) * time.Millisecond,
			Max:    time.Duration(conf.HeartbeatRetryIntervalMaxMs) * time.Millisecond,
		},
		heartbeatPeriod:          time.Duration(conf.HeartbeatIntervalMs) * time.Millisecond,
		context:                  ctx,
		defaultPort:              defaultPort,
		connConfig:               connConfig,
		currentContactPoint:      nil,
		username:                 username,
		password:                 password,
		counterLock:              &sync.RWMutex{},
		consecutiveFailures:      0,
		OpenConnectionTimeout:    time.Duration(conf.ClusterConnectionTimeoutMs) * time.Millisecond,
		cqlConnLock:              &sync.Mutex{},
		genericTypeCodec:         NewDefaultGenericTypeCodec(ccProtocolVersion),
		topologyLock:             &sync.RWMutex{},
		hostsInLocalDc:           nil,
		assignedHosts:            nil,
		currentAssignment:        0,
		refreshHostsDebouncer:    make(chan bool, 1),
		systemLocalInfo:          nil,
		preferredIpColumnExists:  false,
		virtualHosts:             nil,
		proxyRand:                proxyRand,
	}
}

func (cc *ControlConn) Start(wg *sync.WaitGroup, ctx context.Context) error {
	_, err := cc.Open(true, ctx)
	if err != nil {
		return err
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer log.Infof("Shutting down refresh topology debouncer of control connection %v.", cc.connConfig.GetClusterType())
		for ; cc.context.Err() == nil; {
			select {
			case <-cc.context.Done():
				return
			case <-cc.refreshHostsDebouncer:
			}

			conn, _ := cc.getConnAndContactPoint()
			if conn == nil {
				log.Debugf("Topology refresh scheduled but no connection available.")
				continue
			}

			_, err = cc.RefreshHosts(conn)
			if err != nil {
				log.Warnf("Error refreshing topology (triggered by event): %v", err)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cc.Close()
		defer log.Infof("Shutting down control connection to %v,", cc.connConfig.GetClusterType())
		lastOpenSuccessful := true
		for cc.context.Err() == nil {
			conn, _ := cc.getConnAndContactPoint()
			if conn == nil {
				if !lastOpenSuccessful {
					log.Infof("Refreshing contact points and reopening control connection to %v.", cc.connConfig.GetClusterType())
					_, err = cc.connConfig.RefreshContactPoints()
					if err != nil {
						log.Warnf("Failed to refresh contact points, reopening control connection to %v with old contact points.", cc.connConfig.GetClusterType())
					}
				} else {
					log.Infof("Reopening control connection to %v.", cc.connConfig.GetClusterType())
				}
				newConn, err := cc.Open(false, nil)
				if err != nil {
					lastOpenSuccessful = false
					timeUntilRetry := cc.retryBackoffPolicy.Duration()
					log.Errorf("Failed to open control connection to %v, retrying in %v: %v",
						cc.connConfig.GetClusterType(), timeUntilRetry, err)
					cc.IncrementFailureCounter()
					sleepWithContext(timeUntilRetry, cc.context)
					continue
				} else {
					lastOpenSuccessful = true
					conn = newConn
					cc.ResetFailureCounter()
					cc.retryBackoffPolicy.Reset()
				}
			}

			err = conn.SendHeartbeat(cc.context)
			if cc.context.Err() != nil {
				continue
			}

			if err != nil {
				log.Warnf("Heartbeat failed on %v. Closing and opening a new connection: %v.", conn, err)
				cc.IncrementFailureCounter()
				cc.Close()
			} else {
				logMsg := "Heartbeat successful on %v, waiting %v until next heartbeat."
				if cc.ReadFailureCounter() != 0 {
					log.Infof(logMsg, conn, cc.heartbeatPeriod)
					cc.ResetFailureCounter()
				} else {
					log.Debugf(logMsg, conn, cc.heartbeatPeriod)
				}
				sleepWithContext(cc.heartbeatPeriod, cc.context)
			}
		}
	}()
	return nil
}

func (cc *ControlConn) IncrementFailureCounter() {
	cc.counterLock.Lock()
	defer cc.counterLock.Unlock()
	cc.consecutiveFailures++
	if cc.consecutiveFailures < 0 {
		cc.consecutiveFailures = math.MaxInt32
	}
}

func (cc *ControlConn) ResetFailureCounter() {
	cc.counterLock.Lock()
	defer cc.counterLock.Unlock()
	cc.consecutiveFailures = 0
}

func (cc *ControlConn) ReadFailureCounter() int {
	cc.counterLock.RLock()
	defer cc.counterLock.RUnlock()
	return cc.consecutiveFailures
}

func (cc *ControlConn) Open(start bool, ctx context.Context) (CqlConnection, error) {
	if ctx == nil {
		ctx = cc.context
	}

	oldConn, _ := cc.getConnAndContactPoint()

	var conn CqlConnection
	var endpoint Endpoint
	contactPoints := cc.connConfig.GetContactPoints()
	firstContactPointIndex := cc.proxyRand.Intn(len(contactPoints))
	for i := 0; i < len(contactPoints); i++ {
		currentIndex := (firstContactPointIndex + i) % len(contactPoints)
		endpoint = contactPoints[currentIndex]
		tcpConn, _, err := openConnection(cc.connConfig, endpoint, ctx, false)
		if err != nil {
			log.Warnf("Failed to open control connection to %v using endpoint %v: %v",
				cc.connConfig.GetClusterType(), endpoint.GetEndpointIdentifier(), err)
			continue
		}

		newConn := NewCqlConnection(tcpConn, cc.username, cc.password, ccReadTimeout, ccWriteTimeout)
		err = newConn.InitializeContext(ccProtocolVersion, ctx)
		if err == nil {
			_, err = cc.RefreshHosts(newConn)
			if err != nil && start == false {
				log.Warnf("Error occured while refreshing hosts of %v: %v. " +
					"Ignoring this error because the control connection was already initialized.",
					cc.connConfig.GetClusterType(), err)
				err = nil
			}
		}

		if err != nil {
			log.Warnf("Error while initializing a new cql connection for the control connection of %v: %v",
				cc.connConfig.GetClusterType(), err)
			err2 := newConn.Close()
			if err2 != nil {
				log.Errorf("Failed to close cql connection: %v", err2)
			}

			continue
		}

		conn = newConn
		log.Infof("Successfully opened control connection to %v using endpoint %v.",
			cc.connConfig.GetClusterType(), endpoint.String())
		break
	}

	if conn == nil {
		return nil, fmt.Errorf("could not open control connection to %v, tried endpoints: %v", cc.connConfig.GetClusterType(), contactPoints)
	}

	conn.SetEventHandler(func(f *frame.Frame) {
		switch f.Body.Message.(type) {
		case *message.TopologyChangeEvent:
			select {
			case cc.refreshHostsDebouncer <- true:
			default:
				log.Debugf("Discarding event %v in %v because a topology refresh is already scheduled.",
					cc.connConfig.GetClusterType(), f.Body.Message)
			}
		default:
			return
		}
	})

	conn, endpoint = cc.setConn(oldConn, conn, endpoint)
	return conn, nil
}

func (cc *ControlConn) Close() {
	cc.cqlConnLock.Lock()
	conn := cc.cqlConn
	cc.cqlConn = nil
	cc.cqlConnLock.Unlock()

	if conn != nil {
		err := conn.Close()
		if err != nil {
			log.Warnf("Failed to close connection (possible leaked connection): %v", err)
		}
	}
}

func (cc *ControlConn) RefreshHosts(conn CqlConnection) ([]*Host, error) {
	localQueryResult, err := conn.Query("SELECT * FROM system.local", cc.genericTypeCodec)
	if err != nil {
		return nil, fmt.Errorf("could not fetch information from system.local table: %w", err)
	}

	localInfo, localHost, err := ParseSystemLocalResult(localQueryResult, cc.defaultPort)
	if err != nil {
		return nil, err
	}

	partitioner := localInfo.partitioner.AsNillableString()
	if partitioner != nil && !strings.Contains(*partitioner, "Murmur3Partitioner") && cc.topologyConfig.VirtualizationEnabled {
		return nil, fmt.Errorf("virtualization is enabled and partitioner is not Murmur3 but instead %v", *partitioner)
	}

	peersQuery, err := conn.Query("SELECT * FROM system.peers", cc.genericTypeCodec)
	if err != nil {
		return nil, fmt.Errorf("could not fetch information from system.peers table: %w", err)
	}

	orderedLocalHosts, preferredIpExists := ParseSystemPeersResult(peersQuery, cc.defaultPort, false)

	orderedLocalHosts = append([]*Host{localHost}, orderedLocalHosts...)

	cc.topologyLock.RLock()
	currentDc := cc.datacenter
	cc.topologyLock.RUnlock()

	orderedLocalHosts, currentDc, err = filterHosts(orderedLocalHosts, currentDc, cc.connConfig, localHost)
	if err != nil {
		return nil, err
	}

	sort.Slice(orderedLocalHosts, func(i, j int) bool {
		if orderedLocalHosts[i].Rack == orderedLocalHosts[j].Rack {
			return orderedLocalHosts[i].HostId.String() < orderedLocalHosts[j].HostId.String()
		}

		return orderedLocalHosts[i].Rack < orderedLocalHosts[j].Rack
	})

	assignedHosts := computeAssignedHosts(cc.topologyConfig.Index, cc.topologyConfig.Count, orderedLocalHosts)
	shuffleHosts(cc.proxyRand, assignedHosts)

	var virtualHosts []*VirtualHost
	if cc.topologyConfig.VirtualizationEnabled {
		virtualHosts, err = computeVirtualHosts(cc.topologyConfig, orderedLocalHosts)
		if err != nil {
			return nil, err
		}
	} else {
		virtualHosts = make([]*VirtualHost, 0)
	}

	log.Infof("Refreshed %v hostsInLocalDc. Assigned Hosts: %v, VirtualHosts: %v, ProxyIndex: %v",
		cc.connConfig.GetClusterType(), assignedHosts, virtualHosts, cc.topologyConfig.Index)

	cc.topologyLock.Lock()
	if cc.datacenter == "" {
		cc.datacenter = currentDc
	}
	cc.hostsInLocalDc = orderedLocalHosts
	cc.assignedHosts = assignedHosts
	cc.systemLocalInfo = localInfo
	cc.preferredIpColumnExists = preferredIpExists
	cc.virtualHosts = virtualHosts
	cc.topologyLock.Unlock()

	return orderedLocalHosts, nil
}

func (cc *ControlConn) GetHostsInLocalDatacenter() ([]*Host, error) {
	cc.topologyLock.RLock()
	defer cc.topologyLock.RUnlock()

	if cc.hostsInLocalDc == nil {
		return nil, fmt.Errorf("could not get hostsInLocalDc because topology information has not been retrieved yet")
	}

	return cc.hostsInLocalDc, nil
}

func (cc *ControlConn) GetVirtualHosts() ([]*VirtualHost, error) {
	cc.topologyLock.RLock()
	defer cc.topologyLock.RUnlock()

	if !cc.topologyConfig.VirtualizationEnabled {
		return nil, fmt.Errorf("could not get virtual hosts because virtualization is not enabled")
	}

	if cc.virtualHosts == nil {
		return nil, fmt.Errorf("could not get virtual hosts because topology information has not been retrieved yet")
	}

	return cc.virtualHosts, nil
}

func (cc *ControlConn) GetLocalVirtualHostIndex() int {
	return cc.topologyConfig.Index
}

func (cc *ControlConn) GetAssignedHosts() ([]*Host, error) {
	cc.topologyLock.RLock()
	defer cc.topologyLock.RUnlock()

	if cc.assignedHosts == nil {
		return nil, fmt.Errorf("could not get assigned hosts because topology information has not been retrieved yet")
	}

	return cc.assignedHosts, nil
}

func (cc *ControlConn) NextAssignedHost() (*Host, error) {
	cc.topologyLock.RLock()
	defer cc.topologyLock.RUnlock()

	if cc.assignedHosts == nil {
		return nil, fmt.Errorf("could not get assigned hosts because topology information has not been retrieved yet")
	}

	assignment := cc.incCurrentAssignmentCounter(len(cc.assignedHosts))

	return cc.assignedHosts[assignment], nil
}

func (cc *ControlConn) GetClusterName() string {
	cc.topologyLock.RLock()
	defer cc.topologyLock.RUnlock()

	clusterName := cc.systemLocalInfo.clusterName.AsNillableString()
	if clusterName == nil {
		return ""
	}

	return *clusterName
}

func (cc *ControlConn) PreferredIpColumnExists() bool {
	cc.topologyLock.RLock()
	defer cc.topologyLock.RUnlock()

	return cc.preferredIpColumnExists
}

func (cc *ControlConn) GetSystemLocalInfo() *systemLocalInfo {
	cc.topologyLock.RLock()
	defer cc.topologyLock.RUnlock()

	return cc.systemLocalInfo
}

func (cc *ControlConn) GetGenericTypeCodec() *GenericTypeCodec {
	cc.topologyLock.RLock()
	defer cc.topologyLock.RUnlock()

	return cc.genericTypeCodec
}

func (cc *ControlConn) GetCurrentContactPoint() Endpoint {
	cc.cqlConnLock.Lock()
	contactPoint := cc.currentContactPoint
	defer cc.cqlConnLock.Unlock()
	return contactPoint
}

func (cc *ControlConn) setConn(oldConn CqlConnection, newConn CqlConnection, newContactPoint Endpoint) (CqlConnection, Endpoint) {
	cc.cqlConnLock.Lock()
	defer cc.cqlConnLock.Unlock()
	if cc.cqlConn == oldConn || oldConn == nil {
		cc.cqlConn = newConn
		cc.currentContactPoint = newContactPoint
		return newConn, newContactPoint
	}

	if newConn != nil {
		log.Infof("Another control connection attempt to %v was successful in parallel, closing this connection (%v).",
			cc.connConfig.GetClusterType(), newContactPoint.String())
		err := newConn.Close()
		if err != nil {
			log.Errorf("Failed to close cql connection: %v", err)
		}
	}

	return cc.cqlConn, cc.currentContactPoint
}

func (cc *ControlConn) getConnAndContactPoint() (CqlConnection, Endpoint) {
	cc.cqlConnLock.Lock()
	conn := cc.cqlConn
	contactPoint := cc.currentContactPoint
	defer cc.cqlConnLock.Unlock()
	return conn, contactPoint
}

// should be called with a read or write lock on topologyLock
func (cc *ControlConn) getCurrentAssignmentCounter(assignedHostsLength int) int64 {
	return atomic.LoadInt64(&cc.currentAssignment) % int64(assignedHostsLength)
}

// should be called with a read or write lock on topologyLock
func (cc *ControlConn) incCurrentAssignmentCounter(assignedHostsLength int) int64 {
	value := atomic.AddInt64(&cc.currentAssignment, 1) % int64(assignedHostsLength)
	if value == 0 {
		atomic.AddInt64(&cc.currentAssignment, int64(-assignedHostsLength))
	}
	return value
}

func computeAssignedHosts(index int, count int, orderedHosts []*Host) []*Host {
	i := 0
	assignedHosts := make([]*Host, 0)
	hostsCount := len(orderedHosts)
	for _, h := range orderedHosts {
		if i == (index % hostsCount) {
			assignedHosts = append(assignedHosts, h)
		}

		i = (i + 1) % count
	}

	return assignedHosts
}

func shuffleHosts(rnd *rand.Rand, hosts []*Host) {
	rnd.Shuffle(len(hosts), func(i, j int) {
		temp := hosts[i]
		hosts[i] = hosts[j]
		hosts[j] = temp
	})
}

func computeVirtualHosts(topologyConfig *config.TopologyConfig, orderedHosts []*Host) ([]*VirtualHost, error) {
	proxyAddresses := topologyConfig.Addresses
	numTokens := topologyConfig.NumTokens
	twoPow64 := new(big.Int).Exp(big.NewInt(2), big.NewInt(64), nil)
	twoPow63 := new(big.Int).Exp(big.NewInt(2), big.NewInt(63), nil)
	proxyAddressesCount := len(proxyAddresses)
	proxyAddressesCountBig := big.NewInt(int64(proxyAddressesCount))
	assignedHostsForVirtualization := computeAssignedHostsForVirtualization(proxyAddressesCount, orderedHosts)
	virtualHosts := make([]*VirtualHost, proxyAddressesCount)
	numTokensBig := big.NewInt(int64(numTokens))
	for i := 0; i < proxyAddressesCount; i++ {
		tokens := make([]string, numTokens)
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
			token := fmt.Sprintf("%d", tokenInt)
			tokens[t] = token
		}
		hostId := uuid.NewSHA1(uuid.Nil, proxyAddresses[i])
		primitiveHostId, err := primitive.ParseUuid(hostId.String())
		if err != nil {
			return nil, fmt.Errorf("could not compute virtual hosts due to proxy host id parsing error: %w", err)
		}

		host := assignedHostsForVirtualization[i]
		dc := topologyConfig.VirtualDatacenter
		if dc == "" {
			dc = host.Datacenter
		}

		virtualHosts[i] = &VirtualHost{
			Tokens:     tokens,
			Addr:       proxyAddresses[i],
			Host:       host,
			HostId:     primitiveHostId,
			Datacenter: dc,
			Rack:       ProxyVirtualRack,
		}
	}
	return virtualHosts, nil
}

func filterHostsByDatacenter(datacenter string, hosts []*Host) []*Host {
	filteredHosts := make([]*Host, 0, len(hosts))
	for _, h := range hosts {
		if h.Datacenter == datacenter {
			filteredHosts = append(filteredHosts, h)
		}
	}
	return filteredHosts
}

func filterHosts(hosts []*Host, currentDc string, connConfig ConnectionConfig, localHost *Host) ([]*Host, string, error) {
	if currentDc != "" {
		filteredOrderedHosts := filterHostsByDatacenter(currentDc, hosts)
		if len(filteredOrderedHosts) == 0 {
			return nil, "", fmt.Errorf("current DC was already set to '%v' but no hosts with this DC were found: %v",
				currentDc, hosts)
		}
		return filteredOrderedHosts, currentDc, nil
	}

	datacenter := connConfig.GetLocalDatacenter()
	if datacenter != "" {
		filteredOrderedHosts := filterHostsByDatacenter(datacenter, hosts)
		if len(filteredOrderedHosts) == 0 {
			log.Warnf("datacenter was set to '%v' but no hosts were found with that DC " +
				"so falling back to local host's DC '%v' (hosts=%v)",
				datacenter, localHost.Datacenter, hosts)
		} else {
			return filteredOrderedHosts, datacenter, nil
		}
	}

	filteredOrderedHosts := filterHostsByDatacenter(localHost.Datacenter, hosts)
	if len(filteredOrderedHosts) == 0 {
		return nil, "", fmt.Errorf("no hosts found with inferred local DC '%v': %v", localHost.Datacenter, hosts)
	}
	return filteredOrderedHosts, localHost.Datacenter, nil
}

func computeAssignedHostsForVirtualization(count int, orderedHosts []*Host) []*Host {
	assignedHostsForVirtualization := make([]*Host, count)
	hostsCount := len(orderedHosts)
	for i := 0; i < count; i++ {
		assignedHostsForVirtualization[i] = orderedHosts[i%hostsCount]
	}

	return assignedHostsForVirtualization
}

type VirtualHost struct {
	Tokens     []string
	Addr       net.IP
	Host       *Host
	HostId     *primitive.UUID
	Datacenter string
	Rack       string
}

func (recv *VirtualHost) String() string {
	return fmt.Sprintf("VirtualHost{addr: %v, host_id: %v, datacenter: %v, rack: %v, tokens: %v, host: %v}",
		recv.Addr,
		recv.HostId,
		recv.Datacenter,
		recv.Rack,
		recv.Tokens,
		recv.Host)
}