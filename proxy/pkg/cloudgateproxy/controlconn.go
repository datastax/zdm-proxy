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
	virtualConfig            *config.TopologyConfig
	cqlConn                  CqlConnection
	retryBackoffPolicy       *backoff.Backoff
	heartbeatPeriod          time.Duration
	context                  context.Context
	defaultPort              int
	connConfig               *ConnectionConfig
	contactPoints            []Endpoint
	currentContactPoint      Endpoint
	currentContactPointIndex int
	username                 string
	password                 string
	counterLock              *sync.RWMutex
	consecutiveFailures      int
	OpenConnectionTimeout    time.Duration
	cqlConnLock              *sync.Mutex
	genericTypeCodec         *GenericTypeCodec
	topologyLock             *sync.RWMutex
	hosts                    []*Host
	assignedHosts            []*Host
	currentAssignment        int64
	refreshHostsDebouncer    chan bool
	systemLocalInfo          *systemLocalInfo
	preferredIpColumnExists  bool
	virtualHosts             []*VirtualHost
	proxyRand                *rand.Rand
}

const ccProtocolVersion = primitive.ProtocolVersion3
const ccWriteTimeout = 5 * time.Second
const ccReadTimeout = 10 * time.Second

func NewControlConn(ctx context.Context, defaultPort int, connConfig *ConnectionConfig, contactPoints []Endpoint,
	username string, password string, conf *config.Config, virtualConf *config.TopologyConfig, proxyRand *rand.Rand) *ControlConn {
	return &ControlConn{
		conf:          conf,
		virtualConfig: virtualConf,
		cqlConn:       nil,
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
		contactPoints:            contactPoints,
		currentContactPoint:      nil,
		currentContactPointIndex: 0,
		username:                 username,
		password:                 password,
		counterLock:              &sync.RWMutex{},
		consecutiveFailures:      0,
		OpenConnectionTimeout:    time.Duration(conf.ClusterConnectionTimeoutMs) * time.Millisecond,
		cqlConnLock:              &sync.Mutex{},
		genericTypeCodec:         NewDefaultGenericTypeCodec(ccProtocolVersion),
		topologyLock:             &sync.RWMutex{},
		hosts:                    nil,
		assignedHosts:            nil,
		currentAssignment:        0,
		refreshHostsDebouncer:    make(chan bool, 1),
		systemLocalInfo:          nil,
		preferredIpColumnExists:  false,
		virtualHosts:             nil,
		proxyRand:                proxyRand,
	}
}

func (cc *ControlConn) Start(wg *sync.WaitGroup, ctx context.Context, firstContactPointIndex int) error {
	connectionEstablished := false

	for i := 0; i < len(cc.contactPoints); i++ {
		currentIndex := (firstContactPointIndex + i) % len(cc.contactPoints)
		endpoint := cc.setCurrentContactPoint(currentIndex)
		conn, _, err := openConnection(cc.connConfig, endpoint, ctx, false)
		if err != nil || conn == nil {
			// could not establish connection using this endpoint, try the next one
			log.Warnf("Could not establish a control connection to endpoint %v due to %v, trying the next contact point if available.", endpoint.GetEndpointIdentifier(), err)
			continue
		}

		cqlConn := NewCqlConnection(conn, cc.username, cc.password, ccReadTimeout, ccWriteTimeout)
		newConn, newContactPoint, err := cc.Open(cqlConn, endpoint, ctx)
		if err != nil {
			connCloseErr := conn.Close()
			if connCloseErr != nil {
				log.Warnf("Failed to close connection after failure to start control connection: %v", connCloseErr)
			}

			log.Warnf("Could not establish a control connection to endpoint %v due to %v, "+
				"trying the next contact point if available.", endpoint.GetEndpointIdentifier(), err)
			continue
		} else {
			// connection established, no need to try any remaining endpoints
			cc.setConn(nil, newConn, newContactPoint)
			connectionEstablished = true
			log.Debugf(
				"Control connection (%v) successfully established to %v.",
				cc.connConfig.clusterType, newContactPoint.GetEndpointIdentifier())
			break
		}
	}

	if !connectionEstablished {
		return fmt.Errorf("could not connect to any of the endpoints provided (tried %v)", cc.contactPoints)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer log.Infof("Shutting down refresh topology debouncer of control connection %v.", cc.connConfig.clusterType)
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

			_, err := cc.RefreshHosts(conn)
			if err != nil {
				log.Warnf("Error refreshing topology (triggered by event): %v", err)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cc.Close()
		defer log.Infof("Shutting down control connection to %v,", cc.connConfig.clusterType)
		for cc.context.Err() == nil {
			conn, contactPoint := cc.getConnAndContactPoint()
			if conn == nil || !conn.IsInitialized() {
				log.Infof("Reopening control connection to %v.", cc.connConfig.clusterType)
				newConn, newContactPoint, err := cc.Open(conn, contactPoint, nil)
				if err != nil {
					cc.setConn(conn, nil, nil)
					timeUntilRetry := cc.retryBackoffPolicy.Duration()
					log.Warnf("Failed to open control connection to %v, retrying in %v: %v", cc.connConfig.clusterType, timeUntilRetry, err)
					cc.IncrementFailureCounter()
					sleepWithContext(timeUntilRetry, cc.context)
					continue
				} else {
					if !cc.setConn(conn, newConn, newContactPoint) {
						log.Infof("Failed to set the new control connection, race condition?")
						timeUntilRetry := cc.retryBackoffPolicy.Duration()
						sleepWithContext(timeUntilRetry, cc.context)
						continue
					}
					conn = newConn
					contactPoint = newContactPoint
					log.Infof("Control connection to %v opened successfully using endpoint %v.", cc.connConfig.clusterType, newContactPoint.GetEndpointIdentifier())
					cc.ResetFailureCounter()
					cc.retryBackoffPolicy.Reset()
				}
			}

			err := conn.SendHeartbeat(cc.context)
			action := success
			if err != nil {
				action = failure
			}

			if cc.context.Err() != nil {
				continue
			}

			switch action {
			case fatalFailure:
				log.Errorf("Closing control connection to %v and will NOT attempt to re-open it due to a fatal failure: %v", cc.connConfig.clusterType, err)
				cc.Close()
				return
			case failure:
				log.Warnf("Heartbeat failed on %v. Closing and opening a new connection: %v.", conn, err)
				cc.IncrementFailureCounter()
				cc.Close()
			case success:
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

type heartbeatResultAction int

const (
	failure      = heartbeatResultAction(2)
	success      = heartbeatResultAction(3)
	fatalFailure = heartbeatResultAction(4)
)

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

func (cc *ControlConn) Open(conn CqlConnection, contactPoint Endpoint, ctx context.Context) (CqlConnection, Endpoint, error) {
	if ctx == nil {
		ctx = cc.context
	}

	if conn == nil {
		contactPoint = cc.moveToNextContactPoint()
		tcpConn, _, err := openConnection(cc.connConfig, contactPoint, cc.context, false)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to open connection to %v: %w", contactPoint.GetEndpointIdentifier(), err)
		}
		conn = NewCqlConnection(tcpConn, cc.username, cc.password, ccReadTimeout, ccWriteTimeout)
	}

	err := conn.InitializeContext(ccProtocolVersion, ctx)
	if err == nil {
		_, err = cc.RefreshHosts(conn)
	}

	conn.SetEventHandler(func(f *frame.Frame) {
		switch f.Body.Message.(type) {
		case *message.TopologyChangeEvent:
			select {
			case cc.refreshHostsDebouncer <- true:
			default:
				log.Debugf("Discarding event %v because a topology refresh is already scheduled.", f.Body.Message)
			}
		default:
			return
		}
	})

	if err != nil {
		log.Infof("Error while opening control connection, triggering shutdown of connection: %v", err)
		err2 := conn.Close()
		if err2 != nil {
			log.Warnf("Failed to close cql connection: %v", err2)
		}

		return nil, nil, fmt.Errorf("cql connection initialization failure: %w", err)
	}

	return conn, contactPoint, nil
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
	if partitioner != nil && !strings.Contains(*partitioner, "Murmur3Partitioner") && cc.virtualConfig.VirtualizationEnabled {
		return nil, fmt.Errorf("virtualization is enabled and partitioner is not Murmur3 but instead %v", *partitioner)
	}

	peersQuery, err := conn.Query("SELECT * FROM system.peers", cc.genericTypeCodec)
	if err != nil {
		return nil, fmt.Errorf("could not fetch information from system.peers table: %w", err)
	}

	orderedHosts, preferredIpExists := ParseSystemPeersResult(peersQuery, cc.defaultPort, false)

	orderedHosts = append([]*Host{localHost}, orderedHosts...)
	sort.Slice(orderedHosts, func(i, j int) bool {
		if orderedHosts[i].Rack == orderedHosts[j].Rack {
			return orderedHosts[i].HostId.String() < orderedHosts[j].HostId.String()
		}

		return orderedHosts[i].Rack < orderedHosts[j].Rack
	})

	assignedHosts := computeAssignedHosts(cc.virtualConfig.Index, cc.virtualConfig.Count, orderedHosts)
	shuffleHosts(cc.proxyRand, assignedHosts)

	var virtualHosts []*VirtualHost
	if cc.virtualConfig.VirtualizationEnabled {
		virtualHosts, err = computeVirtualHosts(cc.virtualConfig.Addresses, orderedHosts, cc.virtualConfig.NumTokens)
		if err != nil {
			return nil, err
		}
	} else {
		virtualHosts = make([]*VirtualHost, 0)
	}

	log.Infof("Refreshed %v hosts. Assigned Hosts: %v, VirtualHosts: %v, ProxyIndex: %v",
		cc.connConfig.clusterType, assignedHosts, virtualHosts, cc.virtualConfig.Index)

	cc.topologyLock.Lock()
	cc.hosts = orderedHosts
	cc.assignedHosts = assignedHosts
	cc.systemLocalInfo = localInfo
	cc.preferredIpColumnExists = preferredIpExists
	cc.virtualHosts = virtualHosts
	cc.topologyLock.Unlock()

	return orderedHosts, nil
}

func (cc *ControlConn) GetHosts() ([]*Host, error) {
	cc.topologyLock.RLock()
	defer cc.topologyLock.RUnlock()

	if cc.hosts == nil {
		return nil, fmt.Errorf("could not get hosts because topology information has not been retrieved yet")
	}

	return cc.hosts, nil
}

func (cc *ControlConn) GetVirtualHosts() ([]*VirtualHost, error) {
	cc.topologyLock.RLock()
	defer cc.topologyLock.RUnlock()

	if !cc.virtualConfig.VirtualizationEnabled {
		return nil, fmt.Errorf("could not get virtual hosts because virtualization is not enabled")
	}

	if cc.virtualHosts == nil {
		return nil, fmt.Errorf("could not get virtual hosts because topology information has not been retrieved yet")
	}

	return cc.virtualHosts, nil
}

func (cc *ControlConn) GetLocalVirtualHostIndex() int {
	return cc.virtualConfig.Index
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

func (cc *ControlConn) setConn(oldConn CqlConnection, newConn CqlConnection, newContactPoint Endpoint) bool {
	cc.cqlConnLock.Lock()
	defer cc.cqlConnLock.Unlock()
	if cc.cqlConn == oldConn {
		cc.cqlConn = newConn
		cc.currentContactPoint = newContactPoint
		return true
	}

	return false
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

func (cc *ControlConn) moveToNextContactPoint() Endpoint {
	cc.cqlConnLock.Lock()
	defer cc.cqlConnLock.Unlock()
	cc.currentContactPointIndex = (cc.currentContactPointIndex + 1) % len(cc.contactPoints)
	return cc.contactPoints[cc.currentContactPointIndex]
}

func (cc *ControlConn) setCurrentContactPoint(index int) Endpoint {
	cc.cqlConnLock.Lock()
	defer cc.cqlConnLock.Unlock()
	cc.currentContactPointIndex = index % len(cc.contactPoints)
	return cc.contactPoints[cc.currentContactPointIndex]
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

func computeVirtualHosts(proxyAddresses []net.IP, orderedHosts []*Host, numTokens int) ([]*VirtualHost, error) {
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
		//tokenInt := int((maxUint65/uint64(proxyAddressesCount))*uint64(i) - twoPow63)
		hostId := uuid.NewSHA1(uuid.Nil, proxyAddresses[i])
		primitiveHostId, err := primitive.ParseUuid(hostId.String())
		if err != nil {
			return nil, fmt.Errorf("could not compute virtual hosts due to proxy host id parsing error: %w", err)
		}
		virtualHosts[i] = &VirtualHost{
			Tokens: tokens,
			Addr:   proxyAddresses[i],
			Host:   assignedHostsForVirtualization[i],
			HostId: primitiveHostId,
		}
	}
	return virtualHosts, nil
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
	Tokens []string
	Addr   net.IP
	Host   *Host
	HostId *primitive.UUID
}

func (recv *VirtualHost) String() string {
	return fmt.Sprintf("VirtualHost{addr: %v, host_id: %v, tokens: %v, host: %v}",
		recv.Addr,
		recv.HostId,
		recv.Tokens,
		recv.Host)
}