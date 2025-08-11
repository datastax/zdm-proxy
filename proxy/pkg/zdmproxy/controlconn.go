package zdmproxy

import (
	"context"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/zdm-proxy/proxy/pkg/common"
	"github.com/datastax/zdm-proxy/proxy/pkg/config"
	"github.com/datastax/zdm-proxy/proxy/pkg/metrics"
	"github.com/google/uuid"
	"github.com/jpillora/backoff"
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
	topologyConfig           *common.TopologyConfig
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
	topologyLock             *sync.RWMutex
	datacenter               string
	orderedHostsInLocalDc    []*Host
	hostsInLocalDcById       map[uuid.UUID]*Host
	assignedHosts            []*Host
	currentAssignment        int64
	refreshHostsDebouncer    chan CqlConnection
	systemLocalColumnData    map[string]*optionalColumn
	systemPeersColumnNames   map[string]bool
	virtualHosts             []*VirtualHost
	proxyRand                *rand.Rand
	reconnectCh              chan bool
	protocolEventSubscribers map[ProtocolEventObserver]interface{}
	authEnabled              *atomic.Value
	metricsHandler           *metrics.MetricHandler
	controlConnProtoVersion  *atomic.Value
	supportedResponse        *atomic.Value
}

const ProxyVirtualRack = "rack0"
const ProxyVirtualPartitioner = "org.apache.cassandra.dht.Murmur3Partitioner"
const ccWriteTimeout = 5 * time.Second
const ccReadTimeout = 10 * time.Second

func NewControlConn(ctx context.Context, defaultPort int, connConfig ConnectionConfig,
	username string, password string, conf *config.Config, topologyConfig *common.TopologyConfig, proxyRand *rand.Rand,
	metricsHandler *metrics.MetricHandler) *ControlConn {
	authEnabled := &atomic.Value{}
	authEnabled.Store(true)
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
		OpenConnectionTimeout:    time.Duration(connConfig.GetConnectionTimeoutMs()) * time.Millisecond,
		cqlConnLock:              &sync.Mutex{},
		topologyLock:             &sync.RWMutex{},
		orderedHostsInLocalDc:    nil,
		hostsInLocalDcById:       map[uuid.UUID]*Host{},
		assignedHosts:            nil,
		currentAssignment:        0,
		refreshHostsDebouncer:    make(chan CqlConnection, 1),
		systemLocalColumnData:    nil,
		systemPeersColumnNames:   nil,
		virtualHosts:             nil,
		proxyRand:                proxyRand,
		reconnectCh:              make(chan bool, 1),
		protocolEventSubscribers: map[ProtocolEventObserver]interface{}{},
		authEnabled:              authEnabled,
		metricsHandler:           metricsHandler,
		controlConnProtoVersion:  &atomic.Value{},
		supportedResponse:        &atomic.Value{},
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
		for cc.context.Err() == nil {
			var eventConnection CqlConnection
			select {
			case <-cc.context.Done():
				return
			case eventConnection = <-cc.refreshHostsDebouncer:
			}

			log.Infof("Received topology event from %v, refreshing topology.", cc.connConfig.GetClusterType())

			conn, _ := cc.GetConnAndContactPoint()
			if conn == nil {
				log.Debugf("Topology refresh scheduled but the control connection isn't open. " +
					"Falling back to the connection where the event was received.")
				conn = eventConnection
			}

			_, err = cc.RefreshHosts(conn, cc.context)
			if err != nil && cc.context.Err() == nil {
				log.Errorf("Error refreshing topology (triggered by event), triggering reconnection: %v", err)
				select {
				case cc.reconnectCh <- true:
				default:
				}
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cc.Close()
		defer log.Infof("Shutting down control connection to %v,", cc.connConfig.GetClusterType())
		lastOpenSuccessful := true
		reconnect := false
		for cc.context.Err() == nil {
			select {
			case <-cc.reconnectCh:
				reconnect = true
			default:
			}

			if reconnect {
				reconnect = false
				cc.Close()
			}

			conn, _ := cc.GetConnAndContactPoint()
			if conn == nil {
				useContactPointsOnly := false
				if !lastOpenSuccessful {
					useContactPointsOnly = true
					log.Infof("Refreshing contact points and reopening control connection to %v.", cc.connConfig.GetClusterType())
					_, err = cc.connConfig.RefreshContactPoints(cc.context)
					if err != nil {
						log.Warnf("Failed to refresh contact points, reopening control connection to %v with old contact points.", cc.connConfig.GetClusterType())
						useContactPointsOnly = false
					}
				} else {
					log.Infof("Reopening control connection to %v.", cc.connConfig.GetClusterType())
				}
				newConn, err := cc.Open(useContactPointsOnly, cc.context)
				if cc.context.Err() != nil {
					continue
				}
				if err != nil {
					lastOpenSuccessful = false
					timeUntilRetry := cc.retryBackoffPolicy.Duration()
					log.Errorf("Failed to open control connection to %v, retrying in %v: %v",
						cc.connConfig.GetClusterType(), timeUntilRetry, err)
					cc.IncrementFailureCounter()
					sleepWithContext(timeUntilRetry, cc.context, nil)
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
				_, reconnect = sleepWithContext(cc.heartbeatPeriod, cc.context, cc.reconnectCh)
			}
		}
	}()
	return nil
}

func (cc *ControlConn) IsAuthEnabled() (bool, error) {
	if authEnabled := cc.authEnabled.Load(); authEnabled != nil {
		return authEnabled.(bool), nil
	}

	return true, fmt.Errorf("could not check whether auth is enabled or not because " +
		"the control connection has not been initialized")
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

func (cc *ControlConn) Open(contactPointsOnly bool, ctx context.Context) (CqlConnection, error) {
	oldConn, _ := cc.GetConnAndContactPoint()
	if oldConn != nil {
		cc.Close()
		oldConn = nil
	}

	var conn CqlConnection
	var endpoint Endpoint
	var triedEndpoints []Endpoint

	if contactPointsOnly {
		contactPoints := cc.connConfig.GetContactPoints()
		conn, endpoint = cc.openInternal(contactPoints, ctx)
		triedEndpoints = contactPoints
	} else {
		allEndpointsById := make(map[string]Endpoint)
		hostEndpoints := make([]Endpoint, 0)
		hosts, err := cc.GetHostsInLocalDatacenter()
		if err == nil {
			for _, h := range hosts {
				endpt := cc.connConfig.CreateEndpoint(h)
				allEndpointsById[endpt.GetEndpointIdentifier()] = endpt
				hostEndpoints = append(hostEndpoints, endpt)
			}
		}
		contactPointsNotInHosts := make([]Endpoint, 0)
		for _, contactPoint := range cc.connConfig.GetContactPoints() {
			if _, exists := allEndpointsById[contactPoint.GetEndpointIdentifier()]; !exists {
				allEndpointsById[contactPoint.GetEndpointIdentifier()] = contactPoint
				contactPointsNotInHosts = append(contactPointsNotInHosts, contactPoint)
			}
		}

		if len(hostEndpoints) > 0 {
			conn, endpoint = cc.openInternal(hostEndpoints, ctx)
			triedEndpoints = hostEndpoints
		}

		if conn == nil && len(contactPointsNotInHosts) > 0 {
			conn, endpoint = cc.openInternal(contactPointsNotInHosts, ctx)
			triedEndpoints = append(triedEndpoints, contactPointsNotInHosts...)
		}
	}

	if conn == nil {
		return nil, fmt.Errorf("could not open control connection to %v, tried endpoints: %v",
			cc.connConfig.GetClusterType(), triedEndpoints)
	}

	conn, endpoint = cc.setConn(oldConn, conn, endpoint)
	return conn, nil
}

func (cc *ControlConn) openInternal(endpoints []Endpoint, ctx context.Context) (CqlConnection, Endpoint) {
	if ctx == nil {
		ctx = cc.context
	}

	var conn CqlConnection
	var endpoint Endpoint

	firstEndpointIndex := cc.proxyRand.Intn(len(endpoints))
	for i := 0; i < len(endpoints); i++ {
		if ctx.Err() != nil {
			break
		}

		currentIndex := (firstEndpointIndex + i) % len(endpoints)
		endpoint = endpoints[currentIndex]

		maxProtoVer, _ := cc.conf.ParseControlConnMaxProtocolVersion()
		newConn, err := cc.connAndNegotiateProtoVer(endpoint, maxProtoVer, ctx)

		if err == nil {
			newConn.SetEventHandler(func(f *frame.Frame, c CqlConnection) {
				switch f.Body.Message.(type) {
				case *message.TopologyChangeEvent:
					select {
					case cc.refreshHostsDebouncer <- c:
					default:
						log.Debugf("Discarding event %v in %v because a topology refresh is already scheduled.",
							cc.connConfig.GetClusterType(), f.Body.Message)
					}
				default:
					return
				}
			})

			err = newConn.SubscribeToProtocolEvents(ctx, []primitive.EventType{primitive.EventTypeTopologyChange})
			if err == nil {
				_, err = cc.RefreshHosts(newConn, ctx)
			}
		}

		if err != nil {
			if ctx.Err() == nil {
				log.Warnf("Error while initializing a new cql connection for the control connection of %v: %v",
					cc.connConfig.GetClusterType(), err)
			}
			if newConn != nil {
				err2 := newConn.Close()
				if err2 != nil {
					log.Errorf("Failed to close cql connection: %v", err2)
				}
			}

			continue
		}

		conn = newConn
		log.Infof("Successfully opened control connection to %v using endpoint %v with %v.",
			cc.connConfig.GetClusterType(), endpoint.String(), newConn.GetProtocolVersion())
		break
	}

	return conn, endpoint
}

func (cc *ControlConn) connAndNegotiateProtoVer(endpoint Endpoint, initialProtoVer primitive.ProtocolVersion, ctx context.Context) (CqlConnection, error) {
	protoVer := initialProtoVer
	for {
		tcpConn, _, err := openConnection(cc.connConfig, endpoint, ctx, false)
		if err != nil {
			log.Warnf("Failed to open control connection to %v using endpoint %v: %v",
				cc.connConfig.GetClusterType(), endpoint.GetEndpointIdentifier(), err)
			return nil, err
		}
		newConn := NewCqlConnection(cc, endpoint, tcpConn, cc.username, cc.password, ccReadTimeout, ccWriteTimeout, cc.conf, protoVer)
		err = newConn.InitializeContext(protoVer, ctx)
		var respErr *ResponseError
		if err != nil && errors.As(err, &respErr) && respErr.IsProtocolError() && strings.Contains(err.Error(), "Invalid or unsupported protocol version") {
			// unsupported protocol version
			// protocol renegotiation requires opening a new TCP connection
			err2 := newConn.Close()
			if err2 != nil {
				log.Errorf("Failed to close cql connection: %v", err2)
			}
			protoVer = downgradeProtocol(protoVer)
			log.Debugf("Downgrading protocol version: %v", protoVer)
			if protoVer == 0 {
				// we cannot downgrade anymore
				return nil, err
			}
			continue // retry lower protocol version
		} else {
			return newConn, err // we may have successfully established connection or faced other error
		}
	}
}

func downgradeProtocol(version primitive.ProtocolVersion) primitive.ProtocolVersion {
	switch version {
	case primitive.ProtocolVersionDse2:
		return primitive.ProtocolVersionDse1
	case primitive.ProtocolVersionDse1:
		return primitive.ProtocolVersion4
	case primitive.ProtocolVersion4:
		return primitive.ProtocolVersion3
	case primitive.ProtocolVersion3:
		return primitive.ProtocolVersion2
	}
	return 0
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

func (cc *ControlConn) RefreshHosts(conn CqlConnection, ctx context.Context) ([]*Host, error) {
	localQueryResult, err := conn.Query("SELECT * FROM system.local", GetDefaultGenericTypeCodec(), ctx)
	if err != nil {
		return nil, fmt.Errorf("could not fetch information from system.local table: %w", err)
	}

	localInfo, localHost, err := ParseSystemLocalResult(localQueryResult, conn.GetEndpoint(), cc.defaultPort)
	if err != nil {
		return nil, err
	}

	partitionerColValue, partitionerExists := localInfo[partitionerColumn.Name]
	var partitioner *string
	if partitionerExists {
		partitioner = partitionerColValue.AsNillableString()
	}
	if partitioner != nil && !strings.Contains(*partitioner, "Murmur3Partitioner") && cc.topologyConfig.VirtualizationEnabled {
		if strings.Contains(*partitioner, "RandomPartitioner") {
			log.Debugf("Cluster %v uses the Random partitioner, but the proxy will return Murmur3 to the client instead. This is the expected behaviour.", cc.connConfig.GetClusterType())
		} else {
			return nil, fmt.Errorf("virtualization is enabled and partitioner is not Murmur3 or Random but instead %v", *partitioner)
		}
	}

	peersQuery, err := conn.Query("SELECT * FROM system.peers", GetDefaultGenericTypeCodec(), ctx)
	if err != nil {
		return nil, fmt.Errorf("could not fetch information from system.peers table: %w", err)
	}

	hostsById := ParseSystemPeersResult(peersQuery, cc.defaultPort, false)

	var peersColumns map[string]bool // nil if no peers
	if len(hostsById) > 0 {
		peersColumns = map[string]bool{}
		var peerHostExample *Host
		for _, h := range hostsById {
			peerHostExample = h
			break
		}
		for name, optionalCol := range peerHostExample.ColumnData {
			if optionalCol.exists {
				peersColumns[name] = true
			}
		}
	}

	oldLocalhost, localHostExists := hostsById[localHost.HostId]
	if localHostExists {
		log.Warnf("Local host is also on the peers list: %v vs %v, ignoring the former one.", oldLocalhost, localHost)
	}
	hostsById[localHost.HostId] = localHost
	orderedLocalHosts := make([]*Host, 0, len(hostsById))
	for _, h := range hostsById {
		orderedLocalHosts = append(orderedLocalHosts, h)
	}

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

	log.Infof("Refreshed %v orderedHostsInLocalDc. Assigned Hosts: %v, VirtualHosts: %v, ProxyTopologyIndex: %v",
		cc.connConfig.GetClusterType(), assignedHosts, virtualHosts, cc.topologyConfig.Index)

	cc.topologyLock.Lock()
	if cc.datacenter == "" {
		cc.datacenter = currentDc
	}
	oldHosts := cc.hostsInLocalDcById
	cc.orderedHostsInLocalDc = orderedLocalHosts
	cc.hostsInLocalDcById = hostsById
	cc.assignedHosts = assignedHosts
	cc.systemLocalColumnData = localInfo
	cc.systemPeersColumnNames = peersColumns
	cc.virtualHosts = virtualHosts

	if oldHosts != nil && len(oldHosts) > 0 {
		removedHosts := make([]*Host, 0)
		for oldHostId, oldHost := range oldHosts {
			if _, found := hostsById[oldHostId]; !found {
				removedHosts = append(removedHosts, oldHost)
			}
		}

		for _, removedHost := range removedHosts {
			for observer := range cc.protocolEventSubscribers {
				observer.OnHostRemoved(removedHost)
			}
		}
	}
	cc.topologyLock.Unlock()

	return orderedLocalHosts, nil
}

func (cc *ControlConn) GetHostsInLocalDatacenter() (map[uuid.UUID]*Host, error) {
	cc.topologyLock.RLock()
	defer cc.topologyLock.RUnlock()

	if cc.hostsInLocalDcById == nil {
		return nil, fmt.Errorf("could not get hostsInLocalDcById because topology information has not been retrieved yet")
	}

	return cc.hostsInLocalDcById, nil
}

func (cc *ControlConn) GetOrderedHostsInLocalDatacenter() ([]*Host, error) {
	cc.topologyLock.RLock()
	defer cc.topologyLock.RUnlock()

	if cc.orderedHostsInLocalDc == nil {
		return nil, fmt.Errorf("could not get orderedHostsInLocalDc because topology information has not been retrieved yet")
	}

	return cc.orderedHostsInLocalDc, nil
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

	clusterNameCol, clusterNameExists := cc.systemLocalColumnData[clusterNameColumn.Name]
	clusterName := ""
	if clusterNameExists {
		clusterNameValue := clusterNameCol.AsNillableString()
		if clusterNameValue != nil {
			clusterName = *clusterNameValue
		}
	}

	return clusterName
}

func (cc *ControlConn) GetSystemLocalColumnData() map[string]*optionalColumn {
	cc.topologyLock.RLock()
	defer cc.topologyLock.RUnlock()

	return cc.systemLocalColumnData
}

func (cc *ControlConn) GetSystemPeersColumnNames() map[string]bool {
	cc.topologyLock.RLock()
	defer cc.topologyLock.RUnlock()

	return cc.systemPeersColumnNames
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
		authEnabled, err := newConn.IsAuthEnabled()
		if err != nil {
			log.Errorf("Error detected when trying to set whether auth is enabled or not in control connection, "+
				"this is a bug, please report: %v", err)
		} else {
			cc.authEnabled.Store(authEnabled)
		}
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

func (cc *ControlConn) GetConnAndContactPoint() (CqlConnection, Endpoint) {
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

func (cc *ControlConn) RegisterObserver(observer ProtocolEventObserver) {
	cc.topologyLock.Lock()
	defer cc.topologyLock.Unlock()
	_, ok := cc.protocolEventSubscribers[observer]
	if ok {
		log.Warnf("Duplicate observer found while registering protocol event observer.")
	}
	cc.protocolEventSubscribers[observer] = nil
}

func (cc *ControlConn) RemoveObserver(observer ProtocolEventObserver) {
	cc.topologyLock.Lock()
	defer cc.topologyLock.Unlock()
	delete(cc.protocolEventSubscribers, observer)
}

func (cc *ControlConn) StoreProtoVersion(protoVersion primitive.ProtocolVersion) {
	cc.controlConnProtoVersion.Store(protoVersion)
}

func (cc *ControlConn) LoadProtoVersion() (primitive.ProtocolVersion, error) {
	var protoVersion primitive.ProtocolVersion
	protoVersionAny := cc.controlConnProtoVersion.Load()
	if protoVersionAny == nil {
		return protoVersion, errors.New("protocol version could not be retrieved")
	}

	var ok bool
	protoVersion, ok = protoVersionAny.(primitive.ProtocolVersion)
	if ok {
		return protoVersion, nil
	}

	panic("invalid type for protocol version")
}

func (cc *ControlConn) SetSupportedResponse(supported *message.Supported) {
	cc.supportedResponse.Store(supported)
}

func (cc *ControlConn) GetSupportedResponse() *message.Supported {
	return cc.supportedResponse.Load().(*message.Supported)
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

func computeVirtualHosts(topologyConfig *common.TopologyConfig, orderedHosts []*Host) ([]*VirtualHost, error) {
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
		primitiveHostId := primitive.UUID(hostId)

		host := assignedHostsForVirtualization[i]
		virtualHosts[i] = &VirtualHost{
			Tokens:      tokens,
			Addr:        proxyAddresses[i],
			Host:        host,
			HostId:      &primitiveHostId,
			Rack:        ProxyVirtualRack,
			Partitioner: ProxyVirtualPartitioner,
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
			log.Warnf("datacenter was set to '%v' but no hosts were found with that DC "+
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
	Tokens      []string
	Addr        net.IP
	Host        *Host
	HostId      *primitive.UUID
	Rack        string
	Partitioner string
}

func (recv *VirtualHost) String() string {
	return fmt.Sprintf("VirtualHost{addr: %v, host_id: %v, rack: %v, tokens: %v, host: %v, partitioner: %v}",
		recv.Addr,
		recv.HostId,
		recv.Rack,
		recv.Tokens,
		recv.Host,
		recv.Partitioner)
}

type ProtocolEventObserver interface {
	OnHostRemoved(host *Host)
}
