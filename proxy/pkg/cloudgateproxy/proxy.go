package cloudgateproxy

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/jpillora/backoff"
	"github.com/riptano/cloud-gate/proxy/pkg/config"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics"
	"io"
	"net"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	// TODO: Make these configurable
	maxQueryRetries = 5
	queryTimeout    = 2 * time.Second

	cassHdrLen = 9
	cassMaxLen = 256 * 1024 * 1024 // 268435456 // 256 MB, per spec		// TODO is this an actual limit?
)

// [Alice] for opcode values and meaning see cqlparser.go

type CloudgateProxy struct {

	Conf *config.Config

	originCassandraIP string
	targetCassandraIP string

	// Connection maps for OriginCassandra and TargetCassandra. One connection for each client connected to the proxy.
	// These maps are keyed on the client IP address.
	originCassandraConnections map[string]net.Conn
	targetCassandraConnections map[string]net.Conn
	connectionLocks            map[string]*sync.RWMutex
	lock                       *sync.RWMutex

	// Channel signalling that the proxy is now ready to process queries
	// TODO is this still needed?
	ReadyChan chan struct{}

	// Channel to signal to coordinator that there are no more open connections to the Client's Database
	// and that the coordinator can redirect Envoy to point directly to TargetCassandra without any negative side effects
	//TODO this will probably go in the end but it is here to make the main method work for the moment
	ReadyForRedirect chan struct{}

	clientListeners []net.Listener

	// Map containing the statement to be prepared and whether it is a read or a write by streamID
	// This is kind of transient: it only contains statements that are being prepared at the moment.
	// Once the response to the prepare request is processed, the statement is removed from this map
	statementsBeingPrepared map[uint16]PreparedStatementInfo

	// Map containing the prepared queries (raw bytes) keyed on prepareId
	preparedStatementCache map[string]PreparedStatementInfo

	// map of maps holding the response channels from TargetCassandra keyed on stream for each client
	// the outer map is keyed on client IP, the inner maps are keyed on stream
	targetCassandraResponseChannels map[string]map[uint16]chan *Frame
	// map of channels for service-related communication from TargetCassandra (keyed on clientIPAddress)
	targetCassandraServiceResponseChannels map[string]chan *Frame

	// map of maps holding the response channels from OriginCassandra keyed on streamID for each client
	// the outer map is keyed on client IP, the inner maps are keyed on streamID
	originCassandraResponseChannels map[string]map[uint16]chan *Frame
	// map of channels for service-related communication from TargetCassandra (keyed on clientIPAddress)
	originCassandraServiceResponseChannels map[string]chan *Frame

	// map of overall response channels - there is one for each client connection (keyed on client IP).
	responseForClientChannels map[string]chan []byte

	currentOriginCassandraKeyspacePerClient map[string]string // Keeps track of the current keyspace that each CLIENT is in
	currentTargetCassandraKeyspacePerClient map[string]string // Keeps track of the current keyspace that the PROXY is in while connected to TargetCassandra

	shutdown bool

	Metrics *metrics.Metrics
}

//	Method that initializes everything when a new client connects to the proxy.
//TODO it is doing more than initialising: find a better name
func (p *CloudgateProxy) initializeStructuresForClientConnection(clientAppConn net.Conn) {
	originCassandraConn := establishConnection(p.originCassandraIP)
	targetCassandraConn := establishConnection(p.targetCassandraIP)

	clientApplicationIP := clientAppConn.RemoteAddr().String()
	log.Debugf("clientApplicationIP %s", clientApplicationIP) // [Alice]

	p.lock.Lock()
	p.originCassandraConnections[clientApplicationIP] = originCassandraConn
	p.targetCassandraConnections[clientApplicationIP] = targetCassandraConn
	p.connectionLocks[clientApplicationIP] = &sync.RWMutex{}

	p.originCassandraResponseChannels[clientApplicationIP] = make(map[uint16] chan *Frame)
	p.targetCassandraResponseChannels[clientApplicationIP] = make(map[uint16] chan *Frame)

	p.originCassandraServiceResponseChannels[clientApplicationIP] = make(chan *Frame)
	p.targetCassandraServiceResponseChannels[clientApplicationIP] = make(chan *Frame)
	p.responseForClientChannels[clientApplicationIP] = make(chan []byte)

	p.statementsBeingPrepared = make(map[uint16]PreparedStatementInfo)
	p.preparedStatementCache = make(map[string]PreparedStatementInfo)

	p.lock.Unlock()

	// start listening for replies on each cluster connection
	go p.listenOnClusterConnectionForReplies(clientApplicationIP, false)
	go p.listenOnClusterConnectionForReplies(clientApplicationIP, true)

	go p.dispatchResponsesToClient(clientAppConn)
}

// Start starts up the proxy and start listening for client connections.
func (p *CloudgateProxy) Start() error {
	p.reset()
	p.checkDatabaseConnections()

	err := p.listenFromClient(p.Conf.ProxyQueryPort, p.listenOnClientConnection)
	if err != nil {
		return err
	}

	log.Infof("Proxy connected and ready to accept queries on port %d", p.Conf.ProxyQueryPort)
	return nil
}

// TODO: Is there a better way to check that we can connect to both databases?
func (p *CloudgateProxy) checkDatabaseConnections() {
	// Wait until the source database is up and ready to accept TCP connections.
	originCassandra := establishConnection(p.originCassandraIP)
	originCassandra.Close()

	// Wait until the TargetCassandra database is up and ready to accept TCP connections.
	targetCassandra := establishConnection(p.targetCassandraIP)
	targetCassandra.Close()
}

// listenFromClient creates a listener on the passed in port argument, and every connection
// that is received over that port is handled by the passed in handler function.
func (p *CloudgateProxy) listenFromClient(port int, handler func(net.Conn) error) error {
	log.Debugf("Proxy connected and ready to accept queries on port %d", port)
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}

	p.lock.Lock()
	p.clientListeners = append(p.clientListeners, l)
	p.lock.Unlock()

	go func() {
		defer l.Close()
		for {
			conn, err := l.Accept()
			if err != nil {
				if p.shutdown {
					log.Debugf("Shutting down client listener on port %d", port)
					return
				}
				log.Error(err)
				continue
			}
			// long-lived goroutine that handles any request coming over this connection
			// there is a goroutine call for each connection made by a client
			go handler(conn)
		}
	}()

	return nil
}

// long-lived method that will run indefinitely and spawn a new goroutine for each client request
func (p *CloudgateProxy) listenOnClientConnection(clientAppConn net.Conn) error {

	log.Debugf("listenOnClientConnection")

	p.initializeStructuresForClientConnection(clientAppConn)
	clientApplicationIP := clientAppConn.RemoteAddr().String()
	authenticated := false

	// Main listening loop
	// creating this outside the loop to avoid creating a slice every time
	// which would be heavy on the GC (see https://medium.com/go-walkthrough/go-walkthrough-io-package-8ac5e95a9fbd)
	frameHeader := make([]byte, cassHdrLen)
	for {

		/*  - parse frame
		    - parse request
		    - create query object
		    - get type
		    - determine if read or write and also if prepared statement is involved
		*/
		f, err := parseFrame(clientAppConn, frameHeader, p.Metrics)

		if err != nil {
			if err != io.EOF {
				log.Debugf("%s disconnected", clientAppConn)
			} else {
				log.Debugf("error reading frame header")
				log.Error(err)
			}
			return err
		}

		if f.Direction != 0 {
			log.Debugf("Unexpected frame direction %d", f.Direction)
			log.Error(errors.New("unexpected direction: frame not from client to db - skipping frame"))
			continue
		}

		if !authenticated {
			log.Debugf("not authenticated")
			// Handle client authentication
			authenticated, err = p.handleStartupFrame(f, clientAppConn)
			if err != nil {
				log.Error(err)
			}
			log.Debugf("authenticated? %t", authenticated)
			continue
		}

		// One goroutine for each request, so each request is executed concurrently
		go p.handleRequest(f, clientApplicationIP)
	}
}

// listens on overallResponseChan for that client
// dequeues any responses from the channel and sends them to the client
func (p *CloudgateProxy) dispatchResponsesToClient(clientAppConn net.Conn) error {
	log.Debugf("dispatchResponsesToClient")

	clientApplicationIP := clientAppConn.RemoteAddr().String()

	for {
		log.Debugf("Waiting for next response to dispatch to client %s", clientApplicationIP)
		// dequeue responses from channel
		response := <- p.responseForClientChannels[clientApplicationIP]
		log.Debugf("response received, dispatching to client %s. Opcode %d", clientApplicationIP, response[3])

		// send responses on the client connection on which the corresponding request was received
		_, err := clientAppConn.Write(response)
		if err != nil {
			return err
		}
		log.Debugf("response dispatched to client %s", clientApplicationIP)
	}
}

func (p *CloudgateProxy) forwardToCluster(queryToForward *Query, isServiceRequest bool, responseToCallerChan chan *Frame) error {
	// submits the request on cluster connection (initially single connection to keep it simple, but it will probably have to be a pool)
	// creates a channel (responseFromClusterChan) on which it will send the response to the request being handled to the caller (handleRequest)
	// adds an entry to a pendingRequestMap keyed on streamID and whose value is a channel. this channel is used by the dequeuer to communicate the response back to this goroutine
	// it is this goroutine that has to receive the response, so it can enforce the timeout in case of connection disruption
	log.Debugf("Forwarding query of type %v with opcode %v and path %v for stream %v to %s", queryToForward.Type, queryToForward.Opcode, queryToForward.Paths[0], queryToForward.Stream, queryToForward.Destination)

	targetCassandra := queryToForward.Destination == TARGET_CASSANDRA

	var responseFromClusterChan chan *Frame
	if !isServiceRequest {
		// create a channel on which the loop that listens to the database connection will put the response for this stream and register it in the map
		// this is necessary because the response comes back on a shared connection and it will be returned asynchronously
		log.Debugf("Not a service request, creating a response channel for stream %d and setting it in the map. targetCassandra? %t", queryToForward.Stream, targetCassandra)
		responseFromClusterChan = p.createResponseFromClusterChan(queryToForward, targetCassandra)
		// this channel will be cleaned up when this goroutine terminates as it is no longer needed
		defer p.deleteResponseFromClusterChan(queryToForward, targetCassandra)
	} else {
		log.Debugf("Service request, retrieving the service response channel. targetCassandra? %t", targetCassandra)
		responseFromClusterChan = p.getServiceResponseChannel(targetCassandra, queryToForward.SourceIPAddress)
		// the service response channel is long-lived and not request-specific, no cleanup required
	}

	var connectionToCluster net.Conn
	if targetCassandra {
		connectionToCluster = p.getTargetCassandraConnection(queryToForward.SourceIPAddress)
	} else {
		connectionToCluster = p.getOriginCassandraConnection(queryToForward.SourceIPAddress)
	}

	err := p.sendRequestOnConnection(queryToForward, connectionToCluster)
	if err != nil {
		return err
	}

	timeout := time.NewTimer(queryTimeout)
	for {
		select {
		case response := <-responseFromClusterChan:
			log.Debugf("Received response from %s for query with stream id %d", queryToForward.Destination, response.Stream)
			responseToCallerChan <- response
			timeout.Stop()
		case <- timeout.C:
			log.Debugf("Timeout for query %d from %s", queryToForward.Stream, queryToForward.Destination)
			// TODO clean up channel for that stream
			// TODO return something upstream otherwise caller is left hanging
			return fmt.Errorf("timeout for query %d from %s", queryToForward.Stream, queryToForward.Destination)

		}
	}

	return err
}


func (p *CloudgateProxy) createResponseFromClusterChan(q *Query, toTargetCassandra bool) chan *Frame {
	p.lock.Lock()
	defer p.lock.Unlock()

	if toTargetCassandra {
		p.targetCassandraResponseChannels[q.SourceIPAddress][q.Stream] = make(chan *Frame, 1)
		return p.targetCassandraResponseChannels[q.SourceIPAddress][q.Stream]
	} else {
		p.originCassandraResponseChannels[q.SourceIPAddress][q.Stream] = make(chan *Frame, 1)
		return p.originCassandraResponseChannels[q.SourceIPAddress][q.Stream]
	}
}

func (p *CloudgateProxy) sendRequestOnConnection(queryToForward *Query, connectionToCluster net.Conn) error {
	p.connectionLocks[queryToForward.SourceIPAddress].Lock()
	defer p.connectionLocks[queryToForward.SourceIPAddress].Unlock()
	log.Debugf("Executing %v on cluster with address %v", string(*&queryToForward.Query), queryToForward.SourceIPAddress)
	buf := bytes.NewBuffer(queryToForward.Query)
	n := len(queryToForward.Query)
	_, err := io.CopyN(connectionToCluster, buf, int64(n))
	return err
}

func (p *CloudgateProxy) deleteResponseFromClusterChan(q *Query, toTargetCassandra bool) {
	p.lock.Lock()
	defer p.lock.Unlock()

	if toTargetCassandra {
		close(p.targetCassandraResponseChannels[q.SourceIPAddress][q.Stream])
		delete(p.targetCassandraResponseChannels[q.SourceIPAddress], q.Stream)
	} else {
		close(p.originCassandraResponseChannels[q.SourceIPAddress][q.Stream])
		delete(p.originCassandraResponseChannels[q.SourceIPAddress], q.Stream)
	}
}

// listenOnClusterConnectionForReplies will read the response from a cluster and
// send it back on the channel for the corresponding request
/*
 	0x00    ERROR
    0x01    STARTUP
    0x02    READY
    0x03    AUTHENTICATE
    0x05    OPTIONS
    0x06    SUPPORTED
    0x07    QUERY
    0x08    RESULT
    0x09    PREPARE
    0x0A    EXECUTE
    0x0B    REGISTER
    0x0C    EVENT
    0x0D    BATCH
    0x0E    AUTH_CHALLENGE
    0x0F    AUTH_RESPONSE
    0x10    AUTH_SUCCESS
 */
func (p *CloudgateProxy) listenOnClusterConnectionForReplies(clientIPAddress string, fromTargetCassandra bool) {

	// initialization of this long-running routine
	var connection net.Conn
	var responseChannels map[string]map[uint16]chan *Frame
	var serviceResponseChannel chan *Frame

	if fromTargetCassandra {
		connection = p.getTargetCassandraConnection(clientIPAddress)
		responseChannels = p.targetCassandraResponseChannels
		serviceResponseChannel = p.targetCassandraServiceResponseChannels[clientIPAddress]
	} else {
		connection = p.getOriginCassandraConnection(clientIPAddress)
		responseChannels = p.originCassandraResponseChannels
		serviceResponseChannel = p.originCassandraServiceResponseChannels[clientIPAddress]
	}

	log.Debugf("Listening to replies sent by cluster: targetCassandra? %v", fromTargetCassandra)
	// long-running loop that listens for replies being sent by the cluster on this connection
	frameHeader := make([]byte, cassHdrLen)
	for {
		response, _ := parseFrame(connection, frameHeader, p.Metrics)

		log.Debugf("Opcode? %v -- stream? %v", response.Opcode, response.Stream)

		// TODO error responses need to be handled as results or service depending on the specific error code. Add this logic!!
		if response.Opcode == 0x00 || response.Opcode == 0x08 {
			p.lock.Lock()

			if responseChannel, ok := responseChannels[clientIPAddress][response.Stream]; !ok {
				log.Errorf("could not find stream %d in responseChannels for client %s. fromTargetCassandra? %v", response.Stream, clientIPAddress, fromTargetCassandra)
			} else {
				// Note: the boolean response is sent on the channel here - this will unblock the forwardToCluster goroutine waiting on this
				responseChannel <- response
			}

			p.lock.Unlock()
		} else {
			serviceResponseChannel <- response
		}

	}
}

func (p *CloudgateProxy) checkError(body []byte) {
	errCode := binary.BigEndian.Uint16(body[0:2])
	switch errCode {
	case 0x0000:
		// Server Error
		p.Metrics.IncrementServerErrors()
	case 0x1100:
		// Write Timeout
		p.Metrics.IncrementWriteFails()
	case 0x1200:
		// Read Timeout
		p.Metrics.IncrementReadFails()
	}

}

// Establishes a TCP connection with the passed in IP. Retries using exponential backoff.
func establishConnection(ip string) net.Conn {
	b := &backoff.Backoff{
		Min:    100 * time.Millisecond,
		Max:    10 * time.Second,
		Factor: 2,
		Jitter: false,
	}

	log.Debugf("Attempting to connect to %s...", ip)
	for {
		conn, err := net.Dial("tcp", ip)
		if err != nil {
			nextDuration := b.Duration()
			log.Errorf("Couldn't connect to %s, retrying in %s...", ip, nextDuration.String())
			time.Sleep(nextDuration)
			continue
		}
		log.Infof("Successfully established connection with %s", conn.RemoteAddr())
		return conn
	}
}

// handleStartupFrame will check the frame opcodes to determine what startup actions to take
// The process, at a high level, is that the proxy directly tunnels startup communications
// to TargetCassandra (since the client logs on with TargetCassandra credentials), and then the proxy manually
// initiates startup with the client's old database
func (p *CloudgateProxy) handleStartupFrame(f *Frame, clientAppConn net.Conn) (bool, error) {
	clientAppIP := clientAppConn.RemoteAddr().String()
	originCassandraConnection := p.getOriginCassandraConnection(clientAppIP)
	targetCassandraConnection := p.getTargetCassandraConnection(clientAppIP)

	switch f.Opcode {
		// OPTIONS - this might be sent prior to the startup message to find out which options are supported
		// The OPTIONS message is only sent to TargetCassandra - TODO why?
		case 0x05:
			// forward OPTIONS to TargetCassandra
			// [Alice] this call sends the options message and deals with the response (supported / not supported)
			// this exchange does not authenticate yet
			// is this also where the native protocol version is negotiated?
			log.Debugf("Handling OPTIONS message")
			err := HandleOptions(clientAppIP, targetCassandraConnection, f.RawBytes, p.targetCassandraServiceResponseChannels[clientAppIP], p.responseForClientChannels[clientAppIP])
			if err != nil {
				return false, fmt.Errorf("client %s unable to negotiate options with %s",
					clientAppIP, targetCassandraConnection.RemoteAddr().String())
			}
			log.Debugf("OPTIONS message successfully handled")
			// TODO what does this method return here? it should return false

		// STARTUP - the STARTUP message is sent to both TargetCassandra and OriginCassandra
		case 0x01:
			log.Debugf("Handling STARTUP message")
			err := HandleTargetCassandraStartup(clientAppConn, targetCassandraConnection, f.RawBytes, p.targetCassandraServiceResponseChannels[clientAppIP])
			if err != nil {
				return false, err
			}
			log.Debugf("STARTUP message successfully handled on TargetCassandra, now proceeding with OriginCassandra")
			err = HandleOriginCassandraStartup(clientAppIP, originCassandraConnection, f.RawBytes,
													p.Conf.OriginCassandraUsername, p.Conf.OriginCassandraPassword, p.originCassandraServiceResponseChannels[clientAppIP])
			if err != nil {
				return false, err
			}
			log.Debugf("STARTUP message successfully handled on OriginCassandra")
			return true, nil
	}
	return false, fmt.Errorf("received non STARTUP or OPTIONS query from unauthenticated client %s", clientAppIP)
}
func (p *CloudgateProxy) getServiceResponseChannel(toTargetCassandra bool, clientIPAddress string) chan *Frame {
	p.lock.Lock()
	defer p.lock.Unlock()

	if toTargetCassandra {
		return p.targetCassandraServiceResponseChannels[clientIPAddress]
	} else {
		return p.originCassandraServiceResponseChannels[clientIPAddress]
	}
}

func (p *CloudgateProxy) getTargetCassandraConnection(clientIPAddress string) net.Conn {
	p.lock.Lock()
	defer p.lock.Unlock()

	return p.targetCassandraConnections[clientIPAddress]
}

func (p *CloudgateProxy) getOriginCassandraConnection(clientIPAddress string) net.Conn {
	p.lock.Lock()
	defer p.lock.Unlock()

	return p.originCassandraConnections[clientIPAddress]
}

// reset will reset all context within the proxy service and instantiate everything from scratch
//p.queryResponses = make(map[string]map[uint16]chan bool)
func (p *CloudgateProxy) reset() {
	p.originCassandraIP = fmt.Sprintf("%s:%d", p.Conf.OriginCassandraHostname, p.Conf.OriginCassandraPort)
	p.targetCassandraIP = fmt.Sprintf("%s:%d", p.Conf.TargetCassandraHostname, p.Conf.TargetCassandraPort)

	p.originCassandraConnections = make(map[string]net.Conn)
	p.targetCassandraConnections = make(map[string]net.Conn)
	p.connectionLocks = make(map[string]*sync.RWMutex)
	p.lock = &sync.RWMutex{}

	p.ReadyChan = make(chan struct{})
	p.ReadyForRedirect = make(chan struct{})
	p.clientListeners = []net.Listener{}

	p.statementsBeingPrepared = make(map[uint16]PreparedStatementInfo)
	p.preparedStatementCache = make(map[string]PreparedStatementInfo)

	p.originCassandraResponseChannels = make(map[string]map[uint16]chan *Frame)
	p.targetCassandraResponseChannels = make(map[string]map[uint16]chan *Frame)
	p.originCassandraServiceResponseChannels = make(map[string]chan *Frame)
	p.targetCassandraServiceResponseChannels = make(map[string]chan *Frame)

	p.responseForClientChannels = make(map[string]chan []byte)

	p.currentOriginCassandraKeyspacePerClient =  make(map[string]string)
	p.currentTargetCassandraKeyspacePerClient =  make(map[string]string)

	p.shutdown = false
	p.Metrics = metrics.New(p.Conf.ProxyMetricsPort)
	p.Metrics.Expose()

	//p.outstandingQueries = make(map[string]map[uint16]*frame.Frame)
	//p.outstandingUpdates = make(map[string]chan bool)
	//p.migrationComplete = p.Conf.MigrationComplete
	//p.preparedQueries = &cqlparser.PreparedQueries{
	//	PreparedQueryPathByStreamID:   make(map[uint16]string),
	//	PreparedQueryPathByPreparedID: make(map[string]string),
	//}
	//p.preparedIDs = make(map[uint16]string)
	//p.mappedPreparedIDs = make(map[string]string)
	//p.outstandingPrepares = make(map[uint16][]byte)
	//p.prepareIDToKeyspace = make(map[string]string)
}
