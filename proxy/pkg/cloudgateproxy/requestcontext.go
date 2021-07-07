package cloudgateproxy

import (
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

// Type that manages creation and closing of request contexts under a single RWMutex.
//
// Only one request context is active at a time for each instance of requestContextHolder.
//
// Objects of this type are used as values in a map where the key is the stream id.
//
// Each stream id is associated with a single requestContextHolder instance and these instances are reused throughout the
// ClusterConnector or ClientHandler's lifetime.
//
// This removes the need of deleting key value pairs from the map and therefore we can use a map type that is designed for
// inserts and updates (not deletes).
type requestContextHolder struct {
	reqCtx *RequestContext
	lock  *sync.RWMutex
}

func NewRequestContextHolder() *requestContextHolder {
	return &requestContextHolder{
		reqCtx: nil,
		lock:  &sync.RWMutex{},
	}
}

// Sets a request context if the request context holder is empty.
// Returns an error if the holder is not empty.
func (recv *requestContextHolder) SetIfEmpty(ctx *RequestContext) (err error) {
	recv.lock.Lock()
	defer recv.lock.Unlock()

	if recv.reqCtx != nil {
		return fmt.Errorf("could not set request context because the holder wasn't empty")
	}

	recv.reqCtx = ctx
	return nil
}

// Returns the request context that is being held by the request context holder object or null if it is empty.
func (recv *requestContextHolder) Get() *RequestContext {
	recv.lock.RLock()
	defer recv.lock.RUnlock()

	return recv.reqCtx
}

// Clears the request context holder if it is not empty and the provided request matches the one that is being held.
// Returns an error if the holder is empty or if the provided context doesn't match.
func (recv *requestContextHolder) Clear(ctx *RequestContext) error {
	recv.lock.Lock()
	defer recv.lock.Unlock()

	if recv.reqCtx == nil {
		return fmt.Errorf("could not clear request context holder because it wasn't set")
	}

	if recv.reqCtx != ctx {
		return fmt.Errorf("could not clear request context holder because request context didn't match")
	}

	recv.reqCtx = nil
	return nil
}

const (
	RequestPending = iota
	RequestTimedOut
	RequestDone
)

type RequestContext struct {
	request        *frame.RawFrame
	stmtInfo       StatementInfo
	originResponse *frame.RawFrame
	targetResponse *frame.RawFrame
	state          int
	timer          *time.Timer
	lock           *sync.Mutex
	startTime      time.Time
	customResponseChannel chan *frame.RawFrame
}

func NewRequestContext(req *frame.RawFrame, stmtInfo StatementInfo, starTime time.Time, customResponseChannel chan *frame.RawFrame) *RequestContext {
	return &RequestContext{
		request:        req,
		stmtInfo:       stmtInfo,
		originResponse: nil,
		targetResponse: nil,
		state:          RequestPending,
		timer:          nil,
		lock:           &sync.Mutex{},
		startTime:      starTime,
		customResponseChannel: customResponseChannel,
	}
}

func (recv *RequestContext) SetTimer(timer *time.Timer) {
	recv.timer = timer
}

func (recv *RequestContext) SetTimeout(nodeMetrics *metrics.NodeMetrics, req *frame.RawFrame) bool {
	recv.lock.Lock()
	defer recv.lock.Unlock()

	if recv.state != RequestPending {
		// already done
		return false
	}

	// check if it's the same request (could be a timeout for a previous one that has since completed)
	if recv.request == req {
		recv.state = RequestTimedOut
		sentOrigin := false
		sentTarget := false
		switch recv.stmtInfo.GetForwardDecision() {
		case forwardToBoth:
			sentOrigin = true
			sentTarget = true
		case forwardToOrigin:
			sentOrigin = true
		case forwardToTarget:
			sentTarget = true
		}
		if sentOrigin && recv.originResponse == nil {
			nodeMetrics.OriginMetrics.OriginClientTimeouts.Add(1)
		}
		if sentTarget && recv.targetResponse == nil {
			nodeMetrics.TargetMetrics.TargetClientTimeouts.Add(1)
		}
		return true
	}

	return false
}

func (recv *RequestContext) SetResponse(nodeMetrics *metrics.NodeMetrics, f *frame.RawFrame, cluster ClusterType) bool {
	state, updated := recv.updateInternalState(f, cluster)
	if !updated {
		return false
	}

	finished := state == RequestDone
	if finished {
		recv.timer.Stop() // if timer is not stopped, there's a memory leak because the timer callback holds references!
	}

	switch cluster {
	case OriginCassandra:
		log.Tracef("Received response from %v for query with stream id %d", OriginCassandra, f.Header.StreamId)
		nodeMetrics.OriginMetrics.OriginRequestDuration.Track(recv.startTime)
	case TargetCassandra:
		log.Tracef("Received response from %v for query with stream id %d", TargetCassandra, f.Header.StreamId)
		nodeMetrics.TargetMetrics.TargetRequestDuration.Track(recv.startTime)
	default:
		log.Errorf("could not recognize cluster type %v", cluster)
	}

	return finished
}

func (recv *RequestContext) updateInternalState(f *frame.RawFrame, cluster ClusterType) (state int, updated bool) {
	recv.lock.Lock()
	defer recv.lock.Unlock()

	if recv.state != RequestPending {
		// already done
		return recv.state, false
	}

	switch cluster {
	case OriginCassandra:
		recv.originResponse = f
	case TargetCassandra:
		recv.targetResponse = f
	default:
		log.Errorf("could not recognize cluster type %v", cluster)
	}

	done := false
	switch recv.stmtInfo.GetForwardDecision() {
	case forwardToTarget:
		done = recv.targetResponse != nil
	case forwardToOrigin:
		done = recv.originResponse != nil
	case forwardToBoth:
		done = recv.originResponse != nil && recv.targetResponse != nil
	case forwardToNone:
		done = true
	default:
		log.Errorf("unrecognized decision %v", recv.stmtInfo.GetForwardDecision())
	}

	if done {
		recv.state = RequestDone
	}

	return recv.state, true
}