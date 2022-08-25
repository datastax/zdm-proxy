package cloudgateproxy

import (
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/zdm-proxy/proxy/pkg/metrics"
	log "github.com/sirupsen/logrus"
	"sync"
)

const MaxStreams = 2048

type pendingRequests struct {
	pending     *sync.Map
	streams     chan int16
	nodeMetrics *metrics.NodeMetrics
}

func newPendingRequests(maxStreams int16, nodeMetrics *metrics.NodeMetrics) *pendingRequests {
	streams := make(chan int16, maxStreams)
	for i := int16(0); i < maxStreams; i++ {
		streams <- i
	}
	return &pendingRequests{
		pending:     &sync.Map{},
		streams:     streams,
		nodeMetrics: nodeMetrics,
	}
}

// Creates request context holder for the provided request context and adds it to the map.
// If a holder already exists, return it instead.
func (p *pendingRequests) getOrCreateRequestContextHolder(streamId int16) *requestContextHolder {
	holder, ok := p.pending.Load(streamId)
	if ok {
		return holder.(*requestContextHolder)
	} else {
		holder, _ =  p.pending.LoadOrStore(streamId, NewRequestContextHolder())
		return holder.(*requestContextHolder)
	}
}

func (p *pendingRequests) store(reqCtx RequestContext) (int16, error) {
	streamId, err := p.reserveStreamId()
	if err != nil {
		return -1, fmt.Errorf("stream id map ran out of stream ids: %w", err)
	}
	holder := getOrCreateRequestContextHolder(p.pending, streamId)
	err = holder.SetIfEmpty(reqCtx)
	if err != nil {
		return -1, fmt.Errorf("stream id collision (%d)", streamId)
	}

	return streamId, nil
}

func (p *pendingRequests) timeOut(streamId int16, reqCtx RequestContext, req *frame.RawFrame) bool {
	holder := p.getOrCreateRequestContextHolder(streamId)
	if reqCtx.SetTimeout(p.nodeMetrics, req) {
		clearPendingRequestState(streamId, holder, reqCtx)
		return true
	}
	return false
}

func (p *pendingRequests) cancel(streamId int16, reqCtx RequestContext) bool {
	holder := p.getOrCreateRequestContextHolder(streamId)
	if reqCtx.Cancel(p.nodeMetrics) {
		clearPendingRequestState(streamId, holder, reqCtx)
		return true
	}
	return false
}

func (p *pendingRequests) markAsDone(
	streamId int16, f *frame.RawFrame, cluster ClusterType, connectorType ClusterConnectorType) (RequestContext, bool) {
	holder := p.getOrCreateRequestContextHolder(streamId)
	reqCtx := holder.Get()
	if reqCtx == nil {
		log.Warnf("Could not find async request context for stream id %d received from async connector. " +
			"It either timed out or a protocol error occurred.", streamId)
		return nil, false
	}
	if reqCtx.SetResponse(p.nodeMetrics, f, cluster, connectorType) {
		var err error
		if clearPendingRequestState(streamId, holder, reqCtx) {
			err = p.releaseStreamId(streamId)
		} else {
			err = errors.New("could not clear pending request state")
		}
		if err != nil {
			log.Errorf("Could not free stream id %v, this is most likely a bug, please report: %v", streamId, err.Error())
		}
		return reqCtx, true
	}
	return reqCtx, false
}

func (p *pendingRequests) clear(onCancelFunc func(ctx RequestContext)) {
	p.pending.Range(func(key, value interface{}) bool {
		reqCtxHolder := value.(*requestContextHolder)
		reqCtx := reqCtxHolder.Get()
		if reqCtx == nil {
			return true
		}
		canceled := reqCtx.Cancel(p.nodeMetrics)
		if canceled {
			onCancelFunc(reqCtx)
			clearPendingRequestState(key.(int16), reqCtxHolder, reqCtx)
		}
		return true
	})
}

func (p *pendingRequests) releaseStreamId(streamId int16) error {
	select {
	case p.streams <- streamId:
		return nil
	default:
		return errors.New("channel was full")
	}
}

func (p *pendingRequests) reserveStreamId() (int16, error) {
	select {
	case streamId := <- p.streams:
		return streamId, nil
	default:
		return -1, errors.New("channel was empty")
	}
}

func clearPendingRequestState(streamId int16, holder *requestContextHolder, reqCtx RequestContext) bool {
	err := holder.Clear(reqCtx)
	if err != nil {
		log.Debugf("could not clean up pending request with streamid %v: %v", streamId, err.Error())
		return false
	}
	return true
}