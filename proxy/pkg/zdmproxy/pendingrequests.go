package zdmproxy

import (
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/zdm-proxy/proxy/pkg/common"
	"github.com/datastax/zdm-proxy/proxy/pkg/metrics"
	log "github.com/sirupsen/logrus"
	"sync"
)

type pendingRequests struct {
	pending     *sync.Map
	nodeMetrics *metrics.NodeMetrics
}

func newPendingRequests(nodeMetrics *metrics.NodeMetrics) *pendingRequests {
	return &pendingRequests{
		pending:     &sync.Map{},
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
		holder, _ = p.pending.LoadOrStore(streamId, NewRequestContextHolder())
		return holder.(*requestContextHolder)
	}
}

func (p *pendingRequests) store(reqCtx RequestContext, frame *frame.RawFrame) error {
	holder := getOrCreateRequestContextHolder(p.pending, frame.Header.StreamId)
	err := holder.SetIfEmpty(reqCtx)
	if err != nil {
		return fmt.Errorf("stream id collision (%d)", frame.Header.StreamId)
	}

	return nil
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
	streamId int16, f *frame.RawFrame, cluster common.ClusterType, connectorType ClusterConnectorType) (RequestContext, bool) {
	holder := p.getOrCreateRequestContextHolder(streamId)
	reqCtx := holder.Get()
	if reqCtx == nil {
		log.Warnf("Could not find async request context for stream id %d received from async connector. "+
			"It either timed out or a protocol error occurred.", streamId)
		return nil, false
	}
	if reqCtx.SetResponse(p.nodeMetrics, f, cluster, connectorType) {
		var err error
		if !clearPendingRequestState(streamId, holder, reqCtx) {
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

func clearPendingRequestState(streamId int16, holder *requestContextHolder, reqCtx RequestContext) bool {
	err := holder.Clear(reqCtx)
	if err != nil {
		log.Debugf("could not clean up pending request with streamid %v: %v", streamId, err.Error())
		return false
	}
	return true
}
