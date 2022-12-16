package zdmproxy

import (
	"fmt"
	"github.com/datastax/zdm-proxy/proxy/pkg/metrics"
	"sync"
)

// StreamIdMapper is used to map the incoming stream ids from the client/driver to internal ids managed by the proxy
// This is required because we also generate requests inside the proxy that go to the cluster through the same connection,
// hence they must have non-conflicting ids with user's requests.
type StreamIdMapper interface {
	GetNewIdFor(streamId int16) (int16, error)
	ReleaseId(syntheticId int16) (int16, error)
	Close()
}

type streamIdMapper struct {
	sync.Mutex
	idMapper   map[int16]int16
	clusterIds chan int16
	metrics    metrics.Gauge
}

type internalStreamIdMapper struct {
	clusterIds chan int16
	metrics    metrics.Gauge
}

// NewInternalStreamIdMapper is used to assign unique ids to frames that have no initial stream id defined, such as
// CQL queries initiated by the proxy or ASYNC requests.
func NewInternalStreamIdMapper(maxStreamIds int, metrics metrics.Gauge) StreamIdMapper {
	streamIdsQueue := make(chan int16, maxStreamIds)
	for i := int16(0); i < int16(maxStreamIds); i++ {
		streamIdsQueue <- i
	}
	return &internalStreamIdMapper{
		clusterIds: streamIdsQueue,
		metrics:    metrics,
	}
}

func (csid *internalStreamIdMapper) GetNewIdFor(_ int16) (int16, error) {
	select {
	case id := <-csid.clusterIds:
		if csid.metrics != nil {
			csid.metrics.Add(1)
		}
		return id, nil
	default:
		return -1, fmt.Errorf("no stream id available")
	}
}

func (csid *internalStreamIdMapper) ReleaseId(syntheticId int16) (int16, error) {
	if syntheticId < 0 || int(syntheticId) >= cap(csid.clusterIds) {
		return -1, fmt.Errorf("can not release invalid stream id %v (max id: %v)", syntheticId, cap(csid.clusterIds)-1)
	}
	select {
	case csid.clusterIds <- syntheticId:
		if csid.metrics != nil {
			csid.metrics.Subtract(1)
		}
	default:
		return -1, fmt.Errorf("stream ids channel full, ignoring id %v", syntheticId)
	}
	return syntheticId, nil
}

func (csid *internalStreamIdMapper) Close() {
	if csid.metrics != nil && cap(csid.clusterIds) != len(csid.clusterIds) {
		csid.metrics.Subtract(cap(csid.clusterIds) - len(csid.clusterIds))
	}
}

func NewStreamIdMapper(maxStreamIds int, metrics metrics.Gauge) StreamIdMapper {
	idMapper := make(map[int16]int16)
	streamIdsQueue := make(chan int16, maxStreamIds)
	for i := int16(0); i < int16(maxStreamIds); i++ {
		streamIdsQueue <- i
	}
	return &streamIdMapper{
		idMapper:   idMapper,
		clusterIds: streamIdsQueue,
		metrics:    metrics,
	}
}

func (sim *streamIdMapper) GetNewIdFor(streamId int16) (int16, error) {
	select {
	case id := <-sim.clusterIds:
		if sim.metrics != nil {
			sim.metrics.Add(1)
		}
		sim.Lock()
		if _, contains := sim.idMapper[id]; contains {
			sim.Unlock()
			return -1, fmt.Errorf("stream id collision, mapper already contains id %v", id)
		}
		sim.idMapper[id] = streamId
		sim.Unlock()
		return id, nil
	default:
		return -1, fmt.Errorf("no stream id available")
	}
}

func (sim *streamIdMapper) ReleaseId(syntheticId int16) (int16, error) {
	if syntheticId < 0 || int(syntheticId) >= cap(sim.clusterIds) {
		return -1, fmt.Errorf("can not release invalid stream id %v (max id: %v)", syntheticId, cap(sim.clusterIds)-1)
	}
	sim.Lock()
	originalId, contains := sim.idMapper[syntheticId]
	if !contains {
		sim.Unlock()
		return -1, fmt.Errorf("trying to release a stream id not found in mapper: %v", syntheticId)
	}
	delete(sim.idMapper, syntheticId)
	sim.Unlock()
	select {
	case sim.clusterIds <- syntheticId:
		if sim.metrics != nil {
			sim.metrics.Subtract(1)
		}
	default:
		return -1, fmt.Errorf("stream ids channel full, ignoring id %v", syntheticId)
	}

	return originalId, nil
}

func (sim *streamIdMapper) Close() {
	if sim.metrics != nil && cap(sim.clusterIds) != len(sim.clusterIds) {
		sim.metrics.Subtract(cap(sim.clusterIds) - len(sim.clusterIds))
	}
}
