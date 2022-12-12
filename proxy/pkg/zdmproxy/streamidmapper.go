package zdmproxy

import (
	"fmt"
	"sync"
)

// StreamIdMapper is used to map the incoming stream ids from the client/driver to internal ids managed by the proxy
// This is required because we also generate requests inside the proxy that go to the cluster through the same connection,
// hence they must have non-conflicting ids with user's requests.
type StreamIdMapper interface {
	GetNewIdFor(streamId int16) (int16, error)
	ReleaseId(syntheticId int16) (int16, error)
}

type streamIdMapper struct {
	sync.Mutex
	idMapper   map[int16]int16
	clusterIds chan int16
}

type internalStreamIdMapper struct {
	clusterIds chan int16
}

// NewInternalStreamIdMapper is used to assign unique ids to frames that have no initial stream id defined, such as
// CQL queries initiated by the proxy or ASYNC requests.
func NewInternalStreamIdMapper(maxStreamIds int) StreamIdMapper {
	streamIdsQueue := make(chan int16, maxStreamIds)
	for i := int16(0); i < int16(maxStreamIds); i++ {
		streamIdsQueue <- i
	}
	return &internalStreamIdMapper{
		clusterIds: streamIdsQueue,
	}
}

func (csid *internalStreamIdMapper) GetNewIdFor(_ int16) (int16, error) {
	select {
	case id, ok := <-csid.clusterIds:
		if ok {
			return id, nil
		} else {
			return -1, fmt.Errorf("stream id channel closed")
		}
	default:
		return -1, fmt.Errorf("no stream id available")
	}
}

func (csid *internalStreamIdMapper) ReleaseId(syntheticId int16) (int16, error) {
	select {
	case csid.clusterIds <- syntheticId:
	default:
		return -1, fmt.Errorf("stream ids channel full, ignoring id %v", syntheticId)
	}
	return syntheticId, nil
}

func NewStreamIdMapper(maxStreamIds int) StreamIdMapper {
	idMapper := make(map[int16]int16)
	streamIdsQueue := make(chan int16, maxStreamIds)
	for i := int16(0); i < int16(maxStreamIds); i++ {
		streamIdsQueue <- i
	}
	return &streamIdMapper{
		idMapper:   idMapper,
		clusterIds: streamIdsQueue,
	}
}

func (sim *streamIdMapper) GetNewIdFor(streamId int16) (int16, error) {
	select {
	case id, ok := <-sim.clusterIds:
		if ok {
			sim.Lock()
			if _, contains := sim.idMapper[id]; contains {
				sim.Unlock()
				return -1, fmt.Errorf("stream id collision, mapper already contains id %v", id)
			}
			sim.idMapper[id] = streamId
			sim.Unlock()
			return id, nil
		} else {
			return -1, fmt.Errorf("stream id channel closed")
		}
	default:
		return -1, fmt.Errorf("no stream id available")
	}
}

func (sim *streamIdMapper) ReleaseId(syntheticId int16) (int16, error) {
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
	default:
		return originalId, fmt.Errorf("stream ids channel full, ignoring id %v", syntheticId)
	}
	return originalId, nil
}
