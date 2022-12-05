package zdmproxy

import (
	"fmt"
	log "github.com/sirupsen/logrus"
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
	sim.Lock()
	defer sim.Unlock()
	select {
	case id, ok := <-sim.clusterIds:
		if ok {
			sim.idMapper[id] = streamId
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
	defer sim.Unlock()
	var originalId = sim.idMapper[syntheticId]
	delete(sim.idMapper, syntheticId)
	select {
	case sim.clusterIds <- syntheticId:
	default:
		log.Errorf("stream ids channel full, ignoring id %v", syntheticId)
	}
	return originalId, nil
}
