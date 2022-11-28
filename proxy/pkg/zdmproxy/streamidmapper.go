package zdmproxy

import (
	"fmt"
	"sync"
)

var maxStreamIds int16 = 2048

// StreamIdMapper is used to map the incoming stream ids from the client/driver to internal ids managed by the proxy
// This is required because we also generate requests inside the proxy that go to the cluster through the same connection,
// hence they must have non-conflicting ids with user's requests.
type StreamIdMapper interface {
	GetNewIdFor(streamId int16) (int16, error)
	RestoreId(syntheticId int16) (int16, error)
	ReleaseId(syntheticId int16) (int16, error)
}

type cqlStreamIdMapper struct {
	sync.Mutex
	ids chan int16
}

func NewCqlStreamIdMapper(maxStreamIds int) StreamIdMapper {
	var ids = make(chan int16, maxStreamIds)
	for i := int16(0); i < int16(maxStreamIds); i++ {
		ids <- i
	}
	return &cqlStreamIdMapper{
		ids: ids,
	}
}

func (icsim *cqlStreamIdMapper) GetNewIdFor(_ int16) (int16, error) {
	icsim.Lock()
	defer icsim.Unlock()
	return <-icsim.ids, nil
}

func (icsim *cqlStreamIdMapper) RestoreId(_ int16) (int16, error) {
	return -1, fmt.Errorf("internal CQL ids have no original ids to be restored")
}

func (icsim *cqlStreamIdMapper) ReleaseId(syntheticId int16) (int16, error) {
	icsim.Lock()
	defer icsim.Unlock()
	icsim.ids <- syntheticId
	return -1, nil
}

type streamIdMapper struct {
	sync.Mutex
	idMapper   map[int16]int16
	synMapper  map[int16]int16
	clusterIds chan int16
}

func NewStreamIdMapper(maxStreamIds int) StreamIdMapper {
	idMapper := make(map[int16]int16)
	synMapper := make(map[int16]int16)
	streamIdsQueue := make(chan int16, maxStreamIds)
	for i := int16(0); i < int16(maxStreamIds); i++ {
		streamIdsQueue <- i
	}
	return &streamIdMapper{
		idMapper:   idMapper,
		clusterIds: streamIdsQueue,
		synMapper:  synMapper,
	}
}

func (sim *streamIdMapper) GetNewIdFor(streamId int16) (int16, error) {
	sim.Lock()
	defer sim.Unlock()
	syntheticId, contains := sim.idMapper[streamId]
	if contains {
		return syntheticId, nil
	}
	syntheticId = <-sim.clusterIds
	sim.idMapper[streamId] = syntheticId
	sim.synMapper[syntheticId] = streamId
	return syntheticId, nil
}

func (sim *streamIdMapper) RestoreId(syntheticId int16) (int16, error) {
	sim.Lock()
	defer sim.Unlock()
	originalId, contains := sim.synMapper[syntheticId]
	if contains {
		return originalId, nil
	}
	return -1, fmt.Errorf("no matching id found for synthetic id %v", syntheticId)
}

func (sim *streamIdMapper) ReleaseId(syntheticId int16) (int16, error) {
	sim.Lock()
	defer sim.Unlock()
	var originalId = sim.synMapper[syntheticId]
	delete(sim.idMapper, originalId)
	delete(sim.synMapper, syntheticId)

	sim.clusterIds <- syntheticId
	return originalId, nil
}
