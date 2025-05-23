package zdmproxy

import (
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/zdm-proxy/proxy/pkg/config"
	"github.com/datastax/zdm-proxy/proxy/pkg/metrics"
	"math"
	"sync"
)

// StreamIdMapper is used to map the incoming stream ids from the client/driver to internal ids managed by the proxy
// This is required because we also generate requests inside the proxy that go to the cluster through the same connection,
// hence they must have non-conflicting ids with user's requests.
type StreamIdMapper interface {
	// GetNewId shall be called to generate stream IDs for requests originated by ZDM internally
	GetNewId() (int16, error)
	GetNewIdFor(streamId int16) (int16, error)
	ReleaseId(syntheticId int16) (int16, error)
	Close()
}

type streamIdMapper struct {
	sync.Mutex
	idMapper        map[int16]int16
	clusterIds      chan int16
	metrics         metrics.Gauge
	protocolVersion primitive.ProtocolVersion
}

type internalStreamIdMapper struct {
	clusterIds      chan int16
	metrics         metrics.Gauge
	protocolVersion primitive.ProtocolVersion
}

// NewInternalStreamIdMapper is used to assign unique ids to frames that have no initial stream id defined, such as
// CQL queries initiated by the proxy or ASYNC requests.
func NewInternalStreamIdMapper(protocolVersion primitive.ProtocolVersion, config *config.Config, metrics metrics.Gauge) StreamIdMapper {
	maximumStreamIds := maxStreamIds(protocolVersion, config)
	streamIdsQueue := make(chan int16, maximumStreamIds)
	for i := int16(0); i < int16(maximumStreamIds); i++ {
		streamIdsQueue <- i
	}
	return &internalStreamIdMapper{
		protocolVersion: protocolVersion,
		clusterIds:      streamIdsQueue,
		metrics:         metrics,
	}
}

func (csid *internalStreamIdMapper) GetNewId() (int16, error) {
	return csid.GetNewIdFor(-1)
}

func (csid *internalStreamIdMapper) GetNewIdFor(_ int16) (int16, error) {
	// do not validate provided stream ID
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

func NewStreamIdMapper(protocolVersion primitive.ProtocolVersion, config *config.Config, metrics metrics.Gauge) StreamIdMapper {
	maximumStreamIds := maxStreamIds(protocolVersion, config)
	idMapper := make(map[int16]int16)
	streamIdsQueue := make(chan int16, maximumStreamIds)
	for i := int16(0); i < int16(maximumStreamIds); i++ {
		streamIdsQueue <- i
	}
	return &streamIdMapper{
		protocolVersion: protocolVersion,
		idMapper:        idMapper,
		clusterIds:      streamIdsQueue,
		metrics:         metrics,
	}
}

func (sim *streamIdMapper) GetNewId() (int16, error) {
	return sim.getNewIdFor(-1, false)
}

func (sim *streamIdMapper) GetNewIdFor(streamId int16) (int16, error) {
	return sim.getNewIdFor(streamId, true)
}

func (sim *streamIdMapper) getNewIdFor(streamId int16, validate bool) (int16, error) {
	if validate {
		if err := validateStreamId(sim.protocolVersion, streamId); err != nil {
			return -1, err
		}
	}
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

func maxStreamIds(protoVer primitive.ProtocolVersion, conf *config.Config) int {
	maxSupported := maxStreamIdsV3
	if protoVer == primitive.ProtocolVersion2 {
		maxSupported = maxStreamIdsV2
	}
	if maxSupported < conf.ProxyMaxStreamIds {
		return maxSupported
	}
	return conf.ProxyMaxStreamIds
}

func validateStreamId(version primitive.ProtocolVersion, streamId int16) error {
	if version < primitive.ProtocolVersion3 {
		if streamId > math.MaxInt8 || streamId < math.MinInt8 {
			return fmt.Errorf("stream id out of range for %v: %v", version, streamId)
		}
	}
	if streamId < 0 {
		return fmt.Errorf("negative stream id: %v", streamId)
	}
	return nil
}
