package zdmproxy

import (
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/zdm-proxy/proxy/pkg/metrics"
	log "github.com/sirupsen/logrus"
)

// FrameProcessor manages the mapping of incoming stream ids to the actual ids that are sent over to the clusters.
// This is important to prevent overlapping ids of client-side requests and proxy internal requests (such as heartbeats)
// that are written through the same connection to the cluster.
type FrameProcessor interface {
	AssignUniqueId(frame *frame.RawFrame) error
	RestoreId(frame *frame.RawFrame) error
	ReleaseId(frame *frame.RawFrame)
}

// InternalCqlFrameProcessor manages the stream ids of requests generated internally by the proxy. This is meant
// for requests that are created manually and have no initial stream id.
type InternalCqlFrameProcessor interface {
	AssignUniqueId(frame *frame.Frame) error
	ReleaseId(frame *frame.Frame)
}

// StreamIdProcessor replaces the incoming stream/request ids by internal, synthetic, ids before sending the
// frames over the wire. When the proxy receives a response, it changes the ids back to its original number.
// This way we guarantee that, through the same connection, we have non-overlapping ids with requests coming
// from the client and requests generated internally by the proxy, such as the heartbeat requests.
type streamIdProcessor struct {
	mapper   StreamIdMapper
	connType ClusterConnectorType
	metrics  metrics.Gauge
}

type internalCqlStreamIdProcessor struct {
	mapper   StreamIdMapper
	connType ClusterConnectorType
	metrics  metrics.Gauge
}

func NewInternalCqlStreamIdProcessor(connType ClusterConnectorType, maxStreamIds int, metrics metrics.Gauge) InternalCqlFrameProcessor {
	return &internalCqlStreamIdProcessor{
		mapper:   NewCqlStreamIdMapper(maxStreamIds),
		connType: connType,
		metrics:  metrics,
	}
}

func NewStreamIdProcessor(connType ClusterConnectorType, maxStreamIds int, metrics metrics.Gauge) FrameProcessor {
	return &streamIdProcessor{
		mapper:   NewStreamIdMapper(maxStreamIds),
		connType: connType,
		metrics:  metrics,
	}
}

func (sip *streamIdProcessor) AssignUniqueId(frame *frame.RawFrame) error {
	if frame == nil {
		return nil
	}
	var newId, err = sip.mapper.GetNewIdFor(frame.Header.StreamId)
	if err != nil {
		return err
	}
	setFrameStreamId(frame, newId)
	sip.metrics.Add(1)
	return nil
}

func (sip *streamIdProcessor) RestoreId(frame *frame.RawFrame) error {
	if frame == nil {
		return nil
	}
	var originalId, err = sip.mapper.RestoreId(frame.Header.StreamId)
	if err != nil {
		log.Error(err)
		return err
	}
	setFrameStreamId(frame, originalId)
	return nil
}

func (sip *streamIdProcessor) ReleaseId(frame *frame.RawFrame) {
	if frame == nil {
		return
	}
	sip.mapper.ReleaseId(frame.Header.StreamId)
	sip.metrics.Subtract(1)
}

func (sip *internalCqlStreamIdProcessor) AssignUniqueId(frame *frame.Frame) error {
	if frame == nil {
		return nil
	}
	var newId, err = sip.mapper.GetNewIdFor(frame.Header.StreamId)
	if err != nil {
		log.Error(err)
		return err
	}
	setRawFrameStreamId(frame, newId)
	sip.metrics.Add(1)
	return nil
}

func (sip *internalCqlStreamIdProcessor) ReleaseId(frame *frame.Frame) {
	if frame == nil {
		return
	}
	sip.mapper.ReleaseId(frame.Header.StreamId)
	sip.metrics.Subtract(1)
}


func setFrameStreamId(f *frame.RawFrame, id int16) {
	newHeader := f.Header.Clone()
	newHeader.StreamId = id
	f.Header = newHeader
}

func setRawFrameStreamId(f *frame.Frame, id int16) {
	newHeader := f.Header.Clone()
	newHeader.StreamId = id
	f.Header = newHeader
}