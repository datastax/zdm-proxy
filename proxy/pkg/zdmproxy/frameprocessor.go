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
	AssignUniqueIdFrame(frame *frame.Frame) error
	ReleaseId(frame *frame.RawFrame) error
	ReleaseIdFrame(frame *frame.Frame) error
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
	setRawFrameStreamId(frame, newId)
	sip.metrics.Add(1)
	return nil
}

func (sip *streamIdProcessor) AssignUniqueIdFrame(frame *frame.Frame) error {
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

func (sip *streamIdProcessor) ReleaseId(frame *frame.RawFrame) error {
	if frame == nil {
		return nil
	}
	var originalId, err = sip.mapper.ReleaseId(frame.Header.StreamId)
	if err != nil {
		log.Trace(err)
		return err
	}
	setRawFrameStreamId(frame, originalId)
	sip.metrics.Subtract(1)
	return nil
}

func (sip *streamIdProcessor) ReleaseIdFrame(frame *frame.Frame) error {
	if frame == nil {
		return nil
	}
	var _, err = sip.mapper.ReleaseId(frame.Header.StreamId)
	if err != nil {
		log.Trace(err)
		return err
	}
	sip.metrics.Subtract(1)
	return nil
}

func setRawFrameStreamId(f *frame.RawFrame, id int16) {
	newHeader := f.Header.Clone()
	newHeader.StreamId = id
	f.Header = newHeader
}

func setFrameStreamId(f *frame.Frame, id int16) {
	newHeader := f.Header.Clone()
	newHeader.StreamId = id
	f.Header = newHeader
}
