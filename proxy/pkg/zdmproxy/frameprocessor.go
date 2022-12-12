package zdmproxy

import (
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/zdm-proxy/proxy/pkg/metrics"
)

// FrameProcessor manages the mapping of incoming stream ids to the actual ids that are sent over to the clusters.
// This is important to prevent overlapping ids of client-side requests and proxy internal requests (such as heartbeats)
// that are written through the same connection to the cluster.
type FrameProcessor interface {
	AssignUniqueId(rawFrame *frame.RawFrame) (*frame.RawFrame, error)
	AssignUniqueIdFrame(frame *frame.Frame) (*frame.Frame, error)
	ReleaseId(rawFrame *frame.RawFrame) (*frame.RawFrame, error)
	ReleaseIdFrame(frame *frame.Frame) (*frame.Frame, error)
}

// streamIdProcessor replaces the incoming stream/request ids by internal, synthetic, ids before sending the
// frames over the wire. When the proxy receives a response, it changes the ids back to its original number.
// This way we guarantee that, through the same connection, we have non-overlapping ids with requests coming
// from the client and requests generated internally by the proxy, such as the heartbeat requests.
type streamIdProcessor struct {
	mapper   StreamIdMapper
	connType ClusterConnectorType
	metrics  metrics.Gauge
}

func NewStreamIdProcessor(mapper StreamIdMapper, connType ClusterConnectorType, metrics metrics.Gauge) FrameProcessor {
	return &streamIdProcessor{
		mapper:   mapper,
		connType: connType,
		metrics:  metrics,
	}
}

func (sip *streamIdProcessor) AssignUniqueId(rawFrame *frame.RawFrame) (*frame.RawFrame, error) {
	if rawFrame == nil {
		return rawFrame, nil
	}
	var newId, err = sip.mapper.GetNewIdFor(rawFrame.Header.StreamId)
	if err != nil {
		return rawFrame, err
	}
	if sip.metrics != nil {
		sip.metrics.Add(1)
	}
	return setRawFrameStreamId(rawFrame, newId), nil
}

func (sip *streamIdProcessor) AssignUniqueIdFrame(frame *frame.Frame) (*frame.Frame, error) {
	if frame == nil {
		return frame, nil
	}
	var newId, err = sip.mapper.GetNewIdFor(frame.Header.StreamId)
	if err != nil {
		return frame, err
	}
	if sip.metrics != nil {
		sip.metrics.Add(1)
	}
	frame.Header.StreamId = newId
	return setFrameStreamId(frame, newId), nil
}

func (sip *streamIdProcessor) ReleaseId(rawFrame *frame.RawFrame) (*frame.RawFrame, error) {
	if rawFrame == nil {
		return rawFrame, nil
	}
	var originalId, err = sip.mapper.ReleaseId(rawFrame.Header.StreamId)
	if sip.metrics != nil && err != nil {
		sip.metrics.Subtract(1)
	}
	return setRawFrameStreamId(rawFrame, originalId), err
}

func (sip *streamIdProcessor) ReleaseIdFrame(frame *frame.Frame) (*frame.Frame, error) {
	if frame == nil {
		return frame, nil
	}
	var originalId, err = sip.mapper.ReleaseId(frame.Header.StreamId)
	if sip.metrics != nil && err != nil {
		sip.metrics.Subtract(1)
	}
	return setFrameStreamId(frame, originalId), err
}

func setRawFrameStreamId(f *frame.RawFrame, id int16) *frame.RawFrame {
	// If the new id is the same as the original id (most likely an internal request), then just return the original
	// frame
	if f.Header.StreamId == id {
		return f
	}
	newHeader := f.Header.Clone()
	newHeader.StreamId = id
	return &frame.RawFrame{
		Header: newHeader,
		Body:   f.Body,
	}
}

func setFrameStreamId(f *frame.Frame, id int16) *frame.Frame {
	// If the new id is the same as the original id (most likely an internal request), then just return the original
	// frame
	if f.Header.StreamId == id {
		return f
	}
	newHeader := f.Header.Clone()
	newHeader.StreamId = id
	return &frame.Frame{
		Header: newHeader,
		Body:   f.Body,
	}
}
