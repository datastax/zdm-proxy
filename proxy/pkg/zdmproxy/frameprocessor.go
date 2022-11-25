package zdmproxy

import "github.com/datastax/go-cassandra-native-protocol/frame"

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
	mapper StreamIdMapper
	name   string
}

type internalCqlStreamIdProcessor struct {
	mapper InternalCqlStreamIdMapper
	name   string
}

func NewInternalCqlStreamIdProcessor(name string, maxStreamIds int) InternalCqlFrameProcessor {
	return &internalCqlStreamIdProcessor{
		mapper: NewCqlStreamIdMapper(maxStreamIds),
		name:   name,
	}
}

func NewStreamIdProcessor(name string, maxStreamIds int) FrameProcessor {
	return &streamIdProcessor{
		mapper: NewStreamIdMapper(maxStreamIds),
		name:   name,
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
	frame.Header.StreamId = newId
	return nil
}

func (sip *streamIdProcessor) RestoreId(frame *frame.RawFrame) error {
	if frame == nil {
		return nil
	}
	var originalId, err = sip.mapper.RestoreId(frame.Header.StreamId)
	if err != nil {
		return err
	}
	frame.Header.StreamId = originalId
	return nil
}

func (sip *streamIdProcessor) ReleaseId(frame *frame.RawFrame) {
	if frame == nil {
		return
	}
	sip.mapper.ReleaseId(frame.Header.StreamId)
}

func (sip *internalCqlStreamIdProcessor) AssignUniqueId(frame *frame.Frame) error {
	if frame == nil {
		return nil
	}
	var newId, err = sip.mapper.GetNewId()
	if err != nil {
		return err
	}
	frame.Header.StreamId = newId
	return nil
}

func (sip *internalCqlStreamIdProcessor) ReleaseId(frame *frame.Frame) {
	if frame == nil {
		return
	}
	sip.mapper.ReleaseId(frame.Header.StreamId)
}
