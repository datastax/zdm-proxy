package zdmproxy

import "github.com/datastax/go-cassandra-native-protocol/frame"

// FrameProcessor defines how a frame should be processed before the frame is sent over the wire and how it's
// transformed back after receiving the response. This is useful for replacing stream ids with synthetic ids.
type FrameProcessor interface {
	Before(frame *frame.RawFrame) error
	After(frame *frame.RawFrame) error
}

type InternalCqlFrameProcessor interface {
	Before(frame *frame.Frame) error
	After(frame *frame.Frame)
}

// StreamIdProcessor replaces the incoming stream/request ids by internal, synthetic, ids before sending the
// frames over the wire. When the proxy receives a response, it changes the ids back to its original number.
// This way we guarantee that, through the same connection, we have non-overlapping ids with requests coming
// from the client and requests generated internally by the proxy, such as the heartbeat requests.
type streamIdProcessor struct {
	mapper StreamIdMapper
	name string
}

type internalCqlStreamIdProcessor struct {
	mapper InternalCqlStreamIdMapper
	name string
}

func NewInternalCqlStreamIdProcessor(name string) InternalCqlFrameProcessor {
	return &internalCqlStreamIdProcessor{
		mapper: NewCqlStreamIdMapper(),
		name: name,
	}
}


func NewStreamIdProcessor(name string) FrameProcessor {
	return &streamIdProcessor{
		mapper: NewStreamIdMapper(),
		name: name,
	}
}

func (sip *streamIdProcessor) Before(frame *frame.RawFrame) error {
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

func (sip *streamIdProcessor) After(frame *frame.RawFrame) error {
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

func (sip *internalCqlStreamIdProcessor) Before(frame *frame.Frame) error {
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

func (sip *internalCqlStreamIdProcessor) After(frame *frame.Frame) {
	if frame == nil {
		return
	}
	sip.mapper.ReleaseId(frame.Header.StreamId)
}