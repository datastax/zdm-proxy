package zdmproxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/go-cassandra-native-protocol/segment"
)

// SegmentAccumulator provides a way for the caller to build frames from segments.
//
// The caller appends segment payloads to this accumulator by calling AppendSegmentPayload
// and then retrieves frames by calling ReadFrame.
//
// The caller can check whether a frame is ready to be read by calling FrameReady().
//
// This type is not "thread-safe".
type SegmentAccumulator interface {
	ReadFrame() (*frame.RawFrame, error)
	AppendSegmentPayload(payload []byte) error
	FrameReady() bool
}

type segmentAcc struct {
	buf           *bytes.Buffer
	accumLength   int
	targetLength  int
	hdr           *frame.Header
	codec         frame.RawDecoder
	payloadReader *bytes.Reader
	version       primitive.ProtocolVersion
	hdrBuf        *bytes.Buffer
}

func NewSegmentAccumulator(codec frame.RawDecoder) SegmentAccumulator {
	return &segmentAcc{
		buf:           nil,
		accumLength:   0,
		targetLength:  0,
		hdr:           nil,
		codec:         codec,
		payloadReader: nil,
		version:       0,
		hdrBuf:        bytes.NewBuffer(make([]byte, 0, primitive.FrameHeaderLengthV3AndHigher)),
	}
}

func (a *segmentAcc) FrameReady() bool {
	return a.accumLength >= a.targetLength && a.hdr != nil
}

func (a *segmentAcc) ReadFrame() (*frame.RawFrame, error) {
	if !a.FrameReady() {
		return nil, errors.New("frame is not ready")
	}
	payload := a.buf.Bytes()
	actualPayload := payload[:a.targetLength]
	var extraBytes []byte
	if a.accumLength > a.targetLength {
		extraBytes = payload[a.targetLength:]
	}
	hdr := a.hdr
	a.reset()
	err := a.AppendSegmentPayload(extraBytes)
	if err != nil {
		return nil, fmt.Errorf("could not carry over extra payload bytes to new payload: %w", err)
	}
	if hdr.Version.SupportsModernFramingLayout() && hdr.Flags.Contains(primitive.HeaderFlagCompressed) {
		hdr.Flags = hdr.Flags.Remove(primitive.HeaderFlagCompressed) // gocql workaround (https://issues.apache.org/jira/browse/CASSGO-98)
	}
	return &frame.RawFrame{
		Header: hdr,
		Body:   actualPayload,
	}, nil
}

func (a *segmentAcc) reset() {
	a.buf = nil // do not zero/reset current buffer, just allocate a new one
	a.accumLength = 0
	a.targetLength = 0
	a.version = 0
	a.hdr = nil
	a.hdrBuf.Reset()
}

func (a *segmentAcc) AppendSegmentPayload(payload []byte) error {
	if len(payload) == 0 {
		return nil
	}

	if a.payloadReader == nil {
		a.payloadReader = bytes.NewReader(payload)
	} else {
		a.payloadReader.Reset(payload)
	}

	if a.version == 0 {
		v, err := a.readVersion(a.payloadReader)
		if err != nil {
			return fmt.Errorf("cannot read frame version in multipart segment: %w", err)
		}
		a.version = v
	}

	if a.hdr == nil {
		remainingBytes := a.version.FrameHeaderLengthInBytes() - a.hdrBuf.Len()
		bytesToCopy := remainingBytes
		done := true
		if a.payloadReader.Len() < remainingBytes {
			bytesToCopy = a.payloadReader.Len()
			done = false
		}
		_, err := io.CopyN(a.hdrBuf, a.payloadReader, int64(bytesToCopy))
		if err != nil {
			return fmt.Errorf("cannot read frame header bytes: %w", err)
		}
		if done {
			a.hdr, err = a.codec.DecodeHeader(a.hdrBuf)
			if err != nil {
				return fmt.Errorf("cannot read frame header in multipart segment: %w", err)
			}
			a.targetLength = int(a.hdr.BodyLength)
			a.buf = bytes.NewBuffer(make([]byte, 0, a.targetLength))
		}
	}

	if a.payloadReader.Len() > 0 {
		n, err := a.buf.ReadFrom(a.payloadReader)
		if err != nil {
			return fmt.Errorf("cannot copy payload to buffer: %w", err)
		}
		a.accumLength += int(n)
	}
	return nil
}

func (a *segmentAcc) readVersion(reader *bytes.Reader) (primitive.ProtocolVersion, error) {
	versionAndDirection, err := reader.ReadByte()
	if err != nil {
		return 0, fmt.Errorf("cannot decode header version and direction: %w", err)
	}
	_ = reader.UnreadByte()

	version := primitive.ProtocolVersion(versionAndDirection & 0b0111_1111)
	err = primitive.CheckSupportedProtocolVersion(version)
	if err != nil {
		return 0, err
	}
	return version, nil
}

type SegmentWriter struct {
	payload              *bytes.Buffer
	connectionAddr       string
	clientHandlerContext context.Context
	maxBufferSize        int
}

func NewSegmentWriter(writeBuffer *bytes.Buffer, maxBufferSize int, connectionAddr string, clientHandlerContext context.Context) *SegmentWriter {
	return &SegmentWriter{
		payload:              writeBuffer,
		connectionAddr:       connectionAddr,
		clientHandlerContext: clientHandlerContext,
		maxBufferSize:        maxBufferSize,
	}
}

func FrameUncompressedLength(f *frame.RawFrame) (int, error) {
	if f.Header.Flags.Contains(primitive.HeaderFlagCompressed) {
		return -1, fmt.Errorf("cannot obtain uncompressed length of compressed frame: %v", f.String())
	}
	return f.Header.Version.FrameHeaderLengthInBytes() + len(f.Body), nil
}

func (w *SegmentWriter) GetWriteBuffer() *bytes.Buffer {
	return w.payload
}

func (w *SegmentWriter) canWriteFrameInternal(frameLength int) bool {
	if frameLength > segment.MaxPayloadLength { // frame needs multiple segments
		if w.payload.Len() > 0 {
			// if frame needs multiple segments and there is already a frame in the payload then need to flush first
			return false
		} else {
			return true
		}
	} else { // frame can be self contained
		if w.payload.Len()+frameLength > segment.MaxPayloadLength {
			// if frame can be self contained but adding it to the current payload exceeds the max length then need to flush first
			return false
		} else if w.payload.Len() >= 0 && w.payload.Len() >= w.maxBufferSize {
			// if there is already data in the current payload and it exceeds the configured max buffer size then need to flush first
			// max buffer size can be exceeded if payload is currently empty (otherwise the frame couldn't be written)
			return false
		} else {
			return true
		}
	}
}

func (w *SegmentWriter) WriteSegments(dst io.Writer, state *connState) error {
	payload := w.payload.Bytes()
	payloadLength := len(payload)

	if payloadLength <= 0 {
		return errors.New("cannot write segment with empty payload")
	}

	if payloadLength > segment.MaxPayloadLength {
		segmentCount := payloadLength / segment.MaxPayloadLength
		isExactMultiple := payloadLength%segment.MaxPayloadLength == 0
		if !isExactMultiple {
			segmentCount++
		}

		// Split the payload buffer into segments
		for i := range segmentCount {
			segmentLength := segment.MaxPayloadLength
			if i == segmentCount-1 && !isExactMultiple {
				segmentLength = payloadLength % segment.MaxPayloadLength
			}
			start := i * segment.MaxPayloadLength
			seg := &segment.Segment{
				Payload: &segment.Payload{UncompressedData: payload[start : start+segmentLength]},
				Header:  &segment.Header{IsSelfContained: false},
			}
			err := state.segmentCodec.EncodeSegment(seg, dst)
			if err != nil {
				return adaptConnErr(
					w.connectionAddr,
					w.clientHandlerContext,
					fmt.Errorf("cannot write segment %d of %d: %w", i+1, segmentCount, err))
			}
		}
	} else {
		seg := &segment.Segment{
			Payload: &segment.Payload{UncompressedData: w.payload.Bytes()},
			Header:  &segment.Header{IsSelfContained: true},
		}
		err := state.segmentCodec.EncodeSegment(seg, dst)
		if err != nil {
			return adaptConnErr(w.connectionAddr, w.clientHandlerContext, fmt.Errorf("cannot write segment: %w", err))
		}
	}
	w.payload.Reset()
	return nil
}

func (w *SegmentWriter) AppendFrameToSegmentPayload(frm *frame.RawFrame) (bool, error) {
	frameLength, err := FrameUncompressedLength(frm)
	if err != nil {
		return false, err
	}
	if !w.canWriteFrameInternal(frameLength) {
		return false, nil
	}

	err = w.writeToPayload(frm)
	if err != nil {
		return false, fmt.Errorf("cannot write frame to segment payload: %w", err)
	}
	return true, nil
}

func (w *SegmentWriter) writeToPayload(f *frame.RawFrame) error {
	// frames are always uncompressed in v5 (segments can be compressed)
	return adaptConnErr(w.connectionAddr, w.clientHandlerContext, defaultFrameCodec.EncodeRawFrame(f, w.payload))
}
