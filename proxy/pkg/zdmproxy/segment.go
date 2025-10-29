package zdmproxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/go-cassandra-native-protocol/segment"
)

// SegmentAccumulator provides a way for the caller to build frames from segments.
//
// The caller appends segment payloads to this accumulator by calling WriteSegmentPayload
// and then retrieves frames by calling ReadFrame.
//
// The caller can check whether a frame is ready to be read by checking the boolean output of WriteSegmentPayload
// or calling FrameReady().
//
// This type is not "thread-safe".
type SegmentAccumulator interface {
	ReadFrame() (*frame.RawFrame, error)
	WriteSegmentPayload(payload []byte) error
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
	err := a.WriteSegmentPayload(extraBytes)
	if err != nil {
		return nil, fmt.Errorf("could not carry over extra payload bytes to new payload: %w", err)
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

func (a *segmentAcc) WriteSegmentPayload(payload []byte) error {
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
		if len(payload) < remainingBytes {
			bytesToCopy = len(payload)
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

	a.buf.Write(payload)
	a.accumLength += len(payload)
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

type connState struct {
	useSegments  bool // Protocol v5+ outer frame (segment) handling. See: https://github.com/apache/cassandra/blob/c713132aa6c20305a4a0157e9246057925ccbf78/doc/native_protocol_v5.spec
	frameCodec   frame.RawCodec
	segmentCodec segment.Codec
}

var emptyConnState = &connState{
	useSegments:  false,
	frameCodec:   defaultFrameCodec,
	segmentCodec: nil,
}

type connCodecHelper struct {
	src      io.Reader
	state    atomic.Pointer[connState]
	segAccum SegmentAccumulator
}

func newConnCodecHelper(src io.Reader) *connCodecHelper {
	return &connCodecHelper{
		src:      src,
		segAccum: NewSegmentAccumulator(defaultFrameCodec),
	}
}

func (recv *connCodecHelper) ReadRawFrame(reader io.Reader, connectionAddr string, ctx context.Context) (*frame.RawFrame, error) {
	state := recv.GetState()
	if !state.useSegments {
		rawFrame, err := defaultFrameCodec.DecodeRawFrame(reader) // body is not being decompressed, so we can use default codec
		if err != nil {
			return nil, adaptConnErr(connectionAddr, ctx, err)
		}

		return rawFrame, nil
	} else {
		for !recv.segAccum.FrameReady() {
			sgmt, err := state.segmentCodec.DecodeSegment(reader)
			if err != nil {
				return nil, adaptConnErr(connectionAddr, ctx, err)
			}
			err = recv.segAccum.WriteSegmentPayload(sgmt.Payload.UncompressedData)
			if err != nil {
				return nil, err
			}
		}
		return recv.segAccum.ReadFrame()
	}
}

func (recv *connCodecHelper) SetState(compression primitive.Compression, useSegments bool) error {
	if useSegments {
		sCodec, ok := segmentCodecs[compression]
		if !ok {
			return fmt.Errorf("unknown segment compression %v", compression)
		}
		recv.state.Store(&connState{
			useSegments:  true,
			frameCodec:   defaultFrameCodec,
			segmentCodec: sCodec,
		})
		return nil
	}

	fCodec, ok := frameCodecs[compression]
	if !ok {
		return fmt.Errorf("unknown frame compression %v", compression)
	}
	recv.state.Store(&connState{
		useSegments:  false,
		frameCodec:   fCodec,
		segmentCodec: nil,
	})
	return nil
}

func (recv *connCodecHelper) GetState() *connState {
	state := recv.state.Load()
	if state == nil {
		return emptyConnState
	}
	return state
}
