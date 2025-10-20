package zdmproxy

import (
	"bytes"
	"fmt"
	"io"

	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
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
	Header() *frame.Header
	ReadFrame() ([]byte, error)
	WriteSegmentPayload(payload []byte) (completed bool, err error)
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

func NewSegmentAccumulator(buf *bytes.Buffer, codec frame.RawDecoder) SegmentAccumulator {
	return &segmentAcc{
		buf:           buf,
		accumLength:   0,
		targetLength:  0,
		hdr:           nil,
		codec:         codec,
		payloadReader: nil,
		version:       0,
		hdrBuf:        bytes.NewBuffer(make([]byte, 0, primitive.FrameHeaderLengthV3AndHigher)),
	}
}

func (a *segmentAcc) Header() *frame.Header {
	return a.hdr
}

func (a *segmentAcc) FrameReady() bool {
	return a.accumLength >= a.targetLength
}

func (a *segmentAcc) ReadFrame() ([]byte, error) {
	payload := a.buf.Bytes()
	actualPayload := payload[:a.targetLength]
	var extraBytes []byte
	if a.accumLength > a.targetLength {
		extraBytes = payload[a.targetLength:]
	}
	a.reset()
	_, err := a.WriteSegmentPayload(extraBytes)
	if err != nil {
		return nil, fmt.Errorf("could not carry over extra payload bytes to new payload: %w", err)
	}
	return actualPayload, nil
}

func (a *segmentAcc) reset() {
	a.buf = nil // do not zero/reset current buffer, just allocate a new one
	a.accumLength = 0
	a.targetLength = 0
	a.version = 0
	a.hdr = nil
	a.hdrBuf.Reset()
}

func (a *segmentAcc) WriteSegmentPayload(payload []byte) (frameReady bool, e error) {
	if len(payload) == 0 {
		return false, nil
	}

	if a.payloadReader == nil {
		a.payloadReader = bytes.NewReader(payload)
	} else {
		a.payloadReader.Reset(payload)
	}

	if a.version == 0 {
		v, err := a.readVersion(a.payloadReader)
		if err != nil {
			return false, fmt.Errorf("cannot read frame version in multipart segment: %w", err)
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
			return false, fmt.Errorf("cannot read frame header bytes: %w", err)
		}
		if done {
			a.hdr, err = a.codec.DecodeHeader(a.hdrBuf)
			if err != nil {
				return false, fmt.Errorf("cannot read frame header in multipart segment: %w", err)
			}
			a.targetLength = int(a.hdr.BodyLength)
			a.buf = bytes.NewBuffer(make([]byte, 0, a.targetLength))
		}
	}

	a.buf.Write(payload)
	a.accumLength += len(payload)
	return a.accumLength >= a.targetLength, nil
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
