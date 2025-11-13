package zdmproxy

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
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

	n, err := a.buf.ReadFrom(a.payloadReader)
	if err != nil {
		return fmt.Errorf("cannot copy payload to buffer: %w", err)
	}
	a.accumLength += int(n)
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

func NewSegmentWriter(writeBuffer *bytes.Buffer, connectionAddr string, clientHandlerContext context.Context) *SegmentWriter {
	return &SegmentWriter{
		payload:              writeBuffer,
		connectionAddr:       connectionAddr,
		clientHandlerContext: clientHandlerContext,
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
		} else if w.payload.Len() > 0 && (w.payload.Len()+frameLength > w.maxBufferSize) {
			// if there is already data in the current payload and adding this frame to it exceeds the configured max buffer size then need to flush first
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
	state       atomic.Pointer[connState]
	compression *atomic.Value

	src                *bufio.Reader
	waitReadDataBuf    []byte // buf to block waiting for data (1 byte)
	waitReadDataReader *bytes.Reader
	dualReader         *DualReader

	segAccum SegmentAccumulator

	segWriter *SegmentWriter

	connectionAddr  string
	shutdownContext context.Context
}

func newConnCodecHelper(conn net.Conn, readBufferSizeBytes int, compression *atomic.Value, shutdownContext context.Context) *connCodecHelper {
	writeBuffer := bytes.NewBuffer(make([]byte, 0, initialBufferSize))
	connectionAddr := conn.RemoteAddr().String()

	bufferedReader := bufio.NewReaderSize(conn, readBufferSizeBytes)
	waitBuf := make([]byte, 1) // buf to block waiting for data (1 byte)
	waitBufReader := bytes.NewReader(waitBuf)
	return &connCodecHelper{
		state:              atomic.Pointer[connState]{},
		compression:        compression,
		src:                bufferedReader,
		segAccum:           NewSegmentAccumulator(defaultFrameCodec),
		waitReadDataBuf:    waitBuf,
		waitReadDataReader: waitBufReader,
		segWriter:          NewSegmentWriter(writeBuffer, connectionAddr, shutdownContext),
		connectionAddr:     connectionAddr,
		shutdownContext:    shutdownContext,
		dualReader:         NewDualReader(waitBufReader, bufferedReader),
	}
}

func (recv *connCodecHelper) ReadRawFrame() (*frame.RawFrame, *connState, error) {
	// block until data is available outside of codecHelper so that we can check the state (segments/compression)
	// before reading the frame/segment otherwise it will check the state then enter a blocking state inside a codec
	// but the state can be modified in the meantime
	_, err := io.ReadFull(recv.src, recv.waitReadDataBuf)
	if err != nil {
		return nil, nil, adaptConnErr(recv.connectionAddr, recv.shutdownContext, err)
	}
	_ = recv.waitReadDataReader.UnreadByte() // reset reader1 to initial position
	recv.dualReader.Reset()
	state := recv.GetState()
	if !state.useSegments {
		rawFrame, err := defaultFrameCodec.DecodeRawFrame(recv.dualReader) // body is not being decompressed, so we can use default codec
		if err != nil {
			return nil, state, adaptConnErr(recv.connectionAddr, recv.shutdownContext, err)
		}

		return rawFrame, state, nil
	} else {
		for !recv.segAccum.FrameReady() {
			sgmt, err := state.segmentCodec.DecodeSegment(recv.dualReader)
			if err != nil {
				return nil, state, adaptConnErr(recv.connectionAddr, recv.shutdownContext, err)
			}
			err = recv.segAccum.WriteSegmentPayload(sgmt.Payload.UncompressedData)
			if err != nil {
				return nil, state, err
			}
		}
		f, err := recv.segAccum.ReadFrame()
		return f, state, err
	}
}

// SetStartupCompression should be called as soon as the STARTUP request is received and the atomic.Value
// holding the primitive.Compression value is set. This method will update the state of this codec helper
// according to the value of Compression.
//
// This method should only be called once STARTUP is received and before the handshake proceeds because it
// will forcefully set a state where segments are disabled.
func (recv *connCodecHelper) SetStartupCompression() error {
	return recv.SetState(false)
}

// MaybeEnableSegments is a helper method to conditionally switch to segments if the provided protocol version supports them.
func (recv *connCodecHelper) MaybeEnableSegments(version primitive.ProtocolVersion) error {
	if version.SupportsModernFramingLayout() {
		return recv.SetState(true)
	}
	return nil
}

// SetState updates the state of this codec helper loading the compression type from the atomic.Value provided
// during initialization and sets the underlying codecs to use segments or not according to the parameter.
func (recv *connCodecHelper) SetState(useSegments bool) error {
	compression := recv.GetCompression()
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

func (recv *connCodecHelper) GetCompression() primitive.Compression {
	return recv.compression.Load().(primitive.Compression)
}

// DualReader returns a Reader that's the logical concatenation of
// the provided input readers. They're read sequentially. Once all
// inputs have returned EOF, Read will return EOF.  If any of the readers
// return a non-nil, non-EOF error, Read will return that error.
// It is identical to io.MultiReader but fixed to 2 readers so it avoids allocating a slice
type DualReader struct {
	reader1     io.Reader
	reader2     io.Reader
	skipReader1 bool
}

func (mr *DualReader) Read(p []byte) (n int, err error) {
	currentReader := mr.reader1
	if mr.skipReader1 {
		currentReader = mr.reader2
	}
	for currentReader != nil {
		n, err = currentReader.Read(p)
		if err == io.EOF {
			if mr.skipReader1 {
				currentReader = nil
			} else {
				mr.skipReader1 = true
				currentReader = mr.reader2
			}
		}
		if n > 0 || err != io.EOF {
			if err == io.EOF && currentReader != nil {
				err = nil
			}
			return
		}
	}
	return 0, io.EOF
}

func (mr *DualReader) Reset() {
	mr.skipReader1 = false
}

func NewDualReader(reader1 io.Reader, reader2 io.Reader) *DualReader {
	return &DualReader{reader1: reader1, reader2: reader2, skipReader1: false}
}
