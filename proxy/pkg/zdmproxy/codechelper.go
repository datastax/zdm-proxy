package zdmproxy

import (
	"bufio"
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

func newConnCodecHelper(src io.Reader, connectionAddr string, readBufferSizeBytes int, writeBufferSizeBytes int, compression *atomic.Value,
	shutdownContext context.Context) *connCodecHelper {
	writeBuffer := bytes.NewBuffer(make([]byte, 0, initialBufferSize))

	bufferedReader := bufio.NewReaderSize(src, readBufferSizeBytes)
	waitBuf := make([]byte, 1) // buf to block waiting for data (1 byte)
	waitBufReader := bytes.NewReader(waitBuf)
	return &connCodecHelper{
		state:              atomic.Pointer[connState]{},
		compression:        compression,
		src:                bufferedReader,
		segAccum:           NewSegmentAccumulator(defaultFrameCodec),
		waitReadDataBuf:    waitBuf,
		waitReadDataReader: waitBufReader,
		segWriter:          NewSegmentWriter(writeBuffer, writeBufferSizeBytes, connectionAddr, shutdownContext),
		connectionAddr:     connectionAddr,
		shutdownContext:    shutdownContext,
		dualReader:         NewDualReader(waitBufReader, bufferedReader),
	}
}

func (recv *connCodecHelper) ReadRawFrame() (*frame.RawFrame, *connState, error) {
	// Check if we already have a frame ready in the accumulator
	if recv.segAccum.FrameReady() {
		state := recv.GetState()
		if !state.useSegments {
			return nil, state, errors.New("unexpected state after checking that frame is ready to be read")
		}
		f, err := recv.segAccum.ReadFrame()
		return f, state, err
	}

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
			err = recv.segAccum.AppendSegmentPayload(sgmt.Payload.UncompressedData)
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
