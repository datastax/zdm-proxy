package zdmproxy

// This file contains integration tests for connCodecHelper.
//
// These tests use the top-level connCodecHelper API (ReadRawFrame, SetState, etc.) to test
// frame and segment handling as it would be used in production. This provides integration-level
// testing of the complete codec helper pipeline.
//
// Tests that require direct access to internal components (SegmentAccumulator, SegmentWriter,
// DualReader) remain in segment_test.go.
//
// Key scenarios tested here:
// - Reading single and multiple frames with/without segmentation
// - Protocol version transitions (v3, v4, v5)
// - Large frames split across multiple segments
// - Multiple envelopes in one segment
// - Partial envelope data across segments
// - State management and compression

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"testing"

	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/go-cassandra-native-protocol/segment"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper to create a connCodecHelper for testing with a buffer as source
func createTestConnCodecHelper(src *bytes.Buffer) *connCodecHelper {
	compression := &atomic.Value{}
	compression.Store(primitive.CompressionNone)
	ctx := context.Background()
	return newConnCodecHelper(src, "test-addr:9042", 4096, compression, ctx)
}

// Helper to write a frame as a segment to a buffer
func writeFrameAsSegment(t *testing.T, buf *bytes.Buffer, frm *frame.RawFrame, useSegments bool) {
	if useSegments {
		// Encode frame to get envelope
		envelopeBytes := encodeRawFrameToBytes(t, frm)
		
		// Wrap in segment
		seg := &segment.Segment{
			Payload: &segment.Payload{UncompressedData: envelopeBytes},
			Header:  &segment.Header{IsSelfContained: true},
		}
		
		err := defaultSegmentCodec.EncodeSegment(seg, buf)
		require.NoError(t, err)
	} else {
		// Write frame directly (no segmentation)
		err := defaultFrameCodec.EncodeRawFrame(frm, buf)
		require.NoError(t, err)
	}
}

// TestConnCodecHelper_ReadSingleFrame_NoSegments tests reading a single frame without segmentation (v4)
func TestConnCodecHelper_ReadSingleFrame_NoSegments(t *testing.T) {
	// Create a test frame
	bodyContent := []byte("test query body")
	testFrame := createTestRawFrame(primitive.ProtocolVersion4, 1, bodyContent)
	
	// Write frame to buffer (no segments for v4)
	buf := &bytes.Buffer{}
	writeFrameAsSegment(t, buf, testFrame, false)
	
	// Create codec helper
	helper := createTestConnCodecHelper(buf)
	
	// Read the frame
	readFrame, state, err := helper.ReadRawFrame()
	require.NoError(t, err)
	require.NotNil(t, readFrame)
	require.NotNil(t, state)
	
	// Verify state shows no segments
	assert.False(t, state.useSegments)
	
	// Verify the frame
	assert.Equal(t, testFrame.Header.Version, readFrame.Header.Version)
	assert.Equal(t, testFrame.Header.StreamId, readFrame.Header.StreamId)
	assert.Equal(t, testFrame.Header.OpCode, readFrame.Header.OpCode)
	assert.Equal(t, testFrame.Body, readFrame.Body)
}

// TestConnCodecHelper_ReadSingleFrame_WithSegments tests reading a single frame with v5 segmentation
func TestConnCodecHelper_ReadSingleFrame_WithSegments(t *testing.T) {
	// Create a test frame
	bodyContent := []byte("test query body for v5")
	testFrame := createTestRawFrame(primitive.ProtocolVersion5, 1, bodyContent)
	
	// Write frame as segment to buffer
	buf := &bytes.Buffer{}
	writeFrameAsSegment(t, buf, testFrame, true)
	
	// Create codec helper and enable segments
	helper := createTestConnCodecHelper(buf)
	err := helper.MaybeEnableSegments(primitive.ProtocolVersion5)
	require.NoError(t, err)
	
	// Read the frame
	readFrame, state, err := helper.ReadRawFrame()
	require.NoError(t, err)
	require.NotNil(t, readFrame)
	require.NotNil(t, state)
	
	// Verify state shows segments enabled
	assert.True(t, state.useSegments)
	
	// Verify the frame
	assert.Equal(t, testFrame.Header.Version, readFrame.Header.Version)
	assert.Equal(t, testFrame.Header.StreamId, readFrame.Header.StreamId)
	assert.Equal(t, testFrame.Header.OpCode, readFrame.Header.OpCode)
	assert.Equal(t, testFrame.Body, readFrame.Body)
}

// TestConnCodecHelper_ReadMultipleFrames_NoSegments tests reading multiple frames without segmentation
func TestConnCodecHelper_ReadMultipleFrames_NoSegments(t *testing.T) {
	// Create multiple test frames
	frame1 := createTestRawFrame(primitive.ProtocolVersion4, 1, []byte("first frame"))
	frame2 := createTestRawFrame(primitive.ProtocolVersion4, 2, []byte("second frame"))
	frame3 := createTestRawFrame(primitive.ProtocolVersion4, 3, []byte("third frame"))
	
	// Write frames to buffer
	buf := &bytes.Buffer{}
	writeFrameAsSegment(t, buf, frame1, false)
	writeFrameAsSegment(t, buf, frame2, false)
	writeFrameAsSegment(t, buf, frame3, false)
	
	// Create codec helper
	helper := createTestConnCodecHelper(buf)
	
	// Read and verify each frame
	frames := []*frame.RawFrame{frame1, frame2, frame3}
	for i, expectedFrame := range frames {
		readFrame, _, err := helper.ReadRawFrame()
		require.NoError(t, err, "Failed to read frame %d", i+1)
		require.NotNil(t, readFrame)
		
		assert.Equal(t, expectedFrame.Header.StreamId, readFrame.Header.StreamId,
			"Frame %d stream ID mismatch", i+1)
		assert.Equal(t, expectedFrame.Body, readFrame.Body,
			"Frame %d body mismatch", i+1)
	}
}

// TestConnCodecHelper_ReadMultipleFrames_WithSegments tests reading multiple frames with v5 segmentation
func TestConnCodecHelper_ReadMultipleFrames_WithSegments(t *testing.T) {
	// Create multiple test frames
	frame1 := createTestRawFrame(primitive.ProtocolVersion5, 1, []byte("first v5 frame"))
	frame2 := createTestRawFrame(primitive.ProtocolVersion5, 2, []byte("second v5 frame"))
	frame3 := createTestRawFrame(primitive.ProtocolVersion5, 3, []byte("third v5 frame"))
	
	// Write frames as segments to buffer
	buf := &bytes.Buffer{}
	writeFrameAsSegment(t, buf, frame1, true)
	writeFrameAsSegment(t, buf, frame2, true)
	writeFrameAsSegment(t, buf, frame3, true)
	
	// Create codec helper and enable segments
	helper := createTestConnCodecHelper(buf)
	err := helper.MaybeEnableSegments(primitive.ProtocolVersion5)
	require.NoError(t, err)
	
	// Read and verify each frame
	frames := []*frame.RawFrame{frame1, frame2, frame3}
	for i, expectedFrame := range frames {
		readFrame, state, err := helper.ReadRawFrame()
		require.NoError(t, err, "Failed to read frame %d", i+1)
		require.NotNil(t, readFrame)
		assert.True(t, state.useSegments, "Segments should be enabled")
		
		assert.Equal(t, expectedFrame.Header.StreamId, readFrame.Header.StreamId,
			"Frame %d stream ID mismatch", i+1)
		assert.Equal(t, expectedFrame.Body, readFrame.Body,
			"Frame %d body mismatch", i+1)
	}
}

// TestConnCodecHelper_SingleSegmentFrame tests reading a frame from a single self-contained segment
func TestConnCodecHelper_SingleSegmentFrame(t *testing.T) {
	// Create a test frame
	bodyContent := []byte("test query body")
	testFrame := createTestRawFrame(primitive.ProtocolVersion5, 1, bodyContent)
	
	// Write frame as a self-contained segment to buffer
	buf := &bytes.Buffer{}
	writeFrameAsSegment(t, buf, testFrame, true)
	
	// Create codec helper and enable segments
	helper := createTestConnCodecHelper(buf)
	err := helper.MaybeEnableSegments(primitive.ProtocolVersion5)
	require.NoError(t, err)
	
	// Verify frame is ready state is correct (internal check through reading)
	readFrame, state, err := helper.ReadRawFrame()
	require.NoError(t, err)
	require.NotNil(t, readFrame)
	require.True(t, state.useSegments)
	
	// Verify the frame
	assert.Equal(t, testFrame.Header.Version, readFrame.Header.Version)
	assert.Equal(t, testFrame.Header.StreamId, readFrame.Header.StreamId)
	assert.Equal(t, testFrame.Header.OpCode, readFrame.Header.OpCode)
	assert.Equal(t, testFrame.Body, readFrame.Body)
}

// TestConnCodecHelper_MultipleSegmentPayloads tests accumulating a frame from multiple non-self-contained segments
func TestConnCodecHelper_MultipleSegmentPayloads(t *testing.T) {
	// Create a frame with larger body
	bodyContent := make([]byte, 100)
	for i := range bodyContent {
		bodyContent[i] = byte(i % 256)
	}
	testFrame := createTestRawFrame(primitive.ProtocolVersion5, 2, bodyContent)
	
	// Encode the frame
	fullPayload := encodeRawFrameToBytes(t, testFrame)
	
	// Split the payload into multiple non-self-contained segments
	buf := &bytes.Buffer{}
	part1 := fullPayload[:40]  // First part
	part2 := fullPayload[40:]  // Rest
	
	// Write first non-self-contained segment
	seg1 := &segment.Segment{
		Payload: &segment.Payload{UncompressedData: part1},
		Header:  &segment.Header{IsSelfContained: false},
	}
	err := defaultSegmentCodec.EncodeSegment(seg1, buf)
	require.NoError(t, err)
	
	// Write second non-self-contained segment
	seg2 := &segment.Segment{
		Payload: &segment.Payload{UncompressedData: part2},
		Header:  &segment.Header{IsSelfContained: false},
	}
	err = defaultSegmentCodec.EncodeSegment(seg2, buf)
	require.NoError(t, err)
	
	// Create codec helper and enable segments
	helper := createTestConnCodecHelper(buf)
	err = helper.MaybeEnableSegments(primitive.ProtocolVersion5)
	require.NoError(t, err)
	
	// Read the frame (should accumulate from both segments automatically)
	readFrame, state, err := helper.ReadRawFrame()
	require.NoError(t, err)
	require.NotNil(t, readFrame)
	require.True(t, state.useSegments)
	
	// Verify the frame
	assert.Equal(t, testFrame.Header.Version, readFrame.Header.Version)
	assert.Equal(t, testFrame.Header.StreamId, readFrame.Header.StreamId)
	assert.Equal(t, testFrame.Body, readFrame.Body)
}

// TestConnCodecHelper_SequentialFramesInSeparateSegments tests reading multiple frames,
// each in its own self-contained segment
func TestConnCodecHelper_SequentialFramesInSeparateSegments(t *testing.T) {
	// Create multiple test frames
	frame1 := createTestRawFrame(primitive.ProtocolVersion5, 1, []byte("first frame"))
	frame2 := createTestRawFrame(primitive.ProtocolVersion5, 2, []byte("second frame"))
	frame3 := createTestRawFrame(primitive.ProtocolVersion5, 3, []byte("third frame"))
	
	// Write each frame as a separate self-contained segment to buffer
	buf := &bytes.Buffer{}
	writeFrameAsSegment(t, buf, frame1, true)
	writeFrameAsSegment(t, buf, frame2, true)
	writeFrameAsSegment(t, buf, frame3, true)
	
	// Create codec helper and enable segments
	helper := createTestConnCodecHelper(buf)
	err := helper.MaybeEnableSegments(primitive.ProtocolVersion5)
	require.NoError(t, err)
	
	// Read and verify each frame
	frames := []*frame.RawFrame{frame1, frame2, frame3}
	for i, expectedFrame := range frames {
		readFrame, state, err := helper.ReadRawFrame()
		require.NoError(t, err, "Failed to read frame %d", i+1)
		require.NotNil(t, readFrame)
		require.True(t, state.useSegments)
		
		assert.Equal(t, expectedFrame.Header.StreamId, readFrame.Header.StreamId,
			"Frame %d stream ID mismatch", i+1)
		assert.Equal(t, expectedFrame.Body, readFrame.Body,
			"Frame %d body mismatch", i+1)
	}
}

// TestConnCodecHelper_EmptyBufferEOF tests that reading from empty buffer returns EOF
func TestConnCodecHelper_EmptyBufferEOF(t *testing.T) {
	// Create empty buffer
	buf := &bytes.Buffer{}
	helper := createTestConnCodecHelper(buf)
	err := helper.MaybeEnableSegments(primitive.ProtocolVersion5)
	require.NoError(t, err)
	
	// Try to read - should get EOF
	readFrame, _, err := helper.ReadRawFrame()
	require.Error(t, err)
	require.Nil(t, readFrame)
	assert.Contains(t, err.Error(), "EOF")
}

// TestConnCodecHelper_MultipleEnvelopesInOneSegment tests that connCodecHelper can handle
// multiple envelopes packed into a single self-contained segment (per Protocol v5 spec Section 1).
// This is a CRITICAL test - if it fails, it indicates a bug in connCodecHelper.ReadRawFrame()
// where it doesn't check the internal accumulator before reading from the network.
func TestConnCodecHelper_MultipleEnvelopesInOneSegment(t *testing.T) {
	testCases := []struct {
		name          string
		envelopeCount int
	}{
		{name: "Two envelopes in one segment", envelopeCount: 2},
		{name: "Three envelopes in one segment", envelopeCount: 3},
		{name: "Four envelopes in one segment", envelopeCount: 4},
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create multiple envelopes
			var envelopes []*frame.RawFrame
			var combinedEnvelopePayload []byte
			
			for i := 0; i < tc.envelopeCount; i++ {
				bodyContent := []byte(fmt.Sprintf("envelope_%d_data", i+1))
				envelope := createTestRawFrame(primitive.ProtocolVersion5, int16(i+1), bodyContent)
				envelopes = append(envelopes, envelope)
				
				// Encode envelope and append to combined payload
				encodedEnvelope := encodeRawFrameToBytes(t, envelope)
				combinedEnvelopePayload = append(combinedEnvelopePayload, encodedEnvelope...)
			}
			
			// Create ONE segment containing all envelopes
			buf := &bytes.Buffer{}
			seg := &segment.Segment{
				Payload: &segment.Payload{UncompressedData: combinedEnvelopePayload},
				Header:  &segment.Header{IsSelfContained: true},
			}
			err := defaultSegmentCodec.EncodeSegment(seg, buf)
			require.NoError(t, err)
			
			// Create codec helper and enable segments
			helper := createTestConnCodecHelper(buf)
			err = helper.MaybeEnableSegments(primitive.ProtocolVersion5)
			require.NoError(t, err)
			
			// Read all envelopes back - THIS IS THE BUG TEST
			// If ReadRawFrame() doesn't check the accumulator first, it will fail with EOF
			// on the second call instead of returning the cached envelope
			for i := 0; i < tc.envelopeCount; i++ {
				readEnvelope, state, err := helper.ReadRawFrame()
				
				// If this fails with EOF on i > 0, it's the bug!
				require.NoError(t, err, 
					"BUG: Failed to read envelope %d of %d - ReadRawFrame() should check accumulator before reading from source", 
					i+1, tc.envelopeCount)
				require.NotNil(t, readEnvelope)
				assert.True(t, state.useSegments)
				
				// Verify envelope content
				assert.Equal(t, envelopes[i].Header.StreamId, readEnvelope.Header.StreamId,
					"Envelope %d stream ID mismatch", i+1)
				assert.Equal(t, envelopes[i].Body, readEnvelope.Body,
					"Envelope %d body mismatch", i+1)
			}
		})
	}
}

// TestConnCodecHelper_LargeFrameMultipleSegments tests reading a large frame split across multiple segments
func TestConnCodecHelper_LargeFrameMultipleSegments(t *testing.T) {
	// Create a large frame that will require multiple segments
	largeBody := make([]byte, segment.MaxPayloadLength*2+1000)
	for i := range largeBody {
		largeBody[i] = byte(i % 256)
	}
	testFrame := createTestRawFrame(primitive.ProtocolVersion5, 1, largeBody)
	
	// Encode the frame
	envelopeBytes := encodeRawFrameToBytes(t, testFrame)
	
	// Split into multiple non-self-contained segments
	buf := &bytes.Buffer{}
	payloadLength := len(envelopeBytes)
	
	for offset := 0; offset < payloadLength; offset += segment.MaxPayloadLength {
		end := offset + segment.MaxPayloadLength
		if end > payloadLength {
			end = payloadLength
		}
		
		seg := &segment.Segment{
			Payload: &segment.Payload{UncompressedData: envelopeBytes[offset:end]},
			Header:  &segment.Header{IsSelfContained: false}, // Not self-contained
		}
		err := defaultSegmentCodec.EncodeSegment(seg, buf)
		require.NoError(t, err)
	}
	
	// Create codec helper and enable segments
	helper := createTestConnCodecHelper(buf)
	err := helper.MaybeEnableSegments(primitive.ProtocolVersion5)
	require.NoError(t, err)
	
	// Read the frame (should accumulate from multiple segments)
	readFrame, state, err := helper.ReadRawFrame()
	require.NoError(t, err)
	require.NotNil(t, readFrame)
	assert.True(t, state.useSegments)
	
	// Verify the frame
	assert.Equal(t, testFrame.Header.Version, readFrame.Header.Version)
	assert.Equal(t, testFrame.Header.StreamId, readFrame.Header.StreamId)
	assert.Equal(t, testFrame.Body, readFrame.Body)
}

// TestConnCodecHelper_StateTransitions tests state transitions for enabling/disabling segments
func TestConnCodecHelper_StateTransitions(t *testing.T) {
	buf := &bytes.Buffer{}
	helper := createTestConnCodecHelper(buf)
	
	// Initially, state should be empty (no segments)
	state := helper.GetState()
	assert.False(t, state.useSegments)
	
	// Enable segments for v5
	err := helper.MaybeEnableSegments(primitive.ProtocolVersion5)
	require.NoError(t, err)
	
	state = helper.GetState()
	assert.True(t, state.useSegments)
	assert.NotNil(t, state.segmentCodec)
	
	// Disable segments (e.g., for startup)
	err = helper.SetStartupCompression()
	require.NoError(t, err)
	
	state = helper.GetState()
	assert.False(t, state.useSegments)
	
	// Enable again for v5
	err = helper.MaybeEnableSegments(primitive.ProtocolVersion5)
	require.NoError(t, err)
	
	state = helper.GetState()
	assert.True(t, state.useSegments)
}

// TestConnCodecHelper_MixedProtocolVersions tests handling different protocol versions
func TestConnCodecHelper_MixedProtocolVersions(t *testing.T) {
	testCases := []struct {
		name            string
		version         primitive.ProtocolVersion
		shouldUseSegments bool
	}{
		{name: "v3 - no segments", version: primitive.ProtocolVersion3, shouldUseSegments: false},
		{name: "v4 - no segments", version: primitive.ProtocolVersion4, shouldUseSegments: false},
		{name: "v5 - with segments", version: primitive.ProtocolVersion5, shouldUseSegments: true},
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a test frame
			bodyContent := []byte(fmt.Sprintf("test for %s", tc.name))
			testFrame := createTestRawFrame(tc.version, 1, bodyContent)
			
			// Write frame to buffer
			buf := &bytes.Buffer{}
			writeFrameAsSegment(t, buf, testFrame, tc.shouldUseSegments)
			
			// Create codec helper
			helper := createTestConnCodecHelper(buf)
			
			// Enable segments if protocol supports it
			err := helper.MaybeEnableSegments(tc.version)
			require.NoError(t, err)
			
			// Verify state
			state := helper.GetState()
			assert.Equal(t, tc.shouldUseSegments, state.useSegments,
				"Segment usage mismatch for %s", tc.name)
			
			// Read and verify frame
			readFrame, _, err := helper.ReadRawFrame()
			require.NoError(t, err)
			require.NotNil(t, readFrame)
			
			assert.Equal(t, testFrame.Header.Version, readFrame.Header.Version)
			assert.Equal(t, testFrame.Body, readFrame.Body)
		})
	}
}

// TestConnCodecHelper_PartialEnvelopeAcrossSegments tests the edge case where a single envelope
// (frame) is split across multiple segments with partial header bytes.
// This ensures that connCodecHelper correctly accumulates partial envelope data across segments.
func TestConnCodecHelper_PartialEnvelopeAcrossSegments(t *testing.T) {
	// Create a test frame
	bodyContent := []byte("test body content for edge case")
	testFrame := createTestRawFrame(primitive.ProtocolVersion5, 1, bodyContent)
	fullEnvelope := encodeRawFrameToBytes(t, testFrame)

	// Protocol v5 header is 9 bytes
	// Split envelope across 3 segments:
	// Segment 1: First 3 bytes of envelope header (incomplete)
	// Segment 2: Next 4 bytes of header (bytes 3-6, still incomplete - total 7 < 9)
	// Segment 3: Remaining header bytes (bytes 7-8) + body
	
	buf := &bytes.Buffer{}
	
	// Write segment 1 with partial header (3 bytes)
	seg1 := &segment.Segment{
		Payload: &segment.Payload{UncompressedData: fullEnvelope[:3]},
		Header:  &segment.Header{IsSelfContained: false},
	}
	err := defaultSegmentCodec.EncodeSegment(seg1, buf)
	require.NoError(t, err)
	
	// Write segment 2 with more partial header (4 bytes)
	seg2 := &segment.Segment{
		Payload: &segment.Payload{UncompressedData: fullEnvelope[3:7]},
		Header:  &segment.Header{IsSelfContained: false},
	}
	err = defaultSegmentCodec.EncodeSegment(seg2, buf)
	require.NoError(t, err)
	
	// Write segment 3 with remaining header + body
	seg3 := &segment.Segment{
		Payload: &segment.Payload{UncompressedData: fullEnvelope[7:]},
		Header:  &segment.Header{IsSelfContained: false},
	}
	err = defaultSegmentCodec.EncodeSegment(seg3, buf)
	require.NoError(t, err)
	
	// Create codec helper and enable segments
	helper := createTestConnCodecHelper(buf)
	err = helper.MaybeEnableSegments(primitive.ProtocolVersion5)
	require.NoError(t, err)
	
	// Read the frame - should succeed despite header being split across 3 segments
	readFrame, state, err := helper.ReadRawFrame()
	require.NoError(t, err)
	require.NotNil(t, readFrame)
	assert.True(t, state.useSegments)
	
	// Verify frame content
	assert.Equal(t, testFrame.Header.StreamId, readFrame.Header.StreamId)
	assert.Equal(t, testFrame.Body, readFrame.Body)
}

// TestConnCodecHelper_HeaderCompletionWithBodyInSegment tests the edge case where one segment
// completes the envelope header AND contains body bytes.
// This ensures the accumulator correctly transitions from header parsing to body accumulation.
func TestConnCodecHelper_HeaderCompletionWithBodyInSegment(t *testing.T) {
	// Create a test frame with larger body
	bodyContent := make([]byte, 50)
	for i := range bodyContent {
		bodyContent[i] = byte(i)
	}
	testFrame := createTestRawFrame(primitive.ProtocolVersion5, 1, bodyContent)
	fullEnvelope := encodeRawFrameToBytes(t, testFrame)

	// v5 header is 9 bytes
	// Segment 1: First 7 bytes of header (incomplete)
	// Segment 2: Remaining 2 header bytes (7-8) + first 11 body bytes (9-19)
	//            This segment completes header AND has body data
	// Segment 3: Remaining body bytes (20+)
	
	buf := &bytes.Buffer{}
	
	// Write segment 1 with partial header (7 bytes)
	seg1 := &segment.Segment{
		Payload: &segment.Payload{UncompressedData: fullEnvelope[:7]},
		Header:  &segment.Header{IsSelfContained: false},
	}
	err := defaultSegmentCodec.EncodeSegment(seg1, buf)
	require.NoError(t, err)
	
	// Write segment 2 with header completion + some body bytes
	seg2 := &segment.Segment{
		Payload: &segment.Payload{UncompressedData: fullEnvelope[7:20]},
		Header:  &segment.Header{IsSelfContained: false},
	}
	err = defaultSegmentCodec.EncodeSegment(seg2, buf)
	require.NoError(t, err)
	
	// Write segment 3 with remaining body bytes
	seg3 := &segment.Segment{
		Payload: &segment.Payload{UncompressedData: fullEnvelope[20:]},
		Header:  &segment.Header{IsSelfContained: false},
	}
	err = defaultSegmentCodec.EncodeSegment(seg3, buf)
	require.NoError(t, err)
	
	// Create codec helper and enable segments
	helper := createTestConnCodecHelper(buf)
	err = helper.MaybeEnableSegments(primitive.ProtocolVersion5)
	require.NoError(t, err)
	
	// Read the frame
	readFrame, state, err := helper.ReadRawFrame()
	require.NoError(t, err)
	require.NotNil(t, readFrame)
	assert.True(t, state.useSegments)
	
	// Verify frame content
	assert.Equal(t, testFrame.Header.StreamId, readFrame.Header.StreamId)
	assert.Equal(t, bodyContent, readFrame.Body)
}

// === DualReader Tests ===
// DualReader is an internal component of connCodecHelper

// TestDualReader_NewDualReader tests the constructor
func TestDualReader_NewDualReader(t *testing.T) {
	reader1 := bytes.NewReader([]byte("data1"))
	reader2 := bytes.NewReader([]byte("data2"))

	dualReader := NewDualReader(reader1, reader2)

	require.NotNil(t, dualReader)
	assert.Equal(t, reader1, dualReader.reader1)
	assert.Equal(t, reader2, dualReader.reader2)
	assert.False(t, dualReader.skipReader1)
}

// TestDualReader_Read tests reading from both readers
func TestDualReader_Read(t *testing.T) {
	data1 := []byte("first reader data")
	data2 := []byte("second reader data")
	reader1 := bytes.NewReader(data1)
	reader2 := bytes.NewReader(data2)

	dualReader := NewDualReader(reader1, reader2)

	// Read all data
	result := make([]byte, len(data1)+len(data2))
	n, err := io.ReadFull(dualReader, result)
	require.NoError(t, err)
	assert.Equal(t, len(data1)+len(data2), n)

	// Verify data
	expectedData := append(data1, data2...)
	assert.Equal(t, expectedData, result)

	// Further reads should return EOF
	buf := make([]byte, 10)
	n, err = dualReader.Read(buf)
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)
}

// TestDualReader_Read_FirstReaderOnly tests reading when second reader is empty
func TestDualReader_Read_FirstReaderOnly(t *testing.T) {
	data1 := []byte("only first reader")
	reader1 := bytes.NewReader(data1)
	reader2 := bytes.NewReader([]byte{})

	dualReader := NewDualReader(reader1, reader2)

	result := make([]byte, len(data1))
	n, err := io.ReadFull(dualReader, result)
	require.NoError(t, err)
	assert.Equal(t, len(data1), n)
	assert.Equal(t, data1, result)
}

// TestDualReader_Read_SecondReaderOnly tests reading when first reader is empty
func TestDualReader_Read_SecondReaderOnly(t *testing.T) {
	data2 := []byte("only second reader")
	reader1 := bytes.NewReader([]byte{})
	reader2 := bytes.NewReader(data2)

	dualReader := NewDualReader(reader1, reader2)

	result := make([]byte, len(data2))
	n, err := io.ReadFull(dualReader, result)
	require.NoError(t, err)
	assert.Equal(t, len(data2), n)
	assert.Equal(t, data2, result)
}

// TestDualReader_Reset tests resetting the reader
func TestDualReader_Reset(t *testing.T) {
	data1 := []byte("first")
	data2 := []byte("second")
	reader1 := bytes.NewReader(data1)
	reader2 := bytes.NewReader(data2)

	dualReader := NewDualReader(reader1, reader2)

	// Read some data to move past first reader
	// Use io.ReadFull to ensure we read from both readers
	buf := make([]byte, len(data1)+2)
	n, err := io.ReadFull(dualReader, buf)
	require.NoError(t, err)
	assert.Equal(t, len(data1)+2, n) // Should have read from both readers

	// Reset
	dualReader.Reset()
	assert.False(t, dualReader.skipReader1)

	// Reset the underlying readers too
	reader1.Seek(0, io.SeekStart)
	reader2.Seek(0, io.SeekStart)

	// Read again
	result := make([]byte, len(data1)+len(data2))
	n, err = io.ReadFull(dualReader, result)
	require.NoError(t, err)
	assert.Equal(t, len(data1)+len(data2), n)

	expectedData := append(data1, data2...)
	assert.Equal(t, expectedData, result)
}

// TestDualReader_Read_InChunks tests reading in multiple small chunks
func TestDualReader_Read_InChunks(t *testing.T) {
	data1 := []byte("12345")
	data2 := []byte("67890")
	reader1 := bytes.NewReader(data1)
	reader2 := bytes.NewReader(data2)

	dualReader := NewDualReader(reader1, reader2)

	// Read in small chunks
	var result []byte
	chunkSize := 2
	for {
		buf := make([]byte, chunkSize)
		n, err := dualReader.Read(buf)
		if n > 0 {
			result = append(result, buf[:n]...)
		}
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}

	expectedData := append(data1, data2...)
	assert.Equal(t, expectedData, result)
}
