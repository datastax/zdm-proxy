package zdmproxy

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/go-cassandra-native-protocol/segment"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to create a simple raw frame for testing
func createTestRawFrame(version primitive.ProtocolVersion, streamId int16, bodyContent []byte) *frame.RawFrame {
	return &frame.RawFrame{
		Header: &frame.Header{
			Version:    version,
			Flags:      primitive.HeaderFlag(0),
			StreamId:   streamId,
			OpCode:     primitive.OpCodeQuery,
			BodyLength: int32(len(bodyContent)),
		},
		Body: bodyContent,
	}
}

// Helper function to encode a raw frame to bytes
func encodeRawFrameToBytes(t *testing.T, frm *frame.RawFrame) []byte {
	buf := &bytes.Buffer{}
	err := defaultFrameCodec.EncodeRawFrame(frm, buf)
	require.NoError(t, err)
	return buf.Bytes()
}

// TestFrameUncompressedLength tests the FrameUncompressedLength function
func TestFrameUncompressedLength(t *testing.T) {
	// Test with uncompressed frame
	bodyContent := []byte("test body")
	testFrame := createTestRawFrame(primitive.ProtocolVersion4, 1, bodyContent)

	length, err := FrameUncompressedLength(testFrame)
	require.NoError(t, err)

	expectedLength := primitive.ProtocolVersion4.FrameHeaderLengthInBytes() + len(bodyContent)
	assert.Equal(t, expectedLength, length)
}

// TestFrameUncompressedLength_Compressed tests that compressed frames return error
func TestFrameUncompressedLength_Compressed(t *testing.T) {
	bodyContent := []byte("test body")
	testFrame := createTestRawFrame(primitive.ProtocolVersion4, 1, bodyContent)
	testFrame.Header.Flags = primitive.HeaderFlagCompressed

	length, err := FrameUncompressedLength(testFrame)
	require.Error(t, err)
	assert.Equal(t, -1, length)
	assert.Contains(t, err.Error(), "cannot obtain uncompressed length of compressed frame")
}

// TestSegmentWriter_NewSegmentWriter tests the constructor
func TestSegmentWriter_NewSegmentWriter(t *testing.T) {
	buf := &bytes.Buffer{}
	ctx := context.Background()
	addr := "127.0.0.1:9042"

	writer := NewSegmentWriter(buf, 128, addr, ctx)

	require.NotNil(t, writer)
	assert.Equal(t, buf, writer.payload)
	assert.Equal(t, addr, writer.connectionAddr)
	assert.Equal(t, ctx, writer.clientHandlerContext)
}

// TestSegmentWriter_GetWriteBuffer tests getting the write buffer
func TestSegmentWriter_GetWriteBuffer(t *testing.T) {
	buf := &bytes.Buffer{}
	ctx := context.Background()
	writer := NewSegmentWriter(buf, 128, "127.0.0.1:9042", ctx)

	returnedBuf := writer.GetWriteBuffer()
	assert.Equal(t, buf, returnedBuf)
}

// TestSegmentWriter_CanWriteFrameInternal tests the internal frame capacity check
func TestSegmentWriter_CanWriteFrameInternal(t *testing.T) {
	buf := &bytes.Buffer{}
	ctx := context.Background()
	writer := NewSegmentWriter(buf, 10000, "127.0.0.1:9042", ctx)

	// Test 1: Empty payload, frame fits in one segment
	assert.True(t, writer.canWriteFrameInternal(1000))

	// Test 2: Empty payload, frame needs multiple segments
	assert.True(t, writer.canWriteFrameInternal(segment.MaxPayloadLength+1))

	// Test 3: Write some data first
	writer.payload.Write(make([]byte, 1000))

	// Small frame that fits
	assert.True(t, writer.canWriteFrameInternal(1000))

	// Test 4: Frame that would exceed segment max payload after merging and there's already data in the payload
	assert.False(t, writer.canWriteFrameInternal(segment.MaxPayloadLength-500))

	// Test 5: Payload has data, adding frame would need multiple segments
	writer.payload.Reset()
	writer.payload.Write(make([]byte, 100))
	assert.False(t, writer.canWriteFrameInternal(segment.MaxPayloadLength+1))
}

// TestSegmentWriter_AppendFrameToSegmentPayload tests appending frames
func TestSegmentWriter_AppendFrameToSegmentPayload(t *testing.T) {
	buf := &bytes.Buffer{}
	ctx := context.Background()
	writer := NewSegmentWriter(buf, 100000, "127.0.0.1:9042", ctx)

	bodyContent := []byte("test")
	testFrame := createTestRawFrame(primitive.ProtocolVersion4, 1, bodyContent)

	// Append frame
	written, err := writer.AppendFrameToSegmentPayload(testFrame)
	require.NoError(t, err)
	require.True(t, written)

	// Check that buffer has content
	assert.Greater(t, buf.Len(), 0)
}

// TestSegmentWriter_AppendFrameToSegmentPayload_CannotWrite tests when frame cannot be written
func TestSegmentWriter_AppendFrameToSegmentPayload_CannotWrite(t *testing.T) {
	buf := &bytes.Buffer{}
	ctx := context.Background()
	writer := NewSegmentWriter(buf, 100, "127.0.0.1:9042", ctx)

	// Fill the buffer
	writer.payload.Write(make([]byte, 1000))

	// Try to append a frame that cannot fit
	bodyContent := make([]byte, 5000)
	testFrame := createTestRawFrame(primitive.ProtocolVersion4, 1, bodyContent)

	written, err := writer.AppendFrameToSegmentPayload(testFrame)
	require.NoError(t, err)
	require.False(t, written) // Should not be written
}

// TestSegmentWriter_WriteSegments_SelfContained tests writing a self-contained segment
func TestSegmentWriter_WriteSegments_SelfContained(t *testing.T) {
	testCases := []struct {
		name       string
		frameCount int
	}{
		{name: "Single frame", frameCount: 1},
		{name: "Two frames", frameCount: 2},
		{name: "Three frames", frameCount: 3},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			ctx := context.Background()
			writer := NewSegmentWriter(buf, 100000, "127.0.0.1:9042", ctx)

			// Create a conn state with segment codec
			state := &connState{
				useSegments:  true,
				frameCodec:   defaultFrameCodec,
				segmentCodec: defaultSegmentCodec,
			}

			// Append multiple frames to the payload
			var expectedEnvelopes [][]byte
			for i := 0; i < tc.frameCount; i++ {
				bodyContent := []byte(fmt.Sprintf("frame_%d_body", i+1))
				testFrame := createTestRawFrame(primitive.ProtocolVersion5, int16(i+1), bodyContent)

				// Append frame to segment payload
				written, err := writer.AppendFrameToSegmentPayload(testFrame)
				require.NoError(t, err, "Failed to append frame %d", i+1)
				require.True(t, written, "Frame %d was not written", i+1)

				// Store expected envelope bytes
				expectedEnvelopes = append(expectedEnvelopes, encodeRawFrameToBytes(t, testFrame))
			}

			// Write segments
			dst := &bytes.Buffer{}
			err := writer.WriteSegments(dst, state)
			require.NoError(t, err)

			// Verify the payload was reset
			assert.Equal(t, 0, writer.payload.Len())

			// Verify something was written to dst
			assert.Greater(t, dst.Len(), 0)

			// Decode the segment to verify
			decodedSegment, err := state.segmentCodec.DecodeSegment(dst)
			require.NoError(t, err)
			assert.True(t, decodedSegment.Header.IsSelfContained)

			// Verify all frames are in the segment payload
			var expectedPayload []byte
			for _, envelope := range expectedEnvelopes {
				expectedPayload = append(expectedPayload, envelope...)
			}
			assert.Equal(t, expectedPayload, decodedSegment.Payload.UncompressedData)

			// Verify we can decode all frames from the segment payload
			payloadReader := bytes.NewReader(decodedSegment.Payload.UncompressedData)
			for i := 0; i < tc.frameCount; i++ {
				decodedFrame, err := defaultFrameCodec.DecodeRawFrame(payloadReader)
				require.NoError(t, err, "Failed to decode frame %d from segment payload", i+1)
				assert.Equal(t, int16(i+1), decodedFrame.Header.StreamId, "Frame %d has wrong stream ID", i+1)
				assert.Equal(t, []byte(fmt.Sprintf("frame_%d_body", i+1)), decodedFrame.Body, "Frame %d has wrong body", i+1)
			}
		})
	}
}

// TestSegmentWriter_WriteSegments_MultipleSegments tests writing multiple segments
func TestSegmentWriter_WriteSegments_MultipleSegments(t *testing.T) {
	buf := &bytes.Buffer{}
	ctx := context.Background()
	writer := NewSegmentWriter(buf, 128, "127.0.0.1:9042", ctx)

	// Add data larger than MaxPayloadLength
	largeData := make([]byte, segment.MaxPayloadLength*2+1000)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}
	writer.payload.Write(largeData)

	// Create a conn state with segment codec
	state := &connState{
		useSegments:  true,
		frameCodec:   defaultFrameCodec,
		segmentCodec: defaultSegmentCodec,
	}

	// Write segments
	dst := &bytes.Buffer{}
	err := writer.WriteSegments(dst, state)
	require.NoError(t, err)

	// Verify the payload was reset
	assert.Equal(t, 0, writer.payload.Len())

	// Decode and verify segments
	var reconstructedData []byte
	for i := 0; i < 3; i++ { // Should have 3 segments
		decodedSegment, err := state.segmentCodec.DecodeSegment(dst)
		require.NoError(t, err, "Failed to decode segment %d", i)
		assert.False(t, decodedSegment.Header.IsSelfContained, "Segment %d should not be self-contained", i)
		reconstructedData = append(reconstructedData, decodedSegment.Payload.UncompressedData...)
	}

	assert.Equal(t, 0, dst.Len())

	// Verify reconstructed data matches original
	assert.Equal(t, largeData, reconstructedData)
}

// TestSegmentWriter_WriteSegments_EmptyPayload tests that writing empty payload returns error
func TestSegmentWriter_WriteSegments_EmptyPayload(t *testing.T) {
	buf := &bytes.Buffer{}
	ctx := context.Background()
	writer := NewSegmentWriter(buf, 128, "127.0.0.1:9042", ctx)

	state := &connState{
		useSegments:  true,
		frameCodec:   defaultFrameCodec,
		segmentCodec: defaultSegmentCodec,
	}

	dst := &bytes.Buffer{}
	err := writer.WriteSegments(dst, state)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot write segment with empty payload")
}
